use std::{fs, sync::Arc, time::Duration};

use arrow::{
    array::{Array, Int32Array, StringArray, StringViewArray},
    record_batch::RecordBatch,
};
use axum::{
    Router,
    routing::{get, get_service},
};
use data_poller::{
    ingestion::HtmlListDataFusionIngestor,
    orchestration::Orchestrator,
    storage::FilePathStorer,
    traits::Pipeline,
    transformation::IdentityTransformer,
};
use datafusion::prelude::*;
use parquet::arrow::arrow_writer::ArrowWriter;
use tempfile::{NamedTempFile, tempdir};
use tokio::{net::TcpListener, time::sleep};
use tower_http::services::ServeFile;

fn create_test_parquet(names: &[&str]) -> NamedTempFile {
    let file = NamedTempFile::new().unwrap();
    let schema = Arc::new(arrow::datatypes::Schema::new(vec![
        arrow::datatypes::Field::new("id", arrow::datatypes::DataType::Int32, false),
        arrow::datatypes::Field::new("name", arrow::datatypes::DataType::Utf8, false),
    ]));

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int32Array::from_iter_values(1..=names.len() as i32)),
            Arc::new(StringArray::from(names.to_vec())),
        ],
    )
    .unwrap();

    let mut writer = ArrowWriter::try_new(file.reopen().unwrap(), schema, None).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
    file
}

async fn read_names_from_parquet(file_path: &str) -> Vec<String> {
    let ctx = SessionContext::new();
    let batches = ctx
        .read_parquet(file_path, ParquetReadOptions::default())
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    let column = batches[0]
        .column(1)
        .as_any()
        .downcast_ref::<StringViewArray>()
        .unwrap();

    (0..column.len()).map(|index| column.value(index).to_string()).collect()
}

#[tokio::test]
async fn test_orchestrator_ingests_and_stores_remote_parquet_files() {
    let first_parquet = create_test_parquet(&["A", "B", "C"]);
    let second_parquet = create_test_parquet(&["X", "Y", "Z"]);

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    let mock_html = format!(
        r#"
        <html>
            <body>
                <div id="target-area">
                    <span><a href="http://{addr}/first.parquet">First</a></span>
                    <span><a href="http://{addr}/second.parquet">Second</a></span>
                </div>
            </body>
        </html>
    "#
    );

    let app = Router::new()
        .route(
            "/",
            get(move || {
                let mock_html = mock_html.clone();
                async move { mock_html }
            }),
        )
        .nest_service(
            "/first.parquet",
            get_service(ServeFile::new(first_parquet.path().to_owned())),
        )
        .nest_service(
            "/second.parquet",
            get_service(ServeFile::new(second_parquet.path().to_owned())),
        );

    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });

    let output_dir = tempdir().unwrap();
    let ingestor = Box::new(
        HtmlListDataFusionIngestor::new(
            "div#target-area span".to_string(),
            "a".to_string(),
            &format!("http://{addr}"),
            "SELECT id, name FROM remote_parquet ORDER BY id",
            Some(false),
        )
        .unwrap(),
    );
    let transformer = Box::new(IdentityTransformer);
    let storer = Box::new(FilePathStorer::new(
        output_dir.path().to_str().unwrap().to_string(),
    ));

    let pipeline = Pipeline::new(ingestor, transformer, storer);
    let mut orchestrator = Orchestrator::new();
    orchestrator.add_pipeline(Box::new(pipeline), 50).unwrap();

    Arc::new(orchestrator).start().await.unwrap();

    let first_stored = output_dir.path().join("first.parquet");
    let second_stored = output_dir.path().join("second.parquet");

    for _ in 0..40 {
        if first_stored.exists() && second_stored.exists() {
            break;
        }
        sleep(Duration::from_millis(50)).await;
    }

    assert!(first_stored.exists());
    assert!(second_stored.exists());

    let stored_files = fs::read_dir(output_dir.path())
        .unwrap()
        .map(|entry| entry.unwrap().file_name().into_string().unwrap())
        .collect::<Vec<_>>();

    assert_eq!(stored_files.len(), 2);
    assert!(stored_files.contains(&"first.parquet".to_string()));
    assert!(stored_files.contains(&"second.parquet".to_string()));

    assert_eq!(
        read_names_from_parquet(first_stored.to_str().unwrap()).await,
        vec!["A", "B", "C"]
    );
    assert_eq!(
        read_names_from_parquet(second_stored.to_str().unwrap()).await,
        vec!["X", "Y", "Z"]
    );
}
