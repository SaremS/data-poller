use std::sync::Arc;

use arrow::array::{Int32Array, StringArray, StringViewArray};
use arrow::record_batch::RecordBatch;
use axum::{
    Router,
    routing::{get, get_service},
};
use data_poller::ingestion::HtmlListDataFusionIngestor;
use data_poller::traits::{IngestionError, Ingestor};
use parquet::arrow::arrow_writer::ArrowWriter;
use tempfile::NamedTempFile;
use tokio::net::TcpListener;
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

#[tokio::test]
async fn test_ingest_remote_parquet_success() {
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

    let ingestor = HtmlListDataFusionIngestor::new(
        "div#target-area span".to_string(),
        "a".to_string(),
        &format!("http://{addr}"),
        "SELECT name FROM remote_parquet WHERE id = 2",
        Some(true),
    )
    .expect("Failed to create ingestor");

    let dataset = ingestor.ingest().await.expect("Ingest failed");

    assert_eq!(dataset.get_name().unwrap(), "second.parquet");

    let results = dataset
        .get_as_dataframe()
        .expect("Failed to get DataFrame")
        .collect()
        .await
        .expect("Failed to collect batches");

    assert!(!results.is_empty());
    let column = results[0]
        .column(0)
        .as_any()
        .downcast_ref::<StringViewArray>()
        .unwrap();

    assert_eq!(column.value(0), "Y");
}

#[tokio::test]
async fn test_ingest_invalid_listing_url() {
    let ingestor = HtmlListDataFusionIngestor::new(
        "div#target-area span".to_string(),
        "a".to_string(),
        "http://127.0.0.1:1",
        "SELECT * FROM remote_parquet",
        None,
    )
    .unwrap();

    let result = ingestor.ingest().await;

    assert!(matches!(result, Err(IngestionError::ExtractError(_))));
}

#[tokio::test]
async fn test_ingest_invalid_parquet_url() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    let mock_html = r#"
        <html>
            <body>
                <div id="target-area">
                    <span><a href="not-a-url">Broken</a></span>
                </div>
            </body>
        </html>
    "#;

    let app = Router::new().route("/", get(move || async move { mock_html.to_string() }));

    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });

    let ingestor = HtmlListDataFusionIngestor::new(
        "div#target-area span".to_string(),
        "a".to_string(),
        &format!("http://{addr}"),
        "SELECT * FROM remote_parquet",
        None,
    )
    .unwrap();

    let result = ingestor.ingest().await;

    assert!(matches!(result, Err(IngestionError::ExtractError(_))));
}
