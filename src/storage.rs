use async_trait::async_trait;

use crate::traits::{FileStorer, StorageError, Storer};

pub struct ConsoleStorer;

#[async_trait]
impl<T> Storer<T> for ConsoleStorer
where
    T: Into<String> + Send + 'static,
{
    async fn store(&self, input: T) -> Result<(), StorageError> {
        println!("{}\n", input.into());
        Ok(())
    }
}

pub struct FilePathStorer {
    file_path: String,
}

impl FilePathStorer {
    pub fn new(file_path: String) -> Self {
        Self { file_path }
    }
}

#[async_trait]
impl<T> Storer<T> for FilePathStorer
where
    T: FileStorer + Send + 'static,
{
    async fn store(&self, input: T) -> Result<(), StorageError> {
        input
            .write_file(&self.file_path)
            .await
            .map_err(|e| StorageError::WriteError(format!("{}", e).into()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use datafusion::{
        arrow::{
            array::StringArray,
            datatypes::{DataType, Field, Schema},
            record_batch::RecordBatch,
        },
        dataframe::DataFrame,
        prelude::*,
    };

    use std::sync::Arc;

    use crate::traits::*;

    #[tokio::test]
    async fn test_file_path_storer() {
        let ctx = SessionContext::new();

        let name_array = StringArray::from(vec!["Alice", "Bob"]);
        let age_array = StringArray::from(vec!["30", "25"]);

        let schema = Arc::new(Schema::new(vec![
            Field::new("name", DataType::Utf8, false),
            Field::new("age", DataType::Utf8, false),
        ]));

        let batch =
            RecordBatch::try_new(schema, vec![Arc::new(name_array), Arc::new(age_array)]).unwrap();

        let data = ctx.read_batch(batch).unwrap();

        let dataset = Dataset::new("test_dataset", data);
        let tmpdir = tempfile::tempdir().unwrap();

        let storer = FilePathStorer::new(tmpdir.path().to_str().unwrap().to_string());

        storer.store(dataset).await.unwrap();

        let parquet_file = tmpdir.path().join("test_dataset.parquet");
        assert!(parquet_file.exists());

        let df = ctx
            .read_parquet(
                parquet_file.to_str().unwrap(),
                ParquetReadOptions::default(),
            )
            .await
            .unwrap();
        let result = df.collect().await.unwrap();
        assert_eq!(result.len(), 1);
        let batch = &result[0];
        assert_eq!(batch.num_rows(), 2);
        assert_eq!(batch.schema().field(0).name(), "name");
        assert_eq!(batch.schema().field(1).name(), "age");
    }
}
