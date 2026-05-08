use std::borrow::Cow;

use async_trait::async_trait;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum IngestionError {
    #[error("Failed to ingest data: {0}")]
    SourceNotAvailable(Cow<'static, str>),
    #[error("Failed to ingest data: {0}")]
    ExtractError(Cow<'static, str>),
    #[error("Failed to load data: {0}")]
    LoadError(Cow<'static, str>),
    #[error("Internal Error: {0}")]
    InternalError(Cow<'static, str>),
}

pub struct Dataset<T> {
    name: String,
    data: T,
}

impl<T> Dataset<T>
where
    T: Clone,
{
    pub fn new(name: &str, data: T) -> Self {
        Dataset {
            name: name.to_string(),
            data,
        }
    }

    pub fn get_name(&self) -> &str {
        &self.name
    }

    pub fn get_data(&self) -> T {
        self.data.clone()
    }
}

#[async_trait]
pub trait Ingestor<T>: Send + Sync {
    async fn ingest(&self) -> Result<T, IngestionError>;
}

#[derive(Error, Debug)]
pub enum TransformationError {
    #[error("Failed to transform data: {0}")]
    CorruptData(Cow<'static, str>),
}

#[async_trait]
pub trait Transformer<T, S>: Send + Sync {
    async fn transform(&self, input: T) -> Result<S, TransformationError>;
}

#[derive(Error, Debug)]
pub enum StorageError {
    #[error("Failed to store data: {0}")]
    StorageUnavailable(Cow<'static, str>),
}

#[async_trait]
pub trait Storer<T>: Send + Sync {
    async fn store(&self, input: T) -> Result<(), StorageError>;
}

#[derive(Error, Debug)]
pub enum PipelineError {
    #[error("Ingestion failed: {0}")]
    Ingest(#[from] IngestionError),
    #[error("Transformation failed: {0}")]
    Transform(#[from] TransformationError),
    #[error("Storage failed: {0}")]
    Store(#[from] StorageError),
}

pub struct Pipeline<T, S> {
    ingestor: Box<dyn Ingestor<T>>,
    transformer: Box<dyn Transformer<T, S>>,
    storer: Box<dyn Storer<S>>,
}

impl<T, S> Pipeline<T, S> {
    pub fn new(
        ingestor: Box<dyn Ingestor<T>>,
        transformer: Box<dyn Transformer<T, S>>,
        storer: Box<dyn Storer<S>>,
    ) -> Self {
        Pipeline {
            ingestor,
            transformer,
            storer,
        }
    }

    pub async fn run(&self) -> Result<(), PipelineError> {
        let input = self.ingestor.ingest().await?;
        let output = self.transformer.transform(input).await?;
        self.storer.store(output).await?;
        Ok(())
    }
}

#[async_trait]
pub trait RunnablePipeline: Send + Sync {
    async fn run(&self) -> Result<(), PipelineError>;
}

#[async_trait]
impl<T, S> RunnablePipeline for Pipeline<T, S>
where
    T: Send + Sync + Sized,
    S: Send + Sync + Sized,
{
    async fn run(&self) -> Result<(), PipelineError> {
        self.run().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::sync::{Arc, Mutex};

    struct MockIngestor;
    struct MockTransformer {
        data: Arc<Mutex<Option<String>>>,
    }
    struct MockStorer {
        stored_data: Arc<Mutex<Option<String>>>,
    }

    #[async_trait]
    impl Ingestor<String> for MockIngestor {
        async fn ingest(&self) -> Result<String, IngestionError> {
            Ok("raw data".to_string())
        }
    }

    #[async_trait]
    impl Transformer<String, String> for MockTransformer {
        async fn transform(&self, input: String) -> Result<String, TransformationError> {
            self.data.lock().unwrap().replace(input.clone());
            Ok(input.to_uppercase())
        }
    }

    #[async_trait]
    impl Storer<String> for MockStorer {
        async fn store(&self, input: String) -> Result<(), StorageError> {
            self.stored_data.lock().unwrap().replace(input);
            Ok(())
        }
    }

    #[tokio::test]
    async fn test_pipeline() {
        let ingestor = Box::new(MockIngestor);

        let data = Arc::new(Mutex::new(None));
        let transformer = Box::new(MockTransformer { data: data.clone() });

        let stored_data = Arc::new(Mutex::new(None));
        let storer = Box::new(MockStorer {
            stored_data: stored_data.clone(),
        });

        let pipeline = Pipeline::new(ingestor, transformer, storer);

        assert!(pipeline.run().await.is_ok());
        assert_eq!(data.lock().unwrap().as_deref(), Some("raw data"));
        assert_eq!(stored_data.lock().unwrap().as_deref(), Some("RAW DATA"));
    }
}
