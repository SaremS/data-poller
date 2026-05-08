use std::borrow::Cow;
use std::sync::Arc;

use async_trait::async_trait;
use thiserror::Error;
use tokio::time::{Duration, sleep};

use crate::traits::RunnablePipeline;

struct ScheduledPipeline {
    pipeline: Box<dyn RunnablePipeline>,
    schedule: Duration,
}

#[derive(Error, Debug)]
pub enum ScheduledPipelineError {
    #[error("Failed to create scheduled pipeline: {0}")]
    CreationError(Cow<'static, str>),
}

impl ScheduledPipeline {
    pub fn new(
        pipeline: Box<dyn RunnablePipeline>,
        schedule_millis: u64,
    ) -> Result<Self, ScheduledPipelineError> {
        if schedule_millis == 0 {
            return Err(ScheduledPipelineError::CreationError(
                "Schedule must be greater than zero".into(),
            ));
        }
        Ok(Self {
            pipeline,
            schedule: Duration::from_millis(schedule_millis),
        })
    }
}

#[derive(Error, Debug)]
pub enum OrchestratorError {
    #[error("Failed to start orchestrator: {0}")]
    StartError(Cow<'static, str>),
    #[error("Failed to create scheduled pipeline: {0}")]
    ScheduledPipelineError(#[from] ScheduledPipelineError),
}

#[derive(Clone)]
pub struct Orchestrator {
    pipelines: Vec<Arc<ScheduledPipeline>>,
}

impl Orchestrator {
    pub fn new() -> Self {
        Self {
            pipelines: Vec::new(),
        }
    }

    pub fn add_pipeline(
        &mut self,
        pipeline: Box<dyn RunnablePipeline>,
        schedule_millis: u64,
    ) -> Result<(), OrchestratorError> {
        let scheduled_pipeline = ScheduledPipeline::new(pipeline, schedule_millis)?;
        self.pipelines.push(scheduled_pipeline.into());
        Ok(())
    }

    pub async fn start(self: Arc<Self>) -> Result<(), OrchestratorError> {
        for scheduled_pipeline in self.pipelines.iter() {
            let pipe = scheduled_pipeline.clone();
            tokio::spawn(async move {
                loop {
                    if let Err(e) = &pipe.pipeline.run().await {
                        eprintln!("Pipeline error: {}", e);
                    }
                    sleep(pipe.schedule).await;
                }
            });
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::traits::*;

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
    async fn test_orchestrator() {
        let ingestor = Box::new(MockIngestor);

        let data = Arc::new(Mutex::new(None));
        let transformer = Box::new(MockTransformer { data: data.clone() });

        let stored_data = Arc::new(Mutex::new(None));
        let storer = Box::new(MockStorer {
            stored_data: stored_data.clone(),
        });

        let pipeline = Pipeline::new(ingestor, transformer, storer);
        let mut orchestrator = Orchestrator::new();
        orchestrator.add_pipeline(Box::new(pipeline), 100).unwrap();

        let orchestrator_arc = Arc::new(orchestrator);
        orchestrator_arc.start().await.unwrap();

        sleep(Duration::from_millis(200)).await;

        assert_eq!(data.lock().unwrap().as_deref(), Some("raw data"));
        assert_eq!(stored_data.lock().unwrap().as_deref(), Some("RAW DATA"));
    }
}
