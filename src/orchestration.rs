use std::collections::{HashMap, hash_map::Entry};
use std::borrow::Cow;
use std::sync::Arc;

use thiserror::Error;
use tokio::time::{Duration, sleep};

use crate::traits::RunnablePipeline;

struct ScheduledPipeline {
    name: String,
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
        name: String,
        pipeline: Box<dyn RunnablePipeline>,
        schedule_millis: u64,
    ) -> Result<Self, ScheduledPipelineError> {
        if schedule_millis == 0 {
            return Err(ScheduledPipelineError::CreationError(
                "Schedule must be greater than zero".into(),
            ));
        }
        Ok(Self {
            name,
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
    #[error("Duplicate pipeline name: {0}")]
    DuplicatePipelineName(String),
}

#[derive(Clone)]
pub struct Orchestrator {
    pipelines: HashMap<String, Arc<ScheduledPipeline>>,
}

impl Orchestrator {
    pub fn new() -> Self {
        Self {
            pipelines: HashMap::new(),
        }
    }

    pub fn add_pipeline(
        &mut self,
        name: String,
        pipeline: Box<dyn RunnablePipeline>,
        schedule_millis: u64,
    ) -> Result<(), OrchestratorError> {
        let scheduled_pipeline = Arc::new(ScheduledPipeline::new(name, pipeline, schedule_millis)?);

        match self.pipelines.entry(scheduled_pipeline.name.clone()) {
            Entry::Vacant(entry) => {
                entry.insert(scheduled_pipeline);
                Ok(())
            }
            Entry::Occupied(_) => {
                Err(OrchestratorError::DuplicatePipelineName(scheduled_pipeline.name.clone()))
            }
        }
    }

    pub async fn start(self: Arc<Self>) -> Result<(), OrchestratorError> {
        for scheduled_pipeline in self.pipelines.values() {
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

    use async_trait::async_trait;
    use std::sync::{Arc, Mutex};

    struct MockIngestor;
    struct MockTransformer {
        data: Arc<Mutex<Option<String>>>,
    }
    struct MockStorer {
        stored_data: Arc<Mutex<Option<String>>>,
    }
    struct MockRunnablePipeline;

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

    #[async_trait]
    impl RunnablePipeline for MockRunnablePipeline {
        async fn run(&self) -> Result<(), PipelineError> {
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
        orchestrator
            .add_pipeline("test-pipeline".to_string(), Box::new(pipeline), 100)
            .unwrap();

        let orchestrator_arc = Arc::new(orchestrator);
        orchestrator_arc.start().await.unwrap();

        sleep(Duration::from_millis(200)).await;

        assert_eq!(data.lock().unwrap().as_deref(), Some("raw data"));
        assert_eq!(stored_data.lock().unwrap().as_deref(), Some("RAW DATA"));
    }

    #[test]
    fn test_orchestrator_rejects_duplicate_pipeline_names() {
        let mut orchestrator = Orchestrator::new();

        orchestrator
            .add_pipeline(
                "duplicate-name".to_string(),
                Box::new(MockRunnablePipeline),
                100,
            )
            .unwrap();

        let error = orchestrator
            .add_pipeline(
                "duplicate-name".to_string(),
                Box::new(MockRunnablePipeline),
                100,
            )
            .unwrap_err();

        assert!(matches!(
            error,
            OrchestratorError::DuplicatePipelineName(name) if name == "duplicate-name"
        ));
    }
}
