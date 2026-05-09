use std::borrow::Cow;

use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::ingestion::HtmlListDataFusionIngestor;
use crate::storage::{ConsoleStorer, FilePathStorer};
use crate::transformation::IdentityTransformer;

use crate::orchestration::{Orchestrator, PipelineSchedule, ScheduledPipeline};
use crate::traits::{DatasetDto, Ingestor, Pipeline, RunnablePipeline, Storer, Transformer};

#[derive(Debug, Serialize, Deserialize, PartialEq)]
pub struct OrchestratorConfig {
    pub pipelines: Vec<ScheduledPipelineConfig>,
}

impl OrchestratorConfig {
    pub fn to_orchestrator(&self) -> Result<Orchestrator, ConfigError> {
        let mut orchestrator = Orchestrator::new();
        for pipeline_config in &self.pipelines {
            let scheduled_pipeline: ScheduledPipeline = pipeline_config.to_scheduled_pipeline()?;
            orchestrator.add_scheduled_pipeline(scheduled_pipeline);
        }
        Ok(orchestrator)
    }
}

#[derive(Debug, Serialize, Deserialize, PartialEq)]
pub struct ScheduledPipelineConfig {
    pub name: Cow<'static, str>,
    pub pipeline: PipelineConfig,
    pub schedule: PipelineSchedule,
}

impl ScheduledPipelineConfig {
    pub fn to_scheduled_pipeline(&self) -> Result<ScheduledPipeline, ConfigError> {
        let pipeline = self.pipeline.to_pipeline()?;
        ScheduledPipeline::new(self.name.to_string(), pipeline, self.schedule.clone()).map_err(
            |e| {
                ConfigError::CreationError(
                    format!("Failed to create scheduled pipeline: {}", e).into(),
                )
            },
        )
    }
}

#[derive(Debug, Serialize, Deserialize, PartialEq)]
pub struct DatasetPipelineConfig {
    pub ingestor: DatasetIngestorConfig,
    pub transformer: DatasetTransformerConfig,
    pub storer: DatasetStorerConfig,
}

#[derive(Debug, Serialize, Deserialize, PartialEq)]
pub enum PipelineConfig {
    Dataset(DatasetPipelineConfig),
}

impl PipelineConfig {
    pub fn to_pipeline(&self) -> Result<Box<dyn RunnablePipeline>, ConfigError> {
        match self {
            PipelineConfig::Dataset(config) => Ok(Box::new(config.to_pipeline()?)),
        }
    }
}

impl DatasetPipelineConfig {
    pub fn to_pipeline(&self) -> Result<Pipeline<DatasetDto, DatasetDto>, ConfigError> {
        let ingestor = self.ingestor.to_ingestor()?;
        let transformer = self.transformer.to_transformer()?;
        let storer = self.storer.to_storer()?;

        Ok(Pipeline::new(ingestor, transformer, storer))
    }
}

#[derive(Debug, Serialize, Deserialize, PartialEq)]
pub enum DatasetIngestorConfig {
    HtmlListDataFusion {
        tree_path: Cow<'static, str>,
        sub_path: Cow<'static, str>,
        target_url: Cow<'static, str>,
        query: Cow<'static, str>,
        ingest_from_back: bool,
    },
}

#[derive(Error, Debug)]
pub enum ConfigError {
    #[error("Failed to create ingestor: {0}")]
    CreationError(Cow<'static, str>),
}

impl DatasetIngestorConfig {
    pub fn to_ingestor(&self) -> Result<Box<dyn Ingestor<DatasetDto>>, ConfigError>
    where
        HtmlListDataFusionIngestor: Ingestor<DatasetDto>,
    {
        match self {
            DatasetIngestorConfig::HtmlListDataFusion {
                tree_path,
                sub_path,
                target_url,
                query,
                ingest_from_back,
            } => Ok(Box::new(
                HtmlListDataFusionIngestor::new(
                    tree_path.to_string(),
                    sub_path.to_string(),
                    target_url,
                    query,
                    Some(*ingest_from_back),
                )
                .map_err(|e| {
                    ConfigError::CreationError(format!("Failed to create ingestor: {}", e).into())
                })?,
            )),
        }
    }
}

#[derive(Debug, Serialize, Deserialize, PartialEq)]
pub enum DatasetTransformerConfig {
    Identity,
}

impl DatasetTransformerConfig {
    pub fn to_transformer(
        &self,
    ) -> Result<Box<dyn Transformer<DatasetDto, DatasetDto>>, ConfigError> {
        match self {
            DatasetTransformerConfig::Identity => Ok(Box::new(IdentityTransformer)),
        }
    }
}

#[derive(Debug, Serialize, Deserialize, PartialEq)]
pub enum DatasetStorerConfig {
    Console,
    FilePath { file_path: Cow<'static, str> },
}

impl DatasetStorerConfig {
    pub fn to_storer(&self) -> Result<Box<dyn Storer<DatasetDto>>, ConfigError> {
        match self {
            DatasetStorerConfig::Console => Ok(Box::new(ConsoleStorer::new())),
            DatasetStorerConfig::FilePath { file_path } => {
                Ok(Box::new(FilePathStorer::new(file_path.to_string())))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_orchestrator_config_to_orchestrator() {
        let config = OrchestratorConfig {
            pipelines: vec![
                ScheduledPipelineConfig {
                    name: "Test Pipeline".into(),
                    pipeline: PipelineConfig::Dataset(DatasetPipelineConfig {
                        ingestor: DatasetIngestorConfig::HtmlListDataFusion {
                            tree_path: "tree".into(),
                            sub_path: "sub".into(),
                            target_url: "http://example.com".into(),
                            query: "SELECT *".into(),
                            ingest_from_back: true,
                        },
                        transformer: DatasetTransformerConfig::Identity,
                        storer: DatasetStorerConfig::Console,
                    }),
                    schedule: PipelineSchedule::FixedMsInterval(1000),
                },
            ],
        };
        let orchestrator = config.to_orchestrator();
        assert!(orchestrator.is_ok());
    }

    #[test]
    fn test_orchestrator_config_yaml_deserialization() {
        let yaml = indoc::indoc! {r#"
        pipelines:
          - name: "Test Pipeline"
            pipeline:
              !Dataset:
                ingestor: 
                  !HtmlListDataFusion
                    tree_path: "tree"
                    sub_path: "sub"
                    target_url: "http://example.com"
                    query: "SELECT *"
                    ingest_from_back: true
                transformer: !Identity
                storer: !Console
            schedule: 
              FixedMsInterval: 1000
        "#};
        let config: OrchestratorConfig = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(config.pipelines.len(), 1);
        let pipeline_config = &config.pipelines[0];
        assert_eq!(pipeline_config.name, "Test Pipeline");
        assert_eq!(
            pipeline_config.pipeline,
            PipelineConfig::Dataset(DatasetPipelineConfig {
                ingestor: DatasetIngestorConfig::HtmlListDataFusion {
                    tree_path: "tree".into(),
                    sub_path: "sub".into(),
                    target_url: "http://example.com".into(),
                    query: "SELECT *".into(),
                    ingest_from_back: true,
                },
                transformer: DatasetTransformerConfig::Identity,
                storer: DatasetStorerConfig::Console,
            })
        );
        assert_eq!(
            pipeline_config.schedule,
            PipelineSchedule::FixedMsInterval(1000)
        );
    }

    #[test]
    fn test_pipeline_config_to_pipeline() {
        let config = DatasetPipelineConfig {
            ingestor: DatasetIngestorConfig::HtmlListDataFusion {
                tree_path: "tree".into(),
                sub_path: "sub".into(),
                target_url: "http://example.com".into(),
                query: "SELECT *".into(),
                ingest_from_back: true,
            },
            transformer: DatasetTransformerConfig::Identity,
            storer: DatasetStorerConfig::Console,
        };
        let pipeline = config.to_pipeline();
        assert!(pipeline.is_ok());
    }

    #[test]
    fn test_pipeline_yaml_deserialization() {
        let yaml = indoc::indoc! {r#"
        ingestor: 
            !HtmlListDataFusion
              tree_path: "tree"
              sub_path: "sub"
              target_url: "http://example.com"
              query: "SELECT *"
              ingest_from_back: true
        transformer: !Identity
        storer: !Console
        "#};
        let config: DatasetPipelineConfig = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(
            config.ingestor,
            DatasetIngestorConfig::HtmlListDataFusion {
                tree_path: "tree".into(),
                sub_path: "sub".into(),
                target_url: "http://example.com".into(),
                query: "SELECT *".into(),
                ingest_from_back: true,
            }
        );
        assert_eq!(config.transformer, DatasetTransformerConfig::Identity);
        assert_eq!(config.storer, DatasetStorerConfig::Console);
    }

    #[test]
    fn test_ingestor_config_to_ingestor() {
        let config = DatasetIngestorConfig::HtmlListDataFusion {
            tree_path: "tree".into(),
            sub_path: "sub".into(),
            target_url: "http://example.com".into(),
            query: "SELECT *".into(),
            ingest_from_back: true,
        };

        let ingestor = config.to_ingestor();
        assert!(ingestor.is_ok());
    }

    #[test]
    fn test_transformer_config_to_transformer() {
        let config = DatasetTransformerConfig::Identity;
        let transformer = config.to_transformer();
        assert!(transformer.is_ok());
    }

    #[test]
    fn test_storer_config_to_storer() {
        let file_config = DatasetStorerConfig::FilePath {
            file_path: "output.txt".into(),
        };
        let file_storer = file_config.to_storer();
        assert!(file_storer.is_ok());
    }
}
