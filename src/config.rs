use std::borrow::Cow;

use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::ingestion::HtmlListDataFusionIngestor;
use crate::traits::Ingestor;

#[derive(Debug, Serialize, Deserialize)]
pub enum IngestorConfig {
    HtmlListDataFusion {
        tree_path: Cow<'static, str>,
        sub_path: Cow<'static, str>,
        target_url: Cow<'static, str>,
        query: Cow<'static, str>,
        ingest_from_back: bool,
    },
}

#[derive(Error, Debug)]
pub enum IngestorConfigError {
    #[error("Failed to create ingestor: {0}")]
    CreationError(Cow<'static, str>),
}

impl IngestorConfig {
    pub fn to_ingestor<T>(&self) -> Result<Box<dyn Ingestor<T>>, IngestorConfigError>
    where
        HtmlListDataFusionIngestor: Ingestor<T>,
    {
        match self {
            IngestorConfig::HtmlListDataFusion {
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
                    IngestorConfigError::CreationError(
                        format!("Failed to create ingestor: {}", e).into(),
                    )
                })?,
            )),
        }
    }
}


#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_ingestor_config_to_ingestor() {
        let config = IngestorConfig::HtmlListDataFusion {
            tree_path: "tree".into(),
            sub_path: "sub".into(),
            target_url: "http://example.com".into(),
            query: "SELECT *".into(),
            ingest_from_back: true,
        };

    }
}
