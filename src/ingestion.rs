use std::borrow::Cow;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use datafusion::{dataframe::DataFrame, prelude::*};
use object_store::{ClientOptions, http::HttpBuilder};
use reqwest;
use scraper::{Html, Selector};
use serde::Deserialize;
use thiserror::Error;
use url::Url;

use crate::traits::{Dataset, DatasetDto, IngestionError, Ingestor};

pub struct HtmlListDataFusionIngestor {
    extractor: HtmlExtractor,
    loader: DataFusionLoader,
    checkpoint: Arc<Mutex<Option<String>>>,
    target_url: Url,
    query: String,
    ingest_from_back: bool,
}

impl HtmlListDataFusionIngestor {
    pub fn new(
        tree_path: String,
        sub_path: String,
        target_url: &str,
        query: &str,
        ingest_from_back: Option<bool>,
    ) -> Result<Self, IngestionError> {
        let extractor = HtmlExtractor::new(tree_path, sub_path).map_err(|e| {
            IngestionError::ExtractError(format!("Failed to create extractor: {}", e).into())
        })?;
        let loader = DataFusionLoader::new().map_err(|e| {
            IngestionError::LoadError(format!("Failed to create loader: {}", e).into())
        })?;

        let url = Url::parse(target_url)
            .map_err(|e| IngestionError::ExtractError(format!("Invalid file URL: {}", e).into()))?;

        Ok(Self {
            extractor,
            loader,
            checkpoint: Arc::new(None.into()),
            target_url: url,
            query: query.to_string(),
            ingest_from_back: ingest_from_back.unwrap_or_else(|| false),
        })
    }

    fn get_dataset_name_from_url(&self, url: &Url) -> Result<String, IngestionError> {
        url.path_segments()
            .and_then(|segments| segments.last())
            .map(|s| s.to_string())
            .ok_or_else(|| {
                IngestionError::ExtractError("Failed to extract dataset name from URL".into())
            })
    }
}

#[async_trait]
impl Ingestor<DatasetDto> for HtmlListDataFusionIngestor {
    async fn ingest(&self) -> Result<DatasetDto, IngestionError> {
        let mut links = self
            .extractor
            .extract_from_url(&self.target_url)
            .await
            .map_err(|_| IngestionError::ExtractError("Could not extract from url".into()))?;

        if links.is_empty() {
            return Ok(DatasetDto::None);
        }

        if self.ingest_from_back {
            links.reverse();
        }

        let latest_link = {
            let checkpoint_guard = self
                .checkpoint
                .lock()
                .map_err(|_| IngestionError::InternalError("Mutex poisoned".into()))?;

            let target_index = match checkpoint_guard.as_ref() {
                Some(c) => match links.iter().position(|link| link == c) {
                    Some(i) => i + 1,
                    None => 0,
                },
                None => 0,
            };

            match links.get(target_index) {
                Some(l) => l.clone(),
                None => return Ok(DatasetDto::None),
            }
        };

        let url = Url::parse(&latest_link)
            .map_err(|e| IngestionError::ExtractError(format!("Invalid file URL: {}", e).into()))?;

        let dataset_name = self.get_dataset_name_from_url(&url)?;

        let result = self
            .loader
            .query_remote_parquet(url, &self.query)
            .await
            .map_err(|_| IngestionError::SourceNotAvailable("".into()))?;

        let mut checkpoint_guard = self
            .checkpoint
            .lock()
            .map_err(|_| IngestionError::InternalError("Mutex poisoned".into()))?;

        *checkpoint_guard = Some(latest_link);

        let result = Dataset::new(&dataset_name, result);

        Ok(DatasetDto::DataFrame(result))
    }
}

struct HtmlExtractor {
    tree_path_selector: Selector,
    sub_path_selector: Selector,
}

#[derive(Error, Debug)]
pub enum HtmlExtractorError {
    #[error("Failed to initialize HtmlExtractor: {0}")]
    InitializationError(Cow<'static, str>),
    #[error("Failed to load url: {0}")]
    UrlLoadError(Cow<'static, str>),
}

impl HtmlExtractor {
    pub fn new(tree_path: String, sub_path: String) -> Result<Self, HtmlExtractorError> {
        let tree_path_selector = Selector::parse(&tree_path).map_err(|e| {
            HtmlExtractorError::InitializationError(format!("Invalid tree path: {}", e).into())
        })?;
        let sub_path_selector = Selector::parse(&sub_path).map_err(|e| {
            HtmlExtractorError::InitializationError(format!("Invalid sub path: {}", e).into())
        })?;

        Ok(Self {
            tree_path_selector,
            sub_path_selector,
        })
    }

    pub fn extract(&self, html_content: &str) -> Vec<String> {
        let document = Html::parse_document(html_content);

        document
            .select(&self.tree_path_selector)
            .flat_map(|element| element.select(&self.sub_path_selector))
            .filter_map(|sub_element| sub_element.value().attr("href"))
            .map(|href| href.to_string())
            .collect()
    }

    pub async fn extract_from_url(&self, url: &Url) -> Result<Vec<String>, HtmlExtractorError> {
        let response = reqwest::get(&url.to_string()).await.map_err(|e| {
            HtmlExtractorError::UrlLoadError(format!("Failed to load URL: {}", e).into())
        })?;

        let content = response.text().await.map_err(|e| {
            HtmlExtractorError::UrlLoadError(format!("Failed to read response: {}", e).into())
        })?;

        Ok(self.extract(&content))
    }
}

struct DataFusionLoader {
    connection: SessionContext,
}

#[derive(Error, Debug)]
pub enum DataFusionLoaderError {
    #[error("Failed to establish connection: {0}")]
    ConnectionError(Cow<'static, str>),
    #[error("Failed to execute query: {0}")]
    QueryError(Cow<'static, str>),
}

impl DataFusionLoader {
    pub fn new() -> Result<Self, DataFusionLoaderError> {
        let config = SessionConfig::new();
        let context = SessionContext::new_with_config(config);
        Ok(Self {
            connection: context,
        })
    }

    pub async fn query_remote_parquet(
        &self,
        file_url: Url,
        query: &str,
    ) -> Result<DataFrame, DataFusionLoaderError> {
        let mut origin_url = file_url.origin().ascii_serialization();
        if !origin_url.ends_with('/') {
            origin_url.push('/');
        }

        let http_store = HttpBuilder::new()
            .with_url(&origin_url)
            .with_client_options(ClientOptions::new().with_allow_http(true))
            .build()
            .map_err(|e| {
                DataFusionLoaderError::ConnectionError(
                    format!("Failed to create HTTP store: {}", e).into(),
                )
            })?;

        let context = self.connection.clone();
        context
            .runtime_env()
            .register_object_store(&file_url, Arc::new(http_store));

        context.deregister_table("remote_parquet").map_err(|e| {
            DataFusionLoaderError::ConnectionError(
                format!("Failed to reset Parquet registration: {}", e).into(),
            )
        })?;

        context
            .register_parquet("remote_parquet", file_url, ParquetReadOptions::default())
            .await
            .map_err(|e| {
                DataFusionLoaderError::ConnectionError(
                    format!("Failed to register Parquet file: {}", e).into(),
                )
            })?;

        let result = context.sql(query).await.map_err(|e| {
            DataFusionLoaderError::QueryError(format!("Failed to execute query: {}", e).into())
        })?;

        Ok(result)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_extract_hrefs_from_spans_in_div() {
        let html = r#"
            <div id="container">
                <span class="item">
                    <a href="https://rust-lang.org">Rust</a>
                </span>
                <span class="item">
                    <a href="https://crates.io">Crates</a>
                </span>
                <span>No link here</span>
            </div>
            <div id="other">
                <span class="item">
                    <a href="https://google.com">Should ignore this</a>
                </span>
            </div>
        "#;

        let extractor = HtmlExtractor::new("div#container span".to_string(), "a".to_string())
            .expect("Failed to create extractor");

        let links = extractor.extract(html);

        assert_eq!(links.len(), 2);
        assert_eq!(links[0], "https://rust-lang.org");
        assert_eq!(links[1], "https://crates.io");
        assert!(!links.contains(&"https://google.com".to_string()));
    }
}
