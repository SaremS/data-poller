use std::{env, fs, sync::Arc};

use data_poller_orchestration::config::OrchestratorConfig;
use thiserror::Error;

#[derive(Debug, Error)]
enum CliError {
    #[error("missing config path argument")]
    MissingConfigPath,
    #[error("failed to read config file: {0}")]
    ReadConfig(#[from] std::io::Error),
    #[error("failed to parse config file: {0}")]
    ParseConfig(#[from] serde_yaml::Error),
    #[error("failed to build orchestrator: {0}")]
    BuildOrchestrator(#[from] data_poller_orchestration::config::ConfigError),
    #[error("failed to start orchestrator: {0}")]
    StartOrchestrator(#[from] data_poller_orchestration::orchestration::OrchestratorError),
}

#[tokio::main]
async fn main() -> Result<(), CliError> {
    let config_path = env::args().nth(1).ok_or(CliError::MissingConfigPath)?;
    let config = fs::read_to_string(config_path)?;
    let config: OrchestratorConfig = serde_yaml::from_str(&config)?;

    let orchestrator = Arc::new(config.to_orchestrator()?);
    orchestrator.start().await?;

    tokio::signal::ctrl_c().await?;
    Ok(())
}
