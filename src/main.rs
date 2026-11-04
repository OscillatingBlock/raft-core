use anyhow::Context;
use bytes::Bytes;
use mini_raft::config::ServerConfig;
use mini_raft::server::Server;
use mini_raft::state_machine::StateMachine;
use std::sync::{Arc, RwLock};
use tracing::{debug, info, instrument};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::registry()
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info,mini_raft=debug".into()),
        )
        .with(tracing_subscriber::fmt::layer())
        .init();

    info!(target: "main", "starting server initialization and configuration loading");
    let config_path = resolve_config_path();
    info!(target: "main", %config_path, "resolved config file path");
    let config = ServerConfig::load_from(&config_path).context("failed to load config")?;

    Server::start(config, Arc::new(SimpleStateMachine::default())).await?;
    Ok(())
}

fn resolve_config_path() -> String {
    // Precedence: `--config <path>` / `--config=<path>`, then MINI_RAFT_CONFIG env, else "config".
    let args: Vec<String> = std::env::args().collect();
    for i in 0..args.len() {
        if args[i] == "--config" && i + 1 < args.len() {
            return args[i + 1].clone();
        }
        if let Some(rest) = args[i].strip_prefix("--config=") {
            return rest.to_string();
        }
    }
    std::env::var("MINI_RAFT_CONFIG").unwrap_or_else(|_| "config".to_string())
}

#[derive(Default)]
pub struct SimpleStateMachine {
    data_store: RwLock<Vec<Bytes>>,
}

impl StateMachine for SimpleStateMachine {
    #[instrument(skip(self), fields(command))]
    fn apply_log(&self, command: &Bytes) -> anyhow::Result<Bytes> {
        debug!(target: "state_machine", "applying log entry to state machine store");
        self.data_store.write().unwrap().push(command.clone());
        info!(target: "state_machine", "successfully applied log entry and updated state");
        Ok(command.clone())
    }
}
