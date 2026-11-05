use anyhow::Context;
use bytes::Bytes;
use mini_raft::config::ServerConfig;
use mini_raft::server::Server;
use mini_raft::state_machine::StateMachine;
use std::sync::{Arc, RwLock};
use tracing::{debug, error, info, instrument};
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

    // The node keeps running in the background; we submit commands to it
    // directly through the returned handle — no client TCP listener needed.
    let raft_node = Server::start(config, Arc::new(SimpleStateMachine::default())).await?;
    info!(target: "main", node_id = %raft_node.id, "node started; proposing demo commands periodically");

    // Demo loop: propose a command every few seconds. When this node is the
    // leader the command commits and `propose` resolves with Ok; when it is
    // a follower the proposal is rejected and retried later.
    let mut n = 0u64;
    loop {
        tokio::time::sleep(std::time::Duration::from_secs(5)).await;
        let command = Bytes::from(format!("demo-{n}"));
        println!("[demo] proposing: {command:?}");
        match raft_node.propose(command.clone()).await {
            Ok(()) => info!(target: "main", command = ?command, "proposed command committed and applied"),
            Err(e) => error!(target: "main", error = %e, "propose rejected (not the leader), will retry"),
        }
        n += 1;
    }
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
