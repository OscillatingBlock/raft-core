use std::collections::HashMap;

use anyhow::Context;
use config::{Environment, File};
use serde::Deserialize;
use tokio::time::Duration;

use crate::server::NodeId;

#[derive(Debug, Clone, Deserialize)]
pub struct ServerConfig {
    pub election_timeout: u64,
    pub heartbeat_interval: u64,
    pub address: String,
    pub total_nodes: u32,
    pub peer_addresses: HashMap<u64, String>,
    pub node_id: u64,
    pub peer_listener_address: String,
}

impl ServerConfig {
    pub fn load() -> anyhow::Result<Self> {
        // Precedence: MINI_RAFT_CONFIG env var, else "config" (backwards compat).
        // CLI --config handling lives in main.rs, which calls load_from directly.
        let path = std::env::var("LOCKIN").unwrap_or_else(|_| "config".to_string());
        Self::load_from(&path)
    }

    pub fn load_from(path: &str) -> anyhow::Result<Self> {
        // Strip a trailing .toml so both "config" and "config-node1.toml" work.
        let stem = path.strip_suffix(".toml").unwrap_or(path);
        let config = config::Config::builder()
            .set_default("election_timeout", 500)?
            .set_default("heartbeat_interval", 200)?
            .add_source(
                File::with_name(stem)
                    .format(config::FileFormat::Toml)
                    .required(true),
            )
            .add_source(
                Environment::with_prefix("LOCKIN")
                    .separator("_")
                    .try_parsing(true),
            )
            .build()
            .context("Failed to build configuration builder")?;
        let app_config: ServerConfig = config
            .try_deserialize()
            .context("Failed to deserialize configuration into Config struct")?;

        Ok(app_config)
    }

    pub fn election_timeout_duration(&self) -> Duration {
        Duration::from_millis(self.election_timeout)
    }

    pub fn heartbeat_interval_duration(&self) -> Duration {
        Duration::from_millis(self.heartbeat_interval)
    }

    pub fn peer_node_addresses(&self) -> HashMap<NodeId, String> {
        self.peer_addresses
            .iter()
            .map(|(id, addr)| (NodeId::new(*id), addr.clone()))
            .collect()
    }

    pub fn node_id(&self) -> NodeId {
        NodeId::new(self.node_id)
    }
}
