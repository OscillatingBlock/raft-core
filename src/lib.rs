pub mod codec;
pub mod config;
pub mod network;
pub mod server;
pub mod state_machine;

use crate::codec::Codec;
use crate::config::ServerConfig;
use crate::network::NetworkManager;
use crate::server::{Incoming, Outgoing, Server};
use crate::state_machine::StateMachine;
use futures::future;
use std::sync::Arc;
use tokio::net::TcpListener;
use tokio::sync::mpsc;

impl Server {
    /// Starts the complete Raft node, including network listeners,
    /// background message dispatchers, and the core consensus loop.
    pub async fn start<SM>(config: ServerConfig, state_machine: Arc<SM>) -> anyhow::Result<()>
    where
        SM: StateMachine + 'static,
    {
        let node_id = config.node_id();
        let peer_listener_address = config.peer_listener_address.clone();
        let client_listener_address = config.client_listener_address.clone();
        let peer_addresses = config.peer_node_addresses();

        let (to_network, from_server) = mpsc::channel::<Outgoing>(32);
        let (to_server, from_network) = mpsc::channel::<Incoming>(32);

        // Instantiate the core server runtime loop
        let mut server = Server::new(
            node_id,
            config,
            state_machine.clone(),
            to_network,
            from_network,
        );

        tokio::spawn(async move {
            server.run().await;
        });

        let codec = Codec::new();
        let network_manager = NetworkManager::new(to_server, codec, node_id);
        let nm_clone = Arc::new(network_manager);

        let nm_clone1 = nm_clone.clone();
        tokio::spawn(async move {
            crate::network::dispatch_server_msg(from_server, nm_clone1).await;
        });

        let client_listener = TcpListener::bind(client_listener_address).await?;
        let peer_listener = TcpListener::bind(peer_listener_address).await?;

        let nm_clone2 = Arc::clone(&nm_clone);
        tokio::spawn(async move {
            _ = nm_clone2.listen(client_listener).await;
        });

        let nm_clone3 = nm_clone.clone();
        tokio::spawn(async move {
            _ = nm_clone3.listen(peer_listener).await;
        });

        nm_clone.dial_peers(peer_addresses, node_id).await;

        // Keep the main task alive by waiting on listeners (or a future)
        // For simplicity, you can await or run forever here
        future::pending().await
    }
}
