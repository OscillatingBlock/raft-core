use std::{collections::HashMap, sync::Arc, time::Duration};

use bytes::Bytes;
use mini_raft::state_machine::StateMachine;
use mini_raft::{
    codec::Message,
    config::ServerConfig,
    server::{NodeId, Server},
};
use std::sync::RwLock;
use tokio::sync::mpsc;

fn test_config(node_id: u64, election_timeout_ms: u64) -> ServerConfig {
    ServerConfig {
        election_timeout: election_timeout_ms,
        heartbeat_interval: 50,
        address: "127.0.0.1".to_string(),
        total_nodes: 3,
        peer_addresses: HashMap::new(),
        node_id,
        client_listener_address: "127.0.0.1:0".to_string(),
        peer_listener_address: "127.0.0.1:0".to_string(),
    }
}

/// Single node with no peers must time out as follower and broadcast RequestVote.
/// This proves `Server::run()` actually drives follower -> candidate.
#[tokio::test]
async fn single_node_times_out_and_requests_votes() {
    let (to_network_tx, mut from_server_rx) = mpsc::channel(32);
    let (to_server_tx, from_network_rx) = mpsc::channel(32);
    // Keep sender alive so Server::run doesn't see a closed channel.
    let _keep = to_server_tx;

    let mut server = Server::new(
        NodeId::new(0),
        test_config(0, 150),
        Arc::new(SimpleStateMachine::default()),
        to_network_tx,
        from_network_rx,
    );
    let handle = tokio::spawn(async move {
        server.run().await;
    });

    let outgoing = tokio::time::timeout(Duration::from_secs(2), from_server_rx.recv())
        .await
        .expect("timed out waiting for RequestVote")
        .expect("channel closed");

    match outgoing.msg {
        Message::RequestVoteType(req) => {
            assert_eq!(req.candidate_id, NodeId::new(0));
            assert!(req.term >= 1);
        }
        other => panic!("expected RequestVote, got {:?}", other.message_type()),
    }

    handle.abort();
}

#[derive(Default)]
pub struct SimpleStateMachine {
    data_store: RwLock<Vec<Bytes>>,
}

impl StateMachine for SimpleStateMachine {
    fn apply_log(&self, command: &Bytes) -> anyhow::Result<Bytes> {
        self.data_store.write().unwrap().push(command.clone());
        Ok(command.clone())
    }
}
