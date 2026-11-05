use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
    time::Duration,
};

use bytes::Bytes;
use mini_raft::{
    codec::Message,
    config::ServerConfig,
    server::{Destination, Incoming, NodeId, Outgoing, RaftNode, Server},
    state_machine::StateMachine,
};
use std::sync::RwLock;
use tokio::sync::mpsc;

/// Simple state machine for testing that interprets bytes as "key=value" pairs.
struct TestStateMachine {
    store: RwLock<HashMap<String, String>>,
}

impl TestStateMachine {
    fn new() -> Self {
        Self {
            store: RwLock::new(HashMap::new()),
        }
    }

    fn get_value(&self, key: String) -> Option<String> {
        self.store.read().unwrap().get(&key).cloned()
    }
}

impl StateMachine for TestStateMachine {
    fn apply_log(&self, command: &Bytes) -> anyhow::Result<Bytes> {
        let cmd_str = String::from_utf8_lossy(command);
        if let Some((k, v)) = cmd_str.split_once('=') {
            self.store
                .write()
                .unwrap()
                .insert(k.to_string(), v.to_string());
        }
        Ok(Bytes::new())
    }
}

struct NodeHandles {
    id: NodeId,
    node: RaftNode,
    state_machine: Arc<TestStateMachine>,
}

fn node_config(node_id: u64, election_timeout_ms: u64) -> ServerConfig {
    let mut peers = HashMap::new();
    peers.insert(0, "127.0.0.1:0".to_string());
    peers.insert(1, "127.0.0.1:0".to_string());
    peers.insert(2, "127.0.0.1:0".to_string());
    ServerConfig {
        election_timeout: election_timeout_ms,
        heartbeat_interval: 50,
        address: "127.0.0.1".to_string(),
        total_nodes: 3,
        peer_addresses: peers,
        node_id,
        peer_listener_address: "127.0.0.1:0".to_string(),
    }
}

/// Mesh router: delivers Outgoing from any node to the right Incoming queue.
/// Also forwards a copy of every message to observers.
async fn router(
    mut from_servers: Vec<(NodeId, mpsc::Receiver<Outgoing>)>,
    to_servers: HashMap<NodeId, mpsc::Sender<Incoming>>,
    observe_tx: mpsc::UnboundedSender<(NodeId, Message)>,
) {
    loop {
        let mut progressed = false;
        for (sender_id, rx) in from_servers.iter_mut() {
            match rx.try_recv() {
                Ok(out) => {
                    progressed = true;
                    let sid = *sender_id;
                    let _ = observe_tx.send((sid, out.msg.clone()));
                    match out.dest {
                        Destination::Node(peer) => {
                            if let Some(tx) = to_servers.get(&peer) {
                                let _ = tx
                                    .send(Incoming {
                                        from: sid,
                                        msg: out.msg,
                                    })
                                    .await;
                            }
                        }
                        Destination::Broadcast => {
                            for (nid, tx) in to_servers.iter() {
                                if *nid == sid {
                                    continue;
                                }
                                let _ = tx
                                    .send(Incoming {
                                        from: sid,
                                        msg: out.msg.clone(),
                                    })
                                    .await;
                            }
                        }
                    }
                }
                Err(mpsc::error::TryRecvError::Empty) => {}
                Err(mpsc::error::TryRecvError::Disconnected) => return,
            }
        }
        if !progressed {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    }
}

async fn spawn_cluster() -> (
    Vec<NodeHandles>,
    mpsc::UnboundedReceiver<(NodeId, Message)>,
) {
    let timeouts = [150u64, 300, 450];
    let mut to_servers = HashMap::new();
    let mut from_servers = Vec::new();
    let mut handles = Vec::new();
    let (observe_tx, observe_rx) = mpsc::unbounded_channel();

    for i in 0..3u64 {
        let id = NodeId::new(i);
        let (to_net_tx, from_srv_rx) = mpsc::channel::<Outgoing>(64);
        let (to_srv_tx, from_net_rx) = mpsc::channel::<Incoming>(64);
        let sm = Arc::new(TestStateMachine::new());
        let mut server = Server::new(
            id,
            node_config(i, timeouts[i as usize]),
            sm.clone(),
            to_net_tx,
            from_net_rx,
        );
        let node = server.raft_node();
        tokio::spawn(async move {
            server.run().await;
        });
        to_servers.insert(id, to_srv_tx.clone());
        from_servers.push((id, from_srv_rx));
        handles.push(NodeHandles {
            id,
            node,
            state_machine: sm,
        });
    }

    tokio::spawn(router(from_servers, to_servers, observe_tx));

    (handles, observe_rx)
}

/// Observes heartbeats until exactly one node has been sending them
/// for a while — that node is the stable leader.
async fn elect_leader(observe_rx: &mut mpsc::UnboundedReceiver<(NodeId, Message)>) -> NodeId {
    let mut senders = HashSet::new();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(6);
    while tokio::time::Instant::now() < deadline {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        match tokio::time::timeout(remaining, observe_rx.recv()).await {
            Ok(Some((from, Message::AppendEntriesType(ae)))) if ae.entries.is_empty() => {
                senders.insert(from);
                if senders.len() > 1 {
                    break;
                }
                if senders.len() == 1 {
                    tokio::time::sleep(Duration::from_millis(600)).await;
                    while let Ok((f2, Message::AppendEntriesType(ae2))) = observe_rx.try_recv() {
                        if ae2.entries.is_empty() {
                            senders.insert(f2);
                        }
                    }
                    break;
                }
            }
            Ok(Some(_)) => continue,
            Ok(None) => break,
            Err(_) => break,
        }
    }
    assert_eq!(
        senders.len(),
        1,
        "expected exactly one heartbeat sender (leader), got {:?}",
        senders
    );
    senders.into_iter().next().unwrap()
}

#[tokio::test]
async fn three_nodes_elect_single_leader() {
    let (_nodes, mut observe_rx) = spawn_cluster().await;
    let leader = elect_leader(&mut observe_rx).await;
    assert!(leader == NodeId::new(0) || leader == NodeId::new(1) || leader == NodeId::new(2));
}

/// The new direct-call API: a leader proposes a command, it commits and
/// replicates to every node without any client TCP layer.
#[tokio::test]
async fn leader_propose_commits_and_replicates_to_all_nodes() {
    let (nodes, mut observe_rx) = spawn_cluster().await;
    let leader = elect_leader(&mut observe_rx).await;

    let leader_handle = nodes.iter().find(|n| n.id == leader).unwrap();
    leader_handle
        .node
        .propose(Bytes::from("foo=bar"))
        .await
        .expect("leader should accept and commit the proposed command");

    let deadline = tokio::time::Instant::now() + Duration::from_secs(6);
    loop {
        let all_ok = nodes
            .iter()
            .all(|n| n.state_machine.get_value("foo".to_string()) == Some("bar".to_string()));
        if all_ok {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "followers did not apply committed log"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// A follower must reject proposals with an error instead of accepting them.
#[tokio::test]
async fn propose_on_non_leader_is_rejected() {
    let (nodes, mut observe_rx) = spawn_cluster().await;
    let leader = elect_leader(&mut observe_rx).await;

    let follower_handle = nodes.iter().find(|n| n.id != leader).unwrap();
    let result = follower_handle
        .node
        .propose(Bytes::from("foo=bar"))
        .await
        .err()
        .expect("follower proposal should be rejected, not committed");

    assert!(
        result.to_string().contains("not the leader"),
        "unexpected error: {result}"
    );
}
