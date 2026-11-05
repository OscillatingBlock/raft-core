# raft_core

*(Previously `mini_raft`)* — a learning project: Raft consensus in Rust, built after
reading the Raft paper, extended into a general-purpose library. Any command type can
be replicated through the Raft protocol as long as you provide a state machine that
knows how to apply it. There is no client socket layer — you submit commands directly
by calling `.propose()` on a running node.

## Layout

| Path | Role |
|---|---|
| `src/server.rs` | Raft core: follower/candidate/leader loops, log replication, `propose` |
| `src/network.rs` | Peer-to-peer TCP transport (handshake + framing), message dispatch |
| `src/codec.rs` | Length-prefixed JSON framing of Raft RPC messages |
| `src/state_machine.rs` | The `StateMachine` trait that library users implement |
| `src/config.rs` | `ServerConfig` — loaded from TOML or constructed programmatically |
| `src/main.rs` | Demo binary: starts one node and periodically proposes commands |
| `tests/` | Integration tests (codec, election, cluster, peer TCP) |
| `config.toml`, `config-node1..5.toml` | Cluster configs for a 5-node local setup |

## How it works

Each node runs a `Server` event loop (`follower` / `candidate` / `leader`) connected to
a `NetworkManager` over `mpsc` channels (`Incoming` / `Outgoing` messages). The
network layer handles the peer-to-peer TCP transport using a length-prefixed JSON
`Codec`, with a `Hello` handshake to identify peer nodes. Leaders replicate proposed
commands with `AppendEntries`, track `next_index`/`match_index` per follower, advance
`commit_index` once a majority acknowledges, and then apply committed entries to the
user-provided `StateMachine`. Elections use randomized-timeout `RequestVote` RPCs.

A `propose()` call enqueues the command internally; the leader's loop picks it up,
appends it to its log and replicates it. The future returned by `propose()` resolves
exactly when the command has been committed by a majority **and applied to the local
state machine**. Nodes that are not the leader reject proposals with an error.

## Using it as a library

Add `raft_core` (as `mini_raft`) to your dependencies, then three steps:

### 1. Implement `StateMachine`

Define what a committed command means for your application. Commands are opaque
`bytes::Bytes`; decode them however you like:

```rust
use bytes::Bytes;
use mini_raft::state_machine::StateMachine;

#[derive(Default)]
struct TodoList {
    items: std::sync::RwLock<Vec<String>>, // "ADD <item>" commands
}

impl StateMachine for TodoList {
    fn apply_log(&self, command: &Bytes) -> anyhow::Result<Bytes> {
        let cmd = String::from_utf8_lossy(command);
        if let Some(item) = cmd.strip_prefix("ADD ") {
            self.items.write().unwrap().push(item.into());
        }
        Ok(Bytes::new()) // optional per-command result
    }
}
```

`apply_log` is called once per committed entry, in log order, on every node — keep it
deterministic.

### 2. Build a config and start the node

You can load a TOML file, or build the config in code:

```rust
use mini_raft::config::ServerConfig;
use mini_raft::server::Server;

// Option A: from TOML (see "Configuration" below)
let config = ServerConfig::load_from("config-node1.toml")?;

// Option B: constructed in code
let config = ServerConfig {
    election_timeout: 500,       // ms before a follower starts an election
    heartbeat_interval: 200,     // ms between leader heartbeats
    address: "127.0.0.1".into(),
    total_nodes: 3,
    node_id: 1,
    peer_listener_address: "127.0.0.1:9001".into(),
    peer_addresses: [
        (1, "127.0.0.1:9001".to_string()),
        (2, "127.0.0.1:9002".to_string()),
        (3, "127.0.0.1:9003".to_string()),
    ].into(),
};

// Spawns the consensus loop + peer networking in the background.
let raft_node = Server::start(config, std::sync::Arc::new(TodoList::default())).await?;
```

`Server::start` binds the peer listener, dials all peers, and returns a
`RaftNode` handle. `RaftNode` is `Clone`, so every task that wants to submit
commands can hold its own copy of the handle.

### 3. Propose commands

```rust
use bytes::Bytes;

// Resolves with Ok(()) once the command is committed by a majority
// and applied to the local state machine:
raft_node.propose(Bytes::from("ADD write the readme")).await?;
```

Semantics of `propose`:

| Result | Meaning |
|---|---|
| `Ok(())` | Command was committed by a majority and applied locally |
| `Err` — "not the leader" | The node is not the leader; the proposal was rejected |
| `Err` — "lost leadership" | The node lost leadership before the command committed |
| `Err` — "proposal queue closed" | The node is shutting down |

Two practical notes:

* `propose()` routes to whatever node handle you call it on — it does not
  automatically redirect to the leader. If you always propose through one handle
  and it is not the leader, keep a handle per node (as above) and pick the leader,
  or retry in a loop:

  ```rust
  // simple retry across all handles
  loop {
      match raft_node.propose(cmd.clone()).await {
          Ok(()) => break,
          Err(_) => tokio::time::sleep(Duration::from_millis(300)).await,
      }
  }
  ```

* Because leadership can change, `propose()` may block during an election instead
  of failing immediately. It will resolve as soon as the command commits or is
  rejected on the next role change.

See `src/main.rs` for `SimpleStateMachine`, a minimal working example of this flow,
and `tests/cluster.rs` for an end-to-end multi-node `propose` test.

## Configuration

`ServerConfig` is a TOML file (plus env overrides via the `config` crate, prefix
`LOCKIN_`):

```toml
election_timeout = 500        # ms of silence before a follower starts an election
heartbeat_interval = 200      # ms between leader heartbeats
address = "127.0.0.1"         # this node's advertised address
total_nodes = 5               # cluster size (drives majority = total_nodes/2 + 1)
node_id = 1                   # unique id in 1..=total_nodes
peer_listener_address = "127.0.0.1:9001"  # TCP listener for peer RPCs

[peer_addresses]              # every cluster member's peer_listener_address
1 = "127.0.0.1:9001"
2 = "127.0.0.1:9002"
3 = "127.0.0.1:9003"
4 = "127.0.0.1:9004"
5 = "127.0.0.1:9005"
```

Note: in this layout every node uses a **distinct port per node** for its peer
listener, but all nodes share the same `[peer_addresses]` map, so node *N*'s own
listener must have the port that the map advertises for id *N*.

## Running the demo cluster

The demo binary starts a node and proposes a `demo-N` command every 5 seconds
(advancing only while this node is the leader — followers log a rejection and
retry).

```sh
cargo build --release

# five terminals, or use `&` on one:
./target/release/raft_core --config config-node1.toml &
./target/release/raft_core --config config-node2.toml &
./target/release/raft_core --config config-node3.toml &
./target/release/raft_core --config config-node4.toml &
./target/release/raft_core --config config-node5.toml &
```

* expect a leader to be elected within ~1s (`"candidate received majority votes, transitioning to leader"`),
* only the leader's terminal will log `"proposed command committed and applied"` —
  the other four will log `"propose rejected (not the leader), will retry"` every 5s,
* kill the leader (`Ctrl+C`) and watch a follower win the next election; re-start
  the killed node and it rejoins as follower.

The config file path can also be given via the `MINI_RAFT_CONFIG` environment
variable (defaults to `config`, i.e. `config.toml`).

## Tests

```sh
cargo test
```

* `src/server.rs` — unit tests of the state machine interactions (election, voting,
  log replication, commit quorum, proposal commit/ack)
* `tests/codec.rs` — framing round-trip and partial-frame handling
* `tests/election.rs` — a single node times out and starts an election
* `tests/cluster.rs` — 3-node in-process cluster: leader election, `propose`
  commits and replicates to all state machines, non-leader proposals rejected
* `tests/tcp.rs` — two `NetworkManagers` over real localhost TCP between peers

---
