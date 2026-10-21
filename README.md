# raft_core

*(Previously `mini_raft`)*: Learning project: Raft consensus in Rust, built after reading the Raft paper, recently rebranded and refactored into `raft_core`.

## Layout

* `src/server.rs` — Raft core (follower/candidate/leader, log replication)
* `src/network.rs` — TCP peers/clients, handshake, dispatch
* `src/codec.rs` — length-prefixed JSON framing
* `src/state_machine.rs` — simple key-value store
* `src/config.rs` / `src/main.rs` — per-node config and binary
* `tests/` — integration tests
* `tools/raft_client.py` — minimal client for manual testing
* `config-node1..5.toml` — 5-node local cluster configs

## Architecture

Each node runs a `Server` event loop (`follower`/`candidate`/`leader`) connected to a
`NetworkManager` via `mpsc` channels (`Incoming`/`Outgoing`). The network layer
handles TCP peers and clients using a length-prefixed JSON `Codec`, with a
`Hello`/`HelloClient` handshake to identify connections. Leaders replicate client
`Set` commands with `AppendEntries`, track `next_index`/`match_index` per follower,
advance `commit_index` on majority ack, then apply entries to the key-value
`StateMachine`. Elections use randomized-timeout `RequestVote` RPCs.

## Run

```sh
cargo build
./target/debug/raft_core --config config-node1.toml
# ... start nodes 1-5 (ports 9001-9005 / 8081-8085)
python3 tools/raft_client.py --nodes 127.0.0.1:8081,127.0.0.1:8082,127.0.0.1:8083,127.0.0.1:8084,127.0.0.1:8085 --key foo --value bar

```

## Test

```sh
cargo test

```

---

Unit tests for the node logic were written by the author. Integration testing (`tests/codec.rs`, `tests/election.rs`, `tests/cluster.rs`, `tests/tcp.rs`) was done by Muse Spark.
