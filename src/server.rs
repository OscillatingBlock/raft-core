use crate::{
    codec::{AppendEntries, AppendEntriesResponse, Message, RequestVote, RequestVoteResponse},
    config::ServerConfig,
};
use anyhow::Context;
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, sync::Arc, time::Duration};
use tokio::{
    sync::{
        mpsc::{self},
        oneshot, watch,
    },
    time::Instant,
};
use tracing::{debug, field::debug, info, instrument, warn};

use crate::state_machine::StateMachine;

#[allow(dead_code)]
pub struct Server {
    pub id: NodeId,
    state: ServerState,
    config: ServerConfig,
    to_network: mpsc::Sender<Outgoing>,
    from_network: tokio::sync::Mutex<mpsc::Receiver<Incoming>>,

    log: Vec<LogEntry>,

    /// Queue drained by the consensus loop: proposals submitted via `propose`.
    proposal_queue: mpsc::Receiver<Proposal>,
    /// Submission end of the proposal queue (handed out by `propose`/`raft_node`).
    proposals: mpsc::Sender<Proposal>,
    /// One ack per un-applied leader proposal; receives the bytes produced by
    /// `apply_log` when the command commits and is applied to the state
    /// machine, or an error if the proposal is rejected / does not apply.
    commit_notifiers: HashMap<i32, oneshot::Sender<anyhow::Result<Bytes>>>,
    /// Published to on every role change so `RaftNode::is_leader` can observe
    /// leadership from other tasks without touching `ServerState` directly.
    leadership_tx: watch::Sender<bool>,
}

#[allow(dead_code)]
pub struct ServerState {
    state: RaftState,
    state_machine: Arc<dyn StateMachine>,

    voted_for: Option<NodeId>,
    last_heartbeat: Instant,

    current_term: u32,
    commit_index: i32,
    last_applied: i32,

    //highest log index sent to follower in the current AppendEntries rpc
    last_sent_index: Vec<i32>,
}

impl ServerState {
    pub fn new(state_machine: Arc<dyn StateMachine>, num_peers: usize) -> Self {
        // +1 so 1-based NodeIds (1..=total_nodes from config.toml) index safely.
        // Index 0 is unused in that scheme; 0-based ids (tests) work too.
        let size = num_peers + 1;
        let last_sent_index = vec![0; size];
        Self {
            state: RaftState::Follower {},
            state_machine,
            current_term: 0,
            // Sentinel value -1 is used because Rust vectors are 0-indexed.
            // Since index 0 is a valid, real log entry, 0 cannot be used
            // to represent "none." A negative sentinel cleanly distinguishes
            // an uninitialized/empty state from actual log indexes.
            commit_index: -1, // -1 means no log entries have been committed yet
            last_applied: -1, // -1 means no log entries have been applied to the state machine yet
            voted_for: None,
            last_heartbeat: Instant::now(),
            last_sent_index,
        }
    }
}

#[derive(PartialEq, Debug, Clone)]
pub enum RaftState {
    Follower {},
    Candidate {
        votes_received: u32,
    },
    Leader {
        match_index: Vec<i32>,
        next_index: Vec<u64>,
    },
}

#[derive(Clone, Copy, PartialEq, PartialOrd, Debug, Eq, Hash, Serialize, Deserialize)]
pub struct NodeId(u64);

impl NodeId {
    fn as_usize(&self) -> usize {
        return self.0 as usize;
    }
}

impl std::fmt::Display for NodeId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl NodeId {
    pub fn new(id: u64) -> Self {
        Self(id)
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct LogEntry {
    pub term: u32,
    pub command: Bytes,
    pub index: u32,
}

#[allow(dead_code)]
pub enum Destination {
    Node(NodeId),
    Broadcast,
}

#[allow(dead_code)]
pub struct Outgoing {
    pub dest: Destination,
    pub msg: Message,
}

#[allow(dead_code)]
#[derive(Clone, Debug)]
pub struct Incoming {
    pub from: NodeId,
    pub msg: Message,
}

/// A user-submitted command awaiting replication through the Raft protocol.
/// `ack` receives the bytes produced by `apply_log` once the command has been
/// committed by a majority and applied to the state machine, or carries an
/// error if the proposal is rejected (e.g. the node is not, or is no longer,
/// the leader).
#[derive(Debug)]
pub struct Proposal {
    pub command: Bytes,
    pub ack: oneshot::Sender<anyhow::Result<Bytes>>,
}

/// User-facing handle to a running Raft node. Cloning shares the same node.
#[derive(Clone, Debug)]
pub struct RaftNode {
    pub id: NodeId,
    proposals: mpsc::Sender<Proposal>,
    /// Streams every leadership change of the node; `is_leader()` reads it.
    leadership: watch::Receiver<bool>,
}

impl Server {
    /// Returns a shareable handle that can be used to submit commands
    /// via [`RaftNode::propose`] while the node runs in the background.
    pub fn raft_node(&self) -> RaftNode {
        RaftNode {
            id: self.id,
            proposals: self.proposals.clone(),
            leadership: self.leadership_tx.subscribe(),
        }
    }

    /// Submits `command` to the Raft protocol and resolves with the bytes
    /// produced by the state machine's `apply_log` once the command has been
    /// committed by a majority and applied to the state machine.
    ///
    /// Errors if the node is shutting down, if it loses / does not hold
    /// leadership before the command commits (callers should route the
    /// command to the current leader or retry), or if applying the command
    /// to the state machine fails.
    pub async fn propose(&self, command: impl Into<Bytes>) -> anyhow::Result<Bytes> {
        enqueue_proposal(&self.proposals, command.into()).await
    }
}

impl RaftNode {
    /// Submits `command` to the Raft protocol and resolves with the bytes
    /// produced by the state machine's `apply_log` once the command has been
    /// committed by a majority and applied to the state machine.
    ///
    /// Errors if the node is shutting down, if it loses / does not hold
    /// leadership before the command commits (callers should route the
    /// command to the current leader or retry), or if applying the command
    /// to the state machine fails.
    pub async fn propose(&self, command: impl Into<Bytes>) -> anyhow::Result<Bytes> {
        enqueue_proposal(&self.proposals, command.into()).await
    }

    /// Reports whether this handle's node currently holds leadership.
    ///
    /// The returned value is eventually-consistent: it lags a role change by
    /// at most one consensus-loop iteration. Consult [`Self::propose`] errors
    /// for the authoritative per-command outcome.
    pub fn is_leader(&self) -> bool {
        *self.leadership.borrow()
    }
}

async fn enqueue_proposal(
    proposals: &mpsc::Sender<Proposal>,
    command: Bytes,
) -> anyhow::Result<Bytes> {
    let (ack_tx, ack_rx) = oneshot::channel();
    proposals
        .send(Proposal {
            command,
            ack: ack_tx,
        })
        .await
        .context("proposal queue closed: node is shutting down")?;

    ack_rx
        .await
        .context("proposal aborted: node lost leadership before commit")?
}

#[allow(dead_code)]
impl Server {
    pub fn new(
        id: NodeId,
        config: ServerConfig,
        state_machine: Arc<dyn StateMachine>,
        to_network: mpsc::Sender<Outgoing>,
        from_network: mpsc::Receiver<Incoming>,
    ) -> Server {
        info!("initializing new server node");
        let state = ServerState::new(state_machine, config.total_nodes as usize);
        let (proposals, proposal_queue) = mpsc::channel(256);
        let from_network = tokio::sync::Mutex::new(from_network);
        Self {
            id,
            config,
            state,
            to_network,
            from_network,
            log: vec![],
            proposal_queue,
            proposals,
            commit_notifiers: HashMap::new(),
            leadership_tx: watch::channel(false).0,
        }
    }

    #[instrument(skip(self), fields(node_id = %self.id))]
    pub async fn run(&mut self) {
        info!("starting server main loop");
        loop {
            self.publish_role();
            match &self.state.state {
                RaftState::Follower {} => self.follower().await,
                RaftState::Candidate { votes_received: _ } => self.candidate().await,
                RaftState::Leader {
                    match_index: _,
                    next_index: _,
                } => self.leader().await,
            }
        }
    }

    /// Publishes the current role to `RaftNode::is_leader` watchers. Called
    /// at every top-of-loop dispatch, which is the one place all role
    /// transitions funnel through (state functions exit on a role change).
    fn publish_role(&mut self) {
        let is_leader = matches!(self.state.state, RaftState::Leader { .. });
        self.leadership_tx.send_if_modified(|current| {
            if *current == is_leader {
                return false;
            }
            *current = is_leader;
            true
        });
    }

    /// Rejects every queued proposal. Called whenever this node takes on a
    /// non-leader role so that `propose()` callers waiting for an ack get a
    /// prompt error instead of waiting for a command that can no longer
    /// originate here.
    fn reject_pending_proposals(&mut self) {
        while let Ok(proposal) = self.proposal_queue.try_recv() {
            let _ = proposal.ack.send(Err(anyhow::anyhow!(
                "proposal rejected: node {} was not the leader when the proposal was processed",
                self.id
            )));
        }
        debug!("drained and rejected queued proposals");
    }

    #[instrument(skip(self), fields(node_id = %self.id))]
    async fn follower(&mut self) {
        info!("entering follower state");
        self.reject_pending_proposals();
        _ = self
            .apply_latest_commited_logs(self.state.commit_index)
            .await;
        let timeout_duration = self.config.election_timeout_duration();

        loop {
            tokio::select! {
                _ = self.recieve_rpc() => {
                    debug!("follower received rpc");
                    break
                }
                _ = set_timer(timeout_duration.clone()) => {
                    self.state.state = RaftState::Candidate{
                        votes_received: 0,
                    };
                    warn!("election timeout expired, transitioning from follower to candidate");
                    break
                }
            }
        }
    }

    #[instrument(skip(self), fields(node_id = %self.id))]
    async fn candidate(&mut self) {
        let RaftState::Candidate { votes_received: _ } = self.state.state else {
            warn!("not a candidate, cant run candidate loop");
            return;
        };
        self.reject_pending_proposals();
        self.become_candidate();

        info!(
            term = self.state.current_term,
            "starting new election as candidate"
        );

        let mut timeout_duration = self.config.election_timeout_duration();

        loop {
            // send request_vote rpcs to all other server
            debug("sending request_vote rpc to peers");
            self.send_request_vote_rpc_to_peers().await;

            tokio::select! {
                _ = self.recieve_rpc() => {
                    // break the candidate loop
                    // either we recieved majority votes and became Leader
                    // or we discovered Leader and became follower

                    let RaftState::Candidate { votes_received : _ } = self.state.state else {
                        info!(new_state = ?self.state.state, "state changed during candidate phase, exiting loop");
                        break
                    };

                }
                _ = set_timer(timeout_duration.clone()) => {
                    warn!("candidate election timeout expired, restarting election");
                    // Fresh OS-seeded jitter each timeout; no RNG held across .await so future stays Send.
                    timeout_duration += Duration::from_micros(rand::random_range(0..500));
                }
            }
        }
    }

    #[instrument(skip(self), fields(node_id = %self.id))]
    async fn send_request_vote_rpc_to_peers(&mut self) {
        debug!("broadcasting request vote rpc to peers");
        let request_vote_msg = Message::RequestVoteType(RequestVote {
            term: self.state.current_term,
            candidate_id: self.id.clone(),
            last_log_index: if self.log.is_empty() {
                0
            } else {
                self.log.len() as i32 - 1
            },
            last_log_term: if self.log.is_empty() {
                0
            } else {
                self.log[self.log.len() - 1].term
            },
        });
        let outgoing_msg = Outgoing {
            dest: Destination::Broadcast,
            msg: request_vote_msg,
        };
        _ = self.to_network.send(outgoing_msg).await;
    }

    #[instrument(skip(self), fields(node_id = %self.id))]
    async fn leader(&mut self) {
        info!("entering leader state");

        _ = self
            .apply_latest_commited_logs(self.state.commit_index)
            .await;

        // upon election send first heartbeat to all peers
        self.send_heartbeat().await;
        self.become_leader();

        loop {
            // Pick up commands submitted via `propose()` while we hold leadership.
            _ = self.process_pending_proposals().await;
            self.handle_commit_index().await;

            tokio::select! {
                _ = set_timer(self.config.heartbeat_interval_duration()) => {
                    debug!("heartbeat interval reached, sending heartbeat");
                    self.send_heartbeat().await;

                }

                _ = self.recieve_rpc() => {
                    // if we discovered that we are no longer leader break leader loop
                    let RaftState::Leader {
                        match_index: _,
                        next_index: _,
                    } = &mut self.state.state
                    else {
                        warn!(new_state = ?self.state.state, "no longer leader, stepping down");
                        break;
                    };
                }
            }
        }
    }

    #[instrument(skip(self), fields(node_id = %self.id))]
    async fn send_heartbeat(&mut self) {
        debug!("sending heartbeat to peers");
        let heartbeat_msg = Message::AppendEntriesType(AppendEntries {
            term: self.state.current_term,
            leader_id: self.id.clone(),
            prev_log_index: self.log.len() as i32 - 1,
            prev_log_term: if self.log.is_empty() {
                0
            } else {
                self.log[self.log.len() - 1].term
            },
            entries: vec![],
            leader_commit: self.state.commit_index,
        });
        let outgoing_msg = Outgoing {
            dest: Destination::Broadcast,
            msg: heartbeat_msg,
        };
        self.to_network.send(outgoing_msg).await.unwrap();
    }

    #[instrument(skip(self), fields(node_id = %self.id))]
    async fn recieve_rpc(&mut self) -> anyhow::Result<()> {
        let msg = self
            .from_network
            .lock()
            .await
            .recv()
            .await
            .context("channel closed")?;

        debug!(
            message_type = msg.msg.message_type(),
            "received rpc message from network"
        );

        match msg.msg {
            Message::AppendEntriesType(append_entries_msg) => {
                let incoming = Incoming {
                    from: msg.from,
                    msg: Message::AppendEntriesType(append_entries_msg),
                };
                self.handle_append_entries(incoming).await?;
            }

            Message::AppendEntriesResponseType(_) => {
                self.handle_append_entries_response(msg).await?;
            }

            Message::RequestVoteType(request_vote_msg) => {
                self.handle_request_vote_rpc(request_vote_msg).await?;
            }

            Message::RequestVoteResponseType(request_vote_response_msg) => {
                self.handle_request_vote_response_rpc(request_vote_response_msg)
                    .await;
            }
            Message::HelloType(_) => {
                debug!("ignoring stray hello message in server loop");
            }
        }
        Ok(())
    }

    #[instrument(skip(self, incoming), fields(node_id = %self.id))]
    async fn handle_append_entries(&mut self, incoming: Incoming) -> anyhow::Result<()> {
        let msg = incoming.msg;
        let Message::AppendEntriesType(msg) = msg else {
            anyhow::bail!("expected append entries type")
        };
        debug!("handling append entries message");
        let response = Message::AppendEntriesResponseType(AppendEntriesResponse {
            term: self.state.current_term,
            success: false,
        });
        let node_id = incoming.from;
        let reply = Outgoing {
            dest: Destination::Node(node_id),
            msg: response,
        };
        info!(term = self.state.current_term);
        // Reply false if term < currentTerm
        if msg.term < self.state.current_term {
            self.reply_false_if_lower_term(Message::AppendEntriesType(msg.clone()), reply)
                .await?;

            return Ok(());
        }

        // Reply false if log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm
        let matched = self
            .reply_false_if_prev_log_does_not_match(&msg, node_id)
            .await?;
        if !matched {
            return Ok(());
        }

        // if we are leader and msg term higher than our current term, then become follower

        if msg.term > self.state.current_term {
            match self.state.state {
                RaftState::Follower { .. } => {
                    self.state.current_term = msg.term;
                }
                _ => {
                    self.become_follower_if_higher_term(msg.term.clone());
                    return Ok(());
                }
            }
        }

        _ = self.apply_latest_commited_logs(msg.leader_commit).await;

        if msg.entries.is_empty() {
            debug!("received valid empty heartbeat from leader");
            return Ok(()); // heartbeat
        };

        // If an existing entry conflicts with a new one (same index
        // but different terms), delete the existing entry and all that
        // follow it
        self.delete_conflicting_and_append_new_entries(&msg);

        // If leaderCommit > commitIndex, set commitIndex =
        // min(leaderCommit, index of last new entry)
        if msg.leader_commit > self.state.commit_index {
            let last_new_entry_index = msg.entries[msg.entries.len() - 1].index;
            self.state.commit_index = std::cmp::min(msg.leader_commit, last_new_entry_index as i32);

            debug!(
                commit_index = self.state.commit_index,
                "updated local commit index"
            );
        }

        let response = Message::AppendEntriesResponseType(AppendEntriesResponse {
            term: self.state.current_term,
            success: true,
        });
        let outgoing_msg = Outgoing {
            dest: Destination::Node(node_id),
            msg: response,
        };
        self.to_network.send(outgoing_msg).await?;

        Ok(())
    }

    async fn reply_false_if_lower_term(
        &mut self,
        msg: Message,
        reply: Outgoing,
    ) -> anyhow::Result<()> {
        let term = match msg {
            Message::AppendEntriesType(msg) => msg.term,
            Message::RequestVoteType(msg) => msg.term,
            _ => {
                unreachable!()
            }
        };
        if term < self.state.current_term {
            match reply.msg {
                Message::AppendEntriesResponseType(_) => {
                    warn!(
                        leader_term = term,
                        current_term = self.state.current_term,
                        "rejecting append entries: leader term is lower than local term"
                    );
                }
                Message::RequestVoteResponseType(_) => {
                    debug!("denying vote: candidate term is lower than current term");
                }
                _ => {}
            }

            return self
                .to_network
                .send(reply)
                .await
                .context("failed to send append entries");
        }
        Ok(())
    }

    async fn reply_false_if_prev_log_does_not_match(
        &mut self,
        msg: &AppendEntries,
        leader_id: NodeId,
    ) -> anyhow::Result<bool> {
        let mut prev_log_index_matching = true;

        match self.log.get(msg.prev_log_index as usize) {
            Some(log_entry) => {
                if log_entry.term != msg.prev_log_term {
                    prev_log_index_matching = false;
                }
            }
            None => {
                // if self.log.len == 0, its the first msg from leader , accept it
                if self.log.len() != 0 {
                    prev_log_index_matching = false;
                }
            }
        }

        if !prev_log_index_matching {
            let response = Message::AppendEntriesResponseType(AppendEntriesResponse {
                term: self.state.current_term,
                success: false,
            });
            let outgoing_msg = Outgoing {
                dest: Destination::Node(leader_id),
                msg: response,
            };
            warn!(
                prev_log_index = msg.prev_log_index,
                "rejecting append entries: previous log index/term does not match"
            );
            self.to_network
                .send(outgoing_msg)
                .await
                .context("failed to send append entries")?;

            Ok(false)
        } else {
            Ok(true)
        }
    }

    fn become_follower_if_higher_term(&mut self, term: u32) {
        let prev_state = self.state.state.clone();
        self.become_follower(term);

        match prev_state {
            RaftState::Leader { .. } => {
                info!(
                    old_term = self.state.current_term,
                    new_term = term,
                    "Leader discovered higher term, stepping down to follower"
                );
            }
            RaftState::Candidate { .. } => {
                info!(
                    old_term = self.state.current_term,
                    new_term = term,
                    "Candidate discovered leader, stepping down to follower"
                );
            }
            _ => {}
        }
    }

    fn delete_conflicting_and_append_new_entries(&mut self, msg: &AppendEntries) {
        let mut new_entries = vec![];
        for entry in msg.entries.iter() {
            let ith_log_entry = self.log.get(entry.index as usize);
            match ith_log_entry {
                Some(log_entry) => {
                    if log_entry.term != entry.term {
                        warn!(
                            index = entry.index,
                            "log conflict detected, truncating conflicting entries"
                        );
                        self.log.truncate(entry.index as usize);
                        // Entries from this index on no longer exist locally;
                        // drop their proposal acks so `propose()` callers fail
                        // fast instead of waiting forever.
                        self.commit_notifiers
                            .retain(|index, _| *index < entry.index as i32);
                        new_entries.push(entry.clone());
                    }
                }
                None => {
                    new_entries.push(entry.clone());
                }
            }
        }

        // Append any new entries not already in the log
        for entry in new_entries.iter() {
            debug!(index = entry.index, "appending new entry to local log");
            self.log.insert(entry.index as usize, entry.clone());
        }
    }

    #[instrument(skip(self, msg), fields(node_id = %self.id, candidate_id = %msg.candidate_id))]
    async fn handle_request_vote_rpc(&mut self, msg: RequestVote) -> anyhow::Result<()> {
        debug!("handling request vote rpc");
        let no_vote_response_msg = Message::RequestVoteResponseType(RequestVoteResponse {
            term: self.state.current_term,
            vote_granted: false,
        });
        let reply = Outgoing {
            dest: Destination::Node(msg.candidate_id.clone()),
            msg: no_vote_response_msg.clone(),
        };

        //Reply false if term < currentTerm
        if msg.term < self.state.current_term {
            self.reply_false_if_lower_term(Message::RequestVoteType(msg.clone()), reply)
                .await?;

            return Ok(());
        }

        match self.state.voted_for {
            Some(ref voted_for) => {
                if voted_for != &msg.candidate_id
                    || msg.last_log_index < self.log[self.log.len() - 1].index as i32
                    || msg.last_log_term != self.log[self.log.len() - 1].term
                {
                    debug!(
                        "denying vote: already voted for another candidate or log is less up-to-date"
                    );
                    self.to_network.send(reply).await?;
                    return Ok(());
                }
            }
            None => {}
        }

        info!(candidate_id = %msg.candidate_id, "granting vote to candidate");
        let vote_response = RequestVoteResponse {
            term: self.state.current_term,
            vote_granted: true,
        };
        let vote_response_msg = Message::RequestVoteResponseType(vote_response);
        let outgoing_msg = Outgoing {
            dest: Destination::Node(msg.candidate_id),
            msg: vote_response_msg.clone(),
        };
        self.to_network.send(outgoing_msg).await?;

        Ok(())
    }

    #[instrument(skip(self, msg), fields(node_id = %self.id, term = msg.term))]
    async fn handle_request_vote_response_rpc(&mut self, msg: RequestVoteResponse) {
        debug!("handling request vote response rpc");
        let RaftState::Candidate { votes_received } = &mut self.state.state else {
            warn!("Ignoring handle request vote response rpc, as curent role not condidate");
            return;
        };

        if msg.term > self.state.current_term {
            self.become_follower_if_higher_term(msg.term);
            return;
        }

        if msg.vote_granted {
            // count votes
            *votes_received += 1;
            debug!(
                votes_received = votes_received,
                "vote granted, tallying votes"
            );
            // if majority votes, become leader
            if *votes_received >= (self.config.total_nodes / 2) + 1 {
                self.become_leader();
                info!("candidate received majority votes, transitioning to leader");
            }
            // else stay candidate
        } else {
            debug!("vote denied by peer");
        }
    }

    fn become_leader(&mut self) {
        let next_index_each = if self.state.last_applied < 0 {
            0
        } else {
            self.state.last_applied as u64 + 1
        };
        self.state.state = RaftState::Leader {
            match_index: vec![-1; self.config.total_nodes as usize + 1],
            next_index: vec![next_index_each; self.config.total_nodes as usize + 1],
        };
    }

    #[instrument(skip(self), fields(node_id = %self.id))]
    async fn handle_commit_index(&self) {}

    // applies latest commited logs to state machine
    #[instrument(skip(self), fields(node_id = %self.id))]
    async fn apply_latest_commited_logs(&mut self, leader_commit: i32) -> anyhow::Result<()> {
        // Advance commit_index, clamped to our last log index.
        if leader_commit > self.state.commit_index {
            let last_idx = self.log.len() as i32 - 1;
            if last_idx >= 0 {
                self.state.commit_index = std::cmp::min(leader_commit, last_idx);
            }
        }
        // Apply every committed-but-unapplied entry in order.
        while self.state.last_applied < self.state.commit_index {
            let next = self.state.last_applied + 1;
            let Some(log) = self.log.get(next as usize).cloned() else {
                break;
            };

            debug!(
                last_applied = next,
                "applying committed log to state machine"
            );
            match self.state.state_machine.apply_log(&log.command) {
                Ok(applied) => {
                    self.state.last_applied = next;

                    // The entry is now durable and applied: hand the
                    // state machine's output to any `propose()` caller
                    // waiting on this index.
                    if let Some(ack) = self.commit_notifiers.remove(&next) {
                        let _ = ack.send(Ok(applied));
                    }
                }
                Err(err) => {
                    // Surface the failure to the originating `propose()`
                    // caller instead of leaving it waiting forever.
                    // `last_applied` stays put, so the entry is retried
                    // on the next apply pass.
                    if let Some(ack) = self.commit_notifiers.remove(&next) {
                        let _ = ack.send(Err(err));
                    }
                    break;
                }
            }
        }

        Ok(())
    }

    /// Drains proposals submitted via `propose()` while leader: each command is
    /// appended to the local log and replicated to followers via AppendEntries.
    /// The command's ack is stored in `commit_notifiers` and fires (in
    /// `apply_latest_commited_logs`) once the majority commits and it is applied.
    #[instrument(skip(self), fields(node_id = %self.id, command = ?command))]
    async fn append_proposed_command_to_log(
        &mut self,
        command: Bytes,
        ack: oneshot::Sender<anyhow::Result<Bytes>>,
    ) -> anyhow::Result<()> {
        let RaftState::Leader {
            match_index: _,
            next_index,
        } = &mut self.state.state
        else {
            warn!("not appending proposed command, current state not leader");
            let _ = ack.send(Err(anyhow::anyhow!(
                "proposal rejected: node {} lost leadership before the proposal was processed",
                self.id
            )));
            return Ok(());
        };

        let log_entry = LogEntry {
            term: self.state.current_term,
            index: self.log.len() as u32,
            command,
        };

        info!(
            log_index = log_entry.index,
            term = log_entry.term,
            "appending proposed command to local log"
        );
        self.log.push(log_entry.clone());
        self.commit_notifiers.insert(log_entry.index as i32, ack);

        for (peer_id, _) in &self.config.peer_node_addresses() {
            if *peer_id == self.id {
                continue;
            }
            let i = peer_id.as_usize();
            let prev_log_index = if next_index[i] == 0 {
                -1 as i32
            } else {
                next_index[i] as i32 - 1
            };
            let prev_log_term = if prev_log_index == -1 {
                0
            } else {
                self.log[prev_log_index as usize].term
            };
            self.state.last_sent_index[i] = self.log.len() as i32 - 1;

            debug!(peer_id = ?peer_id, prev_log_index = prev_log_index, "broadcasting append entries to peer for new log entry");
            self.to_network
                .send(Outgoing {
                    dest: Destination::Broadcast,
                    msg: Message::AppendEntriesType(AppendEntries {
                        term: self.state.current_term,
                        leader_id: self.id,
                        prev_log_index,
                        prev_log_term,
                        entries: vec![log_entry.clone()],
                        leader_commit: self.state.commit_index,
                    }),
                })
                .await?;
        }

        Ok(())
    }

    /// Awakened after every consensus round: processes queued proposals, which
    /// works only when we are the leader. While a non-leader role has control,
    /// it silently returns, and queued proposals are rejected by
    /// `reject_pending_proposals` instead.
    #[instrument(skip(self), fields(node_id = %self.id))]
    async fn process_pending_proposals(&mut self) -> anyhow::Result<()> {
        while let Ok(proposal) = self.proposal_queue.try_recv() {
            if !matches!(self.state.state, RaftState::Leader { .. }) {
                let _ = proposal.ack.send(Err(anyhow::anyhow!(
                    "proposal rejected: node {} lost leadership before the proposal was processed",
                    self.id
                )));
                return Ok(());
            }

            self.append_proposed_command_to_log(proposal.command, proposal.ack)
                .await?;
        }

        Ok(())
    }

    #[instrument(skip(self, incoming), fields(node_id = %self.id))]
    async fn handle_append_entries_response(&mut self, incoming: Incoming) -> anyhow::Result<()> {
        let msg = incoming.msg;
        let Message::AppendEntriesResponseType(msg) = msg else {
            return Ok(());
        };

        let RaftState::Leader {
            match_index,
            next_index,
        } = &mut self.state.state
        else {
            debug!("ignoring append entries response: node is no longer leader");
            return Ok(());
        };

        debug!(peer_id = ?incoming.from, success = msg.success, term = msg.term, "received append entries response from peer");

        // If RPC response contains term T > currentTerm:
        // set currentTerm = T, convert to follower
        if msg.term > self.state.current_term {
            self.become_follower_if_higher_term(msg.term);
            return Ok(());
        }

        let peer_id = incoming.from;
        let i = peer_id.as_usize();

        if msg.success {
            // If successful: update nextIndex and matchIndex for follower
            match_index[i] = self.state.last_sent_index[i];
            next_index[i] = match_index[i] as u64 + 1;
            debug!(
                peer_id = ?incoming.from,
                match_index = match_index[i],
                next_index = next_index[i],
                "updated follower indices on successful replication"
            );
        } else {
            // If AppendEntries fails because of log inconsistency: decrement nextIndex and retry
            next_index[i] = if next_index[i] > 0 {
                next_index[i] - 1
            } else {
                0
            };
            warn!(
                peer_id = ?incoming.from,
                new_next_index = next_index[i],
                "append entries rejected due to log inconsistency, decrementing next_index and retrying"
            );

            let prev_log_index = next_index[i] as i32;
            let prev_log_term = self.log[prev_log_index as usize].term;
            let entries = self.log[next_index[i] as usize..].to_vec();

            self.to_network
                .send(Outgoing {
                    dest: Destination::Node(peer_id),
                    msg: Message::AppendEntriesType(AppendEntries {
                        term: self.state.current_term,
                        leader_id: self.id,
                        prev_log_index,
                        entries,
                        prev_log_term,
                        leader_commit: self.state.commit_index,
                    }),
                })
                .await?;
        }

        // If there exists an N such that N > commitIndex, a majority
        // of matchIndex[i] ≥ N, and log[N].term == currentTerm:
        // set commitIndex = N
        let last_log_index = if self.log.is_empty() {
            0
        } else {
            self.log.len() as i32 - 1
        };

        let start_index = self.state.commit_index + 1;

        let mut commit_index_updated = false;
        for n in start_index..=last_log_index {
            debug!(
                "checking if majority replication is achieved for log index {}",
                n
            );
            if self.log[n as usize].term == self.state.current_term {
                let mut match_count = 0;

                for match_index in match_index.iter() {
                    println!("match_index = {match_index}, match_count = {match_count}");
                    if *match_index >= n {
                        match_count += 1;
                    }
                }

                let total_nodes = self.config.total_nodes;
                if match_count > total_nodes / 2 - 1 {
                    info!(
                        new_commit_index = n,
                        match_count = match_count,
                        "majority replication achieved, advancing commit index"
                    );
                    self.state.commit_index = n;
                    commit_index_updated = true;
                    break;
                }
            }
        }
        let commit_index = self.state.commit_index;
        if msg.success && commit_index_updated {
            for n in self.state.last_applied + 1..=self.state.commit_index {
                debug!(log_index = n, "applying committed log to state machine");
                _ = self.apply_latest_commited_logs(commit_index).await;
            }

            self.state.last_applied = self.state.commit_index;
            // Re-borrow match_index safely just before using it,
            // since the previous references have finished their lifecycle.
            let match_index = match &mut self.state.state {
                RaftState::Leader { match_index, .. } => match_index,
                _ => return Ok(()),
            };
            match_index[self.id.as_usize()] = self.state.commit_index;

            info!(
                commit_index = self.state.commit_index,
                "committed entry applied; pending propose() callers acked"
            );
        }

        Ok(())
    }

    fn become_follower(&mut self, term: u32) {
        self.state.current_term = term;
        self.state.state = RaftState::Follower {};
        self.state.voted_for = None;
    }

    fn become_candidate(&mut self) {
        self.state.current_term += 1;
        self.state.last_heartbeat = Instant::now();
        self.state.voted_for = Some(self.id.clone());
    }
}

async fn set_timer(timeout_duration: tokio::time::Duration) {
    tokio::time::sleep(timeout_duration).await;
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use std::sync::RwLock;
    use std::{collections::HashMap, sync::Arc};

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

    fn create_new_server_and_mock_msg_sender()
    -> (Server, mpsc::Sender<Incoming>, mpsc::Receiver<Outgoing>) {
        println!("Creating new server and mock message sender");
        let _ = tracing_subscriber::fmt()
            .with_test_writer()
            .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
            .try_init();

        let mut peer_addreses = HashMap::new();
        peer_addreses.insert(0, "localhost:8080".to_string());
        peer_addreses.insert(1, "localhost:8080".to_string());
        peer_addreses.insert(2, "localhost:8080".to_string());
        peer_addreses.insert(3, "localhost:8080".to_string());

        let node_id = NodeId(0);
        let config = ServerConfig {
            election_timeout: 200,
            heartbeat_interval: 50,
            address: "localhost:8080".to_string(),
            total_nodes: 5,
            peer_addresses: peer_addreses,
            node_id: 0,
            peer_listener_address: "127.0.0.1:9001".to_string(),
        };
        let state_machine = SimpleStateMachine::default();
        let server_state_machine =
            ServerState::new(Arc::new(state_machine), config.total_nodes as usize);
        let (node_tx, adapter_rx) = mpsc::channel::<Outgoing>(32); // Node → adapter
        let (adapter_tx, node_rx) = mpsc::channel::<Incoming>(32); // adapter → Node

        let server = Server::new(
            node_id,
            config,
            server_state_machine.state_machine.clone(),
            node_tx,
            node_rx,
        );
        (server, adapter_tx, adapter_rx)
    }

    async fn populate_follower_log(mock_adapter_tx: &mpsc::Sender<Incoming>) {
        mock_adapter_tx
            .send(Incoming {
                from: NodeId(67),
                msg: Message::AppendEntriesType(AppendEntries {
                    term: 1,
                    leader_id: NodeId(1),
                    prev_log_index: 0,
                    prev_log_term: 0,
                    entries: vec![LogEntry {
                        term: 1,
                        command: Bytes::from("test-command"),
                        index: 0,
                    }],
                    leader_commit: 0,
                }),
            })
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn test_follower_timeout_triggers_candidate() {
        let (mut server, _do_not_remove_this_unused_variable, _this_also) =
            create_new_server_and_mock_msg_sender();
        server.follower().await;

        let current_state = server.state.state;
        assert_eq!(current_state, RaftState::Candidate { votes_received: 0 });
    }

    #[tokio::test]
    async fn test_leader_heartbeat_prevents_follower_timeout() {
        let (mut server, mock_adapter_tx, _) = create_new_server_and_mock_msg_sender();
        mock_adapter_tx
            .send(Incoming {
                from: NodeId(67),
                msg: Message::AppendEntriesType(AppendEntries {
                    term: 1,
                    leader_id: NodeId(1),
                    prev_log_index: 0,
                    prev_log_term: 0,
                    entries: vec![],
                    leader_commit: 0,
                }),
            })
            .await
            .unwrap();

        server.follower().await;
        let current_state = server.state.state;
        assert_eq!(current_state, RaftState::Follower {});
    }

    #[tokio::test]
    async fn test_follower_rejects_incosistent_log_append_entries() {
        let (mut server, mock_adapter_tx, mut mock_adapter_rx) =
            create_new_server_and_mock_msg_sender();

        populate_follower_log(&mock_adapter_tx).await;

        server.follower().await;
        let first_response = mock_adapter_rx.recv().await.unwrap();
        let Message::AppendEntriesResponseType(first_response) = first_response.msg else {
            panic!("Expected AppendEntriesResponseType");
        };
        assert!(matches!(first_response.success, true));

        mock_adapter_tx
            .send(Incoming {
                from: NodeId(67),
                msg: Message::AppendEntriesType(AppendEntries {
                    term: 1,
                    leader_id: NodeId(1),
                    prev_log_index: 1,
                    prev_log_term: 0,
                    entries: vec![LogEntry {
                        term: 1,
                        command: Bytes::from("conflicting-command"),
                        index: 0,
                    }],
                    leader_commit: 0,
                }),
            })
            .await
            .unwrap();

        server.follower().await;

        let second_response = mock_adapter_rx.recv().await.unwrap();
        let Message::AppendEntriesResponseType(second_response) = second_response.msg else {
            panic!("Expected AppendEntriesResponseType");
        };
        assert!(matches!(second_response.success, false));
    }

    #[tokio::test]
    async fn test_follower_accepts_valid_append_entries() {
        let (mut server, mock_adapter_tx, mut mock_adapter_rx) =
            create_new_server_and_mock_msg_sender();

        populate_follower_log(&mock_adapter_tx).await;

        server.follower().await;
        let first_response = mock_adapter_rx.recv().await.unwrap();
        let Message::AppendEntriesResponseType(first_response) = first_response.msg else {
            panic!("Expected AppendEntriesResponseType");
        };
        assert!(matches!(first_response.success, true));

        let first_log_entry = LogEntry {
            term: 1,
            command: Bytes::from("cmd1"),
            index: 1,
        };
        let second_log_entry = LogEntry {
            term: 1,
            command: Bytes::from("cmd2"),
            index: 2,
        };
        mock_adapter_tx
            .send(Incoming {
                from: NodeId(67),
                msg: Message::AppendEntriesType(AppendEntries {
                    term: 1,
                    leader_id: NodeId(1),
                    prev_log_index: 0,
                    prev_log_term: 1,
                    entries: vec![first_log_entry.clone(), second_log_entry.clone()],
                    leader_commit: 0,
                }),
            })
            .await
            .unwrap();
        server.follower().await;

        let response = mock_adapter_rx.recv().await.unwrap();
        let Message::AppendEntriesResponseType(response) = response.msg else {
            panic!("Expected AppendEntriesResponseType");
        };
        assert!(matches!(response.success, true));

        let server_first_log_entry = server.log.get(1).unwrap();
        let server_second_log_entry = server.log.get(2).unwrap();

        assert_eq!(server_first_log_entry, &first_log_entry);
        assert_eq!(server_second_log_entry, &second_log_entry);
    }

    #[tokio::test]
    async fn test_follower_grants_vote_to_valid_candidate() {
        let (mut server, mock_adapter_tx, mut mock_adapter_rx) =
            create_new_server_and_mock_msg_sender();

        populate_follower_log(&mock_adapter_tx).await;

        server.follower().await;
        let first_response = mock_adapter_rx.recv().await.unwrap();
        let Message::AppendEntriesResponseType(first_response) = first_response.msg else {
            panic!("Expected AppendEntriesResponseType");
        };
        assert!(matches!(first_response.success, true));

        mock_adapter_tx
            .send(Incoming {
                from: NodeId(67),
                msg: Message::RequestVoteType(RequestVote {
                    term: 2,
                    candidate_id: NodeId(2),
                    last_log_index: 0,
                    last_log_term: 1,
                }),
            })
            .await
            .unwrap();
        server.follower().await;

        let vote_response = mock_adapter_rx.recv().await.unwrap();
        let Message::RequestVoteResponseType(vote_response) = vote_response.msg else {
            panic!("Expected RequestVoteResponseType");
        };
        assert!(vote_response.vote_granted);
    }

    #[tokio::test]
    async fn test_new_candidate_sends_request_vote_rpc_to_peers() {
        tokio::time::pause();
        let (mut server, mock_adapter_tx, mut mock_adapter_rx) =
            create_new_server_and_mock_msg_sender();

        server.follower().await;

        let heartbeat = Message::AppendEntriesType(AppendEntries {
            term: server.state.current_term + 2,
            leader_id: NodeId(1),
            prev_log_index: 0,
            prev_log_term: 1,
            entries: vec![],
            leader_commit: 0,
        });
        mock_adapter_tx
            .send(Incoming {
                from: NodeId(99),
                msg: heartbeat,
            })
            .await
            .unwrap();

        server.candidate().await;

        let request_vote_msg = mock_adapter_rx.recv().await.unwrap();

        let Message::RequestVoteType(req) = request_vote_msg.msg else {
            println!("Expected RequestVoteType, got {:?}", request_vote_msg.msg);
            panic!("Expected RequestVoteType")
        };
        assert_eq!(req.term + 1, server.state.current_term);
        assert_eq!(req.candidate_id, server.id);
        assert_eq!(req.last_log_index, 0);
        assert_eq!(req.last_log_term, 0);
    }

    #[tokio::test]
    async fn test_candidate_reverts_to_follower_on_higher_term() {
        let (mut server, mock_adapter_tx, mut _mock_adapter_rx) =
            create_new_server_and_mock_msg_sender();

        server.follower().await;

        let heartbeat = Message::AppendEntriesType(AppendEntries {
            term: server.state.current_term + 2,
            leader_id: NodeId(1),
            prev_log_index: 0,
            prev_log_term: 1,
            entries: vec![],
            leader_commit: 0,
        });
        mock_adapter_tx
            .send(Incoming {
                from: NodeId(99),
                msg: heartbeat,
            })
            .await
            .unwrap();

        server.candidate().await;

        assert_eq!(server.state.state, RaftState::Follower {});
    }

    #[tokio::test]
    async fn test_candidate_becomes_leader_on_majority_votes() {
        let (mut server, mock_adapter_tx, mut mock_adapter_rx) =
            create_new_server_and_mock_msg_sender();

        simulate_follower_becomes_leader(&mut server, &mock_adapter_tx, &mut mock_adapter_rx).await;
        assert_eq!(
            server.state.state,
            RaftState::Leader {
                next_index: vec![0; server.config.total_nodes as usize + 1],
                match_index: vec![-1; server.config.total_nodes as usize + 1]
            }
        );
    }

    #[tokio::test]
    async fn test_leader_becomes_follower_on_higher_term() {
        let (mut server, mock_adapter_tx, mut mock_adapter_rx) =
            create_new_server_and_mock_msg_sender();

        simulate_follower_becomes_leader(&mut server, &mock_adapter_tx, &mut mock_adapter_rx).await;

        let higher_term_msg = Message::AppendEntriesType(AppendEntries {
            term: server.state.current_term + 1,
            leader_id: NodeId(1),
            prev_log_index: 0,
            prev_log_term: 1,
            entries: vec![],
            leader_commit: 0,
        });
        mock_adapter_tx
            .send(Incoming {
                from: NodeId(67),
                msg: higher_term_msg,
            })
            .await
            .unwrap();

        server.leader().await;

        assert_eq!(server.state.state, RaftState::Follower {});
    }

    #[tokio::test]
    async fn test_leader_sends_periodic_heartbeats() {
        let (mut server, mock_adapter_tx, mut mock_adapter_rx) =
            create_new_server_and_mock_msg_sender();

        simulate_follower_becomes_leader(&mut server, &mock_adapter_tx, &mut mock_adapter_rx).await;

        let heartbeat_duration = server.config.heartbeat_interval_duration();
        tokio::task::spawn(async move {
            tokio::time::sleep(heartbeat_duration * 3).await;
            let higher_term_msg = Message::AppendEntriesType(AppendEntries {
                term: server.state.current_term + 1,
                leader_id: NodeId(4),
                prev_log_index: 0,
                prev_log_term: 1,
                entries: vec![],
                leader_commit: 0,
            });

            mock_adapter_tx
                .send(Incoming {
                    from: NodeId(67),
                    msg: higher_term_msg,
                })
                .await
                .unwrap();
        });
        server.leader().await;

        let first_response = mock_adapter_rx.recv().await.unwrap();
        let Message::AppendEntriesType(append_entries) = first_response.msg else {
            panic!("Expected heartbeat message got {:?}", first_response.msg);
        };
        assert_eq!(append_entries.entries.len(), 0);

        let second_response = mock_adapter_rx.recv().await.unwrap();
        let Message::AppendEntriesType(append_entries) = second_response.msg else {
            panic!("Expected heartbeat message got {:?}", second_response.msg);
        };
        assert_eq!(append_entries.entries.len(), 0);
    }

    async fn simulate_follower_becomes_leader(
        server: &mut Server,
        mock_adapter_tx: &mpsc::Sender<Incoming>,
        mock_adapter_rx: &mut mpsc::Receiver<Outgoing>,
    ) {
        server.follower().await;

        for _ in 0..(server.config.total_nodes / 2) + 1 {
            mock_adapter_tx
                .send(Incoming {
                    from: NodeId(67),
                    msg: Message::RequestVoteResponseType(RequestVoteResponse {
                        term: server.state.current_term,
                        vote_granted: true,
                    }),
                })
                .await
                .unwrap();
        }

        server.candidate().await;

        for _ in 0..(server.config.total_nodes / 2) + 1 {
            _ = mock_adapter_rx.recv().await.unwrap();
        }
    }

    #[tokio::test]
    async fn test_leader_commits_on_quorom_majority() {
        let (mut server, mock_adapter_tx, mut mock_adapter_rx) =
            create_new_server_and_mock_msg_sender();

        simulate_follower_becomes_leader(&mut server, &mock_adapter_tx, &mut mock_adapter_rx).await;

        // Submit a command through the direct-`propose` path instead of a
        // networked client request.
        let (ack_tx, mut ack_rx) = oneshot::channel();
        server
            .proposals
            .send(Proposal {
                command: Bytes::from("test-bytes"),
                ack: ack_tx,
            })
            .await
            .unwrap();

        let node_1_response = Incoming {
            from: NodeId(1),
            msg: Message::AppendEntriesResponseType(AppendEntriesResponse {
                term: server.state.current_term,
                success: true,
            }),
        };
        let node_2_response = Incoming {
            from: NodeId(2),
            msg: Message::AppendEntriesResponseType(AppendEntriesResponse {
                term: server.state.current_term,
                success: true,
            }),
        };
        let node_3_response = Incoming {
            from: NodeId(3),
            msg: Message::AppendEntriesResponseType(AppendEntriesResponse {
                term: server.state.current_term,
                success: true,
            }),
        };

        let higher_term_msg = Incoming {
            from: NodeId(8),
            msg: Message::AppendEntriesType(AppendEntries {
                term: server.state.current_term + 1,
                leader_id: NodeId(4),
                prev_log_index: 0,
                prev_log_term: 1,
                entries: vec![],
                leader_commit: 0,
            }),
        };

        let msg_to_send = vec![
            node_1_response,
            node_2_response,
            node_3_response,
            higher_term_msg,
        ];
        for msg in msg_to_send {
            mock_adapter_tx.send(msg).await.unwrap();
        }

        server.leader().await;
        assert_eq!(server.state.commit_index, 0);
        // The proposal must have been acked with the state machine's output
        // once committed and applied.
        let applied = ack_rx.try_recv().unwrap().unwrap();
        assert_eq!(applied, Bytes::from("test-bytes"));
    }

    #[tokio::test]
    async fn test_leader_retries_on_log_incosistency() {
        let (mut server, mock_adapter_tx, mut mock_adapter_rx) =
            create_new_server_and_mock_msg_sender();

        simulate_follower_becomes_leader(&mut server, &mock_adapter_tx, &mut mock_adapter_rx).await;

        // Submit a command through the direct-`propose` path instead of a
        // networked client request.
        let (ack_tx, mut ack_rx) = oneshot::channel();
        server
            .proposals
            .send(Proposal {
                command: Bytes::from("cmd1"),
                ack: ack_tx,
            })
            .await
            .unwrap();

        let node_1_response = Incoming {
            from: NodeId(1),
            msg: Message::AppendEntriesResponseType(AppendEntriesResponse {
                term: server.state.current_term,
                success: true,
            }),
        };
        let node_2_response = Incoming {
            from: NodeId(2),
            msg: Message::AppendEntriesResponseType(AppendEntriesResponse {
                term: server.state.current_term,
                success: true,
            }),
        };

        let higher_term_msg = Incoming {
            from: NodeId(67),
            msg: Message::AppendEntriesType(AppendEntries {
                term: server.state.current_term + 1,
                leader_id: NodeId(4),
                prev_log_index: 0,
                prev_log_term: 1,
                entries: vec![],
                leader_commit: 0,
            }),
        };

        let msg_to_send = vec![node_1_response, node_2_response, higher_term_msg.clone()];
        for msg in msg_to_send {
            mock_adapter_tx.send(msg).await.unwrap();
        }

        server.leader().await;

        assert_eq!(server.state.commit_index, 0);
        // The proposal must have been acked with the state machine's output
        // once committed and applied.
        let applied = ack_rx.try_recv().unwrap().unwrap();
        assert_eq!(applied, Bytes::from("cmd1"));

        warn!("changing node state back to leader, for running further test");
        server.state.state = RaftState::Leader {
            match_index: vec![-1, server.config.total_nodes as i32 + 1],
            next_index: vec![0, server.config.total_nodes as u64 + 1],
        };

        let (ack_tx2, mut ack_rx2) = oneshot::channel();
        server
            .proposals
            .send(Proposal {
                command: Bytes::from("cmd2"),
                ack: ack_tx2,
            })
            .await
            .unwrap();
        let node_1_response2 = Incoming {
            from: NodeId(1),
            msg: Message::AppendEntriesResponseType(AppendEntriesResponse {
                term: server.state.current_term,
                success: true,
            }),
        };
        let node_2_response2 = Incoming {
            from: NodeId(2),
            msg: Message::AppendEntriesResponseType(AppendEntriesResponse {
                term: server.state.current_term,
                success: true,
            }),
        };
        let node_3_second_log_failure = Incoming {
            from: NodeId(3),
            msg: Message::AppendEntriesResponseType(AppendEntriesResponse {
                term: server.state.current_term,
                success: false,
            }),
        };
        let node_3_second_log_success = Incoming {
            from: NodeId(3),
            msg: Message::AppendEntriesResponseType(AppendEntriesResponse {
                term: server.state.current_term,
                success: true,
            }),
        };
        let higher_term_msg = Incoming {
            from: NodeId(67),
            msg: Message::AppendEntriesType(AppendEntries {
                term: server.state.current_term + 1,
                leader_id: NodeId(4),
                prev_log_index: 0,
                prev_log_term: 1,
                entries: vec![],
                leader_commit: 0,
            }),
        };
        let msg_to_send_again = vec![
            node_1_response2,
            node_2_response2,
            node_3_second_log_failure,
            node_3_second_log_success,
            higher_term_msg,
        ];
        for msg in msg_to_send_again {
            mock_adapter_tx.send(msg).await.unwrap();
        }

        server.leader().await;
        assert_eq!(server.state.commit_index, 1);
        let applied2 = ack_rx2.try_recv().unwrap().unwrap();
        assert_eq!(applied2, Bytes::from("cmd2"));
    }

    #[tokio::test]
    async fn test_leader_does_not_commit_log_from_previous_term() {
        let (mut server, mock_adapter_tx, mut mock_adapter_rx) =
            create_new_server_and_mock_msg_sender();

        simulate_follower_becomes_leader(&mut server, &mock_adapter_tx, &mut mock_adapter_rx).await;

        server.state.current_term += 1;
        server.state.commit_index = 1;

        mock_adapter_tx
            .send(Incoming {
                from: NodeId(3),
                msg: Message::AppendEntriesResponseType(AppendEntriesResponse {
                    term: server.state.current_term - 1,
                    success: true,
                }),
            })
            .await
            .unwrap();
        mock_adapter_tx
            .send(Incoming {
                from: NodeId(67),
                msg: Message::AppendEntriesType(AppendEntries {
                    term: server.state.current_term + 1,
                    leader_id: NodeId(1),
                    prev_log_index: 0,
                    prev_log_term: 1,
                    entries: vec![],
                    leader_commit: 0,
                }),
            })
            .await
            .unwrap();

        server.leader().await;

        assert_eq!(server.state.commit_index, 1);
    }
}
