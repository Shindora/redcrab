//! Raft consensus core, one instance per replicated log: each partition, and the cluster
//! metadata log.
//!
//! The Raft log is the Kafka log. Coordinates are Kafka offsets: an entry is one record batch
//! covering `[base_offset, base_offset + record_count)`, and a log end is the next offset. Every
//! batch carries the term of the leader that appended it in its `partitionLeaderEpoch` field,
//! called the epoch here. The core never sees batch bytes; it reasons about offsets, epochs and
//! the start offset of each epoch.
//!
//! The core is sans-IO: no I/O, clock, threads or randomness of its own. Every call takes the
//! current time, election jitter comes from [`Config::seed`], and everything to be done is
//! returned as [`Action`]s.
//!
//! # Driver obligations
//!
//! 1. Build a node with [`Node::new`] from durable state only: the last persisted [`HardState`],
//!    and the epoch history and end offset of the log as recovered from disk, all of which then
//!    counts as flushed. The epoch history must list, in order, every epoch present in the log
//!    with the base offset of its first batch. The commit offset restarts at 0 and is relearned.
//! 2. Call [`Node::tick`] often (every few milliseconds, well under the heartbeat interval) and
//!    [`Node::step`] for every message received, passing the same monotonic clock to every call.
//! 3. Perform the actions returned by one call in order, and finish every log write and read
//!    among them before the next call into the node:
//!    - [`Action::PersistHardState`] must be durable before any later action is performed.
//!    - [`Action::Truncate`] must be durable once the next flush that starts after it completes.
//!    - [`Action::Replicate`] reads the log at once; the transport may send it later.
//!    - Messages may be dropped, delayed, duplicated or reordered by the transport.
//! 4. After [`Node::propose`] returns a [`Proposal`], write that batch at its base offset,
//!    stamped with its epoch, before the next call into the node. Acknowledge a proposal only on
//!    an [`Action::Commit`] covering its last offset that arrives while the latest
//!    [`Action::LeaderChanged`] still names this node leader in the proposal's epoch, and fail
//!    every unacknowledged proposal on any other `LeaderChanged`. A failed proposal may still be
//!    committed later, so clients must retry idempotently.
//! 5. Flush appended data promptly: take [`Node::flush_point`] just before an fsync starts and
//!    hand it to [`Node::flushed`] once the fsync has completed. Only flushed data counts: a
//!    follower acknowledges, and a leader counts itself, only up to its flushed end, and the
//!    commit offset (the Kafka high watermark) never exceeds the flushed end.
//!
//! Every offset the core asks the driver to read from or truncate at is a batch boundary of the
//! driver's log.
//!
//! # Up-to-date check
//!
//! A candidate advertises the last epoch and end of its *appended* log; a voter grants only if
//! that pair is at least the last epoch and end of its own *flushed* log. An offset is committed
//! once a majority has it flushed, and flushed data survives crashes and is never truncated below
//! the commit offset, so every election majority includes a voter whose flushed log holds every
//! committed batch, and the standard Raft argument applied to that flushed prefix puts all of
//! them in the winner's appended log, which is the log it leads with. Nothing unflushed is relied
//! upon: a voter's unflushed suffix was never acknowledged, and a winner that crashes and loses
//! its unflushed suffix restarts as a follower and leads again only after another election.

use std::mem;
use std::time::Duration;

// ponytail: static voters, no membership change, learners, snapshots or compaction; the log starts at offset 0 and grows forever.

/// Identifies a node, in the same id space as Kafka broker ids.
pub type NodeId = i32;

/// Static settings of one node of one Raft group.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Config {
    /// This node.
    pub id: NodeId,
    /// Every voter of the group, this node included.
    pub voters: Vec<NodeId>,
    /// Lower bound of the randomised election timeout. Also how long a leader may go without
    /// hearing from a majority (`CheckQuorum`), and how long after hearing from a leader a node
    /// refuses pre-votes.
    pub election_timeout_min: Duration,
    /// Exclusive upper bound of the randomised election timeout.
    pub election_timeout_max: Duration,
    /// How often a leader sends an idle follower a heartbeat, and how long it waits for an append
    /// response before retrying.
    pub heartbeat_interval: Duration,
    /// Byte budget of one replicated append request.
    pub max_append_bytes: usize,
    /// Seed of the election timeout jitter.
    pub seed: u64,
}

/// State that must be durable before any message that depends on it is sent.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct HardState {
    /// Latest term this node has seen.
    pub term: i32,
    /// Candidate this node voted for in `term`.
    pub voted_for: Option<NodeId>,
}

/// The first offset of an epoch in the log.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EpochStart {
    /// Leader epoch (term) stamped on the batches.
    pub epoch: i32,
    /// Base offset of the first batch of that epoch.
    pub start_offset: i64,
}

/// What a node recovers from disk at startup.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct DurableState {
    /// The last persisted hard state.
    pub hard_state: HardState,
    /// Every epoch in the recovered log, in log order.
    pub epochs: Vec<EpochStart>,
    /// End offset of the recovered log.
    pub log_end: i64,
}

/// Header facts of one record batch, as carried by an [`AppendRequest`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BatchInfo {
    /// First offset of the batch.
    pub base_offset: i64,
    /// Number of offsets the batch occupies (`lastOffsetDelta + 1`).
    pub record_count: i32,
    /// The batch's `partitionLeaderEpoch`.
    pub epoch: i32,
}

impl BatchInfo {
    /// Offset following the batch.
    #[must_use]
    pub fn end_offset(&self) -> i64 {
        self.base_offset + i64::from(self.record_count)
    }
}

/// Messages exchanged between the nodes of one group.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Message {
    /// Ask for a vote or, with `pre_vote`, whether a vote would be granted.
    VoteRequest(VoteRequest),
    /// Answer to a [`VoteRequest`].
    VoteResponse(VoteResponse),
    /// Batches, or with no batches a heartbeat, from the leader.
    AppendRequest(AppendRequest),
    /// Answer to an [`AppendRequest`], also sent unprompted when a follower's flush completes.
    AppendResponse(AppendResponse),
}

/// A request for a vote.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct VoteRequest {
    /// Term of the election; for a pre-vote, the term the candidate would campaign in.
    pub term: i32,
    /// Ask whether the vote would be granted, without changing anyone's term.
    pub pre_vote: bool,
    /// Epoch of the last batch of the candidate's appended log.
    pub last_epoch: i32,
    /// End offset of the candidate's appended log.
    pub log_end: i64,
}

/// An answer to a [`VoteRequest`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct VoteResponse {
    /// The request's term if granted, otherwise the responder's term.
    pub term: i32,
    /// Answers a pre-vote.
    pub pre_vote: bool,
    /// Whether the vote is (or would be) granted.
    pub granted: bool,
}

/// Leader to follower: append `batches` after `prev_offset`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AppendRequest {
    /// The leader's term.
    pub term: i32,
    /// Offset the batches start at; the follower's log must match the leader's before it.
    pub prev_offset: i64,
    /// Epoch of the leader's batch ending at `prev_offset`, or 0 when `prev_offset` is 0.
    pub prev_epoch: i32,
    /// Offsets below this are committed (the leader's own commit offset may lag it until the
    /// leader's flush catches up).
    pub commit_offset: i64,
    /// Contiguous batches starting at `prev_offset`; empty for a heartbeat.
    pub batches: Vec<BatchInfo>,
}

/// Follower to leader.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AppendResponse {
    /// The follower's term.
    pub term: i32,
    /// What the follower did.
    pub outcome: AppendOutcome,
}

/// Result of an [`AppendRequest`] on a follower.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AppendOutcome {
    /// The follower's log matches the leader's up to `matched_end`, of which `flushed_end` is
    /// flushed.
    Accepted {
        /// End of the prefix known to match the leader's log.
        matched_end: i64,
        /// End of the flushed part of that prefix.
        flushed_end: i64,
    },
    /// The previous offset and epoch did not match. `epoch` is the follower's largest epoch not
    /// above the request's previous epoch and `end_offset` where that epoch ends in the
    /// follower's log; both are 0 when it has no such epoch.
    Mismatch {
        /// Largest epoch of the follower not above the request's previous epoch.
        epoch: i32,
        /// End of that epoch in the follower's log.
        end_offset: i64,
    },
    /// The request's term is older than the follower's.
    StaleTerm,
}

/// Something the driver must do, in the order returned.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Action {
    /// Make this hard state durable before performing any later action.
    PersistHardState(HardState),
    /// Remove every batch at or after `offset`.
    Truncate {
        /// New log end.
        offset: i64,
    },
    /// Append the batches of the [`AppendRequest`] just stepped, starting with the one whose
    /// base offset is `from_offset`, through the last one.
    AppendReceived {
        /// Base offset of the first batch to append.
        from_offset: i64,
    },
    /// Append a control batch of one record at `offset` stamped with `epoch`: the no-op a new
    /// leader commits before anything older.
    AppendNoop {
        /// Base offset of the control batch.
        offset: i64,
        /// Its epoch, the new leader's term.
        epoch: i32,
    },
    /// Send a small, complete message. Sends to the same peer from many groups can share a frame.
    Send {
        /// Destination node.
        to: NodeId,
        /// The message.
        message: Message,
    },
    /// Read whole batches from `request.prev_offset` up to `end_offset`, at most `max_bytes` but
    /// at least one, put their headers in `request.batches` and send them with their bytes.
    Replicate {
        /// Destination node.
        to: NodeId,
        /// The request, missing only its batches.
        request: AppendRequest,
        /// Read no batch at or after this offset.
        end_offset: i64,
        /// Byte budget of the read.
        max_bytes: usize,
    },
    /// The commit offset, which is the Kafka high watermark, advanced to `offset`.
    Commit {
        /// Offsets below this are committed.
        offset: i64,
    },
    /// The current term or the known leader changed.
    LeaderChanged {
        /// The current term.
        term: i32,
        /// The leader of `term`, if known.
        leader: Option<NodeId>,
    },
}

/// Where a proposed batch must be written.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Proposal {
    /// Base offset to write the batch at.
    pub base_offset: i64,
    /// Epoch to stamp in its `partitionLeaderEpoch`.
    pub epoch: i32,
}

/// The log end at the moment a flush started, to report with [`Node::flushed`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FlushPoint {
    end_offset: i64,
    generation: u64,
}

impl FlushPoint {
    /// Offset the flush makes durable up to.
    #[must_use]
    pub fn end_offset(&self) -> i64 {
        self.end_offset
    }
}

/// The largest epoch at or below a requested one, and where it ends: the answer to Kafka's
/// `OffsetsForLeaderEpoch`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EpochEnd {
    /// The epoch found.
    pub epoch: i32,
    /// Offset following its last batch, which is the log end for the last epoch.
    pub end_offset: i64,
}

/// What a node is doing in its current term.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Role {
    /// Following a leader, or waiting for one.
    Follower,
    /// Asking whether it could win an election, without having raised its term.
    PreCandidate,
    /// Campaigning in its current term.
    Candidate,
    /// Leading its current term.
    Leader,
}

/// Read-only view of a node.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Status {
    /// This node.
    pub id: NodeId,
    /// Its role.
    pub role: Role,
    /// Its current term.
    pub term: i32,
    /// The leader of the current term, if known.
    pub leader: Option<NodeId>,
    /// Commit offset, the Kafka high watermark.
    pub commit_offset: i64,
    /// End of the appended log.
    pub log_end: i64,
    /// End of the flushed log.
    pub flushed_end: i64,
}

/// Errors returned by [`Node`].
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum RaftError {
    /// The configuration is unusable.
    #[error("invalid config: {0}")]
    InvalidConfig(&'static str),
    /// The durable state handed to [`Node::new`] is inconsistent.
    #[error("invalid durable state: {0}")]
    InvalidState(&'static str),
    /// A message came from this node itself or from a node outside the group.
    #[error("node {0} is not a peer in this group")]
    UnknownPeer(NodeId),
    /// A message is malformed or contradicts this node's log.
    #[error("invalid message: {0}")]
    InvalidMessage(&'static str),
    /// Only the leader accepts proposals.
    #[error("not the leader; the leader is {leader:?}")]
    NotLeader {
        /// The leader of the current term, if known.
        leader: Option<NodeId>,
    },
    /// A batch must occupy at least one offset.
    #[error("record count {0} is not positive")]
    InvalidRecordCount(i32),
}

/// One node of one Raft group.
#[derive(Debug)]
pub struct Node {
    id: NodeId,
    peers: Vec<NodeId>,
    quorum: usize,
    election_timeout_min: Duration,
    election_timeout_max: Duration,
    heartbeat_interval: Duration,
    max_append_bytes: usize,
    rng: SplitMix64,
    term: i32,
    voted_for: Option<NodeId>,
    role: RoleState,
    leader: Option<NodeId>,
    leader_contact: Duration,
    election_deadline: Duration,
    log: LogState,
    /// Offsets below this are committed and match this node's log.
    known_committed: i64,
    /// `min(known_committed, flushed end)`, never decreasing.
    commit: i64,
    /// End of the prefix of the log known to match the log of the current term's leader.
    matched_end: i64,
    announced: (i32, Option<NodeId>),
    actions: Vec<Action>,
}

#[derive(Debug)]
enum RoleState {
    Follower,
    PreCandidate(Vec<NodeId>),
    Candidate(Vec<NodeId>),
    Leader(LeaderState),
}

#[derive(Debug)]
struct LeaderState {
    /// In the order of `Node::peers`.
    followers: Vec<Progress>,
}

// ponytail: one in-flight append per follower; pipelining with a window of outstanding requests is the upgrade.
#[derive(Debug)]
struct Progress {
    id: NodeId,
    /// Offset the next append starts at.
    next: i64,
    /// Highest flushed end the follower acknowledged in this term.
    flushed: i64,
    /// When the outstanding append was sent, and the offset it starts at.
    in_flight: Option<(Duration, i64)>,
    last_sent: Duration,
    last_heard: Duration,
}

impl Node {
    /// Build a node from its configuration and the state recovered from disk.
    ///
    /// # Errors
    ///
    /// Returns [`RaftError::InvalidConfig`] for an unusable configuration and
    /// [`RaftError::InvalidState`] for an inconsistent durable state.
    pub fn new(config: Config, durable: DurableState, now: Duration) -> Result<Node, RaftError> {
        validate_config(&config)?;
        let HardState { term, voted_for } = durable.hard_state;
        if term < 0 {
            return Err(RaftError::InvalidState("term is negative"));
        }
        if voted_for.is_some_and(|candidate| !config.voters.contains(&candidate)) {
            return Err(RaftError::InvalidState(
                "voted for a node outside the group",
            ));
        }
        let log = LogState::restore(durable)?;
        if log.epochs.last_epoch() > term {
            return Err(RaftError::InvalidState(
                "the log holds an epoch above the term",
            ));
        }
        let Config {
            id,
            voters,
            election_timeout_min,
            election_timeout_max,
            heartbeat_interval,
            max_append_bytes,
            seed,
        } = config;
        let quorum = voters.len() / 2 + 1;
        let peers: Vec<NodeId> = voters.into_iter().filter(|&voter| voter != id).collect();
        let mut node = Node {
            id,
            peers,
            quorum,
            election_timeout_min,
            election_timeout_max,
            heartbeat_interval,
            max_append_bytes,
            rng: SplitMix64(seed ^ u64::from(id.cast_unsigned()).wrapping_mul(GOLDEN_GAMMA)),
            term,
            voted_for,
            role: RoleState::Follower,
            leader: None,
            leader_contact: now,
            election_deadline: now,
            log,
            known_committed: 0,
            commit: 0,
            matched_end: 0,
            announced: (term, None),
            actions: Vec::new(),
        };
        node.reset_election_deadline(now);
        Ok(node)
    }

    /// Read-only view of the node.
    #[must_use]
    pub fn status(&self) -> Status {
        Status {
            id: self.id,
            role: match self.role {
                RoleState::Follower => Role::Follower,
                RoleState::PreCandidate(_) => Role::PreCandidate,
                RoleState::Candidate(_) => Role::Candidate,
                RoleState::Leader(_) => Role::Leader,
            },
            term: self.term,
            leader: self.leader,
            commit_offset: self.commit,
            log_end: self.log.end,
            flushed_end: self.log.flushed,
        }
    }

    /// Epoch of the batch ending at `offset` (0 for offset 0), or `None` outside the log.
    #[must_use]
    pub fn epoch_ending_at(&self, offset: i64) -> Option<i32> {
        (0..=self.log.end)
            .contains(&offset)
            .then(|| self.log.epochs.epoch_ending_at(offset))
    }

    /// The largest epoch in the log at or below `epoch` and where it ends, or `None` if every
    /// epoch in the log is above `epoch`.
    #[must_use]
    pub fn epoch_end(&self, epoch: i32) -> Option<EpochEnd> {
        self.log.epochs.end_of(epoch, self.log.end)
    }

    /// The point to report through [`Node::flushed`] once an fsync starting now completes.
    #[must_use]
    pub fn flush_point(&self) -> FlushPoint {
        FlushPoint {
            end_offset: self.log.end,
            generation: self.log.generation,
        }
    }

    /// Advance time: start elections, send heartbeats and retries, and check the quorum.
    #[must_use = "the actions must be performed"]
    pub fn tick(&mut self, now: Duration) -> Vec<Action> {
        if matches!(self.role, RoleState::Leader(_)) {
            if self.quorum_lost(now) {
                self.become_follower(self.term, None, now);
            } else {
                for index in 0..self.peers.len() {
                    self.replicate_to(index, now, true);
                }
            }
        } else if now >= self.election_deadline {
            self.start_pre_vote(now);
        }
        self.take_actions()
    }

    /// Start a pre-vote now instead of at the election timeout, unless already leading. Voters
    /// grant it only as they would a timed-out one, so this is only ever an earlier start; it lets
    /// the designated first leader of a fresh group skip waiting out a whole election timeout.
    #[must_use = "the actions must be performed"]
    pub fn campaign(&mut self, now: Duration) -> Vec<Action> {
        if !matches!(self.role, RoleState::Leader(_)) {
            self.start_pre_vote(now);
        }
        self.take_actions()
    }

    /// Handle a message received from `from`.
    ///
    /// # Errors
    ///
    /// Returns [`RaftError::UnknownPeer`] or [`RaftError::InvalidMessage`] without changing
    /// anything when the message is rejected.
    ///
    /// # Panics
    ///
    /// Panics when the message proves a safety violation, such as two leaders in one term or a
    /// request to truncate committed data.
    pub fn step(
        &mut self,
        from: NodeId,
        message: Message,
        now: Duration,
    ) -> Result<Vec<Action>, RaftError> {
        if !self.peers.contains(&from) {
            return Err(RaftError::UnknownPeer(from));
        }
        match message {
            Message::VoteRequest(request) => self.on_vote_request(from, request, now)?,
            Message::VoteResponse(response) => self.on_vote_response(from, response, now),
            Message::AppendRequest(request) => self.on_append_request(from, &request, now)?,
            Message::AppendResponse(response) => self.on_append_response(from, response, now)?,
        }
        Ok(self.take_actions())
    }

    /// Assign the next offsets to a batch of `record_count` records produced to the leader.
    ///
    /// # Errors
    ///
    /// Returns [`RaftError::NotLeader`] unless this node leads its term, and
    /// [`RaftError::InvalidRecordCount`] for a count below 1.
    pub fn propose(&mut self, record_count: i32) -> Result<Proposal, RaftError> {
        if record_count < 1 {
            return Err(RaftError::InvalidRecordCount(record_count));
        }
        if !matches!(self.role, RoleState::Leader(_)) {
            return Err(RaftError::NotLeader {
                leader: self.leader,
            });
        }
        let base_offset = self.log.end;
        self.log.append(base_offset, record_count, self.term);
        Ok(Proposal {
            base_offset,
            epoch: self.term,
        })
    }

    /// Report that the fsync started at `point` has completed. A point taken before a later
    /// truncation is ignored.
    #[must_use = "the actions must be performed"]
    pub fn flushed(&mut self, point: FlushPoint) -> Vec<Action> {
        if point.generation == self.log.generation && point.end_offset > self.log.flushed {
            let acknowledged = self.ackable();
            self.log.flushed = point.end_offset.min(self.log.end);
            if matches!(self.role, RoleState::Leader(_)) {
                self.advance_leader_commit();
            } else {
                self.advance_commit();
                if let (RoleState::Follower, Some(leader)) = (&self.role, self.leader)
                    && self.ackable() > acknowledged
                {
                    let accepted = self.accepted();
                    self.send(leader, accepted);
                }
            }
        }
        self.take_actions()
    }

    fn take_actions(&mut self) -> Vec<Action> {
        mem::take(&mut self.actions)
    }

    fn on_vote_request(
        &mut self,
        from: NodeId,
        request: VoteRequest,
        now: Duration,
    ) -> Result<(), RaftError> {
        if request.last_epoch < 0 || request.log_end < 0 {
            return Err(RaftError::InvalidMessage("negative epoch or offset"));
        }
        let log_ok =
            (request.last_epoch, request.log_end) >= (self.log.flushed_epoch(), self.log.flushed);
        if request.pre_vote {
            let granted = request.term > self.term && log_ok && !self.leader_alive(now);
            let term = if granted { request.term } else { self.term };
            self.send(
                from,
                Message::VoteResponse(VoteResponse {
                    term,
                    pre_vote: true,
                    granted,
                }),
            );
            return Ok(());
        }
        if request.term > self.term {
            self.become_follower(request.term, None, now);
        }
        let granted = request.term == self.term
            && log_ok
            && self.voted_for.is_none_or(|candidate| candidate == from);
        if granted {
            if self.voted_for.is_none() {
                self.voted_for = Some(from);
                self.persist_hard_state();
            }
            self.reset_election_deadline(now);
        }
        self.send(
            from,
            Message::VoteResponse(VoteResponse {
                term: self.term,
                pre_vote: false,
                granted,
            }),
        );
        Ok(())
    }

    fn on_vote_response(&mut self, from: NodeId, response: VoteResponse, now: Duration) {
        if response.term > self.term && !(response.pre_vote && response.granted) {
            self.become_follower(response.term, None, now);
            return;
        }
        if !response.granted {
            return;
        }
        let quorum = self.quorum;
        match &mut self.role {
            RoleState::PreCandidate(granted)
                if response.pre_vote && response.term == self.term + 1 =>
            {
                if record_vote(granted, from, quorum) {
                    self.start_election(now);
                }
            }
            RoleState::Candidate(granted) if !response.pre_vote && response.term == self.term => {
                if record_vote(granted, from, quorum) {
                    self.become_leader(now);
                }
            }
            _ => {}
        }
    }

    fn on_append_request(
        &mut self,
        from: NodeId,
        request: &AppendRequest,
        now: Duration,
    ) -> Result<(), RaftError> {
        validate_append_request(request)?;
        if request.term < self.term {
            self.send(
                from,
                Message::AppendResponse(AppendResponse {
                    term: self.term,
                    outcome: AppendOutcome::StaleTerm,
                }),
            );
            return Ok(());
        }
        if request.term > self.term
            || !matches!(self.role, RoleState::Follower)
            || self.leader != Some(from)
        {
            assert!(
                request.term > self.term
                    || (!matches!(self.role, RoleState::Leader(_)) && self.leader.is_none()),
                "node {} saw two leaders in term {}: {:?} and {from}",
                self.id,
                self.term,
                self.leader,
            );
            self.become_follower(request.term, Some(from), now);
        }
        self.leader_contact = now;
        self.reset_election_deadline(now);

        let prev = request.prev_offset;
        if prev > self.log.end || self.log.epochs.epoch_ending_at(prev) != request.prev_epoch {
            let hint = self
                .log
                .epochs
                .end_of(request.prev_epoch, self.log.end)
                .unwrap_or(EpochEnd {
                    epoch: 0,
                    end_offset: 0,
                });
            self.send(
                from,
                Message::AppendResponse(AppendResponse {
                    term: self.term,
                    outcome: AppendOutcome::Mismatch {
                        epoch: hint.epoch,
                        end_offset: hint.end_offset,
                    },
                }),
            );
            return Ok(());
        }

        let mut matched = prev;
        let mut append_from = None;
        for batch in &request.batches {
            let end = batch.end_offset();
            if append_from.is_none() {
                if end <= self.log.end && self.log.epochs.epoch_ending_at(end) == batch.epoch {
                    matched = end;
                    continue;
                }
                if batch.base_offset < self.log.end {
                    self.truncate(batch.base_offset);
                }
                append_from = Some(batch.base_offset);
            }
            self.log
                .append(batch.base_offset, batch.record_count, batch.epoch);
            matched = end;
        }
        if let Some(from_offset) = append_from {
            self.actions.push(Action::AppendReceived { from_offset });
        }
        self.matched_end = self.matched_end.max(matched);
        self.known_committed = self
            .known_committed
            .max(request.commit_offset.min(self.matched_end));
        self.advance_commit();
        let accepted = self.accepted();
        self.send(from, accepted);
        Ok(())
    }

    fn on_append_response(
        &mut self,
        from: NodeId,
        response: AppendResponse,
        now: Duration,
    ) -> Result<(), RaftError> {
        match response.outcome {
            AppendOutcome::Accepted {
                matched_end,
                flushed_end,
            } if flushed_end < 0 || flushed_end > matched_end => {
                return Err(RaftError::InvalidMessage(
                    "flushed end outside the matched log",
                ));
            }
            AppendOutcome::Mismatch { epoch, end_offset } if epoch < 0 || end_offset < 0 => {
                return Err(RaftError::InvalidMessage("negative epoch or offset"));
            }
            _ => {}
        }
        if response.term > self.term {
            self.become_follower(response.term, None, now);
            return Ok(());
        }
        let RoleState::Leader(leader) = &mut self.role else {
            return Ok(());
        };
        let Some(index) = leader.followers.iter().position(|p| p.id == from) else {
            return Ok(());
        };
        if response.term < self.term {
            return Ok(());
        }
        let progress = &mut leader.followers[index];
        match response.outcome {
            AppendOutcome::Accepted {
                matched_end,
                flushed_end,
            } => {
                if matched_end > self.log.end {
                    return Err(RaftError::InvalidMessage(
                        "acknowledged past the leader's log end",
                    ));
                }
                progress.flushed = progress.flushed.max(flushed_end);
                // Only the answer to the outstanding append frees its slot, so duplicated and
                // stale responses never multiply the requests in flight.
                match progress.in_flight {
                    Some((_, start)) if matched_end > start => {
                        progress.next = matched_end;
                        progress.in_flight = None;
                    }
                    Some(_) => {}
                    None => progress.next = matched_end,
                }
            }
            AppendOutcome::Mismatch { epoch, end_offset } => {
                progress.next = self.log.next_after_mismatch(epoch, end_offset);
                progress.in_flight = None;
            }
            AppendOutcome::StaleTerm => {}
        }
        progress.last_heard = now;
        self.advance_leader_commit();
        self.replicate_to(index, now, false);
        Ok(())
    }

    fn start_pre_vote(&mut self, now: Duration) {
        self.reset_election_deadline(now);
        self.role = RoleState::PreCandidate(vec![self.id]);
        self.leader = None;
        self.announce();
        if self.quorum == 1 {
            self.start_election(now);
            return;
        }
        self.broadcast(&Message::VoteRequest(VoteRequest {
            term: self.term + 1,
            pre_vote: true,
            last_epoch: self.log.epochs.last_epoch(),
            log_end: self.log.end,
        }));
    }

    fn start_election(&mut self, now: Duration) {
        self.term += 1;
        self.voted_for = Some(self.id);
        self.matched_end = self.known_committed;
        self.persist_hard_state();
        self.role = RoleState::Candidate(vec![self.id]);
        self.leader = None;
        self.announce();
        self.reset_election_deadline(now);
        if self.quorum == 1 {
            self.become_leader(now);
            return;
        }
        self.broadcast(&Message::VoteRequest(VoteRequest {
            term: self.term,
            pre_vote: false,
            last_epoch: self.log.epochs.last_epoch(),
            log_end: self.log.end,
        }));
    }

    fn become_leader(&mut self, now: Duration) {
        let offset = self.log.end;
        self.log.append(offset, 1, self.term);
        self.actions.push(Action::AppendNoop {
            offset,
            epoch: self.term,
        });
        let followers = self
            .peers
            .iter()
            .map(|&id| Progress {
                id,
                next: offset,
                flushed: 0,
                in_flight: None,
                last_sent: now,
                last_heard: now,
            })
            .collect();
        self.role = RoleState::Leader(LeaderState { followers });
        self.leader = Some(self.id);
        self.announce();
        for index in 0..self.peers.len() {
            self.replicate_to(index, now, false);
        }
        self.advance_leader_commit();
    }

    /// Follow `leader` in `term`, adopting the term if it is newer.
    fn become_follower(&mut self, term: i32, leader: Option<NodeId>, now: Duration) {
        if term > self.term {
            self.term = term;
            self.voted_for = None;
            self.matched_end = self.known_committed;
            self.persist_hard_state();
        }
        if matches!(self.role, RoleState::Leader(_)) {
            self.reset_election_deadline(now);
        }
        self.role = RoleState::Follower;
        self.leader = leader;
        self.announce();
    }

    /// Whether a leader has gone a minimum election timeout without hearing from a majority.
    fn quorum_lost(&self, now: Duration) -> bool {
        let RoleState::Leader(leader) = &self.role else {
            return false;
        };
        let heard = leader
            .followers
            .iter()
            .filter(|p| now < p.last_heard + self.election_timeout_min)
            .count();
        1 + heard < self.quorum
    }

    /// Send follower `index` its next batches if it can take some; with `heartbeat`, send an empty
    /// append instead when nothing else went to it for a heartbeat interval.
    fn replicate_to(&mut self, index: usize, now: Duration, heartbeat: bool) {
        let RoleState::Leader(leader) = &mut self.role else {
            return;
        };
        let progress = &mut leader.followers[index];
        if progress
            .in_flight
            .is_some_and(|(sent, _)| now >= sent + self.heartbeat_interval)
        {
            progress.in_flight = None;
        }
        let has_data = progress.in_flight.is_none() && progress.next < self.log.end;
        let heartbeat_due = heartbeat && now >= progress.last_sent + self.heartbeat_interval;
        if !(has_data || heartbeat_due) {
            return;
        }
        let request = AppendRequest {
            term: self.term,
            prev_offset: progress.next,
            prev_epoch: self.log.epochs.epoch_ending_at(progress.next),
            commit_offset: self.known_committed,
            batches: Vec::new(),
        };
        progress.last_sent = now;
        let to = progress.id;
        if has_data {
            progress.in_flight = Some((now, progress.next));
            self.actions.push(Action::Replicate {
                to,
                request,
                end_offset: self.log.end,
                max_bytes: self.max_append_bytes,
            });
        } else {
            self.actions.push(Action::Send {
                to,
                message: Message::AppendRequest(request),
            });
        }
    }

    fn advance_leader_commit(&mut self) {
        let RoleState::Leader(leader) = &self.role else {
            return;
        };
        let mut flushed: Vec<i64> = leader.followers.iter().map(|p| p.flushed).collect();
        flushed.push(self.log.flushed);
        flushed.sort_unstable_by(|a, b| b.cmp(a));
        let majority = flushed[self.quorum - 1];
        if majority > self.known_committed && self.log.epochs.epoch_ending_at(majority) == self.term
        {
            self.known_committed = majority;
        }
        self.advance_commit();
    }

    fn advance_commit(&mut self) {
        let commit = self.known_committed.min(self.log.flushed);
        if commit > self.commit {
            self.commit = commit;
            self.actions.push(Action::Commit { offset: commit });
        }
    }

    fn truncate(&mut self, offset: i64) {
        assert!(
            offset >= self.known_committed,
            "node {} asked to truncate at {offset}, below its committed offset {}",
            self.id,
            self.known_committed,
        );
        self.log.truncate(offset);
        self.matched_end = self.matched_end.min(offset);
        self.actions.push(Action::Truncate { offset });
    }

    fn ackable(&self) -> i64 {
        self.log.flushed.min(self.matched_end)
    }

    fn accepted(&self) -> Message {
        Message::AppendResponse(AppendResponse {
            term: self.term,
            outcome: AppendOutcome::Accepted {
                matched_end: self.matched_end,
                flushed_end: self.ackable(),
            },
        })
    }

    fn leader_alive(&self, now: Duration) -> bool {
        match self.role {
            RoleState::Leader(_) => true,
            _ => self.leader.is_some() && now < self.leader_contact + self.election_timeout_min,
        }
    }

    fn reset_election_deadline(&mut self, now: Duration) {
        let span = self
            .election_timeout_max
            .saturating_sub(self.election_timeout_min);
        let span_nanos = u64::try_from(span.as_nanos()).unwrap_or(u64::MAX).max(1);
        let jitter = Duration::from_nanos(self.rng.next() % span_nanos);
        self.election_deadline = now + self.election_timeout_min + jitter;
    }

    fn persist_hard_state(&mut self) {
        self.actions.push(Action::PersistHardState(HardState {
            term: self.term,
            voted_for: self.voted_for,
        }));
    }

    fn announce(&mut self) {
        if self.announced != (self.term, self.leader) {
            self.announced = (self.term, self.leader);
            self.actions.push(Action::LeaderChanged {
                term: self.term,
                leader: self.leader,
            });
        }
    }

    fn send(&mut self, to: NodeId, message: Message) {
        self.actions.push(Action::Send { to, message });
    }

    fn broadcast(&mut self, message: &Message) {
        for &to in &self.peers {
            self.actions.push(Action::Send {
                to,
                message: message.clone(),
            });
        }
    }
}

fn record_vote(granted: &mut Vec<NodeId>, from: NodeId, quorum: usize) -> bool {
    if !granted.contains(&from) {
        granted.push(from);
    }
    granted.len() >= quorum
}

fn validate_config(config: &Config) -> Result<(), RaftError> {
    let mut voters = config.voters.clone();
    voters.sort_unstable();
    voters.dedup();
    if voters.len() != config.voters.len() {
        return Err(RaftError::InvalidConfig("duplicate voter"));
    }
    if !voters.contains(&config.id) {
        return Err(RaftError::InvalidConfig("this node is not a voter"));
    }
    if config.heartbeat_interval.is_zero()
        || config.heartbeat_interval >= config.election_timeout_min
        || config.election_timeout_min >= config.election_timeout_max
    {
        return Err(RaftError::InvalidConfig(
            "need 0 < heartbeat interval < minimum election timeout < maximum",
        ));
    }
    if config.max_append_bytes == 0 {
        return Err(RaftError::InvalidConfig("max append bytes is zero"));
    }
    Ok(())
}

fn validate_append_request(request: &AppendRequest) -> Result<(), RaftError> {
    if request.prev_offset < 0
        || request.commit_offset < 0
        || !(0..=request.term).contains(&request.prev_epoch)
    {
        return Err(RaftError::InvalidMessage(
            "negative offset, or previous epoch outside the term",
        ));
    }
    let (mut end, mut epoch) = (request.prev_offset, request.prev_epoch);
    for batch in &request.batches {
        if batch.base_offset != end || batch.record_count < 1 {
            return Err(RaftError::InvalidMessage("batches are not contiguous"));
        }
        if batch.epoch < epoch || batch.epoch > request.term {
            return Err(RaftError::InvalidMessage(
                "batch epochs decrease or exceed the term",
            ));
        }
        end = batch
            .base_offset
            .checked_add(i64::from(batch.record_count))
            .ok_or(RaftError::InvalidMessage("offset overflow"))?;
        epoch = batch.epoch;
    }
    Ok(())
}

/// The core's model of the driver's log: offsets and epochs only.
#[derive(Debug)]
struct LogState {
    epochs: EpochHistory,
    end: i64,
    flushed: i64,
    /// Bumped by every truncation, so flushes started before one are not trusted.
    generation: u64,
}

impl LogState {
    fn restore(durable: DurableState) -> Result<LogState, RaftError> {
        let DurableState {
            epochs: starts,
            log_end,
            ..
        } = durable;
        if log_end < 0 {
            return Err(RaftError::InvalidState("negative log end"));
        }
        if starts.is_empty() != (log_end == 0) {
            return Err(RaftError::InvalidState(
                "a log has an epoch history exactly when it is not empty",
            ));
        }
        let ordered = starts.windows(2).all(|pair| {
            pair[0].epoch < pair[1].epoch && pair[0].start_offset < pair[1].start_offset
        });
        let bounded = starts
            .first()
            .is_none_or(|first| first.start_offset == 0 && first.epoch >= 0)
            && starts.last().is_none_or(|last| last.start_offset < log_end);
        if !ordered || !bounded {
            return Err(RaftError::InvalidState(
                "epochs must rise from offset 0 in log order",
            ));
        }
        Ok(LogState {
            epochs: EpochHistory { starts },
            end: log_end,
            flushed: log_end,
            generation: 0,
        })
    }

    fn append(&mut self, base_offset: i64, record_count: i32, epoch: i32) {
        debug_assert_eq!(base_offset, self.end, "append must extend the log");
        self.epochs.observe(epoch, base_offset);
        self.end = base_offset + i64::from(record_count);
    }

    fn truncate(&mut self, offset: i64) {
        self.epochs.truncate(offset);
        self.end = offset;
        self.flushed = self.flushed.min(offset);
        self.generation += 1;
    }

    fn flushed_epoch(&self) -> i32 {
        self.epochs.epoch_ending_at(self.flushed)
    }

    /// Where to resume a follower that answered `Mismatch { epoch, end_offset }`. Always a batch
    /// boundary of this log, and below the rejected previous offset.
    fn next_after_mismatch(&self, epoch: i32, end_offset: i64) -> i64 {
        match self.epochs.end_of(epoch, self.end) {
            None => 0,
            Some(found) if found.epoch == epoch => found.end_offset.min(end_offset),
            Some(found) => found.end_offset,
        }
    }
}

#[derive(Debug)]
struct EpochHistory {
    /// Strictly increasing in both epoch and start offset.
    starts: Vec<EpochStart>,
}

impl EpochHistory {
    fn last_epoch(&self) -> i32 {
        self.starts.last().map_or(0, |last| last.epoch)
    }

    /// Epoch of the batch holding `offset - 1`; 0 when there is none.
    fn epoch_ending_at(&self, offset: i64) -> i32 {
        let after = self.starts.partition_point(|e| e.start_offset < offset);
        after.checked_sub(1).map_or(0, |at| self.starts[at].epoch)
    }

    fn end_of(&self, epoch: i32, log_end: i64) -> Option<EpochEnd> {
        let after = self.starts.partition_point(|e| e.epoch <= epoch);
        let found = self.starts[..after].last()?;
        Some(EpochEnd {
            epoch: found.epoch,
            end_offset: self.starts.get(after).map_or(log_end, |e| e.start_offset),
        })
    }

    fn observe(&mut self, epoch: i32, base_offset: i64) {
        match self.starts.last() {
            Some(last) if last.epoch >= epoch => {
                debug_assert_eq!(last.epoch, epoch, "epochs must not decrease along the log");
            }
            _ => self.starts.push(EpochStart {
                epoch,
                start_offset: base_offset,
            }),
        }
    }

    fn truncate(&mut self, offset: i64) {
        let keep = self.starts.partition_point(|e| e.start_offset < offset);
        self.starts.truncate(keep);
    }
}

const GOLDEN_GAMMA: u64 = 0x9E37_79B9_7F4A_7C15;

/// `SplitMix64`: a tiny deterministic generator for election jitter.
#[derive(Debug, Clone)]
struct SplitMix64(u64);

impl SplitMix64 {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(GOLDEN_GAMMA);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, HashMap, VecDeque};
    use std::panic::{self, AssertUnwindSafe};
    use std::time::Instant;

    use super::*;

    const BATCH_BYTES: usize = 100;
    const UNLIMITED_BYTES: usize = 1 << 30;

    fn ms(millis: u64) -> Duration {
        Duration::from_millis(millis)
    }

    fn micros(duration: Duration) -> u64 {
        u64::try_from(duration.as_micros()).unwrap()
    }

    fn mix(value: u64) -> u64 {
        SplitMix64(value).next()
    }

    fn test_config(id: NodeId, size: usize, seed: u64, max_append_bytes: usize) -> Config {
        Config {
            id,
            voters: (1..=i32::try_from(size).unwrap()).collect(),
            election_timeout_min: ms(150),
            election_timeout_max: ms(300),
            heartbeat_interval: ms(50),
            max_append_bytes,
            seed,
        }
    }

    struct Rng(SplitMix64);

    impl Rng {
        fn new(seed: u64) -> Rng {
            Rng(SplitMix64(mix(seed)))
        }

        fn below(&mut self, bound: u64) -> u64 {
            self.0.next() % bound.max(1)
        }

        fn between(&mut self, low: u64, high: u64) -> u64 {
            low + self.below(high - low)
        }

        fn chance(&mut self, per_mille: u64) -> bool {
            self.below(1000) < per_mille
        }

        fn pick<T: Copy>(&mut self, items: &[T]) -> T {
            let at = self.below(u64::try_from(items.len()).unwrap());
            items[usize::try_from(at).unwrap()]
        }
    }

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    struct SimBatch {
        base: i64,
        count: i32,
        epoch: i32,
        /// Unique per batch ever created, standing in for its bytes.
        payload: u64,
    }

    impl SimBatch {
        fn end(&self) -> i64 {
            self.base + i64::from(self.count)
        }

        fn info(&self) -> BatchInfo {
            BatchInfo {
                base_offset: self.base,
                record_count: self.count,
                epoch: self.epoch,
            }
        }

        fn bytes(&self) -> usize {
            BATCH_BYTES * usize::try_from(self.count).unwrap()
        }
    }

    /// A log as the driver stores it, with a hash of every prefix so prefixes compare in O(1).
    #[derive(Debug, Clone, Default)]
    struct SimLog {
        batches: Vec<SimBatch>,
        hashes: Vec<u64>,
    }

    impl SimLog {
        fn len(&self) -> usize {
            self.batches.len()
        }

        fn end(&self) -> i64 {
            self.batches.last().map_or(0, SimBatch::end)
        }

        fn push(&mut self, batch: SimBatch) -> u64 {
            let previous = self.hashes.last().copied().unwrap_or(0);
            let shape = u64::from(batch.count.cast_unsigned()) << 32
                | u64::from(batch.epoch.cast_unsigned());
            let hash = mix(previous
                ^ mix(batch.base.cast_unsigned())
                ^ mix(shape).rotate_left(21)
                ^ mix(batch.payload).rotate_left(42));
            self.batches.push(batch);
            self.hashes.push(hash);
            hash
        }

        fn truncate_len(&mut self, len: usize) {
            self.batches.truncate(len);
            self.hashes.truncate(len);
        }

        /// Number of batches ending at or before `offset`, if `offset` is a batch boundary.
        fn boundary(&self, offset: i64) -> Option<usize> {
            let len = self.batches.partition_point(|b| b.end() <= offset);
            let at = len
                .checked_sub(1)
                .map_or(0, |last| self.batches[last].end());
            (at == offset).then_some(len)
        }

        fn same_prefix(&self, other: &SimLog, len: usize) -> bool {
            len == 0
                || (self.len() >= len
                    && other.len() >= len
                    && self.hashes[len - 1] == other.hashes[len - 1])
        }

        fn common_prefix(&self, other: &SimLog) -> usize {
            let (mut low, mut high) = (0, self.len().min(other.len()));
            while low < high {
                let mid = (low + high).div_ceil(2);
                if self.same_prefix(other, mid) {
                    low = mid;
                } else {
                    high = mid - 1;
                }
            }
            low
        }

        fn contains(&self, batch: &SimBatch) -> bool {
            let at = self.batches.partition_point(|b| b.base < batch.base);
            self.batches.get(at) == Some(batch)
        }

        /// Whole batches from `from` below `end`, within `max_bytes` but at least one.
        fn read(&self, from: i64, end: i64, max_bytes: usize) -> Vec<SimBatch> {
            let start = self
                .boundary(from)
                .unwrap_or_else(|| panic!("replication from {from}, not a batch boundary"));
            let mut out = Vec::new();
            let mut bytes = 0;
            for batch in &self.batches[start..] {
                if batch.base >= end || (!out.is_empty() && bytes + batch.bytes() > max_bytes) {
                    break;
                }
                bytes += batch.bytes();
                out.push(*batch);
            }
            out
        }

        fn epochs(&self) -> Vec<EpochStart> {
            let mut epochs: Vec<EpochStart> = Vec::new();
            for batch in &self.batches {
                if epochs.last().is_none_or(|last| last.epoch < batch.epoch) {
                    epochs.push(EpochStart {
                        epoch: batch.epoch,
                        start_offset: batch.base,
                    });
                }
            }
            epochs
        }
    }

    #[derive(Debug, Clone)]
    struct Packet {
        from: NodeId,
        to: NodeId,
        /// Packets addressed to an earlier incarnation are lost with it.
        to_incarnation: u32,
        message: Message,
        payloads: Vec<SimBatch>,
    }

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    enum ProposalState {
        Pending,
        Acked,
        Failed,
    }

    #[derive(Debug)]
    struct ProposalRecord {
        batch: SimBatch,
        state: ProposalState,
    }

    #[derive(Debug)]
    struct PendingFlush {
        ticket: u64,
        point: FlushPoint,
        /// The flush leaves the durable log as its first `keep` batches followed by `tail`.
        keep: usize,
        tail: Vec<SimBatch>,
    }

    /// One machine: a process that may be down, and a disk that survives crashes.
    #[derive(Debug)]
    struct Host {
        node: Option<Node>,
        incarnation: u32,
        hard_state: HardState,
        log: SimLog,
        durable: SimLog,
        flush: Option<PendingFlush>,
        clock_rate_per_mille: u64,
        clock_offset: u64,
        last_commit: i64,
        leader_term: Option<i32>,
        pending: Vec<usize>,
        durable_committed: usize,
    }

    /// The simulated drivers of every node, checking safety invariants after every step.
    struct World {
        size: usize,
        seed: u64,
        max_append_bytes: usize,
        /// Global time in microseconds; each host's clock runs at its own rate.
        now: u64,
        hosts: Vec<Host>,
        outbox: Vec<Packet>,
        next_payload: u64,
        next_ticket: u64,
        proposals: Vec<ProposalRecord>,
        acked: Vec<usize>,
        leaders: HashMap<i32, NodeId>,
        newest_leader: Option<(i32, NodeId)>,
        /// The longest prefix any node ever reported committed.
        committed: SimLog,
        /// Term -> longest committed prefix reported by a node in that term. The committing leader's
        /// term is at most the reporter's, and Raft puts the prefix in every leader of a higher term.
        committed_by_term: BTreeMap<i32, usize>,
        /// Every batch ever appended anywhere: epoch -> base -> (count, payload, prefix hash).
        registry: HashMap<i32, BTreeMap<i64, (i32, u64, u64)>>,
        trace: bool,
    }

    impl World {
        fn new(size: usize, seed: u64, max_append_bytes: usize, clocks: &[(u64, u64)]) -> World {
            let hosts = clocks
                .iter()
                .map(|&(clock_rate_per_mille, clock_offset)| Host {
                    node: None,
                    incarnation: 0,
                    hard_state: HardState::default(),
                    log: SimLog::default(),
                    durable: SimLog::default(),
                    flush: None,
                    clock_rate_per_mille,
                    clock_offset,
                    last_commit: 0,
                    leader_term: None,
                    pending: Vec::new(),
                    durable_committed: 0,
                })
                .collect();
            let mut world = World {
                size,
                seed,
                max_append_bytes,
                now: 0,
                hosts,
                outbox: Vec::new(),
                next_payload: 0,
                next_ticket: 0,
                proposals: Vec::new(),
                acked: Vec::new(),
                leaders: HashMap::new(),
                newest_leader: None,
                committed: SimLog::default(),
                committed_by_term: BTreeMap::new(),
                registry: HashMap::new(),
                trace: false,
            };
            for id in world.ids() {
                world.restart(id);
            }
            world
        }

        fn ids(&self) -> Vec<NodeId> {
            (1..=i32::try_from(self.size).unwrap()).collect()
        }

        fn host(&self, id: NodeId) -> &Host {
            &self.hosts[usize::try_from(id - 1).unwrap()]
        }

        fn host_mut(&mut self, id: NodeId) -> &mut Host {
            &mut self.hosts[usize::try_from(id - 1).unwrap()]
        }

        fn local(&self, id: NodeId) -> Duration {
            let host = self.host(id);
            Duration::from_micros(host.clock_offset + self.now * host.clock_rate_per_mille / 1000)
        }

        fn status(&self, id: NodeId) -> Option<Status> {
            self.host(id).node.as_ref().map(Node::status)
        }

        /// The live leader with the highest term.
        fn leader(&self) -> Option<NodeId> {
            self.ids()
                .into_iter()
                .filter_map(|id| self.status(id))
                .filter(|status| status.role == Role::Leader)
                .max_by_key(|status| status.term)
                .map(|status| status.id)
        }

        fn new_payload(&mut self) -> u64 {
            self.next_payload += 1;
            self.next_payload
        }

        fn restart(&mut self, id: NodeId) {
            let now = self.local(id);
            let config = test_config(id, self.size, self.seed, self.max_append_bytes);
            let host = self.host_mut(id);
            let durable = DurableState {
                hard_state: host.hard_state,
                epochs: host.durable.epochs(),
                log_end: host.durable.end(),
            };
            let node = Node::new(config, durable, now)
                .unwrap_or_else(|error| panic!("node {id} failed to restart: {error}"));
            host.node = Some(node);
            host.incarnation += 1;
            host.log = host.durable.clone();
            host.flush = None;
            host.last_commit = 0;
            host.leader_term = None;
            if self.trace {
                eprintln!("{:>9} restart {id}", self.now);
            }
            self.check_node(id);
        }

        fn crash(&mut self, id: NodeId) {
            let host = self.host_mut(id);
            host.node = None;
            host.incarnation += 1;
            host.log = host.durable.clone();
            host.flush = None;
            host.leader_term = None;
            let pending = mem::take(&mut host.pending);
            self.fail_proposals(pending);
            self.check_durable_committed(id);
            if self.trace {
                eprintln!("{:>9} crash {id}", self.now);
            }
        }

        fn tick(&mut self, id: NodeId) {
            let now = self.local(id);
            let Some(node) = self.host_mut(id).node.as_mut() else {
                return;
            };
            let actions = node.tick(now);
            self.apply(id, actions, &[]);
        }

        fn deliver(&mut self, packet: Packet) {
            let now = self.local(packet.to);
            let (from, to) = (packet.from, packet.to);
            if self.host(to).incarnation != packet.to_incarnation || self.host(to).node.is_none() {
                return;
            }
            if self.trace {
                eprintln!("{:>9} {from} -> {to} {:?}", self.now, packet.message);
            }
            let node = self.host_mut(to).node.as_mut().unwrap();
            let actions = node
                .step(from, packet.message, now)
                .unwrap_or_else(|error| {
                    panic!("node {to} rejected a message from {from}: {error}")
                });
            self.apply(to, actions, &packet.payloads);
        }

        fn propose(&mut self, id: NodeId, count: i32) -> Option<SimBatch> {
            let payload = self.new_payload();
            let proposal = self.host_mut(id).node.as_mut()?.propose(count).ok()?;
            let batch = SimBatch {
                base: proposal.base_offset,
                count,
                epoch: proposal.epoch,
                payload,
            };
            self.append(id, batch);
            let index = self.proposals.len();
            self.host_mut(id).pending.push(index);
            self.proposals.push(ProposalRecord {
                batch,
                state: ProposalState::Pending,
            });
            self.check_node(id);
            Some(batch)
        }

        /// Start an fsync of everything the log holds that the disk does not, returning its ticket.
        fn start_flush(&mut self, id: NodeId) -> Option<u64> {
            let ticket = self.next_ticket + 1;
            let host = self.host_mut(id);
            let node = host.node.as_ref()?;
            let keep = host.durable.common_prefix(&host.log);
            if host.flush.is_some() || (keep == host.log.len() && keep == host.durable.len()) {
                return None;
            }
            let point = node.flush_point();
            assert_eq!(
                point.end_offset(),
                host.log.end(),
                "node {id}: flush point and log end disagree"
            );
            host.flush = Some(PendingFlush {
                ticket,
                point,
                keep,
                tail: host.log.batches[keep..].to_vec(),
            });
            self.next_ticket = ticket;
            Some(ticket)
        }

        fn complete_flush(&mut self, id: NodeId, ticket: u64) {
            let host = self.host_mut(id);
            if host
                .flush
                .as_ref()
                .is_none_or(|flush| flush.ticket != ticket)
            {
                return;
            }
            let flush = host.flush.take().unwrap();
            host.durable.truncate_len(flush.keep);
            for batch in flush.tail {
                host.durable.push(batch);
            }
            let actions = host.node.as_mut().unwrap().flushed(flush.point);
            self.apply(id, actions, &[]);
        }

        fn apply(&mut self, id: NodeId, actions: Vec<Action>, received: &[SimBatch]) {
            for action in actions {
                if self.trace {
                    eprintln!("{:>9}   {id}: {action:?}", self.now);
                }
                match action {
                    Action::PersistHardState(state) => self.persist(id, state),
                    Action::Truncate { offset } => self.truncate(id, offset),
                    Action::AppendReceived { from_offset } => {
                        let start = received
                            .iter()
                            .position(|batch| batch.base == from_offset)
                            .unwrap_or_else(|| {
                                panic!("node {id}: no received batch at {from_offset}")
                            });
                        for batch in &received[start..] {
                            self.append(id, *batch);
                        }
                    }
                    Action::AppendNoop { offset, epoch } => {
                        let payload = self.new_payload();
                        self.append(
                            id,
                            SimBatch {
                                base: offset,
                                count: 1,
                                epoch,
                                payload,
                            },
                        );
                    }
                    Action::Send { to, message } => self.send(id, to, message, Vec::new()),
                    Action::Replicate {
                        to,
                        mut request,
                        end_offset,
                        max_bytes,
                    } => {
                        let payloads =
                            self.host(id)
                                .log
                                .read(request.prev_offset, end_offset, max_bytes);
                        request.batches = payloads.iter().map(SimBatch::info).collect();
                        self.send(id, to, Message::AppendRequest(request), payloads);
                    }
                    Action::Commit { offset } => self.commit(id, offset),
                    Action::LeaderChanged { term, leader } => self.leader_changed(id, term, leader),
                }
            }
            self.check_node(id);
        }

        fn send(&mut self, from: NodeId, to: NodeId, message: Message, payloads: Vec<SimBatch>) {
            let to_incarnation = self.host(to).incarnation;
            self.outbox.push(Packet {
                from,
                to,
                to_incarnation,
                message,
                payloads,
            });
        }

        fn persist(&mut self, id: NodeId, state: HardState) {
            let host = self.host_mut(id);
            let old = host.hard_state;
            assert!(
                state.term >= old.term,
                "node {id}: term went back from {old:?} to {state:?}"
            );
            assert!(
                state.term > old.term
                    || old.voted_for.is_none()
                    || old.voted_for == state.voted_for,
                "node {id} changed its vote within term {}: {old:?} to {state:?}",
                old.term
            );
            host.hard_state = state;
        }

        fn truncate(&mut self, id: NodeId, offset: i64) {
            let matched = self.host(id).log.common_prefix(&self.committed);
            if let Some(last) = matched.checked_sub(1) {
                let committed_end = self.committed.batches[last].end();
                assert!(
                    offset >= committed_end,
                    "node {id} truncated at {offset}, below committed data it holds up to {committed_end}"
                );
            }
            let host = self.host_mut(id);
            let keep = host
                .log
                .boundary(offset)
                .unwrap_or_else(|| panic!("node {id} truncated at {offset}, not a batch boundary"));
            host.log.truncate_len(keep);
        }

        fn append(&mut self, id: NodeId, batch: SimBatch) {
            let host = self.host_mut(id);
            assert_eq!(
                batch.base,
                host.log.end(),
                "node {id} appended {batch:?} away from its log end"
            );
            let hash = host.log.push(batch);
            let epoch = self.registry.entry(batch.epoch).or_default();
            if let Some(&known) = epoch.get(&batch.base) {
                assert_eq!(
                    known,
                    (batch.count, batch.payload, hash),
                    "log matching violated: node {id} holds {batch:?} with another batch or prefix than a peer"
                );
                return;
            }
            if let Some((&base, &(count, ..))) = epoch.range(..batch.base).next_back() {
                assert!(
                    base + i64::from(count) <= batch.base,
                    "node {id}: {batch:?} overlaps another batch of its epoch at {base}"
                );
            }
            if let Some((&base, _)) = epoch.range(batch.base..).next() {
                assert!(
                    base >= batch.end(),
                    "node {id}: {batch:?} overlaps another batch of its epoch at {base}"
                );
            }
            epoch.insert(batch.base, (batch.count, batch.payload, hash));
        }

        fn commit(&mut self, id: NodeId, offset: i64) {
            let host = self.host_mut(id);
            assert!(
                offset > host.last_commit,
                "node {id}: commit moved from {} to {offset}",
                host.last_commit
            );
            host.last_commit = offset;
            let host = self.host(id);
            let len = host
                .log
                .boundary(offset)
                .unwrap_or_else(|| panic!("node {id} committed {offset}, not a batch boundary"));
            assert!(
                host.log
                    .same_prefix(&self.committed, len.min(self.committed.len())),
                "node {id} committed up to {offset} a log that diverges from the committed prefix"
            );
            if len > self.committed.len() {
                let extension = host.log.batches[self.committed.len()..len].to_vec();
                for batch in extension {
                    self.committed.push(batch);
                }
            }
            let term = self.status(id).unwrap().term;
            let known = self.committed_by_term.entry(term).or_default();
            *known = (*known).max(len);
            self.check_committed_everywhere();
            self.acknowledge(id, offset);
        }

        fn acknowledge(&mut self, id: NodeId, offset: i64) {
            let Some(term) = self.host(id).leader_term else {
                return;
            };
            let pending = mem::take(&mut self.host_mut(id).pending);
            let mut waiting = Vec::new();
            for index in pending {
                let batch = self.proposals[index].batch;
                if batch.epoch == term && batch.end() <= offset {
                    assert!(
                        self.committed.contains(&batch),
                        "node {id} acknowledged {batch:?}, which is not in the committed prefix"
                    );
                    self.proposals[index].state = ProposalState::Acked;
                    self.acked.push(index);
                } else {
                    waiting.push(index);
                }
            }
            self.host_mut(id).pending = waiting;
        }

        fn leader_changed(&mut self, id: NodeId, term: i32, leader: Option<NodeId>) {
            let host = self.host_mut(id);
            if leader == Some(id) {
                host.leader_term = Some(term);
                return;
            }
            host.leader_term = None;
            let pending = mem::take(&mut host.pending);
            self.fail_proposals(pending);
        }

        fn fail_proposals(&mut self, pending: Vec<usize>) {
            for index in pending {
                self.proposals[index].state = ProposalState::Failed;
            }
        }

        /// Length of the committed prefix every leader of `term` must hold.
        fn committed_before(&self, term: i32) -> usize {
            self.committed_by_term
                .range(..term)
                .map(|(_, &len)| len)
                .max()
                .unwrap_or(0)
        }

        fn check_committed_everywhere(&mut self) {
            for id in self.ids() {
                if let Some(status) = self.status(id)
                    && status.role == Role::Leader
                {
                    let required = self.committed_before(status.term);
                    assert!(
                        self.host(id).log.common_prefix(&self.committed) >= required,
                        "leader {id} of term {} lacks data committed in an earlier term",
                        status.term
                    );
                }
                self.check_durable_committed(id);
            }
        }

        /// Committed batches a node counts as flushed (all of its disk while it is down) must stay
        /// on its disk. Bytes beyond its flushed end may be a stale copy that a flush started
        /// before a truncation is still allowed to write.
        fn check_durable_committed(&mut self, id: NodeId) {
            let host = self.host(id);
            let counted = host.node.as_ref().map_or(host.durable.len(), |node| {
                let flushed_end = node.status().flushed_end;
                host.durable
                    .batches
                    .partition_point(|b| b.end() <= flushed_end)
            });
            let matched = host.durable.common_prefix(&self.committed).min(counted);
            let host = self.host_mut(id);
            assert!(
                matched >= host.durable_committed,
                "node {id} lost or changed committed data it counted as flushed: {} -> {matched} batches",
                host.durable_committed
            );
            host.durable_committed = matched;
        }

        fn check_node(&mut self, id: NodeId) {
            let Some(status) = self.status(id) else {
                return;
            };
            let host = self.host(id);
            assert_eq!(
                status.log_end,
                host.log.end(),
                "node {id}: core and driver disagree on the log end"
            );
            assert!(
                status.commit_offset <= status.flushed_end,
                "node {id}: commit {} beyond flushed end {}",
                status.commit_offset,
                status.flushed_end
            );
            assert_eq!(
                status.term, host.hard_state.term,
                "node {id}: its term is not durable"
            );
            let flushed = host
                .log
                .boundary(status.flushed_end)
                .unwrap_or_else(|| panic!("node {id}: flushed end is not a batch boundary"));
            assert!(
                host.log.same_prefix(&host.durable, flushed),
                "node {id} believes {} is flushed but its disk differs",
                status.flushed_end
            );
            self.check_durable_committed(id);
            if status.role == Role::Leader {
                if let Some(&other) = self.leaders.get(&status.term) {
                    assert_eq!(other, id, "two leaders in term {}", status.term);
                } else {
                    self.leaders.insert(status.term, id);
                    self.newest_leader = Some((status.term, id));
                    self.check_new_leader(id, status.term);
                }
            }
        }

        fn check_new_leader(&self, id: NodeId, term: i32) {
            let log = &self.host(id).log;
            assert!(
                log.common_prefix(&self.committed) >= self.committed_before(term),
                "new leader {id} of term {term} lacks data committed in an earlier term"
            );
            for &index in &self.acked {
                let batch = self.proposals[index].batch;
                assert!(
                    batch.epoch >= term || log.contains(&batch),
                    "new leader {id} of term {term} lacks acknowledged {batch:?}"
                );
            }
        }
    }

    /// After the network heals, a proposal must commit on every node within this many microseconds.
    const LIVENESS_BOUND: u64 = 5_000_000;
    /// Once that happens, proposals stop and every one must be resolved this much later.
    const SETTLE: u64 = 1_000_000;
    /// Producer backpressure: a leader with this many unresolved proposals gets no more.
    const MAX_PENDING_PROPOSALS: usize = 40;

    #[derive(Debug, Clone, Copy)]
    struct Plan {
        nodes: usize,
        chaos_ms: u64,
        max_append_bytes: usize,
        propose_gap_ms: u64,
        churn: u64,
    }

    impl Plan {
        fn for_seed(seed: u64, nodes: usize, soak: bool) -> Plan {
            let mut rng = Rng::new(seed ^ 0x5EED);
            let (shortest, longest) = if soak {
                (2_000, 12_000)
            } else {
                (1_000, 3_000)
            };
            Plan {
                nodes,
                chaos_ms: rng.between(shortest, longest),
                max_append_bytes: rng.pick(&[
                    BATCH_BYTES,
                    3 * BATCH_BYTES,
                    10 * BATCH_BYTES,
                    UNLIMITED_BYTES,
                ]),
                propose_gap_ms: rng.pick(&[3, 10, 40]),
                churn: rng.pick(&[0, 300, 600]),
            }
        }
    }

    enum Event {
        Deliver(Packet),
        Tick(NodeId),
        FlushDone(NodeId, u64),
        Propose,
        Chaos,
        Restart(NodeId),
        Crash(NodeId),
        Heal,
    }

    #[derive(Debug, Default, Clone, Copy)]
    struct Stats {
        schedules: u64,
        steps: u64,
        simulated_ms: u64,
        proposals: u64,
        acked: u64,
        leaders: u64,
    }

    impl Stats {
        fn add(&mut self, other: Stats) {
            self.schedules += other.schedules;
            self.steps += other.steps;
            self.simulated_ms += other.simulated_ms;
            self.proposals += other.proposals;
            self.acked += other.acked;
            self.leaders += other.leaders;
        }
    }

    fn index(id: NodeId) -> usize {
        usize::try_from(id).unwrap()
    }

    /// A seeded schedule of faults driving a [`World`]: a chaos phase, then a healed phase in
    /// which progress must resume.
    struct Sim {
        world: World,
        plan: Plan,
        rng: Rng,
        queue: BTreeMap<(u64, u64), Event>,
        seq: u64,
        steps: u64,
        /// `blocked[from][to]` drops packets at delivery time.
        blocked: Vec<Vec<bool>>,
        loss: u64,
        duplicate: u64,
        delay: (u64, u64),
        straggle: u64,
        stalled_until: Vec<u64>,
        /// Chance per mille that a newly elected leader crashes within 20 ms, before its term-start
        /// no-op reaches a majority: the shape of Raft's Figure 8.
        churn: u64,
        newest_leader: Option<(i32, NodeId)>,
        healed_at: Option<u64>,
        probe: usize,
        progress_at: Option<u64>,
        proposing: bool,
    }

    impl Sim {
        fn new(seed: u64, plan: Plan) -> Sim {
            let mut rng = Rng::new(seed);
            let clocks: Vec<(u64, u64)> = (0..plan.nodes)
                .map(|_| (rng.between(970, 1031), rng.below(1_000_000)))
                .collect();
            let mut world = World::new(plan.nodes, seed, plan.max_append_bytes, &clocks);
            world.trace = std::env::var_os("RAFT_TRACE").is_some();
            Sim {
                world,
                plan,
                rng,
                queue: BTreeMap::new(),
                seq: 0,
                steps: 0,
                blocked: vec![vec![false; plan.nodes + 1]; plan.nodes + 1],
                loss: 0,
                duplicate: 0,
                delay: (100, 5_000),
                straggle: 0,
                stalled_until: vec![0; plan.nodes + 1],
                churn: plan.churn,
                newest_leader: None,
                healed_at: None,
                probe: 0,
                progress_at: None,
                proposing: true,
            }
        }

        fn schedule(&mut self, after: u64, event: Event) {
            self.seq += 1;
            self.queue.insert((self.world.now + after, self.seq), event);
        }

        fn run(&mut self) {
            for id in self.world.ids() {
                let at = self.rng.below(10_000);
                self.schedule(at, Event::Tick(id));
            }
            self.schedule(1_000, Event::Propose);
            let first_fault = self.rng.between(20_000, 400_000);
            self.schedule(first_fault, Event::Chaos);
            self.schedule(self.plan.chaos_ms * 1_000, Event::Heal);
            while let Some(((time, _), event)) = self.queue.pop_first() {
                self.world.now = time;
                self.steps += 1;
                self.handle(event);
                self.dispatch();
                self.churn_leader();
                if self.finished() {
                    break;
                }
            }
        }

        fn handle(&mut self, event: Event) {
            match event {
                Event::Tick(id) => {
                    self.world.tick(id);
                    self.flush_soon(id);
                    let next = self.rng.between(1_000, 12_000);
                    self.schedule(next, Event::Tick(id));
                }
                Event::Deliver(packet) => {
                    if self.blocked[index(packet.from)][index(packet.to)] {
                        return;
                    }
                    let to = packet.to;
                    self.world.deliver(packet);
                    self.flush_soon(to);
                }
                Event::FlushDone(id, ticket) => {
                    self.world.complete_flush(id, ticket);
                    self.flush_soon(id);
                }
                Event::Propose => self.propose(),
                Event::Chaos => {
                    if self.healed_at.is_none() {
                        self.chaos();
                        let next = self.rng.between(20_000, 250_000);
                        self.schedule(next, Event::Chaos);
                    }
                }
                Event::Restart(id) => {
                    if self.world.host(id).node.is_none() {
                        self.world.restart(id);
                        self.flush_soon(id);
                    }
                }
                Event::Crash(id) => {
                    if self.healed_at.is_none() {
                        self.crash(id);
                    }
                }
                Event::Heal => self.heal(),
            }
        }

        fn propose(&mut self) {
            if !self.proposing {
                return;
            }
            let leaders: Vec<NodeId> = self
                .world
                .ids()
                .into_iter()
                .filter(|&id| {
                    self.world
                        .status(id)
                        .is_some_and(|s| s.role == Role::Leader)
                })
                .collect();
            let leader = (!leaders.is_empty()).then(|| self.rng.pick(&leaders));
            if let Some(leader) = leader
                && self.world.host(leader).pending.len() < MAX_PENDING_PROPOSALS
            {
                for _ in 0..self.rng.between(1, 4) {
                    let count = i32::try_from(self.rng.between(1, 4)).unwrap();
                    self.world.propose(leader, count);
                }
                if self.rng.chance(500) {
                    self.world.tick(leader);
                }
                self.flush_soon(leader);
            }
            let gap = self.rng.between(500, self.plan.propose_gap_ms * 1_000);
            self.schedule(gap, Event::Propose);
        }

        fn flush_soon(&mut self, id: NodeId) {
            if let Some(ticket) = self.world.start_flush(id) {
                let delay = if self.world.now < self.stalled_until[index(id)] {
                    self.rng.between(50_000, 800_000)
                } else {
                    self.rng.between(50, 3_000)
                };
                self.schedule(delay, Event::FlushDone(id, ticket));
            }
        }

        fn dispatch(&mut self) {
            for packet in mem::take(&mut self.world.outbox) {
                if self.rng.chance(self.loss) {
                    continue;
                }
                if self.rng.chance(self.duplicate) {
                    let delay = self.delay();
                    self.schedule(delay, Event::Deliver(packet.clone()));
                }
                let delay = self.delay();
                self.schedule(delay, Event::Deliver(packet));
            }
        }

        fn delay(&mut self) -> u64 {
            let base = self.rng.between(self.delay.0, self.delay.1);
            if self.rng.chance(self.straggle) {
                base + self.rng.between(20_000, 300_000)
            } else {
                base
            }
        }

        fn chaos(&mut self) {
            let ids = self.world.ids();
            match self.rng.below(12) {
                0 | 1 => self.clear_partitions(),
                2 => {
                    let id = self.rng.pick(&ids);
                    self.isolate(&[id]);
                }
                3 => {
                    let group: Vec<NodeId> = ids
                        .iter()
                        .copied()
                        .filter(|_| self.rng.chance(500))
                        .collect();
                    self.isolate(&group);
                }
                4 => {
                    let (from, to) = (self.rng.pick(&ids), self.rng.pick(&ids));
                    self.blocked[index(from)][index(to)] = from != to;
                }
                5 => {
                    if let Some(leader) = self.world.leader() {
                        let mut group = vec![leader];
                        while group.len() < (ids.len() - 1) / 2 {
                            let id = self.rng.pick(&ids);
                            if !group.contains(&id) {
                                group.push(id);
                            }
                        }
                        self.isolate(&group);
                    }
                }
                6 | 7 => {
                    let id = self.rng.pick(&ids);
                    self.crash(id);
                }
                8 => {
                    if let Some(leader) = self.world.leader() {
                        self.crash(leader);
                    }
                }
                9 => {
                    let id = self.rng.pick(&ids);
                    self.stalled_until[index(id)] =
                        self.world.now + self.rng.between(100_000, 2_000_000);
                }
                10 => {
                    self.loss = self.rng.pick(&[0, 0, 10, 50, 200]);
                    self.duplicate = self.rng.pick(&[0, 10, 50]);
                    self.delay = self
                        .rng
                        .pick(&[(100, 2_000), (1_000, 10_000), (5_000, 40_000)]);
                    self.straggle = self.rng.pick(&[0, 20, 100]);
                }
                _ => {
                    let from = self.rng.pick(&ids);
                    self.blocked[index(from)].fill(false);
                }
            }
        }

        fn churn_leader(&mut self) {
            if self.world.newest_leader == self.newest_leader {
                return;
            }
            self.newest_leader = self.world.newest_leader;
            if let Some((_, leader)) = self.newest_leader
                && self.rng.chance(self.churn)
            {
                let after = self.rng.below(20_000);
                self.schedule(after, Event::Crash(leader));
            }
        }

        fn crash(&mut self, id: NodeId) {
            if self.world.host(id).node.is_some() {
                self.world.crash(id);
                let after = self.rng.between(20_000, 1_500_000);
                self.schedule(after, Event::Restart(id));
            }
        }

        fn isolate(&mut self, group: &[NodeId]) {
            self.clear_partitions();
            for a in self.world.ids() {
                for b in self.world.ids() {
                    self.blocked[index(a)][index(b)] = group.contains(&a) != group.contains(&b);
                }
            }
        }

        fn clear_partitions(&mut self) {
            for row in &mut self.blocked {
                row.fill(false);
            }
        }

        fn heal(&mut self) {
            self.healed_at = Some(self.world.now);
            self.clear_partitions();
            self.stalled_until.fill(0);
            (self.loss, self.duplicate, self.straggle) = (0, 0, 0);
            self.delay = (100, 5_000);
            self.probe = self.world.proposals.len();
            for id in self.world.ids() {
                if self.world.host(id).node.is_none() {
                    self.schedule(0, Event::Restart(id));
                }
            }
        }

        /// Checks liveness once healed; true when the schedule is over.
        fn finished(&mut self) -> bool {
            let Some(healed_at) = self.healed_at else {
                return false;
            };
            let now = self.world.now;
            if let Some(progress_at) = self.progress_at {
                if now < progress_at + SETTLE {
                    return false;
                }
                let unresolved = self
                    .world
                    .proposals
                    .iter()
                    .filter(|p| p.state == ProposalState::Pending)
                    .count();
                assert_eq!(
                    unresolved, 0,
                    "proposals left unresolved after the cluster settled"
                );
                return true;
            }
            while self
                .world
                .proposals
                .get(self.probe)
                .is_some_and(|p| p.state == ProposalState::Failed)
            {
                self.probe += 1;
            }
            if let Some(probe) = self.world.proposals.get(self.probe)
                && probe.state == ProposalState::Acked
            {
                let end = probe.batch.end();
                let everywhere = self.world.ids().into_iter().all(|id| {
                    self.world
                        .status(id)
                        .is_some_and(|s| s.commit_offset >= end)
                });
                if everywhere {
                    self.progress_at = Some(now);
                    self.proposing = false;
                }
            }
            assert!(
                self.progress_at.is_some() || now <= healed_at + LIVENESS_BOUND,
                "no proposal committed on every node within {} ms of healing",
                LIVENESS_BOUND / 1_000
            );
            false
        }
    }

    fn panic_message(cause: &(dyn std::any::Any + Send)) -> String {
        cause
            .downcast_ref::<String>()
            .cloned()
            .or_else(|| cause.downcast_ref::<&str>().map(|s| (*s).to_owned()))
            .unwrap_or_else(|| "non-string panic".to_owned())
    }

    fn try_schedule(seed: u64, plan: Plan) -> Result<Stats, String> {
        let mut sim = Sim::new(seed, plan);
        panic::catch_unwind(AssertUnwindSafe(|| sim.run())).map_err(|cause| {
            format!(
                "seed {seed} {plan:?} failed at step {} (t = {} us): {}",
                sim.steps,
                sim.world.now,
                panic_message(&*cause)
            )
        })?;
        Ok(Stats {
            schedules: 1,
            steps: sim.steps,
            simulated_ms: sim.world.now / 1_000,
            proposals: u64::try_from(sim.world.proposals.len()).unwrap(),
            acked: u64::try_from(sim.world.acked.len()).unwrap(),
            leaders: u64::try_from(sim.world.leaders.len()).unwrap(),
        })
    }

    fn sweep(nodes: usize, seeds: std::ops::Range<u64>) {
        let started = Instant::now();
        let mut total = Stats::default();
        for seed in seeds {
            match try_schedule(seed, Plan::for_seed(seed, nodes, false)) {
                Ok(stats) => total.add(stats),
                Err(failure) => panic!("{failure}"),
            }
        }
        eprintln!("{nodes} nodes: {total:?} in {:?}", started.elapsed());
    }

    fn env_u64(name: &str, default: u64) -> u64 {
        std::env::var(name)
            .ok()
            .and_then(|value| value.parse().ok())
            .unwrap_or(default)
    }

    const SWEEP_SEEDS: u64 = 100;

    #[test]
    fn simulation_three_nodes_first_seeds() {
        sweep(3, 0..SWEEP_SEEDS);
    }

    #[test]
    fn simulation_three_nodes_second_seeds() {
        sweep(3, SWEEP_SEEDS..2 * SWEEP_SEEDS);
    }

    #[test]
    fn simulation_three_nodes_third_seeds() {
        sweep(3, 2 * SWEEP_SEEDS..3 * SWEEP_SEEDS);
    }

    #[test]
    fn simulation_five_nodes_first_seeds() {
        sweep(5, 0..SWEEP_SEEDS);
    }

    #[test]
    fn simulation_five_nodes_second_seeds() {
        sweep(5, SWEEP_SEEDS..2 * SWEEP_SEEDS);
    }

    #[test]
    fn simulation_five_nodes_third_seeds() {
        sweep(5, 2 * SWEEP_SEEDS..3 * SWEEP_SEEDS);
    }

    /// Schedules in which the mutation scan saw Raft's Figure 8 play out (an old-term suffix
    /// committed by counting replicas, then overwritten by a stale higher-epoch node). Their shape
    /// depends on the core's exact message pattern, so a change to it may make them ordinary.
    #[test]
    fn simulation_pinned_figure8_schedules() {
        for (seed, nodes) in [(925, 5), (2188, 3), (2766, 3)] {
            if let Err(failure) = try_schedule(seed, Plan::for_seed(seed, nodes, false)) {
                panic!("{failure}");
            }
        }
    }

    /// `RAFT_SOAK_SCHEDULES` (default 20000) schedules from seed `RAFT_SOAK_START`, alternating 3
    /// and 5 nodes, on every core; reports every failing seed. They are longer than the default
    /// run's unless `RAFT_SOAK_SHORT=1`.
    #[test]
    #[ignore = "long soak: cargo test --release soak -- --ignored --nocapture"]
    fn simulation_soak() {
        let count = env_u64("RAFT_SOAK_SCHEDULES", 20_000);
        let long = env_u64("RAFT_SOAK_SHORT", 0) == 0;
        let start = env_u64("RAFT_SOAK_START", 1_000_000);
        let threads = std::thread::available_parallelism().map_or(4, std::num::NonZero::get);
        let threads = u64::try_from(threads).unwrap();
        let started = Instant::now();
        let hook = panic::take_hook();
        panic::set_hook(Box::new(|_| {}));
        let results: Vec<(Stats, Vec<String>)> = std::thread::scope(|scope| {
            let workers: Vec<_> = (0..threads)
                .map(|worker| {
                    scope.spawn(move || {
                        let mut stats = Stats::default();
                        let mut failures = Vec::new();
                        for seed in (start + worker..start + count)
                            .step_by(usize::try_from(threads).unwrap())
                        {
                            let nodes = if seed % 2 == 0 { 3 } else { 5 };
                            match try_schedule(seed, Plan::for_seed(seed, nodes, long)) {
                                Ok(run) => stats.add(run),
                                Err(failure) => failures.push(failure),
                            }
                        }
                        (stats, failures)
                    })
                })
                .collect();
            workers
                .into_iter()
                .map(|worker| worker.join().unwrap())
                .collect()
        });
        panic::set_hook(hook);
        let mut total = Stats::default();
        let mut failures = Vec::new();
        for (stats, failed) in results {
            total.add(stats);
            failures.extend(failed);
        }
        eprintln!(
            "soak: {total:?}, {} failures, in {:?}",
            failures.len(),
            started.elapsed()
        );
        for failure in &failures {
            eprintln!("{}", failure.lines().next().unwrap_or_default());
        }
        assert!(failures.is_empty(), "{} schedules failed", failures.len());
    }

    /// Replays one schedule: `RAFT_SEED`, `RAFT_NODES` (3), `RAFT_SOAK` (0) and `RAFT_TRACE`.
    #[test]
    #[ignore = "replays RAFT_SEED"]
    fn simulation_replay() {
        let seed = env_u64("RAFT_SEED", 0);
        let nodes = usize::try_from(env_u64("RAFT_NODES", 3)).unwrap();
        let plan = Plan::for_seed(seed, nodes, env_u64("RAFT_SOAK", 0) == 1);
        if let Err(failure) = try_schedule(seed, plan) {
            panic!("{failure}");
        }
    }

    /// A hand-driven [`World`] for scripted scenarios: one clock, FIFO delivery, and flushes that
    /// complete at once except on nodes whose flushes are held.
    struct Cluster {
        world: World,
        pending: VecDeque<Packet>,
        blocked: Vec<Vec<bool>>,
        held_flushes: Vec<bool>,
        delivered: Vec<Packet>,
        /// Batches of this epoch are cut from every append before delivery.
        strip_epoch: Option<i32>,
    }

    impl Cluster {
        fn new(size: usize) -> Cluster {
            Cluster::with_append_bytes(size, UNLIMITED_BYTES)
        }

        fn with_append_bytes(size: usize, max_append_bytes: usize) -> Cluster {
            Cluster {
                world: World::new(size, 7, max_append_bytes, &vec![(1000, 0); size]),
                pending: VecDeque::new(),
                blocked: vec![vec![false; size + 1]; size + 1],
                held_flushes: vec![false; size + 1],
                delivered: Vec::new(),
                strip_epoch: None,
            }
        }

        fn status(&self, id: NodeId) -> Status {
            self.world.status(id).expect("node is up")
        }

        fn log(&self, id: NodeId) -> &SimLog {
            &self.world.host(id).log
        }

        fn state(&self, batch: &SimBatch) -> ProposalState {
            self.world
                .proposals
                .iter()
                .find(|proposal| proposal.batch == *batch)
                .expect("a proposal")
                .state
        }

        fn settle(&mut self, id: NodeId) {
            if !self.held_flushes[index(id)] {
                self.flush(id);
            }
            self.pending.extend(self.world.outbox.drain(..));
        }

        fn flush(&mut self, id: NodeId) {
            if let Some(ticket) = self.world.start_flush(id) {
                self.world.complete_flush(id, ticket);
            }
            self.pending.extend(self.world.outbox.drain(..));
        }

        fn hold_flushes(&mut self, id: NodeId, held: bool) {
            self.held_flushes[index(id)] = held;
        }

        fn advance(&mut self, by: Duration) {
            self.world.now += micros(by);
        }

        fn tick(&mut self, id: NodeId) {
            self.world.tick(id);
            self.settle(id);
        }

        /// Deliver, in order, every pending packet `wanted` accepts, including ones that
        /// deliveries produce; the others stay pending.
        fn deliver_where(&mut self, mut wanted: impl FnMut(&Packet) -> bool) {
            while let Some(at) = self.pending.iter().position(&mut wanted) {
                let mut packet = self.pending.remove(at).unwrap();
                if self.blocked[index(packet.from)][index(packet.to)] {
                    continue;
                }
                if let (Some(epoch), Message::AppendRequest(request)) =
                    (self.strip_epoch, &mut packet.message)
                {
                    request.batches.retain(|batch| batch.epoch != epoch);
                    packet.payloads.retain(|batch| batch.epoch != epoch);
                }
                self.delivered.push(packet.clone());
                let to = packet.to;
                self.world.deliver(packet);
                self.settle(to);
            }
        }

        fn deliver_all(&mut self) {
            self.deliver_where(|_| true);
        }

        /// Deliver what `wanted` accepts and lose everything else.
        fn deliver_only(&mut self, wanted: impl FnMut(&Packet) -> bool) {
            self.deliver_where(wanted);
            self.pending.clear();
        }

        fn run_for(&mut self, duration: Duration) {
            for _ in 0..duration.as_millis() / 5 {
                self.advance(ms(5));
                for id in self.world.ids() {
                    self.tick(id);
                }
                self.deliver_all();
            }
        }

        /// Let every lease lapse, then campaign `id` (twice if it first has to learn the term).
        fn elect_where(&mut self, id: NodeId, mut wanted: impl FnMut(&Packet) -> bool) {
            for _ in 0..2 {
                self.advance(ms(1_000));
                self.tick(id);
                self.deliver_only(&mut wanted);
                if self.status(id).role == Role::Leader {
                    return;
                }
            }
            panic!("node {id} did not win: {:?}", self.status(id));
        }

        fn elect(&mut self, id: NodeId) {
            self.elect_where(id, |_| true);
        }

        fn heartbeat(&mut self, leader: NodeId) {
            self.advance(ms(60));
            self.tick(leader);
            self.deliver_all();
        }

        fn propose(&mut self, leader: NodeId, count: i32) -> SimBatch {
            let batch = self
                .world
                .propose(leader, count)
                .expect("the leader accepts proposals");
            self.settle(leader);
            self.tick(leader);
            batch
        }

        fn isolate(&mut self, group: &[NodeId]) {
            for a in self.world.ids() {
                for b in self.world.ids() {
                    self.blocked[index(a)][index(b)] = group.contains(&a) != group.contains(&b);
                }
            }
        }

        fn heal(&mut self) {
            for row in &mut self.blocked {
                row.fill(false);
            }
        }

        fn crash(&mut self, id: NodeId) {
            self.world.crash(id);
        }

        fn restart(&mut self, id: NodeId) {
            self.world.restart(id);
            self.settle(id);
        }

        fn logs_agree(&self, a: NodeId, b: NodeId) -> bool {
            let (a, b) = (self.log(a), self.log(b));
            a.len() == b.len() && a.common_prefix(b) == a.len()
        }
    }

    fn is_pre_vote(packet: &Packet) -> bool {
        matches!(
            packet.message,
            Message::VoteRequest(VoteRequest { pre_vote: true, .. })
                | Message::VoteResponse(VoteResponse { pre_vote: true, .. })
        )
    }

    #[test]
    fn commit_figure8_old_term_entry_is_not_committed_by_counting_replicas() {
        let mut c = Cluster::with_append_bytes(5, BATCH_BYTES);
        c.elect(1);
        c.isolate(&[1, 2]);
        let old = c.propose(1, 1);
        c.deliver_all();
        assert!(c.status(1).commit_offset < old.end());

        c.crash(1);
        c.heal();
        c.isolate(&[2]);
        c.elect_where(5, |p| !matches!(p.message, Message::AppendRequest(_)));
        let rival_term = c.status(5).term;
        c.crash(5);

        c.heal();
        c.restart(1);
        c.strip_epoch = Some(rival_term + 1);
        c.elect(1);
        assert_eq!(c.status(1).term, rival_term + 1);
        let holders = [1, 2, 3, 4]
            .iter()
            .filter(|&&id| c.log(id).contains(&old))
            .count();
        assert!(holders >= 3, "the old entry reached a majority");
        assert!(
            c.status(1).commit_offset < old.end(),
            "an entry of an older term was committed by counting replicas"
        );

        c.strip_epoch = None;
        c.crash(1);
        c.restart(5);
        c.elect(5);
        c.heartbeat(5);
        for id in [2, 3, 4] {
            assert!(!c.log(id).contains(&old));
            assert!(c.logs_agree(id, 5));
        }
        assert_eq!(c.state(&old), ProposalState::Failed);
    }

    #[test]
    fn campaign_elects_before_the_timeout_but_cannot_unseat_a_live_leader() {
        let mut c = Cluster::new(3);
        let now = c.world.local(1);
        let actions = c.world.host_mut(1).node.as_mut().unwrap().campaign(now);
        c.world.apply(1, actions, &[]);
        c.settle(1);
        c.deliver_all();
        assert_eq!(c.status(1).role, Role::Leader);
        let term = c.status(1).term;

        let now = c.world.local(2);
        let actions = c.world.host_mut(2).node.as_mut().unwrap().campaign(now);
        c.world.apply(2, actions, &[]);
        c.settle(2);
        c.deliver_all();
        c.heartbeat(1);
        assert_eq!((c.status(1).role, c.status(1).term), (Role::Leader, term));
        assert_eq!(c.status(2).leader, Some(1));
    }

    #[test]
    fn election_candidate_with_stale_log_loses() {
        let mut c = Cluster::new(3);
        c.elect(1);
        c.isolate(&[3]);
        let committed = c.propose(1, 2);
        c.deliver_all();
        assert!(c.status(1).commit_offset >= committed.end());
        c.crash(1);
        c.heal();

        let term = c.status(3).term;
        c.advance(ms(1_000));
        c.tick(3);
        c.deliver_all();
        assert_eq!(c.status(3).role, Role::PreCandidate);
        assert_eq!(
            c.status(3).term,
            term,
            "a failed pre-vote must not raise the term"
        );

        let request = VoteRequest {
            term: term + 1,
            pre_vote: false,
            last_epoch: c.world.host(3).log.batches.last().unwrap().epoch,
            log_end: c.status(3).log_end,
        };
        c.world
            .send(3, 2, Message::VoteRequest(request), Vec::new());
        c.pending.extend(c.world.outbox.drain(..));
        c.deliver_where(|p| p.to == 2);
        assert!(c.pending.iter().any(|p| matches!(
            p.message,
            Message::VoteResponse(VoteResponse {
                granted: false,
                pre_vote: false,
                ..
            })
        )));
        c.pending.clear();

        c.elect(2);
        assert!(c.log(2).contains(&committed));
    }

    #[test]
    fn restart_losing_unflushed_tail_keeps_committed_data() {
        let mut c = Cluster::new(3);
        c.elect(1);
        c.hold_flushes(1, true);
        c.hold_flushes(3, true);
        let lost = c.propose(1, 1);
        c.deliver_all();
        assert!(
            c.status(1).commit_offset < lost.end(),
            "only node 2 flushed it"
        );
        c.crash(3);
        c.crash(1);
        c.hold_flushes(1, false);
        c.hold_flushes(3, false);
        c.restart(1);
        c.restart(3);
        c.isolate(&[2]);
        c.elect(3);
        c.heal();
        c.heartbeat(3);
        assert!(!c.log(2).contains(&lost));
        assert_eq!(c.state(&lost), ProposalState::Failed);

        let mut c = Cluster::new(3);
        c.elect(1);
        c.hold_flushes(1, true);
        c.hold_flushes(3, true);
        let kept = c.propose(1, 1);
        c.deliver_all();
        c.flush(3);
        c.deliver_all();
        c.heartbeat(1);
        assert!(
            c.status(3).commit_offset >= kept.end(),
            "nodes 2 and 3 flushed it"
        );
        c.crash(1);
        c.crash(3);
        c.hold_flushes(1, false);
        c.hold_flushes(3, false);
        c.restart(1);
        c.restart(3);
        c.isolate(&[2]);
        c.advance(ms(1_000));
        c.tick(1);
        c.deliver_all();
        assert_ne!(
            c.status(1).role,
            Role::Leader,
            "node 1 lost the committed entry"
        );
        c.elect(3);
        c.heal();
        c.heartbeat(3);
        assert!(c.log(1).contains(&kept));
    }

    #[test]
    fn election_voter_compares_candidate_with_its_flushed_log() {
        let mut c = Cluster::new(3);
        c.elect(1);
        c.isolate(&[3]);
        c.hold_flushes(2, true);
        let unflushed = c.propose(1, 1);
        c.deliver_all();
        assert_eq!(c.status(2).log_end, unflushed.end());
        assert!(c.status(2).flushed_end < unflushed.end());

        c.crash(1);
        c.heal();
        c.elect(3);
        assert!(
            !c.log(2).contains(&unflushed),
            "node 2 voted on its flushed log and dropped the unacknowledged tail"
        );
        c.hold_flushes(2, false);
        c.heartbeat(3);
        assert!(c.logs_agree(2, 3));
    }

    #[test]
    fn election_split_vote_resolves_in_a_later_term() {
        let mut c = Cluster::new(5);
        c.advance(ms(1_000));
        c.tick(1);
        c.tick(2);
        c.deliver_where(is_pre_vote);
        assert_eq!(c.status(1).role, Role::Candidate);
        assert_eq!(c.status(2).role, Role::Candidate);
        let split_term = c.status(1).term;
        assert_eq!(c.status(2).term, split_term);

        c.deliver_only(|p| match p.message {
            Message::VoteRequest(_) => matches!((p.from, p.to), (1, 3) | (2, 4)),
            _ => true,
        });
        assert_eq!(c.world.leader(), None, "neither candidate has a majority");

        c.run_for(ms(3_000));
        let leader = c.world.leader().expect("a leader after the split vote");
        assert!(c.status(leader).term > split_term);
        for id in 1..=5 {
            assert_eq!(c.status(id).leader, Some(leader));
        }
    }

    #[test]
    fn prevote_keeps_partitioned_node_from_disrupting_leader() {
        let mut c = Cluster::new(3);
        c.elect(1);
        let term = c.status(1).term;
        c.isolate(&[3]);
        c.run_for(ms(2_000));
        assert_eq!(c.status(3).term, term, "pre-votes never raise the term");
        assert_eq!(c.status(3).role, Role::PreCandidate);

        c.heal();
        c.run_for(ms(2_000));
        assert_eq!(c.status(1).role, Role::Leader);
        assert_eq!(c.status(1).term, term);
        assert_eq!(c.status(3).leader, Some(1));
    }

    #[test]
    fn check_quorum_steps_down_isolated_leader() {
        let mut c = Cluster::new(3);
        c.elect(1);
        c.isolate(&[1]);
        let stranded = c.propose(1, 1);
        c.run_for(ms(320));
        assert_eq!(c.status(1).role, Role::Follower);
        assert_eq!(c.state(&stranded), ProposalState::Failed);
        c.run_for(ms(1_000));
        assert!(matches!(c.world.leader(), Some(2 | 3)));
    }

    #[test]
    fn replication_fast_backtracking_skips_divergent_epoch_in_one_round() {
        let mut c = Cluster::new(3);
        c.elect(1);
        for _ in 0..20 {
            c.propose(1, 1);
        }
        c.deliver_all();
        c.isolate(&[1]);
        for _ in 0..50 {
            c.propose(1, 1);
        }
        c.elect(2);
        for _ in 0..30 {
            c.propose(2, 1);
        }
        c.deliver_all();
        c.crash(2);
        c.advance(ms(200));
        c.tick(1);
        assert_eq!(c.status(1).role, Role::Follower, "CheckQuorum");

        c.heal();
        c.delivered.clear();
        c.elect(3);
        c.heartbeat(3);
        let mismatches = c
            .delivered
            .iter()
            .filter(|p| {
                p.from == 1
                    && matches!(
                        p.message,
                        Message::AppendResponse(AppendResponse {
                            outcome: AppendOutcome::Mismatch { .. },
                            ..
                        })
                    )
            })
            .count();
        assert_eq!(mismatches, 1, "one round skips all 50 divergent batches");
        assert!(c.logs_agree(1, 3));
    }

    #[test]
    fn commit_leader_counts_only_its_flushed_end() {
        let mut c = Cluster::new(3);
        c.elect(1);
        c.hold_flushes(1, true);
        let batch = c.propose(1, 1);
        c.deliver_all();
        c.heartbeat(1);
        assert!(
            c.status(2).commit_offset >= batch.end(),
            "a majority flushed it"
        );
        assert!(
            c.status(1).commit_offset < batch.end(),
            "but not the leader"
        );
        assert_eq!(c.state(&batch), ProposalState::Pending);
        c.flush(1);
        assert_eq!(c.status(1).commit_offset, batch.end());
        assert_eq!(c.state(&batch), ProposalState::Acked);

        c.hold_flushes(1, true);
        c.hold_flushes(3, true);
        let batch = c.propose(1, 1);
        c.deliver_all();
        c.heartbeat(1);
        assert!(
            c.status(2).commit_offset < batch.end(),
            "appended by a majority, flushed by one"
        );
        c.flush(1);
        c.heartbeat(1);
        assert_eq!(c.status(1).commit_offset, batch.end());
    }

    #[test]
    fn commit_single_node_waits_for_flush() {
        let mut c = Cluster::new(1);
        c.hold_flushes(1, true);
        c.elect(1);
        let batch = c.propose(1, 3);
        assert_eq!(c.status(1).commit_offset, 0);
        c.flush(1);
        assert_eq!(c.status(1).commit_offset, batch.end());
        assert_eq!(c.state(&batch), ProposalState::Acked);
    }

    fn lone_follower() -> Node {
        Node::new(
            test_config(1, 3, 1, UNLIMITED_BYTES),
            DurableState::default(),
            Duration::ZERO,
        )
        .unwrap()
    }

    fn append_request(
        term: i32,
        (prev_offset, prev_epoch): (i64, i32),
        commit_offset: i64,
        batches: &[(i64, i32, i32)],
    ) -> Message {
        Message::AppendRequest(AppendRequest {
            term,
            prev_offset,
            prev_epoch,
            commit_offset,
            batches: batches
                .iter()
                .map(|&(base_offset, record_count, epoch)| BatchInfo {
                    base_offset,
                    record_count,
                    epoch,
                })
                .collect(),
        })
    }

    fn epochs(pairs: &[(i32, i64)]) -> Vec<EpochStart> {
        pairs
            .iter()
            .map(|&(epoch, start_offset)| EpochStart {
                epoch,
                start_offset,
            })
            .collect()
    }

    fn restored(pairs: &[(i32, i64)], log_end: i64) -> DurableState {
        DurableState {
            hard_state: HardState {
                term: 5,
                voted_for: None,
            },
            epochs: epochs(pairs),
            log_end,
        }
    }

    #[test]
    fn epoch_history_answers_offset_and_epoch_queries() {
        let node = Node::new(
            test_config(1, 3, 1, UNLIMITED_BYTES),
            restored(&[(1, 0), (3, 10), (4, 15)], 20),
            Duration::ZERO,
        )
        .unwrap();
        let ending = [
            (-1, None),
            (0, Some(0)),
            (1, Some(1)),
            (10, Some(1)),
            (11, Some(3)),
            (15, Some(3)),
            (16, Some(4)),
            (20, Some(4)),
            (21, None),
        ];
        for (offset, epoch) in ending {
            assert_eq!(node.epoch_ending_at(offset), epoch, "offset {offset}");
        }
        let ends = [
            (0, None),
            (1, Some((1, 10))),
            (2, Some((1, 10))),
            (3, Some((3, 15))),
            (4, Some((4, 20))),
            (9, Some((4, 20))),
        ];
        for (epoch, end) in ends {
            let expected = end.map(|(epoch, end_offset)| EpochEnd { epoch, end_offset });
            assert_eq!(node.epoch_end(epoch), expected, "epoch {epoch}");
        }
    }

    #[test]
    fn epoch_history_stays_correct_across_truncation() {
        let mut log = LogState::restore(restored(&[(1, 0), (3, 10), (4, 15)], 20)).unwrap();
        log.truncate(16);
        assert_eq!((log.end, log.flushed, log.generation), (16, 16, 1));
        assert_eq!(log.epochs.epoch_ending_at(16), 4);
        log.truncate(15);
        assert_eq!(log.epochs.starts, epochs(&[(1, 0), (3, 10)]));
        assert_eq!(
            log.epochs.end_of(4, log.end),
            Some(EpochEnd {
                epoch: 3,
                end_offset: 15
            })
        );
        log.append(15, 2, 5);
        assert_eq!(
            log.epochs.end_of(4, log.end),
            Some(EpochEnd {
                epoch: 3,
                end_offset: 15
            })
        );
        assert_eq!(
            log.epochs.end_of(5, log.end),
            Some(EpochEnd {
                epoch: 5,
                end_offset: 17
            })
        );
        log.truncate(10);
        assert_eq!(log.epochs.starts, epochs(&[(1, 0)]));
        assert_eq!(
            log.epochs.end_of(3, log.end),
            Some(EpochEnd {
                epoch: 1,
                end_offset: 10
            })
        );
    }

    #[test]
    fn replication_mismatch_resumes_at_shared_epoch_boundary() {
        let log = LogState::restore(restored(&[(1, 0), (2, 10), (5, 30)], 40)).unwrap();
        assert_eq!(
            log.next_after_mismatch(2, 25),
            25,
            "the follower holds less of epoch 2"
        );
        assert_eq!(
            log.next_after_mismatch(2, 35),
            30,
            "the follower holds more of epoch 2"
        );
        assert_eq!(
            log.next_after_mismatch(3, 33),
            30,
            "epoch 3 never reached this leader"
        );
        assert_eq!(log.next_after_mismatch(1, 4), 4);
        assert_eq!(log.next_after_mismatch(0, 0), 0);
    }

    #[test]
    fn node_new_rejects_unusable_config_and_state() {
        let config = test_config(1, 3, 1, UNLIMITED_BYTES);
        let bad_configs = [
            Config {
                voters: vec![1, 2, 2],
                ..config.clone()
            },
            Config {
                voters: vec![2, 3],
                ..config.clone()
            },
            Config {
                heartbeat_interval: ms(150),
                ..config.clone()
            },
            Config {
                election_timeout_max: ms(150),
                ..config.clone()
            },
            Config {
                max_append_bytes: 0,
                ..config.clone()
            },
        ];
        for bad in bad_configs {
            let result = Node::new(bad, DurableState::default(), Duration::ZERO);
            assert!(matches!(result, Err(RaftError::InvalidConfig(_))));
        }
        let voted_outside = DurableState {
            hard_state: HardState {
                term: 3,
                voted_for: Some(9),
            },
            ..DurableState::default()
        };
        let negative_term = DurableState {
            hard_state: HardState {
                term: -1,
                voted_for: None,
            },
            ..DurableState::default()
        };
        let bad_states = [
            restored(&[(1, 0)], 0),
            restored(&[], 5),
            restored(&[(1, 2)], 5),
            restored(&[(2, 0), (1, 3)], 5),
            restored(&[(1, 0), (2, 5)], 5),
            restored(&[(1, 0), (6, 3)], 5),
            voted_outside,
            negative_term,
        ];
        for bad in bad_states {
            let result = Node::new(config.clone(), bad.clone(), Duration::ZERO);
            assert!(matches!(result, Err(RaftError::InvalidState(_))), "{bad:?}");
        }
    }

    #[test]
    fn step_rejects_malformed_messages_without_changing_state() {
        let mut node = lone_follower();
        let before = node.status();
        let heartbeat = append_request(1, (0, 0), 0, &[]);
        assert_eq!(
            node.step(9, heartbeat.clone(), Duration::ZERO),
            Err(RaftError::UnknownPeer(9))
        );
        assert_eq!(
            node.step(1, heartbeat, Duration::ZERO),
            Err(RaftError::UnknownPeer(1))
        );
        let malformed = [
            append_request(1, (0, 0), 0, &[(0, 1, 1), (2, 1, 1)]),
            append_request(1, (0, 0), 0, &[(0, 0, 1)]),
            append_request(1, (0, 0), 0, &[(0, 1, 2)]),
            append_request(1, (0, 2), 0, &[]),
            Message::AppendResponse(AppendResponse {
                term: 1,
                outcome: AppendOutcome::Accepted {
                    matched_end: 1,
                    flushed_end: 2,
                },
            }),
        ];
        for message in malformed {
            let result = node.step(2, message.clone(), Duration::ZERO);
            assert!(
                matches!(result, Err(RaftError::InvalidMessage(_))),
                "{message:?}"
            );
        }
        assert_eq!(node.status(), before);

        node.step(2, append_request(1, (0, 0), 0, &[]), Duration::ZERO)
            .unwrap();
        assert_eq!(
            node.propose(1),
            Err(RaftError::NotLeader { leader: Some(2) })
        );
        assert_eq!(node.propose(0), Err(RaftError::InvalidRecordCount(0)));
    }

    #[test]
    fn flush_point_taken_before_truncation_is_ignored() {
        let mut node = lone_follower();
        let actions = node
            .step(
                2,
                append_request(1, (0, 0), 0, &[(0, 1, 1), (1, 2, 1)]),
                Duration::ZERO,
            )
            .unwrap();
        assert!(actions.contains(&Action::AppendReceived { from_offset: 0 }));
        let stale = node.flush_point();
        let actions = node
            .step(3, append_request(2, (1, 1), 0, &[(1, 1, 2)]), ms(1))
            .unwrap();
        assert!(actions.contains(&Action::Truncate { offset: 1 }));
        assert_eq!(node.flushed(stale), []);
        assert_eq!(node.status().flushed_end, 0);

        let fresh = node.flush_point();
        let actions = node.flushed(fresh);
        assert_eq!(node.status().flushed_end, 2);
        let ack = Message::AppendResponse(AppendResponse {
            term: 2,
            outcome: AppendOutcome::Accepted {
                matched_end: 2,
                flushed_end: 2,
            },
        });
        assert_eq!(
            actions,
            [Action::Send {
                to: 3,
                message: ack
            }]
        );
    }

    #[test]
    fn follower_trusts_only_prefix_matching_leader() {
        let mut node = lone_follower();
        let stale = append_request(1, (0, 0), 0, &[(0, 1, 1), (1, 1, 1), (2, 1, 1)]);
        node.step(2, stale, Duration::ZERO).unwrap();
        let point = node.flush_point();
        drop(node.flushed(point));
        assert_eq!(node.status().flushed_end, 3);

        let actions = node
            .step(3, append_request(2, (1, 1), 3, &[]), ms(1))
            .unwrap();
        let ack = Message::AppendResponse(AppendResponse {
            term: 2,
            outcome: AppendOutcome::Accepted {
                matched_end: 1,
                flushed_end: 1,
            },
        });
        assert!(actions.contains(&Action::Send {
            to: 3,
            message: ack
        }));
        assert_eq!(
            node.status().commit_offset,
            1,
            "offsets 1..3 may hold other batches on the new leader"
        );
    }

    #[test]
    #[should_panic(expected = "below its committed offset")]
    fn follower_refuses_to_truncate_committed_data() {
        let mut node = lone_follower();
        node.step(
            2,
            append_request(1, (0, 0), 2, &[(0, 1, 1), (1, 1, 1)]),
            Duration::ZERO,
        )
        .unwrap();
        let point = node.flush_point();
        drop(node.flushed(point));
        assert_eq!(node.status().commit_offset, 2);
        node.step(3, append_request(2, (0, 0), 2, &[(0, 1, 2)]), ms(1))
            .unwrap();
    }
}
