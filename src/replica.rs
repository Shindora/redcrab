//! Replicas: the Raft groups this node takes part in, each a [`raft::Node`] over a [`Log`],
//! driven by a fixed set of shard threads.
//!
//! A group lives on one shard, chosen by hashing its key, and only that shard's thread calls into
//! its node and writes its log and hard state. Kafka handlers read the log under its lock (only
//! below the published commit offset) and the published leader. One ticker thread ticks every shard.
//!
//! The driver obligations of [`crate::raft`] are met like this. Actions are performed in order on
//! the shard thread before the next call into the node: hard state is written with an atomically
//! replaced, fsynced file; truncations are durable when [`Log::truncate`] returns; received and
//! proposed batches and the no-op are appended at once; a replicate reads its batches at once.
//! Flushes run on the blocking pool so the shard keeps appending and replicating meanwhile: the
//! flush point is taken with the log end just before [`Log::begin_flush`], and both are handed back
//! once the fsync is done, so one fsync covers every append made before it began (group commit on
//! leader and follower alike, and the leader replicates without waiting for its own fsync). A
//! proposal is acknowledged on a commit covering it while this node still leads its epoch, and failed
//! on any other leader change. Any write or fsync failure exits the process.
//!
//! Hard state writes, epoch checkpoint writes inside appends and truncations are synchronous on the
//! shard thread, so a burst of elections delays the other groups of that shard by their fsyncs. A
//! shard thread that panics exits the process, since its groups would otherwise hang.

use std::collections::{HashMap, VecDeque};
use std::fs;
use std::hash::{DefaultHasher, Hash, Hasher};
use std::io;
use std::mem;
use std::ops::Range;
use std::panic::{self, AssertUnwindSafe};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicUsize, Ordering};
use std::sync::mpsc::{self, Receiver, Sender};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::thread::{self, JoinHandle};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use bytes::{Bytes, BytesMut};
use kafka_protocol::ResponseError;
use kafka_protocol::records::RecordBatchDecoder;
use tokio::sync::oneshot;
use tracing::{debug, info, warn};
use uuid::Uuid;

use crate::cluster::{Cluster, fatal};
use crate::raft::{self, Action, DurableState, FlushPoint, HardState, NodeId, Role};
use crate::storage::{
    Extent, Flush, Log, LogConfig, StorageError, batch_frames, noop_control_batch, replace_file,
};
use crate::transport::{Item, Received};

/// Lower bound of the randomised election timeout.
pub const ELECTION_TIMEOUT_MIN: Duration = Duration::from_millis(1500);
/// Exclusive upper bound of the randomised election timeout.
pub const ELECTION_TIMEOUT_MAX: Duration = Duration::from_millis(3000);
/// How often a leader heartbeats an idle follower.
pub const HEARTBEAT_INTERVAL: Duration = Duration::from_millis(150);
/// How often every group is ticked. Heartbeats fall on ticks, so a coarse tick also groups the
/// heartbeats of many groups into few frames.
pub const TICK_INTERVAL: Duration = Duration::from_millis(50);
/// Byte budget of one replicated append.
pub const MAX_APPEND_BYTES: usize = 1 << 20;
/// How often a leader tells the nodes that are not replicas of its group that it leads it.
pub const HINT_INTERVAL: Duration = Duration::from_secs(1);
const MAX_SHARDS: usize = 4;
const MAX_EVENTS_PER_ROUND: usize = 1024;
/// Received bytes a shard may have queued before more is dropped.
const INBOUND_BYTES: usize = 64 << 20;
const INBOUND_OVERHEAD: usize = 64;
const APPLY_BYTES: usize = 1 << 20;
const HARD_STATE_FILE: &str = "raft-state";
const HARD_STATE_HEADER: &str = "redcrab raft-state 1";

/// Identifies a Raft group: one partition of a topic, or the cluster metadata log.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct GroupKey {
    /// Topic id; nil for the metadata log.
    pub topic: Uuid,
    /// Partition index; 0 for the metadata log.
    pub partition: i32,
}

impl GroupKey {
    /// The cluster metadata log.
    pub const METADATA: GroupKey = GroupKey {
        topic: Uuid::nil(),
        partition: 0,
    };
}

/// A group's leadership as this node last learned it.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct View {
    /// Current term, the Kafka leader epoch.
    pub term: i32,
    /// Leader of `term`, if known.
    pub leader: Option<NodeId>,
    /// This node leads `term` and has committed an entry of it, so its commit offset is current.
    pub ready: bool,
}

/// Outcome of a proposal: the base offset of its first batch, or a Kafka error code.
pub type Proposed = oneshot::Receiver<Result<i64, i16>>;

/// This node's replica of one group, as the rest of the broker sees it.
pub struct Replica {
    /// The group.
    pub key: GroupKey,
    /// Every voter of the group, this node included.
    pub voters: Vec<NodeId>,
    /// Only locked from blocking threads; tokio's mutex is FIFO-fair, so fetches locating batches
    /// are not starved by the shard's appends.
    log: tokio::sync::Mutex<Log>,
    commit: AtomicI64,
    log_end: AtomicI64,
    view: Mutex<View>,
    shard: Sender<Event>,
}

impl Replica {
    /// Leadership as last learned.
    pub fn view(&self) -> View {
        *lock(&self.view)
    }

    /// The commit offset, which is the Kafka high watermark. Restarts at 0 and is relearned.
    pub fn high_watermark(&self) -> i64 {
        self.commit.load(Ordering::Acquire)
    }

    /// End of the local log, committed or not.
    pub fn log_end_offset(&self) -> i64 {
        self.log_end.load(Ordering::Acquire)
    }

    /// Proposes `records`, already split into whole `batches`, to be appended as the leader.
    pub fn propose(&self, records: Vec<u8>, batches: Vec<Range<usize>>) -> Proposed {
        let (reply, proposed) = oneshot::channel();
        let event = Event::Propose {
            key: self.key,
            records,
            batches,
            reply,
        };
        if let Err(mpsc::SendError(Event::Propose { reply, .. })) = self.shard.send(event) {
            let _ = reply.send(Err(ResponseError::NotLeaderOrFollower.code()));
        }
        proposed
    }

    /// Locates whole batches from `offset` that start below `end`, which must not exceed the high
    /// watermark, up to `max_bytes`, and the first even if larger when `at_least_one` is set.
    /// Blocking, but reads only batch headers.
    ///
    /// # Errors
    ///
    /// Returns the Kafka error code for an out-of-range offset or a storage failure.
    pub fn locate(
        &self,
        offset: i64,
        max_bytes: usize,
        at_least_one: bool,
        end: i64,
    ) -> Result<Vec<Extent>, i16> {
        let end = end.min(self.high_watermark());
        let log = self.log.blocking_lock();
        log.locate(offset, max_bytes, at_least_one, end)
            .map_err(|e| error_code(&e))
    }

    /// Offset of the first batch at or after `timestamp_ms` below the high watermark. Blocking.
    pub fn offset_for_timestamp(&self, timestamp_ms: i64) -> Option<i64> {
        let offset = self
            .log
            .blocking_lock()
            .offset_for_timestamp(timestamp_ms)?;
        (offset < self.high_watermark()).then_some(offset)
    }

    /// Kafka's `OffsetsForLeaderEpoch` answer: the largest epoch at or below `epoch` and where it
    /// ends. Blocking.
    pub fn epoch_end(&self, epoch: i32) -> Option<(i32, i64)> {
        self.log.blocking_lock().epoch_end_offset(epoch)
    }

    /// Epoch of the batch holding `offset`, if the log holds it. Blocking.
    pub fn epoch_at(&self, offset: i64) -> Option<i32> {
        (offset >= 0 && offset < self.log_end_offset())
            .then(|| self.log.blocking_lock().epoch_at(offset))
            .flatten()
    }
}

/// Kafka error code for a storage failure.
fn error_code(error: &StorageError) -> i16 {
    match error {
        StorageError::Io(_) => ResponseError::KafkaStorageError.code(),
        StorageError::InvalidBatch(_) => ResponseError::CorruptMessage.code(),
        StorageError::OffsetOutOfRange { .. } => ResponseError::OffsetOutOfRange.code(),
        StorageError::EpochRegression { .. } => ResponseError::FencedLeaderEpoch.code(),
        StorageError::OffsetMismatch { .. } | StorageError::NotBatchBoundary { .. } => {
            ResponseError::UnknownServerError.code()
        }
    }
}

/// What a shard thread is asked to do.
pub(crate) enum Event {
    Tick,
    Peer {
        from: NodeId,
        items: Vec<Received>,
        bytes: usize,
    },
    Propose {
        key: GroupKey,
        records: Vec<u8>,
        batches: Vec<Range<usize>>,
        reply: oneshot::Sender<Result<i64, i16>>,
    },
    Flushed {
        key: GroupKey,
        flush: Flush,
        point: FlushPoint,
        synced: io::Result<()>,
    },
    Open(Box<Opened>),
    Remove(GroupKey),
    Stop,
}

/// A group whose log and hard state were read on the blocking pool.
pub(crate) struct Opened {
    key: GroupKey,
    voters: Vec<NodeId>,
    dir: PathBuf,
    log: Log,
    hard_state: HardState,
}

/// The shard threads and the ticker.
pub(crate) struct Shards {
    senders: Vec<Sender<Event>>,
    receivers: Mutex<Vec<Receiver<Event>>>,
    inbound: Vec<Arc<AtomicUsize>>,
    ticks: Vec<Arc<AtomicBool>>,
    stop: Arc<AtomicBool>,
    threads: Mutex<Vec<JoinHandle<()>>>,
}

impl Shards {
    pub(crate) fn new() -> Shards {
        let count = thread::available_parallelism()
            .map_or(2, std::num::NonZero::get)
            .clamp(1, MAX_SHARDS);
        let (senders, receivers) = (0..count).map(|_| mpsc::channel()).unzip();
        Shards {
            senders,
            receivers: Mutex::new(receivers),
            inbound: (0..count).map(|_| Arc::default()).collect(),
            ticks: (0..count).map(|_| Arc::default()).collect(),
            stop: Arc::default(),
            threads: Mutex::new(Vec::new()),
        }
    }

    /// Starts the shard threads and the ticker.
    pub(crate) fn start(&self, cluster: &Arc<Cluster>) -> io::Result<()> {
        let receivers = mem::take(&mut *lock(&self.receivers));
        let mut threads = lock(&self.threads);
        for (index, events) in receivers.into_iter().enumerate() {
            let shard = Shard {
                cluster: Arc::clone(cluster),
                events,
                me: self.senders[index].clone(),
                inbound: Arc::clone(&self.inbound[index]),
                tick: Arc::clone(&self.ticks[index]),
                drivers: HashMap::new(),
                last_hints: Duration::ZERO,
            };
            threads.push(
                thread::Builder::new()
                    .name(format!("redcrab-shard-{index}"))
                    .spawn(move || {
                        if panic::catch_unwind(AssertUnwindSafe(|| shard.run())).is_err() {
                            fatal(format_args!("shard {index} panicked"));
                        }
                    })?,
            );
        }
        let (senders, ticks, stop) = (
            self.senders.clone(),
            self.ticks.clone(),
            Arc::clone(&self.stop),
        );
        threads.push(
            thread::Builder::new()
                .name("redcrab-ticker".into())
                .spawn(move || run_ticker(&senders, &ticks, &stop))?,
        );
        Ok(())
    }

    fn index(&self, key: GroupKey) -> usize {
        let mut hasher = DefaultHasher::new();
        key.hash(&mut hasher);
        usize::try_from(hasher.finish() % self.senders.len() as u64).unwrap_or(0)
    }

    pub(crate) fn sender(&self, key: GroupKey) -> &Sender<Event> {
        &self.senders[self.index(key)]
    }

    pub(crate) fn send(&self, key: GroupKey, event: Event) {
        let _ = self.sender(key).send(event);
    }

    /// Hands received Raft messages to the shards owning their groups, dropping what would
    /// overfill a shard's queue.
    pub(crate) fn deliver(&self, from: NodeId, items: Vec<Received>) {
        let mut by_shard: Vec<Vec<Received>> =
            (0..self.senders.len()).map(|_| Vec::new()).collect();
        for item in items {
            by_shard[self.index(item.group)].push(item);
        }
        for (index, items) in by_shard.into_iter().enumerate() {
            if items.is_empty() {
                continue;
            }
            let bytes: usize = items
                .iter()
                .map(|item| item.payload.len() + INBOUND_OVERHEAD)
                .sum();
            let queued = &self.inbound[index];
            if queued.fetch_add(bytes, Ordering::AcqRel) + bytes > INBOUND_BYTES {
                queued.fetch_sub(bytes, Ordering::AcqRel);
                debug!(
                    from,
                    shard = index,
                    "shard queue full, dropping received messages"
                );
                continue;
            }
            let _ = self.senders[index].send(Event::Peer { from, items, bytes });
        }
    }

    /// Stops every shard and the ticker and waits for them.
    pub(crate) fn stop(&self) {
        self.stop.store(true, Ordering::Release);
        for sender in &self.senders {
            let _ = sender.send(Event::Stop);
        }
        for thread in lock(&self.threads).drain(..) {
            let _ = thread.join();
        }
    }
}

fn run_ticker(senders: &[Sender<Event>], ticks: &[Arc<AtomicBool>], stop: &AtomicBool) {
    while !stop.load(Ordering::Acquire) {
        thread::sleep(TICK_INTERVAL);
        for (sender, pending) in senders.iter().zip(ticks) {
            if !pending.swap(true, Ordering::AcqRel) && sender.send(Event::Tick).is_err() {
                return;
            }
        }
    }
}

/// Opens the log and hard state of a group in `dir` on the blocking pool, then hands them to the
/// shard that will drive it.
pub(crate) fn open_group(cluster: &Arc<Cluster>, key: GroupKey, voters: Vec<NodeId>, dir: PathBuf) {
    let sender = cluster.shards().sender(key).clone();
    cluster.blocking(move || {
        let log = Log::open(&dir, LogConfig::default())
            .unwrap_or_else(|e| fatal(format_args!("opening the log in {}: {e}", dir.display())));
        let hard_state = read_hard_state(&dir).unwrap_or_else(|e| {
            fatal(format_args!(
                "reading the hard state in {}: {e}",
                dir.display()
            ))
        });
        let _ = sender.send(Event::Open(Box::new(Opened {
            key,
            voters,
            dir,
            log,
            hard_state,
        })));
    });
}

struct Shard {
    cluster: Arc<Cluster>,
    events: Receiver<Event>,
    me: Sender<Event>,
    inbound: Arc<AtomicUsize>,
    tick: Arc<AtomicBool>,
    drivers: HashMap<GroupKey, Driver>,
    last_hints: Duration,
}

impl Shard {
    fn run(mut self) {
        while let Ok(event) = self.events.recv() {
            let mut next = Some(event);
            let mut handled = 0;
            while let Some(event) = next.take() {
                if !self.handle(event) {
                    self.stop();
                    return;
                }
                handled += 1;
                if handled < MAX_EVENTS_PER_ROUND {
                    next = self.events.try_recv().ok();
                }
            }
            self.finish_round();
        }
        self.stop();
    }

    /// Handles one event; `false` to stop.
    fn handle(&mut self, event: Event) -> bool {
        let cluster = &self.cluster;
        match event {
            Event::Tick => {
                self.tick.store(false, Ordering::Release);
                let now = cluster.now();
                for driver in self.drivers.values_mut() {
                    let actions = if driver.keeps_campaigning(now) {
                        driver.node.campaign(now)
                    } else {
                        driver.node.tick(now)
                    };
                    driver.perform(cluster, actions, None);
                }
                if now >= self.last_hints + HINT_INTERVAL {
                    self.last_hints = now;
                    for driver in self.drivers.values() {
                        driver.hint(cluster);
                    }
                }
            }
            Event::Peer { from, items, bytes } => {
                self.inbound.fetch_sub(bytes, Ordering::AcqRel);
                for received in items {
                    if let Some(driver) = self.drivers.get_mut(&received.group) {
                        driver.step(cluster, from, received);
                    }
                }
            }
            Event::Propose {
                key,
                records,
                batches,
                reply,
            } => match self.drivers.get_mut(&key) {
                Some(driver) => driver.propose(records, &batches, reply),
                None => {
                    let _ = reply.send(Err(ResponseError::NotLeaderOrFollower.code()));
                }
            },
            Event::Flushed {
                key,
                flush,
                point,
                synced,
            } => {
                if let Some(driver) = self.drivers.get_mut(&key) {
                    driver.flushed(cluster, &flush, point, synced);
                }
            }
            Event::Open(opened) => self.open(*opened),
            Event::Remove(key) => {
                if let Some(driver) = self.drivers.remove(&key) {
                    driver.remove(cluster);
                }
            }
            Event::Stop => return false,
        }
        true
    }

    fn open(&mut self, opened: Opened) {
        let Opened {
            key,
            voters,
            dir,
            log,
            hard_state,
        } = opened;
        let cluster = &self.cluster;
        if !cluster.hosts(key) {
            debug!(?key, "group removed while its log was opening");
            drop(log);
            remove_dir(cluster, dir);
            return;
        }
        let durable = DurableState {
            hard_state,
            epochs: log
                .epoch_starts()
                .iter()
                .map(|start| raft::EpochStart {
                    epoch: start.epoch,
                    start_offset: start.start_offset,
                })
                .collect(),
            log_end: log.end_offset(),
        };
        let fresh = durable.log_end == 0 && hard_state.term == 0;
        let now = cluster.now();
        let node = raft::Node::new(cluster.raft_config(key, &voters), durable, now)
            .unwrap_or_else(|e| fatal(format_args!("group in {}: {e}", dir.display())));
        let campaign = voters == [cluster.id] || (fresh && voters.first() == Some(&cluster.id));
        let replica = Arc::new(Replica {
            key,
            voters,
            commit: AtomicI64::new(0),
            log_end: AtomicI64::new(log.end_offset()),
            log: tokio::sync::Mutex::new(log),
            view: Mutex::new(View {
                term: hard_state.term,
                ..View::default()
            }),
            shard: self.me.clone(),
        });
        cluster.register(Arc::clone(&replica));
        let mut driver = Driver {
            replica,
            node,
            dir,
            flushing: false,
            unflushed: false,
            kicked: false,
            pending: VecDeque::new(),
            leading: None,
            term: hard_state.term,
            leader: None,
            noop_end: None,
            applied: 0,
            campaign_until: campaign.then_some(now + ELECTION_TIMEOUT_MIN),
        };
        if campaign {
            let actions = driver.node.campaign(now);
            driver.perform(cluster, actions, None);
        }
        self.drivers.insert(key, driver);
    }

    fn finish_round(&mut self) {
        let now = self.cluster.now();
        for driver in self.drivers.values_mut() {
            if mem::take(&mut driver.kicked) {
                let actions = driver.node.tick(now);
                driver.perform(&self.cluster, actions, None);
            }
            if driver.unflushed && !driver.flushing {
                driver.start_flush(&self.cluster, &self.me);
            }
        }
    }

    fn stop(&mut self) {
        for (_, driver) in self.drivers.drain() {
            driver.fail_pending(ResponseError::NotLeaderOrFollower.code());
        }
    }
}

struct Pending {
    end: i64,
    epoch: i32,
    base: i64,
    reply: oneshot::Sender<Result<i64, i16>>,
}

struct Driver {
    replica: Arc<Replica>,
    node: raft::Node,
    dir: PathBuf,
    flushing: bool,
    unflushed: bool,
    kicked: bool,
    pending: VecDeque<Pending>,
    /// The term in which the latest leader change named this node leader.
    leading: Option<i32>,
    term: i32,
    leader: Option<NodeId>,
    noop_end: Option<i64>,
    /// Metadata log only: offsets below this are applied.
    applied: i64,
    /// A fresh group's designated first leader campaigns on every tick until this time or until
    /// a leader is known, since its peers may open the group only after its first campaign.
    // ponytail: only a fresh group's first election favours the preferred replica; later ones go to whoever times out first, and moving leadership back (preferred-leader election) is the upgrade.
    campaign_until: Option<Duration>,
}

impl Driver {
    fn keeps_campaigning(&self, now: Duration) -> bool {
        self.campaign_until.is_some_and(|until| now < until)
            && self.leader.is_none()
            && matches!(self.node.status().role, Role::Follower | Role::PreCandidate)
    }

    fn log(&self) -> tokio::sync::MutexGuard<'_, Log> {
        self.replica.log.blocking_lock()
    }

    fn step(&mut self, cluster: &Arc<Cluster>, from: NodeId, received: Received) {
        let Received {
            message,
            payload,
            ranges,
            ..
        } = received;
        match self.node.step(from, message, cluster.now()) {
            Ok(actions) => self.perform(cluster, actions, Some((&payload, &ranges))),
            Err(e) => debug!(key = ?self.replica.key, from, "dropping message: {e}"),
        }
    }

    fn propose(
        &mut self,
        mut records: Vec<u8>,
        batches: &[Range<usize>],
        reply: oneshot::Sender<Result<i64, i16>>,
    ) {
        let counts: Option<Vec<i32>> = batches
            .iter()
            .map(|range| {
                let frame = batch_frames(records.get(range.clone())?).next()?.ok()?;
                i32::try_from(frame.record_count).ok()
            })
            .collect();
        let Some(counts) = counts.filter(|counts| !counts.is_empty()) else {
            let _ = reply.send(Err(ResponseError::CorruptMessage.code()));
            return;
        };
        let replica = Arc::clone(&self.replica);
        let mut log = replica.log.blocking_lock();
        let (mut first, mut last) = (None, None);
        for (range, count) in batches.iter().zip(counts) {
            let proposal = match self.node.propose(count) {
                Ok(proposal) => proposal,
                Err(raft::RaftError::NotLeader { .. }) if first.is_none() => {
                    let _ = reply.send(Err(ResponseError::NotLeaderOrFollower.code()));
                    return;
                }
                Err(e) => fatal(format_args!(
                    "a proposal failed after earlier ones of its request: {e}"
                )),
            };
            let base = log
                .append(&mut records[range.clone()], proposal.epoch)
                .unwrap_or_else(|e| fatal(format_args!("appending a proposal: {e}")));
            if base != proposal.base_offset {
                fatal(format_args!(
                    "appended at {base}, but the core assigned {}",
                    proposal.base_offset
                ));
            }
            first.get_or_insert(base);
            last = Some((base + i64::from(count), proposal.epoch));
        }
        replica.log_end.store(log.end_offset(), Ordering::Release);
        drop(log);
        if let (Some(base), Some((end, epoch))) = (first, last) {
            self.pending.push_back(Pending {
                end,
                epoch,
                base,
                reply,
            });
            self.unflushed = true;
            self.kicked = true;
        }
    }

    fn perform(
        &mut self,
        cluster: &Arc<Cluster>,
        actions: Vec<Action>,
        received: Option<(&Bytes, &[Range<usize>])>,
    ) {
        let key = self.replica.key;
        for action in actions {
            match action {
                Action::PersistHardState(state) => write_hard_state(&self.dir, state)
                    .unwrap_or_else(|e| {
                        fatal(format_args!("persisting hard state of {key:?}: {e}"))
                    }),
                Action::Truncate { offset } => {
                    let mut log = self.log();
                    log.truncate(offset).unwrap_or_else(|e| {
                        fatal(format_args!("truncating {key:?} at {offset}: {e}"))
                    });
                    self.replica
                        .log_end
                        .store(log.end_offset(), Ordering::Release);
                    drop(log);
                    self.unflushed = true;
                }
                Action::AppendReceived { from_offset } => {
                    let Some((payload, ranges)) = received else {
                        fatal(format_args!("{key:?}: append without a received request"));
                    };
                    let start = ranges
                        .iter()
                        .position(|range| base_offset(&payload[range.clone()]) == from_offset)
                        .unwrap_or_else(|| {
                            fatal(format_args!("{key:?}: no received batch at {from_offset}"))
                        });
                    let mut log = self.log();
                    for range in &ranges[start..] {
                        log.append_replicated(&payload[range.clone()])
                            .unwrap_or_else(|e| {
                                fatal(format_args!("appending replicated batches of {key:?}: {e}"))
                            });
                    }
                    self.replica
                        .log_end
                        .store(log.end_offset(), Ordering::Release);
                    drop(log);
                    self.unflushed = true;
                }
                Action::AppendNoop { offset, epoch } => {
                    let mut batch = noop_control_batch(epoch, wall_clock_millis());
                    let mut log = self.log();
                    let base = log.append(&mut batch, epoch).unwrap_or_else(|e| {
                        fatal(format_args!("appending the no-op of {key:?}: {e}"))
                    });
                    if base != offset {
                        fatal(format_args!(
                            "{key:?}: no-op appended at {base}, not {offset}"
                        ));
                    }
                    self.replica
                        .log_end
                        .store(log.end_offset(), Ordering::Release);
                    drop(log);
                    self.noop_end = Some(offset + 1);
                    self.unflushed = true;
                }
                Action::Send { to, message } => {
                    cluster.send(
                        to,
                        Item::Raft {
                            group: key,
                            message,
                            payload: Bytes::new(),
                        },
                    );
                }
                Action::Replicate {
                    to,
                    request,
                    end_offset,
                    max_bytes,
                } => self.replicate(cluster, to, request, end_offset, max_bytes),
                Action::Commit { offset } => self.committed(cluster, offset),
                Action::LeaderChanged { term, leader } => {
                    self.leader_changed(cluster, term, leader);
                }
            }
        }
    }

    fn replicate(
        &self,
        cluster: &Cluster,
        to: NodeId,
        request: raft::AppendRequest,
        end_offset: i64,
        max_bytes: usize,
    ) {
        let key = self.replica.key;
        let located = self
            .log()
            .locate(request.prev_offset, max_bytes, true, end_offset);
        let extents = match located {
            Ok(extents) => extents,
            Err(e) => {
                warn!(?key, "locating batches to replicate failed: {e}");
                return;
            }
        };
        let mut payload = BytesMut::with_capacity(extents.iter().map(Extent::size).sum());
        for extent in &extents {
            if let Err(e) = extent.read_into(&mut payload) {
                warn!(?key, "reading batches to replicate failed: {e}");
                return;
            }
        }
        cluster.send(
            to,
            Item::Raft {
                group: key,
                message: raft::Message::AppendRequest(request),
                payload: payload.freeze(),
            },
        );
    }

    fn committed(&mut self, cluster: &Arc<Cluster>, offset: i64) {
        self.replica.commit.store(offset, Ordering::Release);
        if self.replica.key == GroupKey::METADATA {
            self.apply(cluster, offset);
        } else {
            cluster.commit_advanced();
        }
        while self.pending.front().is_some_and(|p| p.end <= offset) {
            if let Some(pending) = self.pending.pop_front() {
                let result = if self.leading == Some(pending.epoch) {
                    Ok(pending.base)
                } else {
                    Err(ResponseError::NotLeaderOrFollower.code())
                };
                let _ = pending.reply.send(result);
            }
        }
        self.publish(cluster);
    }

    /// Applies committed metadata entries below `commit`, in order.
    // ponytail: every start replays the metadata log from offset 0, with no snapshot; snapshotting the applied topics and offsets is the upgrade.
    fn apply(&mut self, cluster: &Arc<Cluster>, commit: i64) {
        while self.applied < commit {
            let extents = self
                .log()
                .locate(self.applied, APPLY_BYTES, true, commit)
                .unwrap_or_else(|e| fatal(format_args!("reading the metadata log: {e}")));
            let mut bytes = BytesMut::new();
            for extent in &extents {
                extent
                    .read_into(&mut bytes)
                    .unwrap_or_else(|e| fatal(format_args!("reading the metadata log: {e}")));
            }
            let bytes = bytes.freeze();
            let mut at = 0;
            for frame in batch_frames(&bytes) {
                let frame =
                    frame.unwrap_or_else(|e| fatal(format_args!("corrupt metadata log: {e}")));
                let mut batch = bytes.slice(at..at + frame.len);
                at += frame.len;
                let records = RecordBatchDecoder::decode(&mut batch)
                    .unwrap_or_else(|e| fatal(format_args!("corrupt metadata batch: {e}")));
                for record in records.records {
                    if let (false, Some(value)) = (record.control, record.value) {
                        cluster.apply(value, record.offset);
                    }
                }
                self.applied = frame.base_offset + frame.record_count;
            }
        }
        cluster.applied_to(self.applied);
    }

    fn leader_changed(&mut self, cluster: &Arc<Cluster>, term: i32, leader: Option<NodeId>) {
        let key = self.replica.key;
        if key == GroupKey::METADATA {
            info!(term, ?leader, "controller changed");
        } else {
            debug!(?key, term, ?leader, "leader changed");
        }
        self.leading = (leader == Some(cluster.id)).then_some(term);
        if self.leading.is_none() {
            self.noop_end = None;
        }
        let leading = self.leading;
        for pending in mem::take(&mut self.pending) {
            if leading == Some(pending.epoch) {
                self.pending.push_back(pending);
            } else {
                let _ = pending
                    .reply
                    .send(Err(ResponseError::NotLeaderOrFollower.code()));
            }
        }
        (self.term, self.leader) = (term, leader);
        self.publish(cluster);
        self.hint(cluster);
    }

    /// Publishes the leader and whether this node leads with a current commit offset.
    fn publish(&self, cluster: &Cluster) {
        let view = View {
            term: self.term,
            leader: self.leader,
            ready: self.leading.is_some()
                && self
                    .noop_end
                    .is_some_and(|end| self.replica.high_watermark() >= end),
        };
        let changed = mem::replace(&mut *lock(&self.replica.view), view) != view;
        if changed && self.replica.key == GroupKey::METADATA {
            cluster.controller_changed(view);
        }
    }

    /// Tells the nodes that are not replicas of this group that this node leads it.
    fn hint(&self, cluster: &Cluster) {
        let Some(term) = self.leading else {
            return;
        };
        let group = self.replica.key;
        if group == GroupKey::METADATA {
            return;
        }
        for node in cluster
            .node_ids()
            .filter(|id| !self.replica.voters.contains(id))
        {
            cluster.send(
                node,
                Item::Hint {
                    group,
                    term,
                    leader: cluster.id,
                },
            );
        }
    }

    fn start_flush(&mut self, cluster: &Cluster, shard: &Sender<Event>) {
        let key = self.replica.key;
        let point = self.node.flush_point();
        let flush = {
            let log = self.log();
            if log.end_offset() != point.end_offset() {
                fatal(format_args!(
                    "{key:?}: the log ends at {} but the core at {}",
                    log.end_offset(),
                    point.end_offset()
                ));
            }
            log.begin_flush()
                .unwrap_or_else(|e| fatal(format_args!("starting a flush of {key:?}: {e}")))
        };
        self.flushing = true;
        self.unflushed = false;
        let shard = shard.clone();
        cluster.blocking(move || {
            let synced = flush.sync();
            let _ = shard.send(Event::Flushed {
                key,
                flush,
                point,
                synced,
            });
        });
    }

    fn flushed(
        &mut self,
        cluster: &Arc<Cluster>,
        flush: &Flush,
        point: FlushPoint,
        synced: io::Result<()>,
    ) {
        let key = self.replica.key;
        self.flushing = false;
        self.log()
            .end_flush(flush, synced)
            .unwrap_or_else(|e| fatal(format_args!("flushing {key:?}: {e}")));
        let actions = self.node.flushed(point);
        self.perform(cluster, actions, None);
    }

    fn fail_pending(self, code: i16) {
        for pending in self.pending {
            let _ = pending.reply.send(Err(code));
        }
    }

    fn remove(self, cluster: &Cluster) {
        let key = self.replica.key;
        cluster.unregister(key);
        let dir = self.dir.clone();
        self.fail_pending(ResponseError::UnknownTopicOrPartition.code());
        info!(?key, "removed replica");
        remove_dir(cluster, dir);
    }
}

fn remove_dir(cluster: &Cluster, dir: PathBuf) {
    cluster.blocking(move || {
        if let Err(e) = fs::remove_dir_all(&dir)
            && e.kind() != io::ErrorKind::NotFound
        {
            warn!("removing {} failed: {e}", dir.display());
        }
    });
}

fn base_offset(batch: &[u8]) -> i64 {
    batch
        .get(..8)
        .and_then(|bytes| bytes.try_into().ok())
        .map_or(-1, i64::from_be_bytes)
}

fn wall_clock_millis() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| i64::try_from(d.as_millis()).unwrap_or(i64::MAX))
}

/// The hard state persisted in `dir`, or the initial one when there is none.
fn read_hard_state(dir: &Path) -> io::Result<HardState> {
    match fs::read_to_string(dir.join(HARD_STATE_FILE)) {
        Ok(text) => parse_hard_state(&text).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("corrupt {HARD_STATE_FILE}: {text:?}"),
            )
        }),
        Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(HardState::default()),
        Err(e) => Err(e),
    }
}

fn parse_hard_state(text: &str) -> Option<HardState> {
    let mut lines = text.lines();
    if lines.next()? != HARD_STATE_HEADER {
        return None;
    }
    let (term, vote) = lines.next()?.split_once(' ')?;
    if lines.next().is_some() {
        return None;
    }
    Some(HardState {
        term: term.parse().ok().filter(|term| *term >= 0)?,
        voted_for: match vote {
            "-" => None,
            id => Some(id.parse().ok()?),
        },
    })
}

fn write_hard_state(dir: &Path, state: HardState) -> io::Result<()> {
    let vote = state
        .voted_for
        .map_or_else(|| "-".to_owned(), |id| id.to_string());
    let text = format!("{HARD_STATE_HEADER}\n{} {vote}\n", state.term);
    replace_file(dir, HARD_STATE_FILE, text.as_bytes())
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hard_state_round_trips_and_rejects_garbage() {
        let dir = tempfile::tempdir().unwrap();
        assert_eq!(read_hard_state(dir.path()).unwrap(), HardState::default());
        for state in [
            HardState {
                term: 7,
                voted_for: Some(3),
            },
            HardState {
                term: 8,
                voted_for: None,
            },
        ] {
            write_hard_state(dir.path(), state).unwrap();
            assert_eq!(read_hard_state(dir.path()).unwrap(), state);
        }
        for garbage in [
            "",
            "redcrab raft-state 1\n",
            "redcrab raft-state 1\n-1 -\n",
            "redcrab raft-state 1\n3 x\n",
            "redcrab raft-state 1\n3 -\nextra\n",
            "redcrab raft-state 2\n3 -\n",
        ] {
            assert_eq!(parse_hard_state(garbage), None, "{garbage:?}");
        }
        fs::write(dir.path().join(HARD_STATE_FILE), "torn").unwrap();
        assert!(read_hard_state(dir.path()).is_err());
    }
}
