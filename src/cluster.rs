//! The cluster: static membership, the replicated metadata log and the state it builds (topics and
//! committed consumer offsets), the controller's admin operations and their forwarding from other
//! nodes, and node startup.
//!
//! The metadata log is one Raft group whose voters are every node, kept in `metadata/` of the data
//! directory with the same log and driver as partitions; its leader is the controller, which also
//! coordinates consumer groups. Entries are record batches of one record whose value is a command,
//! big-endian: `version: u8 = 1, kind: u8`, then for kind 1 (create topic) `id: uuid, name: str16,
//! partitions: u32` and per partition `replicas: u16` and that many `node: i32`, the first being the
//! preferred leader; for kind 2 (delete topic) `id: uuid`; for kind 3 (commit offsets) `group:
//! str16, count: u32` and per partition `topic: str16, partition: i32, offset: i64, leader_epoch:
//! i32, metadata_len: i32` (-1 for null) and the metadata. Every node applies committed entries in
//! log order, building its topic table and offsets and opening or removing its own replicas of the
//! partitions, which live in `partitions/<topic id>-<partition>/`. The commit offset restarts at 0,
//! so a restarted node rebuilds everything by replaying the log from the start once it relearns the
//! commit offset.

use std::collections::HashMap;
use std::fmt::Display;
use std::fs;
use std::io;
use std::net::TcpListener;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError, RwLock, Weak};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use bytes::{BufMut, Bytes, BytesMut};
use kafka_protocol::ResponseError;
use kafka_protocol::indexmap::IndexMap;
use kafka_protocol::protocol::StrBytes;
use kafka_protocol::records::{
    Compression, Record, RecordBatchEncoder, RecordEncodeOptions, TimestampType,
};
use tokio::sync::{oneshot, watch};
use tracing::{debug, error, info, warn};
use uuid::Uuid;

use crate::api::valid_topic_name;
use crate::group::{Committed, Offsets};
use crate::raft::{self, NodeId};
use crate::replica::{
    ELECTION_TIMEOUT_MAX, ELECTION_TIMEOUT_MIN, Event, GroupKey, HEARTBEAT_INTERVAL,
    MAX_APPEND_BYTES, Replica, Shards, View, open_group,
};
use crate::storage::replace_file;
use crate::transport::{Inbound, Inbox, Item, Reader, Transport, WireError, put_str16};

/// Most partitions a topic may have.
pub const MAX_PARTITIONS: i32 = 10_000;
/// Largest node id: node ids fill the top bits of producer ids.
pub const MAX_NODE_ID: NodeId = 32_767;
const DEFAULT_REPLICATION_FACTOR: usize = 3;
const NODE_FILE: &str = "node";
const NODE_HEADER: &str = "redcrab node 1";
const METADATA_DIR: &str = "metadata";
const PARTITIONS_DIR: &str = "partitions";
const COMMAND_VERSION: u8 = 1;
const CREATE_TOPIC: u8 = 1;
const DELETE_TOPIC: u8 = 2;
const COMMIT_OFFSETS: u8 = 3;
/// Largest command; bounds what one offset commit may carry.
const MAX_COMMAND_BYTES: usize = 4 << 20;
const FORWARD_CREATE: u8 = 1;
const FORWARD_DELETE: u8 = 2;
/// A leader hint older than this is forgotten; leaders repeat theirs every hint interval.
const HINT_TTL: Duration = Duration::from_secs(3);
const MIN_ADMIN_WAIT: Duration = Duration::from_secs(1);
const CONTROLLER_CATCH_UP: Duration = Duration::from_secs(5);
const POLL: Duration = Duration::from_millis(10);

/// One broker of the cluster.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Peer {
    /// Node id, also its Kafka broker id.
    pub id: NodeId,
    /// `host:port` of its internal RPC listener; unused in a cluster of one.
    pub rpc_address: String,
    /// Host Kafka clients are told to connect to.
    pub host: String,
    /// Port Kafka clients are told to connect to.
    pub port: u16,
}

/// A topic and where its partitions are replicated, as the metadata log built it.
#[derive(Debug)]
pub struct Topic {
    /// Topic name.
    pub name: String,
    /// Stable topic id.
    pub id: Uuid,
    /// Replicas of each partition, the preferred leader first.
    pub replicas: Vec<Vec<NodeId>>,
}

impl Topic {
    /// Number of partitions.
    #[must_use]
    pub fn partition_count(&self) -> i32 {
        i32::try_from(self.replicas.len()).unwrap_or(i32::MAX)
    }

    /// The group of partition `index`, if it exists.
    #[must_use]
    pub fn group(&self, index: i32) -> Option<GroupKey> {
        usize::try_from(index)
            .ok()
            .filter(|&at| at < self.replicas.len())
            .map(|_| GroupKey {
                topic: self.id,
                partition: index,
            })
    }
}

#[derive(Default)]
struct Topics {
    by_name: HashMap<String, Arc<Topic>>,
    by_id: HashMap<Uuid, Arc<Topic>>,
}

/// Why a node cannot start.
#[derive(Debug, thiserror::Error)]
pub enum StartError {
    /// The flags describe an impossible cluster.
    #[error("invalid cluster configuration: {0}")]
    Config(String),
    /// The data directory belongs to something else.
    #[error("data directory {}: {reason}", dir.display())]
    DataDir {
        /// The directory.
        dir: PathBuf,
        /// What is wrong with it.
        reason: String,
    },
    /// A file system or network operation failed.
    #[error(transparent)]
    Io(#[from] io::Error),
}

/// An admin request refused, with the Kafka error to report.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Refusal {
    /// Kafka error code.
    pub code: i16,
    /// Explanation for the client.
    pub message: Option<String>,
}

impl Refusal {
    fn new(error: ResponseError, message: impl Into<String>) -> Refusal {
        Refusal {
            code: error.code(),
            message: Some(message.into()),
        }
    }

    fn bare(error: ResponseError) -> Refusal {
        Refusal {
            code: error.code(),
            message: None,
        }
    }
}

/// A topic to create, as a client asked for it.
#[derive(Debug, Clone)]
pub struct TopicSpec {
    /// Topic name.
    pub name: String,
    /// Partition count, -1 for the default (one, or the number of assignments).
    pub partitions: i32,
    /// Replication factor, -1 for the default (three, or fewer in a smaller cluster).
    pub replication_factor: i16,
    /// Replicas of each partition in partition order, or empty to let the controller choose.
    pub assignments: Vec<Vec<NodeId>>,
}

/// A topic that was created.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CreatedTopic {
    /// Its id.
    pub id: Uuid,
    /// Its partition count.
    pub partitions: i32,
    /// Its replication factor.
    pub replication_factor: i16,
}

/// A topic to delete.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TopicRef {
    /// By name.
    Name(String),
    /// By id.
    Id(Uuid),
}

/// A validated topic creation, as forwarded to the controller.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Plan {
    name: String,
    partitions: i32,
    replication: usize,
    assignment: Vec<Vec<NodeId>>,
}

/// The controller's answer to an admin operation: the topic, and the metadata offset to apply
/// before the change is visible.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Answer {
    id: Uuid,
    name: String,
    partitions: i32,
    replication_factor: i16,
    applied: i64,
}

struct Hint {
    term: i32,
    leader: NodeId,
    at: Instant,
}

/// Shared state of this node of the cluster.
pub struct Cluster {
    /// This node.
    pub id: NodeId,
    /// Every node, sorted by id.
    pub peers: Vec<Peer>,
    data_dir: PathBuf,
    clock: Instant,
    seed: u64,
    runtime: tokio::runtime::Handle,
    shards: Shards,
    transport: Option<Transport>,
    topics: RwLock<Topics>,
    replicas: RwLock<HashMap<GroupKey, Arc<Replica>>>,
    hints: Mutex<HashMap<GroupKey, Hint>>,
    controller: watch::Sender<View>,
    applied: watch::Sender<i64>,
    // ponytail: one node-wide commit signal wakes every waiting fetch; per-partition wake-ups once long polls are many.
    commits: watch::Sender<u64>,
    // ponytail: every offset commit is an entry of the one metadata log, never compacted, so replay time and disk grow with commits; periodic snapshots of the applied state are the upgrade.
    offsets: RwLock<HashMap<StrBytes, Offsets>>,
    forwards: Mutex<HashMap<u64, oneshot::Sender<Bytes>>>,
    correlations: AtomicU64,
}

impl Cluster {
    /// Starts this node: checks the data directory, starts the shards, the transport and the
    /// metadata group. Must run inside a tokio runtime, whose blocking pool runs the fsyncs.
    ///
    /// # Errors
    ///
    /// Fails for an inconsistent configuration, a data directory of another node or format, or a
    /// thread or listener that cannot be started.
    pub(crate) async fn start(
        id: NodeId,
        peers: Vec<Peer>,
        data_dir: PathBuf,
        rpc_listener: Option<TcpListener>,
    ) -> Result<Arc<Cluster>, StartError> {
        let peers = validate_peers(id, peers)?;
        let voters: Vec<NodeId> = peers.iter().map(|p| p.id).collect();
        if voters.len() > 1 && rpc_listener.is_none() {
            return Err(StartError::Config(
                "a cluster of several nodes needs an RPC listener".into(),
            ));
        }
        let (dir, ids) = (data_dir.clone(), voters.clone());
        tokio::task::spawn_blocking(move || prepare_data_dir(&dir, id, &ids))
            .await
            .map_err(io::Error::other)??;
        let fingerprint = fingerprint(&voters);
        let others: Vec<(NodeId, String)> = peers
            .iter()
            .filter(|p| p.id != id)
            .map(|p| (p.id, p.rpc_address.clone()))
            .collect();
        let transport = (!others.is_empty()).then(|| Transport::start(id, fingerprint, &others));
        let cluster = Arc::new(Cluster {
            id,
            peers,
            data_dir,
            clock: Instant::now(),
            seed: Uuid::new_v4().as_u64_pair().0,
            runtime: tokio::runtime::Handle::current(),
            shards: Shards::new(),
            transport,
            topics: RwLock::default(),
            replicas: RwLock::default(),
            hints: Mutex::default(),
            controller: watch::channel(View::default()).0,
            applied: watch::channel(0).0,
            commits: watch::channel(0).0,
            offsets: RwLock::default(),
            forwards: Mutex::default(),
            correlations: AtomicU64::new(0),
        });
        cluster.shards.start(&cluster)?;
        if let (Some(transport), Some(listener)) = (&cluster.transport, rpc_listener) {
            let inbox = Arc::new(ClusterInbox(Arc::downgrade(&cluster)));
            transport.listen(listener, id, fingerprint, inbox)?;
        }
        let key = GroupKey::METADATA;
        open_group(&cluster, key, voters, cluster.group_dir(key));
        info!(node = id, nodes = cluster.peers.len(), "started");
        Ok(cluster)
    }

    /// Stops the shards and the transport. Replicas still referenced elsewhere stay readable.
    pub(crate) fn shutdown(&self) {
        self.shards.stop();
        if let Some(transport) = &self.transport {
            transport.shutdown();
        }
    }

    /// Waits until the metadata log has a leader and this node has caught up with it, and every
    /// local partition has a known leader, with a current high watermark where that leader is this
    /// node; `false` if that took longer than `timeout`.
    pub(crate) async fn wait_ready(&self, timeout: Duration) -> bool {
        let deadline = Instant::now() + timeout;
        loop {
            if self.ready() {
                return true;
            }
            if Instant::now() >= deadline {
                return false;
            }
            tokio::time::sleep(POLL).await;
        }
    }

    fn ready(&self) -> bool {
        let controller = *self.controller.borrow();
        let caught_up = match controller.leader {
            Some(leader) if leader == self.id => controller.ready,
            Some(_) => self
                .replica(GroupKey::METADATA)
                .is_some_and(|metadata| *self.applied.borrow() >= metadata.high_watermark()),
            None => false,
        };
        caught_up
            && self.topics().iter().all(|topic| {
                (0..topic.partition_count())
                    .filter_map(|index| topic.group(index))
                    .filter(|&key| self.hosts(key))
                    .all(|key| {
                        self.replica(key).is_some_and(|replica| {
                            let view = replica.view();
                            view.leader.is_some() && (view.leader != Some(self.id) || view.ready)
                        })
                    })
            })
    }

    /// Every node id, ascending.
    pub fn node_ids(&self) -> impl Iterator<Item = NodeId> + '_ {
        self.peers.iter().map(|p| p.id)
    }

    /// The node with `id`.
    #[must_use]
    pub fn peer(&self, id: NodeId) -> Option<&Peer> {
        self.peers.iter().find(|p| p.id == id)
    }

    /// Topic by name.
    pub fn topic(&self, name: &str) -> Option<Arc<Topic>> {
        read(&self.topics).by_name.get(name).cloned()
    }

    /// Topic by id.
    pub fn topic_by_id(&self, id: Uuid) -> Option<Arc<Topic>> {
        read(&self.topics).by_id.get(&id).cloned()
    }

    /// Every topic, sorted by name.
    pub fn topics(&self) -> Vec<Arc<Topic>> {
        let mut all: Vec<_> = read(&self.topics).by_name.values().cloned().collect();
        all.sort_by(|a, b| a.name.cmp(&b.name));
        all
    }

    /// This node's replica of `key`, once opened.
    pub fn replica(&self, key: GroupKey) -> Option<Arc<Replica>> {
        read(&self.replicas).get(&key).cloned()
    }

    /// Leader and term of `key`: this node's own view if it is a replica, otherwise the latest
    /// hint from the leader.
    pub fn leader(&self, key: GroupKey) -> Option<(NodeId, i32)> {
        if let Some(replica) = self.replica(key) {
            let view = replica.view();
            return view.leader.map(|leader| (leader, view.term));
        }
        lock(&self.hints)
            .get(&key)
            .filter(|hint| hint.at.elapsed() < HINT_TTL)
            .map(|hint| (hint.leader, hint.term))
    }

    /// The controller, the leader of the metadata log, if known.
    pub fn controller(&self) -> Option<NodeId> {
        self.controller.borrow().leader
    }

    /// Changes whenever the controller or its readiness changes.
    pub fn controller_changes(&self) -> watch::Receiver<View> {
        self.controller.subscribe()
    }

    /// Whether this node may act as group coordinator, which is the controller's job.
    ///
    /// # Errors
    ///
    /// Returns `NOT_COORDINATOR` on other nodes, `COORDINATOR_LOAD_IN_PROGRESS` while this node
    /// leads but has not caught up, and `COORDINATOR_NOT_AVAILABLE` while there is no controller.
    pub fn coordinator_check(&self) -> Result<(), i16> {
        let view = *self.controller.borrow();
        match view.leader {
            Some(leader) if leader == self.id && view.ready => Ok(()),
            Some(leader) if leader == self.id => {
                Err(ResponseError::CoordinatorLoadInProgress.code())
            }
            Some(_) => Err(ResponseError::NotCoordinator.code()),
            None => Err(ResponseError::CoordinatorNotAvailable.code()),
        }
    }

    /// Changes after every commit-offset advance of any partition.
    pub fn commits(&self) -> watch::Receiver<u64> {
        self.commits.subscribe()
    }

    /// Validates `spec`, has the controller create the topic, and returns once the creation is
    /// applied on this node and, within `wait`, every partition has a known leader.
    ///
    /// # Errors
    ///
    /// Returns the Kafka error to report for the topic.
    pub async fn create_topic(
        self: &Arc<Self>,
        spec: &TopicSpec,
        wait: Duration,
    ) -> Result<CreatedTopic, Refusal> {
        let deadline = Instant::now() + wait.max(MIN_ADMIN_WAIT);
        let plan = self.plan(spec)?;
        let answer = if self.controller() == Some(self.id) {
            self.create_here(plan).await?
        } else {
            let mut body = BytesMut::new();
            body.put_u8(FORWARD_CREATE);
            encode_plan(&mut body, &plan)
                .map_err(|e| Refusal::new(ResponseError::InvalidRequest, e.to_string()))?;
            self.forward(body.freeze(), deadline).await?
        };
        self.await_applied(answer.applied, deadline).await?;
        if let Some(topic) = self.topic_by_id(answer.id) {
            while Instant::now() < deadline
                && !(0..topic.partition_count())
                    .filter_map(|index| topic.group(index))
                    .all(|key| self.leader(key).is_some())
            {
                tokio::time::sleep(POLL).await;
            }
        }
        Ok(CreatedTopic {
            id: answer.id,
            partitions: answer.partitions,
            replication_factor: answer.replication_factor,
        })
    }

    /// Checks that `spec` could be created, without creating it.
    ///
    /// # Errors
    ///
    /// Returns the Kafka error to report for the topic.
    pub fn validate_topic(&self, spec: &TopicSpec) -> Result<CreatedTopic, Refusal> {
        let plan = self.plan(spec)?;
        if self.topic(&plan.name).is_some() {
            return Err(Refusal::bare(ResponseError::TopicAlreadyExists));
        }
        Ok(CreatedTopic {
            id: Uuid::nil(),
            partitions: plan.partitions,
            replication_factor: i16::try_from(plan.replication).unwrap_or(i16::MAX),
        })
    }

    /// Has the controller delete a topic and returns its id and name once the deletion is applied
    /// on this node.
    ///
    /// # Errors
    ///
    /// Returns the Kafka error to report for the topic.
    pub async fn delete_topic(
        self: &Arc<Self>,
        target: TopicRef,
        wait: Duration,
    ) -> Result<(Uuid, String), Refusal> {
        let deadline = Instant::now() + wait.max(MIN_ADMIN_WAIT);
        let answer = if self.controller() == Some(self.id) {
            self.delete_here(target).await?
        } else {
            let mut body = BytesMut::new();
            body.put_u8(FORWARD_DELETE);
            match &target {
                TopicRef::Name(name) => {
                    body.put_u8(1);
                    put_str16(&mut body, name)
                        .map_err(|e| Refusal::new(ResponseError::InvalidRequest, e.to_string()))?;
                }
                TopicRef::Id(id) => {
                    body.put_u8(0);
                    body.put_slice(id.as_bytes());
                }
            }
            self.forward(body.freeze(), deadline).await?
        };
        self.await_applied(answer.applied, deadline).await?;
        Ok((answer.id, answer.name))
    }

    /// Commits a consumer group's offsets through the metadata log, as its coordinator; they are
    /// durable on a majority, and applied on this node, when this returns.
    ///
    /// # Errors
    ///
    /// Returns the Kafka error code to report for every partition of the commit.
    pub async fn commit_offsets(
        &self,
        group: &StrBytes,
        offsets: Vec<((StrBytes, i32), Committed)>,
    ) -> Result<(), i16> {
        let command = Command::CommitOffsets {
            group: group.to_string(),
            offsets,
        };
        if command.encode().is_err() {
            return Err(ResponseError::InvalidCommitOffsetSize.code());
        }
        self.coordinator_check()?;
        self.propose_metadata(&command)
            .await
            .map(|_| ())
            .map_err(|_| ResponseError::NotCoordinator.code())
    }

    /// The group's committed offsets, as applied on this node.
    pub fn committed(&self, group: &StrBytes) -> Offsets {
        read(&self.offsets).get(group).cloned().unwrap_or_default()
    }

    /// Whether the group has committed offsets.
    pub fn has_offsets(&self, group: &StrBytes) -> bool {
        read(&self.offsets)
            .get(group)
            .is_some_and(|offsets| !offsets.is_empty())
    }

    /// Every group with committed offsets.
    pub fn groups_with_offsets(&self) -> Vec<StrBytes> {
        read(&self.offsets)
            .iter()
            .filter(|(_, offsets)| !offsets.is_empty())
            .map(|(group, _)| group.clone())
            .collect()
    }

    fn plan(&self, spec: &TopicSpec) -> Result<Plan, Refusal> {
        if !valid_topic_name(&spec.name) {
            return Err(Refusal::new(
                ResponseError::InvalidTopicException,
                "topic names are 1-249 characters of [a-zA-Z0-9._-]",
            ));
        }
        let nodes = self.peers.len();
        if !spec.assignments.is_empty() {
            if spec.partitions != -1 || spec.replication_factor != -1 {
                return Err(Refusal::new(
                    ResponseError::InvalidRequest,
                    "with explicit assignments, partition count and replication factor must be -1",
                ));
            }
            if spec.assignments.len() > usize::try_from(MAX_PARTITIONS).unwrap_or(usize::MAX) {
                return Err(Refusal::new(
                    ResponseError::InvalidPartitions,
                    format!("partition count must be between 1 and {MAX_PARTITIONS}"),
                ));
            }
            let replication = spec.assignments[0].len();
            for replicas in &spec.assignments {
                let mut distinct = replicas.clone();
                distinct.sort_unstable();
                distinct.dedup();
                if replicas.is_empty()
                    || replicas.len() != replication
                    || distinct.len() != replicas.len()
                    || replicas.iter().any(|id| self.peer(*id).is_none())
                {
                    return Err(Refusal::new(
                        ResponseError::InvalidReplicaAssignment,
                        "every partition needs the same number of distinct, existing brokers",
                    ));
                }
            }
            return Ok(Plan {
                name: spec.name.clone(),
                partitions: i32::try_from(spec.assignments.len()).unwrap_or(i32::MAX),
                replication,
                assignment: spec.assignments.clone(),
            });
        }
        let partitions = if spec.partitions == -1 {
            1
        } else {
            spec.partitions
        };
        if !(1..=MAX_PARTITIONS).contains(&partitions) {
            return Err(Refusal::new(
                ResponseError::InvalidPartitions,
                format!("partition count must be between 1 and {MAX_PARTITIONS}"),
            ));
        }
        let replication = match spec.replication_factor {
            -1 => DEFAULT_REPLICATION_FACTOR.min(nodes),
            factor => usize::try_from(factor).unwrap_or(0),
        };
        if !(1..=nodes).contains(&replication) {
            return Err(Refusal::new(
                ResponseError::InvalidReplicationFactor,
                format!("replication factor must be between 1 and {nodes}, the number of brokers"),
            ));
        }
        Ok(Plan {
            name: spec.name.clone(),
            partitions,
            replication,
            assignment: Vec::new(),
        })
    }

    /// Replicas for a new topic: consecutive nodes in id order, starting one further for each
    /// partition, so leaders and replicas spread evenly; the starting node continues from the
    /// partitions that already exist.
    fn assign(&self, partitions: i32, replication: usize) -> Vec<Vec<NodeId>> {
        let ids: Vec<NodeId> = self.node_ids().collect();
        let start: usize = read(&self.topics)
            .by_id
            .values()
            .map(|topic| topic.replicas.len())
            .sum();
        (0..usize::try_from(partitions).unwrap_or(0))
            .map(|partition| {
                (0..replication)
                    .map(|replica| ids[(start + partition + replica) % ids.len()])
                    .collect()
            })
            .collect()
    }

    /// Waits a little for a newly elected controller to catch up with the metadata log.
    async fn controller_ready(&self) -> Result<(), Refusal> {
        let mut changes = self.controller.subscribe();
        let settled = changes.wait_for(|view| view.leader != Some(self.id) || view.ready);
        let ready = tokio::time::timeout(CONTROLLER_CATCH_UP, settled)
            .await
            .ok()
            .and_then(Result::ok)
            .is_some_and(|view| view.leader == Some(self.id) && view.ready);
        if ready {
            Ok(())
        } else {
            Err(Refusal::new(
                ResponseError::NotController,
                "this broker is not the controller",
            ))
        }
    }

    async fn create_here(&self, plan: Plan) -> Result<Answer, Refusal> {
        self.controller_ready().await?;
        if self.topic(&plan.name).is_some() {
            return Err(Refusal::bare(ResponseError::TopicAlreadyExists));
        }
        let id = Uuid::new_v4();
        let replicas = if plan.assignment.is_empty() {
            self.assign(plan.partitions, plan.replication)
        } else {
            plan.assignment
        };
        let command = Command::CreateTopic {
            id,
            name: plan.name.clone(),
            replicas,
        };
        let applied = self.propose_metadata(&command).await?;
        match self.topic(&plan.name) {
            Some(topic) if topic.id == id => Ok(Answer {
                id,
                name: plan.name,
                partitions: plan.partitions,
                replication_factor: i16::try_from(plan.replication).unwrap_or(i16::MAX),
                applied,
            }),
            _ => Err(Refusal::bare(ResponseError::TopicAlreadyExists)),
        }
    }

    async fn delete_here(&self, target: TopicRef) -> Result<Answer, Refusal> {
        self.controller_ready().await?;
        let topic = match &target {
            TopicRef::Name(name) => self
                .topic(name)
                .ok_or(Refusal::bare(ResponseError::UnknownTopicOrPartition))?,
            TopicRef::Id(id) => self
                .topic_by_id(*id)
                .ok_or(Refusal::bare(ResponseError::UnknownTopicId))?,
        };
        let applied = self
            .propose_metadata(&Command::DeleteTopic { id: topic.id })
            .await?;
        Ok(Answer {
            id: topic.id,
            name: topic.name.clone(),
            partitions: topic.partition_count(),
            replication_factor: -1,
            applied,
        })
    }

    /// Appends `command` to the metadata log as its leader and returns the offset after it once it
    /// is committed and applied here.
    async fn propose_metadata(&self, command: &Command) -> Result<i64, Refusal> {
        let not_controller = || Refusal::new(ResponseError::NotController, "controller changed");
        let batch = command_batch(command)
            .map_err(|e| Refusal::new(ResponseError::InvalidRequest, e.to_string()))?;
        let replica = self
            .replica(GroupKey::METADATA)
            .ok_or_else(not_controller)?;
        let len = batch.len();
        match replica
            .propose(batch, std::iter::once(0..len).collect())
            .await
        {
            Ok(Ok(base)) => Ok(base + 1),
            _ => Err(not_controller()),
        }
    }

    async fn await_applied(&self, offset: i64, deadline: Instant) -> Result<(), Refusal> {
        let mut applied = self.applied.subscribe();
        match tokio::time::timeout_at(deadline.into(), applied.wait_for(|&at| at >= offset)).await {
            Ok(Ok(_)) => Ok(()),
            _ => Err(Refusal::new(
                ResponseError::RequestTimedOut,
                "the change is not applied on this broker yet",
            )),
        }
    }

    async fn forward(&self, body: Bytes, deadline: Instant) -> Result<Answer, Refusal> {
        let Some(controller) = self.controller() else {
            return Err(Refusal::new(
                ResponseError::NotController,
                "there is no controller right now",
            ));
        };
        let correlation = self.correlations.fetch_add(1, Ordering::Relaxed);
        let (reply, answered) = oneshot::channel();
        lock(&self.forwards).insert(correlation, reply);
        let sent = self.send(controller, Item::Request { correlation, body });
        let answer = if sent {
            tokio::time::timeout_at(deadline.into(), answered)
                .await
                .ok()
        } else {
            None
        };
        lock(&self.forwards).remove(&correlation);
        match answer {
            Some(Ok(body)) => decode_answer(body).unwrap_or_else(|e| {
                Err(Refusal::new(
                    ResponseError::UnknownServerError,
                    e.to_string(),
                ))
            }),
            Some(Err(_)) | None if sent => Err(Refusal::new(
                ResponseError::RequestTimedOut,
                "the controller did not answer in time",
            )),
            _ => Err(Refusal::new(
                ResponseError::NotController,
                "the controller cannot be reached",
            )),
        }
    }

    async fn answer_forward(&self, body: Bytes) -> Bytes {
        let result = match decode_forward(body) {
            Ok(Forward::Create(plan)) => self.create_here(plan).await,
            Ok(Forward::Delete(target)) => self.delete_here(target).await,
            Err(e) => Err(Refusal::new(ResponseError::InvalidRequest, e.to_string())),
        };
        encode_answer(&result)
    }

    // --- Used by the shards ---

    pub(crate) fn shards(&self) -> &Shards {
        &self.shards
    }

    pub(crate) fn now(&self) -> Duration {
        self.clock.elapsed()
    }

    pub(crate) fn blocking(&self, work: impl FnOnce() + Send + 'static) {
        drop(self.runtime.spawn_blocking(work));
    }

    pub(crate) fn raft_config(&self, key: GroupKey, voters: &[NodeId]) -> raft::Config {
        let mut seed = self.seed ^ key.topic.as_u64_pair().1;
        seed ^= u64::from(key.partition.cast_unsigned()).wrapping_mul(0x9E37_79B9_7F4A_7C15);
        raft::Config {
            id: self.id,
            voters: voters.to_vec(),
            election_timeout_min: ELECTION_TIMEOUT_MIN,
            election_timeout_max: ELECTION_TIMEOUT_MAX,
            heartbeat_interval: HEARTBEAT_INTERVAL,
            max_append_bytes: MAX_APPEND_BYTES,
            seed,
        }
    }

    pub(crate) fn group_dir(&self, key: GroupKey) -> PathBuf {
        if key == GroupKey::METADATA {
            self.data_dir.join(METADATA_DIR)
        } else {
            self.data_dir
                .join(PARTITIONS_DIR)
                .join(format!("{}-{}", key.topic, key.partition))
        }
    }

    /// Whether this node is a replica of `key` according to the topic table.
    pub(crate) fn hosts(&self, key: GroupKey) -> bool {
        key == GroupKey::METADATA
            || self.topic_by_id(key.topic).is_some_and(|topic| {
                usize::try_from(key.partition)
                    .ok()
                    .and_then(|at| topic.replicas.get(at))
                    .is_some_and(|replicas| replicas.contains(&self.id))
            })
    }

    pub(crate) fn register(&self, replica: Arc<Replica>) {
        write(&self.replicas).insert(replica.key, replica);
    }

    pub(crate) fn unregister(&self, key: GroupKey) {
        write(&self.replicas).remove(&key);
    }

    pub(crate) fn send(&self, to: NodeId, item: Item) -> bool {
        self.transport
            .as_ref()
            .is_some_and(|transport| transport.send(to, item))
    }

    pub(crate) fn commit_advanced(&self) {
        self.commits.send_modify(|n| *n = n.wrapping_add(1));
    }

    pub(crate) fn applied_to(&self, offset: i64) {
        self.applied.send_if_modified(|applied| {
            let advanced = offset > *applied;
            *applied = (*applied).max(offset);
            advanced
        });
    }

    pub(crate) fn controller_changed(&self, view: View) {
        self.controller.send_if_modified(|current| {
            let changed = *current != view;
            *current = view;
            changed
        });
    }

    /// Applies one committed metadata command found at `offset`.
    pub(crate) fn apply(self: &Arc<Self>, value: Bytes, offset: i64) {
        match Command::decode(value) {
            Err(e) => warn!(
                offset,
                "ignoring a metadata entry that does not decode: {e}"
            ),
            Ok(Command::CreateTopic { id, name, replicas }) => {
                self.apply_create(id, name, replicas, offset);
            }
            Ok(Command::DeleteTopic { id }) => self.apply_delete(id),
            Ok(Command::CommitOffsets { group, offsets }) => {
                write(&self.offsets)
                    .entry(StrBytes::from_string(group))
                    .or_default()
                    .extend(offsets);
            }
        }
    }

    fn apply_create(
        self: &Arc<Self>,
        id: Uuid,
        name: String,
        replicas: Vec<Vec<NodeId>>,
        offset: i64,
    ) {
        let valid = valid_topic_name(&name)
            && !id.is_nil()
            && (1..=usize::try_from(MAX_PARTITIONS).unwrap_or(0)).contains(&replicas.len())
            && replicas.iter().all(|nodes| {
                let mut distinct = nodes.clone();
                distinct.sort_unstable();
                distinct.dedup();
                !nodes.is_empty()
                    && distinct.len() == nodes.len()
                    && nodes.iter().all(|node| self.peer(*node).is_some())
            });
        if !valid {
            warn!(offset, topic = %name, "ignoring an invalid create-topic entry");
            return;
        }
        let topic = Arc::new(Topic { name, id, replicas });
        {
            let mut topics = write(&self.topics);
            if topics.by_name.contains_key(&topic.name) || topics.by_id.contains_key(&id) {
                debug!(offset, topic = %topic.name, "topic exists already; ignoring its second creation");
                return;
            }
            topics
                .by_name
                .insert(topic.name.clone(), Arc::clone(&topic));
            topics.by_id.insert(id, Arc::clone(&topic));
        }
        info!(topic = %topic.name, %id, partitions = topic.replicas.len(), "topic created");
        for index in 0..topic.partition_count() {
            if let Some(key) = topic.group(index).filter(|&key| self.hosts(key)) {
                let voters = topic.replicas[usize::try_from(index).unwrap_or(0)].clone();
                open_group(self, key, voters, self.group_dir(key));
            }
        }
    }

    fn apply_delete(&self, id: Uuid) {
        let Some(topic) = ({
            let mut topics = write(&self.topics);
            let removed = topics.by_id.remove(&id);
            if let Some(topic) = &removed {
                topics.by_name.remove(&topic.name);
            }
            removed
        }) else {
            return;
        };
        info!(topic = %topic.name, %id, "topic deleted");
        for offsets in write(&self.offsets).values_mut() {
            offsets.retain(|(name, _), _| name.as_str() != topic.name);
        }
        let mut hints = lock(&self.hints);
        for index in 0..topic.partition_count() {
            let Some(key) = topic.group(index) else {
                continue;
            };
            hints.remove(&key);
            if topic.replicas[usize::try_from(index).unwrap_or(0)].contains(&self.id) {
                self.shards.send(key, Event::Remove(key));
            }
        }
    }

    fn hint(&self, group: GroupKey, term: i32, leader: NodeId) {
        if term < 0 || self.peer(leader).is_none() || self.topic_by_id(group.topic).is_none() {
            return;
        }
        let mut hints = lock(&self.hints);
        if hints.get(&group).is_none_or(|hint| hint.term <= term) {
            hints.insert(
                group,
                Hint {
                    term,
                    leader,
                    at: Instant::now(),
                },
            );
        }
    }
}

/// Receives for the cluster without keeping it alive.
struct ClusterInbox(Weak<Cluster>);

impl Inbox for ClusterInbox {
    fn deliver(&self, from: NodeId, items: Vec<Inbound>) {
        let Some(cluster) = self.0.upgrade() else {
            return;
        };
        let mut raft = Vec::new();
        for item in items {
            match item {
                Inbound::Raft(received) => raft.push(received),
                Inbound::Hint {
                    group,
                    term,
                    leader,
                } => cluster.hint(group, term, leader),
                Inbound::Request { correlation, body } => {
                    let answering = Arc::clone(&cluster);
                    cluster.runtime.spawn(async move {
                        let body = answering.answer_forward(body).await;
                        answering.send(from, Item::Response { correlation, body });
                    });
                }
                Inbound::Response { correlation, body } => {
                    if let Some(reply) = lock(&cluster.forwards).remove(&correlation) {
                        let _ = reply.send(body);
                    }
                }
            }
        }
        if !raft.is_empty() {
            cluster.shards.deliver(from, raft);
        }
    }
}

/// Logs why the process cannot go on, then exits: after a failed write or fsync, carrying on could
/// acknowledge or vote on data that is not durable.
pub(crate) fn fatal(what: impl Display) -> ! {
    error!("{what}; exiting, since carrying on could break the replication guarantees");
    std::process::exit(70)
}

// ponytail: membership is static, fixed by the first start and checked on every start; adding or replacing nodes needs Raft membership changes and partition reassignment.
fn validate_peers(id: NodeId, mut peers: Vec<Peer>) -> Result<Vec<Peer>, StartError> {
    let invalid = |reason: String| Err(StartError::Config(reason));
    peers.sort_by_key(|p| p.id);
    if let Some(pair) = peers.windows(2).find(|pair| pair[0].id == pair[1].id) {
        return invalid(format!("node {} is listed twice", pair[0].id));
    }
    if let Some(peer) = peers.iter().find(|p| !(0..=MAX_NODE_ID).contains(&p.id)) {
        return invalid(format!("node id {} is outside 0..={MAX_NODE_ID}", peer.id));
    }
    if !peers.iter().any(|p| p.id == id) {
        return invalid(format!("this node, {id}, is not among the cluster's nodes"));
    }
    if let Some(peer) = peers.iter().find(|p| p.host.is_empty() || p.port == 0) {
        return invalid(format!("node {} has no Kafka address", peer.id));
    }
    if peers.len() > 1
        && let Some(peer) = peers.iter().find(|p| p.rpc_address.is_empty())
    {
        return invalid(format!("node {} has no RPC address", peer.id));
    }
    Ok(peers)
}

/// Checks that `dir` holds this node's data, or is new or empty and then claims it.
fn prepare_data_dir(dir: &Path, id: NodeId, voters: &[NodeId]) -> Result<(), StartError> {
    let fail = |reason: String| StartError::DataDir {
        dir: dir.to_path_buf(),
        reason,
    };
    let ids: Vec<String> = voters.iter().map(ToString::to_string).collect();
    let identity = format!("{NODE_HEADER}\nid {id}\nvoters {}\n", ids.join(","));
    match fs::read_to_string(dir.join(NODE_FILE)) {
        Ok(found) if found == identity => return Ok(()),
        Ok(found) => {
            return Err(fail(format!(
                "it belongs to {:?}, not to this node {:?}; data cannot move between nodes and the \
                 cluster's membership cannot change",
                found.trim_end(),
                identity.trim_end()
            )));
        }
        Err(e) if e.kind() == io::ErrorKind::NotFound => {}
        Err(e) => return Err(e.into()),
    }
    if dir.exists() && fs::read_dir(dir)?.next().is_some() {
        return Err(fail(
            "it is not empty but has no redcrab node file, so it was written by a redcrab from \
             before replication or by something else; this version cannot read it, so remove it or \
             use another --data-dir"
                .into(),
        ));
    }
    fs::create_dir_all(dir)?;
    replace_file(dir, NODE_FILE, identity.as_bytes())?;
    Ok(())
}

/// FNV-1a of the voter ids, so peers started with different node lists refuse each other.
fn fingerprint(voters: &[NodeId]) -> u64 {
    voters
        .iter()
        .flat_map(|id| id.to_be_bytes())
        .fold(0xcbf2_9ce4_8422_2325, |hash, byte| {
            (hash ^ u64::from(byte)).wrapping_mul(0x0100_0000_01b3)
        })
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum Command {
    CreateTopic {
        id: Uuid,
        name: String,
        replicas: Vec<Vec<NodeId>>,
    },
    DeleteTopic {
        id: Uuid,
    },
    CommitOffsets {
        group: String,
        offsets: Vec<((StrBytes, i32), Committed)>,
    },
}

impl Command {
    fn encode(&self) -> Result<Bytes, WireError> {
        let mut out = BytesMut::new();
        out.put_u8(COMMAND_VERSION);
        match self {
            Command::CreateTopic { id, name, replicas } => {
                out.put_u8(CREATE_TOPIC);
                out.put_slice(id.as_bytes());
                put_str16(&mut out, name)?;
                put_replicas(&mut out, replicas)?;
            }
            Command::DeleteTopic { id } => {
                out.put_u8(DELETE_TOPIC);
                out.put_slice(id.as_bytes());
            }
            Command::CommitOffsets { group, offsets } => {
                out.put_u8(COMMIT_OFFSETS);
                put_str16(&mut out, group)?;
                out.put_u32(
                    u32::try_from(offsets.len())
                        .map_err(|_| WireError::Malformed("too many offsets"))?,
                );
                for ((topic, partition), committed) in offsets {
                    put_str16(&mut out, topic)?;
                    out.put_i32(*partition);
                    out.put_i64(committed.offset);
                    out.put_i32(committed.leader_epoch);
                    match &committed.metadata {
                        None => out.put_i32(-1),
                        Some(metadata) => {
                            out.put_i32(
                                i32::try_from(metadata.len())
                                    .map_err(|_| WireError::Malformed("metadata too long"))?,
                            );
                            out.put_slice(metadata.as_bytes());
                        }
                    }
                }
            }
        }
        if out.len() > MAX_COMMAND_BYTES {
            return Err(WireError::Malformed("command too large"));
        }
        Ok(out.freeze())
    }

    fn decode(value: Bytes) -> Result<Command, WireError> {
        let mut reader = Reader::new(value);
        if reader.u8()? != COMMAND_VERSION {
            return Err(WireError::Malformed("unknown command version"));
        }
        let command = match reader.u8()? {
            CREATE_TOPIC => Command::CreateTopic {
                id: reader.uuid()?,
                name: reader.str16()?,
                replicas: read_replicas(&mut reader)?,
            },
            DELETE_TOPIC => Command::DeleteTopic { id: reader.uuid()? },
            COMMIT_OFFSETS => {
                let group = reader.str16()?;
                let count = reader.len32()?;
                if count > reader.remaining() {
                    return Err(WireError::Malformed("more offsets than bytes"));
                }
                let offsets = (0..count)
                    .map(|_| {
                        let topic = StrBytes::from_string(reader.str16()?);
                        let partition = reader.i32()?;
                        let offset = reader.i64()?;
                        let leader_epoch = reader.i32()?;
                        let metadata = match reader.i32()? {
                            -1 => None,
                            len => {
                                let len = usize::try_from(len)
                                    .map_err(|_| WireError::Malformed("negative length"))?;
                                let bytes = reader.bytes(len)?;
                                Some(StrBytes::from_string(
                                    String::from_utf8(bytes.to_vec()).map_err(|_| {
                                        WireError::Malformed("metadata is not UTF-8")
                                    })?,
                                ))
                            }
                        };
                        Ok((
                            (topic, partition),
                            Committed {
                                offset,
                                leader_epoch,
                                metadata,
                            },
                        ))
                    })
                    .collect::<Result<_, WireError>>()?;
                Command::CommitOffsets { group, offsets }
            }
            _ => return Err(WireError::Malformed("unknown command")),
        };
        reader.finish()?;
        Ok(command)
    }
}

fn put_replicas(out: &mut BytesMut, replicas: &[Vec<NodeId>]) -> Result<(), WireError> {
    out.put_u32(
        u32::try_from(replicas.len()).map_err(|_| WireError::Malformed("too many partitions"))?,
    );
    for nodes in replicas {
        out.put_u16(
            u16::try_from(nodes.len()).map_err(|_| WireError::Malformed("too many replicas"))?,
        );
        for node in nodes {
            out.put_i32(*node);
        }
    }
    Ok(())
}

fn read_replicas(reader: &mut Reader) -> Result<Vec<Vec<NodeId>>, WireError> {
    let partitions = reader.len32()?;
    if partitions > usize::try_from(MAX_PARTITIONS).unwrap_or(0) {
        return Err(WireError::Malformed("too many partitions"));
    }
    (0..partitions)
        .map(|_| {
            let count = reader.u16()?;
            (0..count).map(|_| reader.i32()).collect()
        })
        .collect()
}

/// One record batch holding `command`, to append to the metadata log.
fn command_batch(command: &Command) -> Result<Vec<u8>, WireError> {
    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| i64::try_from(d.as_millis()).unwrap_or(i64::MAX));
    let record = Record {
        transactional: false,
        control: false,
        delete_horizon: false,
        partition_leader_epoch: -1,
        producer_id: -1,
        producer_epoch: -1,
        timestamp_type: TimestampType::Creation,
        offset: 0,
        sequence: -1,
        timestamp,
        key: None,
        value: Some(command.encode()?),
        headers: IndexMap::new(),
    };
    let mut batch = BytesMut::new();
    let options = RecordEncodeOptions {
        version: 2,
        compression: Compression::None,
    };
    RecordBatchEncoder::encode(&mut batch, [&record], &options)
        .map_err(|_| WireError::Malformed("command does not encode"))?;
    Ok(batch.to_vec())
}

enum Forward {
    Create(Plan),
    Delete(TopicRef),
}

fn encode_plan(out: &mut BytesMut, plan: &Plan) -> Result<(), WireError> {
    put_str16(out, &plan.name)?;
    out.put_i32(plan.partitions);
    out.put_u16(
        u16::try_from(plan.replication).map_err(|_| WireError::Malformed("replication factor"))?,
    );
    put_replicas(out, &plan.assignment)
}

fn decode_forward(body: Bytes) -> Result<Forward, WireError> {
    let mut reader = Reader::new(body);
    let forward = match reader.u8()? {
        FORWARD_CREATE => Forward::Create(Plan {
            name: reader.str16()?,
            partitions: reader.i32()?,
            replication: usize::from(reader.u16()?),
            assignment: read_replicas(&mut reader)?,
        }),
        FORWARD_DELETE => Forward::Delete(if reader.flag()? {
            TopicRef::Name(reader.str16()?)
        } else {
            TopicRef::Id(reader.uuid()?)
        }),
        _ => return Err(WireError::Malformed("unknown forwarded request")),
    };
    reader.finish()?;
    Ok(forward)
}

fn encode_answer(result: &Result<Answer, Refusal>) -> Bytes {
    let mut out = BytesMut::new();
    match result {
        Ok(answer) => {
            out.put_i16(0);
            out.put_slice(answer.id.as_bytes());
            let _ = put_str16(&mut out, &answer.name);
            out.put_i32(answer.partitions);
            out.put_i16(answer.replication_factor);
            out.put_i64(answer.applied);
        }
        Err(refusal) => {
            out.put_i16(if refusal.code == 0 {
                ResponseError::UnknownServerError.code()
            } else {
                refusal.code
            });
            let message = refusal.message.as_deref().unwrap_or_default();
            let mut end = message.len().min(1024);
            while !message.is_char_boundary(end) {
                end -= 1;
            }
            let _ = put_str16(&mut out, &message[..end]);
        }
    }
    out.freeze()
}

fn decode_answer(body: Bytes) -> Result<Result<Answer, Refusal>, WireError> {
    let mut reader = Reader::new(body);
    let code = reader.i16()?;
    let result = if code == 0 {
        Ok(Answer {
            id: reader.uuid()?,
            name: reader.str16()?,
            partitions: reader.i32()?,
            replication_factor: reader.i16()?,
            applied: reader.i64()?,
        })
    } else {
        let message = reader.str16()?;
        Err(Refusal {
            code,
            message: (!message.is_empty()).then_some(message),
        })
    };
    reader.finish()?;
    Ok(result)
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

fn read<T>(lock: &RwLock<T>) -> std::sync::RwLockReadGuard<'_, T> {
    lock.read().unwrap_or_else(PoisonError::into_inner)
}

fn write<T>(lock: &RwLock<T>) -> std::sync::RwLockWriteGuard<'_, T> {
    lock.write().unwrap_or_else(PoisonError::into_inner)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn commands_round_trip_and_reject_garbage() {
        let commands = [
            Command::CreateTopic {
                id: Uuid::from_u128(5),
                name: "orders".into(),
                replicas: vec![vec![1, 2, 3], vec![2, 3, 1]],
            },
            Command::DeleteTopic {
                id: Uuid::from_u128(5),
            },
            Command::CommitOffsets {
                group: "g".into(),
                offsets: vec![
                    (
                        (StrBytes::from_static_str("orders"), 1),
                        Committed {
                            offset: 42,
                            leader_epoch: 3,
                            metadata: Some(StrBytes::from_static_str("meta")),
                        },
                    ),
                    (
                        (StrBytes::from_static_str("orders"), 2),
                        Committed {
                            offset: 7,
                            leader_epoch: -1,
                            metadata: None,
                        },
                    ),
                ],
            },
        ];
        for command in commands {
            let encoded = command.encode().unwrap();
            assert_eq!(Command::decode(encoded.clone()).unwrap(), command);
            for cut in 0..encoded.len() {
                assert!(Command::decode(encoded.slice(..cut)).is_err());
            }
            let mut longer = encoded.to_vec();
            longer.push(0);
            assert!(Command::decode(Bytes::from(longer)).is_err());
        }
        assert!(Command::decode(Bytes::from_static(&[2, 1])).is_err());
        assert!(Command::decode(Bytes::from_static(&[1, 9])).is_err());
    }

    #[test]
    fn forwarded_requests_and_answers_round_trip() {
        let plan = Plan {
            name: "events".into(),
            partitions: 2,
            replication: 3,
            assignment: vec![vec![1, 2, 3], vec![3, 1, 2]],
        };
        let mut body = BytesMut::new();
        body.put_u8(FORWARD_CREATE);
        encode_plan(&mut body, &plan).unwrap();
        assert!(matches!(decode_forward(body.freeze()), Ok(Forward::Create(p)) if p == plan));
        let answer = Answer {
            id: Uuid::from_u128(9),
            name: "events".into(),
            partitions: 2,
            replication_factor: 3,
            applied: 42,
        };
        assert_eq!(
            decode_answer(encode_answer(&Ok(answer.clone()))).unwrap(),
            Ok(answer)
        );
        let refusal = Refusal::new(ResponseError::NotController, "elsewhere");
        assert_eq!(
            decode_answer(encode_answer(&Err(refusal.clone()))).unwrap(),
            Err(refusal)
        );
        assert!(decode_forward(Bytes::from_static(&[7])).is_err());
    }

    #[test]
    fn data_dir_is_claimed_once_and_refused_to_others() {
        let dir = tempfile::tempdir().unwrap();
        let fresh = dir.path().join("node-1");
        prepare_data_dir(&fresh, 1, &[1, 2, 3]).unwrap();
        prepare_data_dir(&fresh, 1, &[1, 2, 3]).unwrap();
        assert!(
            prepare_data_dir(&fresh, 2, &[1, 2, 3]).is_err(),
            "another node"
        );
        assert!(
            prepare_data_dir(&fresh, 1, &[1, 2]).is_err(),
            "other members"
        );
        let old = dir.path().join("old");
        fs::create_dir_all(&old).unwrap();
        fs::write(old.join("topics"), "").unwrap();
        let refused = prepare_data_dir(&old, 0, &[0]).unwrap_err().to_string();
        assert!(refused.contains("before replication"), "{refused}");
    }

    #[test]
    fn peers_are_validated_strictly() {
        let peer = |id, rpc: &str| Peer {
            id,
            rpc_address: rpc.into(),
            host: "localhost".into(),
            port: 9092,
        };
        assert!(validate_peers(1, vec![peer(2, "b:1"), peer(1, "a:1")]).is_ok());
        assert!(validate_peers(3, vec![peer(1, "a:1"), peer(2, "b:1")]).is_err());
        assert!(validate_peers(1, vec![peer(1, "a:1"), peer(1, "b:1")]).is_err());
        assert!(validate_peers(1, vec![peer(1, "a:1"), peer(2, "")]).is_err());
        assert!(validate_peers(-1, vec![peer(-1, "")]).is_err());
        assert!(validate_peers(0, vec![peer(0, "")]).is_ok());
        assert_ne!(fingerprint(&[1, 2, 3]), fingerprint(&[1, 2]));
    }
}
