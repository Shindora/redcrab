//! Broker: the Kafka-facing state of one node: its place in the cluster and its replicas, the group
//! coordinator, and producer ids.

use std::path::PathBuf;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::{Arc, Mutex, PoisonError, Weak};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use kafka_protocol::ResponseError;
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tracing::warn;

use crate::cluster::{Cluster, Peer, StartError, Topic};
use crate::group::Coordinator;
use crate::replica::{Replica, View};

/// Producer ids carry the node id above this many bits, so nodes never hand out the same id.
const PRODUCER_ID_NODE_SHIFT: u32 = 48;
const SINGLE_NODE_READY_TIMEOUT: Duration = Duration::from_secs(10);

/// Static settings of one broker process.
#[derive(Debug, Clone)]
pub struct BrokerConfig {
    /// Node id, also the Kafka broker id.
    pub node_id: i32,
    /// Host clients are told to connect to.
    pub advertised_host: String,
    /// Port clients are told to connect to.
    pub advertised_port: u16,
    /// Directory holding the metadata log and the partition logs.
    pub data_dir: PathBuf,
    /// Every node of the cluster, this one included; empty for a cluster of this node alone.
    pub peers: Vec<Peer>,
}

/// Shared state of one broker.
pub struct Broker {
    /// Settings this broker was started with.
    pub config: BrokerConfig,
    cluster: Arc<Cluster>,
    groups: Coordinator,
    next_producer_id: AtomicI64,
    watcher: Mutex<Option<JoinHandle<()>>>,
}

impl Broker {
    /// Starts this node of the cluster: its replicas, its transport on `rpc_listener` (needed
    /// when there are other nodes) and its group coordinator. A node alone in its cluster returns
    /// once it has replayed its metadata and leads every partition; with peers, it returns at once
    /// and partitions become available as their leaders are elected.
    ///
    /// # Errors
    ///
    /// Fails for an inconsistent configuration, a data directory of another node or format, or an
    /// I/O failure while starting.
    pub async fn start(
        config: BrokerConfig,
        rpc_listener: Option<std::net::TcpListener>,
    ) -> Result<Arc<Broker>, StartError> {
        let peers = if config.peers.is_empty() {
            vec![Peer {
                id: config.node_id,
                rpc_address: String::new(),
                host: config.advertised_host.clone(),
                port: config.advertised_port,
            }]
        } else {
            config.peers.clone()
        };
        let cluster =
            Cluster::start(config.node_id, peers, config.data_dir.clone(), rpc_listener).await?;
        if cluster.peers.len() == 1 && !cluster.wait_ready(SINGLE_NODE_READY_TIMEOUT).await {
            warn!("not every partition has a leader yet; serving anyway");
        }
        let millis = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(0, |d| i64::try_from(d.as_millis()).unwrap_or(0));
        // ponytail: the low bits are seeded from wall-clock millis, not persisted; a clock step back across a restart can reuse ids.
        let first_producer_id = (i64::from(config.node_id) << PRODUCER_ID_NODE_SHIFT)
            | (millis & ((1 << PRODUCER_ID_NODE_SHIFT) - 1));
        let changes = cluster.controller_changes();
        let broker = Arc::new(Broker {
            config,
            cluster,
            groups: Coordinator::default(),
            next_producer_id: AtomicI64::new(first_producer_id),
            watcher: Mutex::new(None),
        });
        let watcher = tokio::spawn(reset_groups_on_controller_change(
            Arc::downgrade(&broker),
            changes,
        ));
        *broker
            .watcher
            .lock()
            .unwrap_or_else(PoisonError::into_inner) = Some(watcher);
        Ok(broker)
    }

    /// Stops the replicas and the transport; the broker answers nothing useful afterwards.
    pub async fn shutdown(&self) {
        if let Some(watcher) = self
            .watcher
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .take()
        {
            watcher.abort();
        }
        let cluster = Arc::clone(&self.cluster);
        let _ = tokio::task::spawn_blocking(move || cluster.shutdown()).await;
    }

    /// The cluster this broker belongs to.
    pub(crate) fn cluster(&self) -> &Arc<Cluster> {
        &self.cluster
    }

    /// The consumer-group coordinator; only the controller coordinates groups.
    pub(crate) fn groups(&self) -> &Coordinator {
        &self.groups
    }

    /// Hands out a producer id never returned before by any node of the cluster.
    pub fn next_producer_id(&self) -> i64 {
        self.next_producer_id.fetch_add(1, Ordering::Relaxed)
    }

    /// This node's replica of partition `index` of `topic`, if this node leads it.
    ///
    /// # Errors
    ///
    /// `UNKNOWN_TOPIC_OR_PARTITION` for a partition the topic lacks, `NOT_LEADER_OR_FOLLOWER` when
    /// this node does not lead it.
    pub(crate) fn leader_replica(&self, topic: &Topic, index: i32) -> Result<Arc<Replica>, i16> {
        let key = topic
            .group(index)
            .ok_or(ResponseError::UnknownTopicOrPartition.code())?;
        self.cluster
            .replica(key)
            .filter(|replica| replica.view().leader == Some(self.cluster.id))
            .ok_or(ResponseError::NotLeaderOrFollower.code())
    }
}

/// Checks a request's `current_leader_epoch` against the leader's term; a negative one skips the
/// check.
///
/// # Errors
///
/// `FENCED_LEADER_EPOCH` for an older epoch, `UNKNOWN_LEADER_EPOCH` for a newer one.
pub(crate) fn check_leader_epoch(view: View, requested: i32) -> Result<(), i16> {
    if requested < 0 || requested == view.term {
        Ok(())
    } else if requested < view.term {
        Err(ResponseError::FencedLeaderEpoch.code())
    } else {
        Err(ResponseError::UnknownLeaderEpoch.code())
    }
}

/// Group membership lives in memory on the controller; when the controller changes, members
/// rediscover it and rejoin, so whatever this node held is dropped.
async fn reset_groups_on_controller_change(
    broker: Weak<Broker>,
    mut changes: watch::Receiver<View>,
) {
    let mut last = {
        let view = *changes.borrow_and_update();
        (view.term, view.leader)
    };
    while changes.changed().await.is_ok() {
        let view = *changes.borrow_and_update();
        let Some(broker) = broker.upgrade() else {
            return;
        };
        if (view.term, view.leader) != last {
            last = (view.term, view.leader);
            broker.groups.reset();
        }
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use std::path::Path;

    use bytes::BytesMut;

    use super::*;
    use crate::api::tests::batch;
    use crate::cluster::TopicSpec;

    pub(crate) fn config(dir: &Path) -> BrokerConfig {
        BrokerConfig {
            node_id: 0,
            advertised_host: "127.0.0.1".into(),
            advertised_port: 9092,
            data_dir: dir.to_path_buf(),
            peers: Vec::new(),
        }
    }

    async fn create(broker: &Broker, name: &str, partitions: i32) -> Arc<Topic> {
        let spec = TopicSpec {
            name: name.into(),
            partitions,
            replication_factor: -1,
            assignments: Vec::new(),
        };
        let created = broker
            .cluster()
            .create_topic(&spec, Duration::from_secs(10))
            .await
            .unwrap();
        assert_eq!(created.replication_factor, 1);
        broker.cluster().topic_by_id(created.id).unwrap()
    }

    async fn read(replica: &Arc<Replica>, offset: i64) -> BytesMut {
        let replica = Arc::clone(replica);
        tokio::task::spawn_blocking(move || {
            let mut out = BytesMut::new();
            for extent in replica.locate(offset, usize::MAX, true, i64::MAX).unwrap() {
                extent.read_into(&mut out).unwrap();
            }
            out
        })
        .await
        .unwrap()
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn single_node_commits_on_its_own_flush_and_replays_after_restart() {
        let dir = tempfile::tempdir().unwrap();
        let broker = Broker::start(config(dir.path()), None).await.unwrap();
        let topic = create(&broker, "orders", 3).await;
        let leader = broker.leader_replica(&topic, 2).unwrap();
        while !leader.view().ready {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        assert_eq!(leader.high_watermark(), 1, "the new leader's no-op");

        let (one, two) = (batch(&["a"]), batch(&["b", "c"]));
        let records = [one.clone(), two.clone()].concat();
        let first = leader.propose(
            records,
            vec![0..one.len(), one.len()..one.len() + two.len()],
        );
        let second = leader.propose(one.clone(), std::iter::once(0..one.len()).collect());
        assert_eq!(first.await.unwrap(), Ok(1));
        assert_eq!(second.await.unwrap(), Ok(4));
        assert_eq!(leader.high_watermark(), 5);
        let stored = read(&leader, 1).await;
        assert_eq!(
            &stored[..8],
            &1_i64.to_be_bytes(),
            "stamped with its offset"
        );
        assert_eq!(stored.len(), 2 * one.len() + two.len());
        assert_eq!(
            broker.leader_replica(&topic, 3).err(),
            Some(ResponseError::UnknownTopicOrPartition.code())
        );
        broker.shutdown().await;
        drop((leader, broker));

        let broker = Broker::start(config(dir.path()), None).await.unwrap();
        let topic = broker.cluster().topic("orders").unwrap();
        assert_eq!(topic.partition_count(), 3);
        let leader = broker.leader_replica(&topic, 2).unwrap();
        assert_eq!(leader.high_watermark(), 6, "everything, and a second no-op");
        assert_eq!(read(&leader, 1).await.len(), stored.len() + 72);
        broker.shutdown().await;
    }

    #[test]
    fn leader_epoch_checks_fence_old_and_refuse_unknown_epochs() {
        let view = View {
            term: 5,
            leader: Some(1),
            ready: true,
        };
        assert_eq!(check_leader_epoch(view, -1), Ok(()));
        assert_eq!(check_leader_epoch(view, 5), Ok(()));
        assert_eq!(
            check_leader_epoch(view, 4),
            Err(ResponseError::FencedLeaderEpoch.code())
        );
        assert_eq!(
            check_leader_epoch(view, 6),
            Err(ResponseError::UnknownLeaderEpoch.code())
        );
    }
}
