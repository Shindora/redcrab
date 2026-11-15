//! Three-node clusters of real broker processes on localhost, on ephemeral ports and temporary
//! data directories, driven through the Kafka protocol. Nodes are stopped with SIGKILL.
//!
//! The tests share the machine's disk and CPU with each other, so they run one cluster at a time.

use std::collections::{HashMap, HashSet};
use std::fs::{self, OpenOptions};
use std::future::Future;
use std::io;
use std::net::TcpListener as StdTcpListener;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::{Duration, Instant};

use bytes::{BufMut, Bytes, BytesMut};
use kafka_protocol::indexmap::IndexMap;
use kafka_protocol::messages::create_topics_request::CreatableTopic;
use kafka_protocol::messages::create_topics_response::CreatableTopicResult;
use kafka_protocol::messages::fetch_request::{FetchPartition, FetchTopic};
use kafka_protocol::messages::fetch_response::PartitionData;
use kafka_protocol::messages::list_offsets_request::{ListOffsetsPartition, ListOffsetsTopic};
use kafka_protocol::messages::offset_commit_request::{
    OffsetCommitRequestPartition, OffsetCommitRequestTopic,
};
use kafka_protocol::messages::offset_fetch_request::{
    OffsetFetchRequestGroup, OffsetFetchRequestTopics,
};
use kafka_protocol::messages::produce_request::{PartitionProduceData, TopicProduceData};
use kafka_protocol::messages::{
    CreateTopicsRequest, FetchRequest, FindCoordinatorRequest, GroupId, ListOffsetsRequest,
    MetadataRequest, OffsetCommitRequest, OffsetFetchRequest, ProduceRequest, RequestHeader,
    ResponseHeader, TopicName,
};
use kafka_protocol::protocol::{Decodable, Encodable, HeaderVersion, Request, StrBytes};
use kafka_protocol::records::{
    Compression, Record, RecordBatchDecoder, RecordBatchEncoder, RecordEncodeOptions, TimestampType,
};
use redcrab::storage::batch_frames;
use tempfile::TempDir;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use uuid::Uuid;

const NOT_LEADER_OR_FOLLOWER: i16 = 6;
const TOPIC_ALREADY_EXISTS: i16 = 36;
const OFFSET_NOT_AVAILABLE: i16 = 78;
const CALL_TIMEOUT: Duration = Duration::from_secs(20);
/// Generous bound for anything that waits on elections, replication or restarts.
const SETTLE: Duration = Duration::from_secs(45);
const POLL: Duration = Duration::from_millis(50);

static ONE_CLUSTER_AT_A_TIME: Mutex<()> = Mutex::new(());

struct Node {
    id: i32,
    kafka: u16,
    rpc: u16,
    dir: PathBuf,
    log: PathBuf,
    child: Option<Child>,
}

/// A cluster of broker processes; killed, and their logs printed if the test failed, on drop.
struct Cluster {
    nodes: Vec<Node>,
    peers: String,
    _temp: TempDir,
    _turn: MutexGuard<'static, ()>,
}

impl Cluster {
    fn start(size: usize) -> Cluster {
        let turn = ONE_CLUSTER_AT_A_TIME
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        let temp = tempfile::tempdir().unwrap();
        let listeners: Vec<StdTcpListener> = (0..2 * size)
            .map(|_| StdTcpListener::bind("127.0.0.1:0").unwrap())
            .collect();
        let ports: Vec<u16> = listeners
            .iter()
            .map(|l| l.local_addr().unwrap().port())
            .collect();
        drop(listeners);
        let nodes: Vec<Node> = (0..size)
            .map(|at| {
                let id = i32::try_from(at + 1).unwrap();
                Node {
                    id,
                    kafka: ports[2 * at],
                    rpc: ports[2 * at + 1],
                    dir: temp.path().join(format!("node-{id}")),
                    log: temp.path().join(format!("node-{id}.log")),
                    child: None,
                }
            })
            .collect();
        let peers = nodes
            .iter()
            .map(|n| format!("{}@127.0.0.1:{}/127.0.0.1:{}", n.id, n.rpc, n.kafka))
            .collect::<Vec<_>>()
            .join(",");
        let mut cluster = Cluster {
            nodes,
            peers,
            _temp: temp,
            _turn: turn,
        };
        for id in cluster.ids() {
            cluster.restart(id);
        }
        cluster
    }

    fn ids(&self) -> Vec<i32> {
        self.nodes.iter().map(|n| n.id).collect()
    }

    fn node(&self, id: i32) -> &Node {
        self.nodes.iter().find(|n| n.id == id).unwrap()
    }

    fn port(&self, id: i32) -> u16 {
        self.node(id).kafka
    }

    fn restart(&mut self, id: i32) {
        let peers = self.peers.clone();
        let node = self.nodes.iter_mut().find(|n| n.id == id).unwrap();
        assert!(node.child.is_none(), "node {id} is running");
        let log = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&node.log)
            .unwrap();
        let child = Command::new(env!("CARGO_BIN_EXE_redcrab"))
            .arg("--node-id")
            .arg(id.to_string())
            .arg("--listen")
            .arg(format!("127.0.0.1:{}", node.kafka))
            .arg("--rpc-listen")
            .arg(format!("127.0.0.1:{}", node.rpc))
            .arg("--data-dir")
            .arg(&node.dir)
            .arg("--peers")
            .arg(peers)
            .env("RUST_LOG", "info")
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(log)
            .spawn()
            .unwrap();
        node.child = Some(child);
    }

    fn kill(&mut self, id: i32) {
        let node = self.nodes.iter_mut().find(|n| n.id == id).unwrap();
        if let Some(mut child) = node.child.take() {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}

impl Drop for Cluster {
    fn drop(&mut self) {
        for id in self.ids() {
            self.kill(id);
        }
        if std::thread::panicking() {
            for node in &self.nodes {
                let log = fs::read_to_string(&node.log).unwrap_or_default();
                let tail: Vec<&str> = log.lines().rev().take(80).collect();
                eprintln!("----- node {} log, last lines -----", node.id);
                for line in tail.iter().rev() {
                    eprintln!("{line}");
                }
            }
        }
    }
}

async fn call<R: Request>(port: u16, request: &R, version: i16) -> io::Result<R::Response> {
    let exchange = async {
        let mut stream = TcpStream::connect(("127.0.0.1", port)).await?;
        let mut frame = BytesMut::new();
        frame.put_i32(0);
        RequestHeader::default()
            .with_request_api_key(R::KEY)
            .with_request_api_version(version)
            .with_correlation_id(1)
            .with_client_id(Some(StrBytes::from_static_str("cluster-test")))
            .encode(&mut frame, R::header_version(version))
            .unwrap();
        request.encode(&mut frame, version).unwrap();
        let len = i32::try_from(frame.len() - 4).unwrap();
        frame[..4].copy_from_slice(&len.to_be_bytes());
        stream.write_all(&frame).await?;
        let len = usize::try_from(stream.read_i32().await?).map_err(io::Error::other)?;
        let mut reply = vec![0; len];
        stream.read_exact(&mut reply).await?;
        let mut reply = Bytes::from(reply);
        ResponseHeader::decode(&mut reply, R::Response::header_version(version))
            .map_err(|e| io::Error::other(e.to_string()))?;
        R::Response::decode(&mut reply, version).map_err(|e| io::Error::other(e.to_string()))
    };
    tokio::time::timeout(CALL_TIMEOUT, exchange)
        .await
        .map_err(|_| io::Error::from(io::ErrorKind::TimedOut))?
}

/// Polls `probe` until it yields something, failing the test after `timeout`.
async fn eventually<T, F, Fut>(what: &str, timeout: Duration, mut probe: F) -> T
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Option<T>>,
{
    let deadline = Instant::now() + timeout;
    loop {
        if let Some(found) = probe().await {
            return found;
        }
        assert!(Instant::now() < deadline, "timed out waiting for {what}");
        tokio::time::sleep(POLL).await;
    }
}

fn batch(values: &[&str]) -> Bytes {
    let records: Vec<Record> = (0_i32..)
        .zip(values)
        .map(|(i, value)| Record {
            transactional: false,
            control: false,
            delete_horizon: false,
            partition_leader_epoch: -1,
            producer_id: -1,
            producer_epoch: -1,
            timestamp_type: TimestampType::Creation,
            offset: i64::from(i),
            sequence: i,
            timestamp: 1_700_000_000_000,
            key: None,
            value: Some(Bytes::copy_from_slice(value.as_bytes())),
            headers: IndexMap::new(),
        })
        .collect();
    let mut buf = BytesMut::new();
    let options = RecordEncodeOptions {
        version: 2,
        compression: Compression::None,
    };
    RecordBatchEncoder::encode(&mut buf, &records, &options).unwrap();
    buf.freeze()
}

/// Leadership of one partition as a node reports it.
#[derive(Debug, Clone, PartialEq, Eq)]
struct View {
    leader: i32,
    epoch: i32,
    replicas: Vec<i32>,
}

async fn topic_view(port: u16, topic: Uuid) -> Option<Vec<View>> {
    let metadata = call(port, &MetadataRequest::default().with_topics(None), 12)
        .await
        .ok()?;
    let topic = metadata.topics.iter().find(|t| t.topic_id == topic)?;
    Some(
        topic
            .partitions
            .iter()
            .map(|p| View {
                leader: p.leader_id.0,
                epoch: p.leader_epoch,
                replicas: p.replica_nodes.iter().map(|b| b.0).collect(),
            })
            .collect(),
    )
}

/// Waits until every node in `among` reports the same leaders for `topic`, all of them in
/// `among` and accepted by `wanted`.
async fn agreed_view(
    cluster: &Cluster,
    among: &[i32],
    topic: Uuid,
    wanted: impl Fn(&View) -> bool,
) -> Vec<View> {
    eventually(
        "the nodes to agree on the partition leaders",
        SETTLE,
        || async {
            let mut views = Vec::new();
            for &id in among {
                views.push(topic_view(cluster.port(id), topic).await?);
            }
            let first = views.first()?.clone();
            let agreed = views.iter().all(|view| *view == first)
                && first.iter().all(|p| among.contains(&p.leader) && wanted(p));
            agreed.then_some(first)
        },
    )
    .await
}

/// Waits until every node in `among` names the same controller, one of them.
async fn controller(cluster: &Cluster, among: &[i32]) -> i32 {
    eventually("the nodes to agree on a controller", SETTLE, || async {
        let mut named = HashSet::new();
        for &id in among {
            let metadata = call(cluster.port(id), &MetadataRequest::default(), 12)
                .await
                .ok()?;
            named.insert(metadata.controller_id.0);
        }
        let controller = *named.iter().next()?;
        (named.len() == 1 && among.contains(&controller)).then_some(controller)
    })
    .await
}

async fn create_topic(
    port: u16,
    name: &'static str,
    partitions: i32,
    replication: i16,
) -> io::Result<CreatableTopicResult> {
    let request = CreateTopicsRequest::default()
        .with_topics(vec![
            CreatableTopic::default()
                .with_name(TopicName(StrBytes::from_static_str(name)))
                .with_num_partitions(partitions)
                .with_replication_factor(replication),
        ])
        .with_timeout_ms(15_000);
    Ok(call(port, &request, 7).await?.topics.remove(0))
}

/// Creates a topic through `port`, retrying while the controller is being elected, and returns
/// its id.
async fn create_topic_retrying(
    port: u16,
    name: &'static str,
    partitions: i32,
    replication: i16,
) -> Uuid {
    eventually(&format!("topic {name} to be created"), SETTLE, || async {
        let created = create_topic(port, name, partitions, replication)
            .await
            .ok()?;
        match created.error_code {
            0 => Some(created.topic_id),
            TOPIC_ALREADY_EXISTS => {
                let metadata = call(port, &MetadataRequest::default().with_topics(None), 12)
                    .await
                    .ok()?;
                let found = metadata
                    .topics
                    .iter()
                    .find(|t| t.name.as_ref().is_some_and(|n| n.as_str() == name))?;
                Some(found.topic_id)
            }
            _ => None,
        }
    })
    .await
}

async fn produce(
    port: u16,
    topic: Uuid,
    partition: i32,
    records: Bytes,
    timeout_ms: i32,
) -> io::Result<(i16, i64)> {
    let request = ProduceRequest::default()
        .with_acks(-1)
        .with_timeout_ms(timeout_ms)
        .with_topic_data(vec![
            TopicProduceData::default()
                .with_topic_id(topic)
                .with_partition_data(vec![
                    PartitionProduceData::default()
                        .with_index(partition)
                        .with_records(Some(records)),
                ]),
        ]);
    let response = call(port, &request, 13).await?;
    let partition = &response.responses[0].partition_responses[0];
    Ok((partition.error_code, partition.base_offset))
}

/// The leader of `partition` as the nodes in `among` report it, highest epoch first.
async fn current_leader(
    cluster: &Cluster,
    among: &[i32],
    topic: Uuid,
    partition: i32,
) -> Option<i32> {
    let mut best: Option<(i32, i32)> = None;
    for &id in among {
        if let Some(views) = topic_view(cluster.port(id), topic).await {
            let view = views.get(usize::try_from(partition).ok()?)?;
            if among.contains(&view.leader) && best.is_none_or(|(epoch, _)| view.epoch > epoch) {
                best = Some((view.epoch, view.leader));
            }
        }
    }
    best.map(|(_, leader)| leader)
}

/// Produces `value` to the current leader until it is acknowledged; returns its offset. A retry
/// can duplicate it.
async fn produce_retrying(
    cluster: &Cluster,
    among: &[i32],
    topic: Uuid,
    partition: i32,
    value: &str,
) -> i64 {
    eventually(&format!("{value} to be acknowledged"), SETTLE, || async {
        let leader = current_leader(cluster, among, topic, partition).await?;
        let (code, base) = produce(
            cluster.port(leader),
            topic,
            partition,
            batch(&[value]),
            5_000,
        )
        .await
        .ok()?;
        (code == 0).then_some(base)
    })
    .await
}

async fn fetch(port: u16, topic: Uuid, partition: i32, offset: i64) -> io::Result<PartitionData> {
    let request = FetchRequest::default()
        .with_max_wait_ms(100)
        .with_min_bytes(1)
        .with_max_bytes(1 << 20)
        .with_topics(vec![
            FetchTopic::default()
                .with_topic_id(topic)
                .with_partitions(vec![
                    FetchPartition::default()
                        .with_partition(partition)
                        .with_fetch_offset(offset)
                        .with_partition_max_bytes(1 << 20),
                ]),
        ]);
    let mut response = call(port, &request, 13).await?;
    Ok(response.responses.remove(0).partitions.remove(0))
}

/// Every record from `offset` up to the high watermark, skipping control records.
async fn read_from(
    port: u16,
    topic: Uuid,
    partition: i32,
    mut offset: i64,
) -> io::Result<Vec<(i64, String)>> {
    let mut out = Vec::new();
    loop {
        let data = fetch(port, topic, partition, offset).await?;
        if data.error_code != 0 {
            return Err(io::Error::other(format!(
                "fetch failed with {}",
                data.error_code
            )));
        }
        let mut records = data.records.clone().unwrap_or_default();
        for set in RecordBatchDecoder::decode_all(&mut records)
            .map_err(|e| io::Error::other(e.to_string()))?
        {
            for record in set.records {
                if record.offset < offset {
                    continue;
                }
                offset = record.offset + 1;
                if !record.control {
                    let value = record.value.unwrap_or_default();
                    out.push((record.offset, String::from_utf8_lossy(&value).into_owned()));
                }
            }
        }
        if offset >= data.high_watermark {
            return Ok(out);
        }
    }
}

fn values(records: &[(i64, String)]) -> Vec<String> {
    records.iter().map(|(_, value)| value.clone()).collect()
}

/// The bytes of the batches below `below` in a node's on-disk log of the partition.
fn stored_batches(dir: &Path, topic: Uuid, partition: i32, below: i64) -> Vec<u8> {
    let log_dir = dir.join("partitions").join(format!("{topic}-{partition}"));
    let Ok(entries) = fs::read_dir(&log_dir) else {
        return Vec::new();
    };
    let mut files: Vec<PathBuf> = entries
        .filter_map(Result::ok)
        .map(|e| e.path())
        .filter(|path| path.extension().is_some_and(|e| e == "log"))
        .collect();
    files.sort();
    let bytes: Vec<u8> = files
        .iter()
        .flat_map(|f| fs::read(f).unwrap_or_default())
        .collect();
    let (mut out, mut at) = (Vec::new(), 0);
    for frame in batch_frames(&bytes) {
        let Ok(frame) = frame else {
            break;
        };
        if frame.base_offset + frame.record_count > below {
            break;
        }
        out.extend_from_slice(&bytes[at..at + frame.len]);
        at += frame.len;
    }
    out
}

fn holds(dir: &Path, topic: Uuid, partition: i32, needle: &[u8]) -> bool {
    let bytes = stored_batches(dir, topic, partition, i64::MAX);
    bytes.windows(needle.len()).any(|window| window == needle)
}

/// Waits until every node in `among` stores exactly the leader's committed batches.
async fn replicas_converge(
    cluster: &Cluster,
    among: &[i32],
    leader: i32,
    topic: Uuid,
    partition: i32,
) {
    let high_watermark = fetch(cluster.port(leader), topic, partition, 0)
        .await
        .unwrap()
        .high_watermark;
    let reference = stored_batches(&cluster.node(leader).dir, topic, partition, high_watermark);
    assert_ne!(reference.len(), 0, "the leader stores committed batches");
    eventually(
        "every replica to store the leader's committed batches",
        SETTLE,
        || async {
            among
                .iter()
                .all(|&id| {
                    stored_batches(&cluster.node(id).dir, topic, partition, high_watermark)
                        == reference
                })
                .then_some(())
        },
    )
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn replicated_topic_is_served_by_its_leaders_and_identical_on_every_replica() {
    let cluster = Cluster::start(3);
    let all = cluster.ids();
    let controller = controller(&cluster, &all).await;
    let other = *all.iter().find(|&&id| id != controller).unwrap();
    let created = create_topic(cluster.port(other), "replicated", 3, 3)
        .await
        .unwrap();
    assert_eq!(
        (
            created.error_code,
            created.num_partitions,
            created.replication_factor
        ),
        (0, 3, 3),
        "{:?}",
        created.error_message
    );
    let topic = created.topic_id;
    let partitions = agreed_view(&cluster, &all, topic, |_| true).await;
    for view in &partitions {
        let mut replicas = view.replicas.clone();
        replicas.sort_unstable();
        assert_eq!(replicas, all);
    }
    let first_replicas: HashSet<i32> = partitions.iter().map(|p| p.replicas[0]).collect();
    assert_eq!(
        first_replicas.len(),
        3,
        "preferred leaders spread over the nodes"
    );

    for (index, view) in (0..).zip(&partitions) {
        let leader = cluster.port(view.leader);
        let mut expected = Vec::new();
        for i in 0..5 {
            let value = format!("p{index}-{i}");
            let (code, base) = produce(leader, topic, index, batch(&[&value]), 15_000)
                .await
                .unwrap();
            assert_eq!(code, 0, "producing {value} to the leader");
            expected.push((base, value));
        }
        assert!(expected.windows(2).all(|pair| pair[0].0 < pair[1].0));
        assert_eq!(read_from(leader, topic, index, 0).await.unwrap(), expected);

        for follower in view.replicas.iter().filter(|&&id| id != view.leader) {
            let port = cluster.port(*follower);
            let (code, _) = produce(port, topic, index, batch(&["refused"]), 5_000)
                .await
                .unwrap();
            assert_eq!(
                code, NOT_LEADER_OR_FOLLOWER,
                "produce to follower {follower}"
            );
            let fetched = fetch(port, topic, index, 0).await.unwrap();
            assert_eq!(
                fetched.error_code, NOT_LEADER_OR_FOLLOWER,
                "fetch from follower {follower}"
            );
        }
        replicas_converge(&cluster, &all, view.leader, topic, index).await;
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn leader_failover_keeps_every_acknowledged_record_and_the_old_leader_catches_up() {
    let mut cluster = Cluster::start(3);
    let all = cluster.ids();
    let controller = controller(&cluster, &all).await;
    let topic = create_topic_retrying(cluster.port(controller), "failover", 1, 3).await;
    let old = agreed_view(&cluster, &all, topic, |_| true).await.remove(0);

    let mut acknowledged = Vec::new();
    for i in 0..10 {
        let value = format!("before-{i}");
        let (code, _) = produce(cluster.port(old.leader), topic, 0, batch(&[&value]), 15_000)
            .await
            .unwrap();
        assert_eq!(code, 0);
        acknowledged.push(value);
    }
    let consumed = read_from(cluster.port(old.leader), topic, 0, 0)
        .await
        .unwrap();
    assert_eq!(values(&consumed), acknowledged);
    let position = consumed[4].0 + 1;

    cluster.kill(old.leader);
    let stopped = Instant::now();
    let survivors: Vec<i32> = all.iter().copied().filter(|&id| id != old.leader).collect();
    let new = agreed_view(&cluster, &survivors, topic, |p| p.epoch > old.epoch)
        .await
        .remove(0);
    eprintln!(
        "node {} took over from node {} {:?} after the stop",
        new.leader,
        old.leader,
        stopped.elapsed()
    );
    assert!(
        stopped.elapsed() < Duration::from_secs(15),
        "failover took too long"
    );

    for i in 0..5 {
        let value = format!("after-{i}");
        produce_retrying(&cluster, &survivors, topic, 0, &value).await;
        acknowledged.push(value);
    }
    let leader = current_leader(&cluster, &survivors, topic, 0)
        .await
        .unwrap();
    let everything = eventually(
        "every acknowledged record on the new leader",
        SETTLE,
        || async {
            let read = read_from(cluster.port(leader), topic, 0, 0).await.ok()?;
            let mut seen = HashSet::new();
            let firsts: Vec<String> = values(&read)
                .into_iter()
                .filter(|value| seen.insert(value.clone()))
                .collect();
            (firsts == acknowledged).then_some(read)
        },
    )
    .await;
    let resumed = read_from(cluster.port(leader), topic, 0, position)
        .await
        .unwrap();
    assert_eq!(resumed.first().map(|(_, v)| v.as_str()), Some("before-5"));
    assert_eq!(resumed, everything[5..]);

    cluster.restart(old.leader);
    replicas_converge(&cluster, &all, leader, topic, 0).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn one_node_of_three_cannot_acknowledge_until_a_second_returns() {
    let mut cluster = Cluster::start(3);
    let all = cluster.ids();
    let controller = controller(&cluster, &all).await;
    let topic = create_topic_retrying(cluster.port(controller), "quorum", 1, 3).await;
    let view = agreed_view(&cluster, &all, topic, |_| true).await.remove(0);
    let followers: Vec<i32> = all
        .iter()
        .copied()
        .filter(|&id| id != view.leader)
        .collect();

    cluster.kill(followers[0]);
    cluster.kill(followers[1]);
    let (code, _) = produce(
        cluster.port(view.leader),
        topic,
        0,
        batch(&["lonely"]),
        3_000,
    )
    .await
    .unwrap();
    assert_ne!(code, 0, "one node of three acknowledged a write");

    cluster.restart(followers[0]);
    let pair = [view.leader, followers[0]];
    let offset = produce_retrying(&cluster, &pair, topic, 0, "together").await;
    let leader = current_leader(&cluster, &pair, topic, 0).await.unwrap();
    let read = read_from(cluster.port(leader), topic, 0, 0).await.unwrap();
    assert!(read.contains(&(offset, "together".to_owned())), "{read:?}");
}

#[tokio::test(flavor = "multi_thread")]
async fn stale_leader_truncates_what_was_never_acknowledged() {
    let mut cluster = Cluster::start(3);
    let all = cluster.ids();
    let controller = controller(&cluster, &all).await;
    let topic = create_topic_retrying(cluster.port(controller), "truncate", 1, 3).await;
    let view = agreed_view(&cluster, &all, topic, |_| true).await.remove(0);
    let stale = view.leader;
    let followers: Vec<i32> = all.iter().copied().filter(|&id| id != stale).collect();
    let (code, _) = produce(cluster.port(stale), topic, 0, batch(&["kept"]), 15_000)
        .await
        .unwrap();
    assert_eq!(code, 0);

    cluster.kill(followers[0]);
    cluster.kill(followers[1]);
    let (code, _) = produce(
        cluster.port(stale),
        topic,
        0,
        batch(&["never-acknowledged"]),
        1_000,
    )
    .await
    .unwrap();
    assert_ne!(code, 0);
    let stale_dir = cluster.node(stale).dir.clone();
    eventually(
        "the lone leader to write what it could not replicate",
        SETTLE,
        || async { holds(&stale_dir, topic, 0, b"never-acknowledged").then_some(()) },
    )
    .await;
    cluster.kill(stale);

    cluster.restart(followers[0]);
    cluster.restart(followers[1]);
    produce_retrying(&cluster, &followers, topic, 0, "new-term").await;
    let leader = current_leader(&cluster, &followers, topic, 0)
        .await
        .unwrap();

    cluster.restart(stale);
    replicas_converge(&cluster, &all, leader, topic, 0).await;
    eventually("the stale suffix to be truncated", SETTLE, || async {
        (!holds(&stale_dir, topic, 0, b"never-acknowledged")).then_some(())
    })
    .await;
    let read = read_from(cluster.port(leader), topic, 0, 0).await.unwrap();
    assert_eq!(values(&read), ["kept", "new-term"]);
}

#[tokio::test(flavor = "multi_thread")]
async fn controller_failover_keeps_topics_and_committed_offsets() {
    let mut cluster = Cluster::start(3);
    let all = cluster.ids();
    let old = controller(&cluster, &all).await;
    let other = *all.iter().find(|&&id| id != old).unwrap();
    create_topic_retrying(cluster.port(other), "before", 2, 3).await;
    let found = call(
        cluster.port(other),
        &FindCoordinatorRequest::default()
            .with_coordinator_keys(vec![StrBytes::from_static_str("g")]),
        6,
    )
    .await
    .unwrap();
    assert_eq!(
        found.coordinators[0].node_id.0, old,
        "the controller coordinates groups"
    );
    let commit = OffsetCommitRequest::default()
        .with_group_id(GroupId(StrBytes::from_static_str("g")))
        .with_generation_id_or_member_epoch(-1)
        .with_topics(vec![
            OffsetCommitRequestTopic::default()
                .with_name(TopicName(StrBytes::from_static_str("before")))
                .with_partitions(vec![
                    OffsetCommitRequestPartition::default()
                        .with_partition_index(1)
                        .with_committed_offset(42),
                ]),
        ]);
    let committed = call(cluster.port(old), &commit, 9).await.unwrap();
    assert_eq!(committed.topics[0].partitions[0].error_code, 0);
    let refused = call(cluster.port(other), &commit, 9).await.unwrap();
    assert_eq!(
        refused.topics[0].partitions[0].error_code, 16,
        "NOT_COORDINATOR elsewhere"
    );

    cluster.kill(old);
    let survivors: Vec<i32> = all.iter().copied().filter(|&id| id != old).collect();
    let new = controller(&cluster, &survivors).await;
    let survivor = *survivors.iter().find(|&&id| id != new).unwrap();
    let after = create_topic_retrying(cluster.port(survivor), "after", 1, 3).await;
    agreed_view(&cluster, &survivors, after, |_| true).await;

    let fetch_offsets = OffsetFetchRequest::default().with_groups(vec![
        OffsetFetchRequestGroup::default()
            .with_group_id(GroupId(StrBytes::from_static_str("g")))
            .with_topics(Some(vec![
                OffsetFetchRequestTopics::default()
                    .with_name(TopicName(StrBytes::from_static_str("before")))
                    .with_partition_indexes(vec![1]),
            ])),
    ]);
    let offset = eventually(
        "the new controller to serve committed offsets",
        SETTLE,
        || async {
            let fetched = call(cluster.port(new), &fetch_offsets, 9).await.ok()?;
            let group = &fetched.groups[0];
            (group.error_code == 0).then(|| group.topics[0].partitions[0].committed_offset)
        },
    )
    .await;
    assert_eq!(offset, 42);

    cluster.restart(old);
    eventually(
        "the old controller to learn the new topic",
        SETTLE,
        || async { topic_view(cluster.port(old), after).await.map(|_| ()) },
    )
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_new_leader_answers_latest_offsets_only_once_its_high_watermark_is_current() {
    let mut cluster = Cluster::start(3);
    let all = cluster.ids();
    let controller = controller(&cluster, &all).await;
    let topic = create_topic_retrying(cluster.port(controller), "latest", 1, 3).await;
    let old = agreed_view(&cluster, &all, topic, |_| true).await.remove(0);
    for i in 0..3 {
        produce_retrying(&cluster, &all, topic, 0, &format!("v{i}")).await;
    }
    cluster.kill(old.leader);
    let survivors: Vec<i32> = all.iter().copied().filter(|&id| id != old.leader).collect();
    let new = agreed_view(&cluster, &survivors, topic, |p| p.epoch > old.epoch)
        .await
        .remove(0);
    let request = ListOffsetsRequest::default().with_topics(vec![
        ListOffsetsTopic::default()
            .with_name(TopicName(StrBytes::from_static_str("latest")))
            .with_partitions(vec![ListOffsetsPartition::default().with_timestamp(-1)]),
    ]);
    let latest = eventually(
        "the new leader to answer latest offsets",
        SETTLE,
        || async {
            let listed = call(cluster.port(new.leader), &request, 6).await.ok()?;
            let partition = &listed.topics[0].partitions[0];
            assert!(
                matches!(partition.error_code, 0 | OFFSET_NOT_AVAILABLE),
                "unexpected error {}",
                partition.error_code
            );
            (partition.error_code == 0).then_some((partition.offset, partition.leader_epoch))
        },
    )
    .await;
    assert!(latest.0 >= 5, "latest went back to {}", latest.0);
    assert_eq!(latest.1, new.epoch);
}

/// A producer writes sequence numbers one at a time with acks=all, retrying each until it is
/// acknowledged, while the partition's leader is killed and restarted `rounds` times; afterwards
/// every acknowledged number must be on the final leader, in order.
async fn acknowledged_records_survive_leader_kills(rounds: usize) {
    let mut cluster = Cluster::start(3);
    let all = cluster.ids();
    let controller = controller(&cluster, &all).await;
    let topic = create_topic_retrying(cluster.port(controller), "durable", 1, 3).await;
    agreed_view(&cluster, &all, topic, |_| true).await;

    let ports: HashMap<i32, u16> = all.iter().map(|&id| (id, cluster.port(id))).collect();
    let stop = Arc::new(AtomicBool::new(false));
    let acknowledged = Arc::new(Mutex::new(Vec::new()));
    let producing = tokio::spawn({
        let (stop, acknowledged, ports) =
            (Arc::clone(&stop), Arc::clone(&acknowledged), ports.clone());
        async move {
            let mut sequence = 0_u64;
            let mut leader: Option<u16> = None;
            while !stop.load(Ordering::Relaxed) {
                let Some(port) = leader else {
                    for &port in ports.values() {
                        if let Some(view) = topic_view(port, topic).await
                            && let Some(&found) = ports.get(&view[0].leader)
                        {
                            leader = Some(found);
                            break;
                        }
                    }
                    tokio::time::sleep(POLL).await;
                    continue;
                };
                if let Ok((0, _)) =
                    produce(port, topic, 0, batch(&[&sequence.to_string()]), 5_000).await
                {
                    acknowledged.lock().unwrap().push(sequence);
                    sequence += 1;
                } else {
                    leader = None;
                    tokio::time::sleep(POLL).await;
                }
            }
        }
    });

    for round in 0..rounds {
        tokio::time::sleep(Duration::from_millis(800)).await;
        let live = cluster.ids();
        let leader = current_leader(&cluster, &live, topic, 0).await;
        let Some(leader) = leader else {
            continue;
        };
        cluster.kill(leader);
        let down = Duration::from_millis(300 + 400 * u64::try_from(round % 4).unwrap());
        tokio::time::sleep(down).await;
        cluster.restart(leader);
    }
    tokio::time::sleep(Duration::from_secs(1)).await;
    stop.store(true, Ordering::Relaxed);
    producing.await.unwrap();

    let acknowledged = acknowledged.lock().unwrap().clone();
    assert!(!acknowledged.is_empty(), "nothing was acknowledged");
    let leader = agreed_view(&cluster, &all, topic, |_| true)
        .await
        .remove(0)
        .leader;
    let stored: Vec<u64> = eventually("the final leader to serve its log", SETTLE, || async {
        let read = read_from(cluster.port(leader), topic, 0, 0).await.ok()?;
        let last = *acknowledged.last()?;
        let numbers: Vec<u64> = read.iter().filter_map(|(_, v)| v.parse().ok()).collect();
        numbers.contains(&last).then_some(numbers)
    })
    .await;
    let mut first_position = HashMap::new();
    for (at, number) in stored.iter().enumerate() {
        first_position.entry(*number).or_insert(at);
    }
    let positions: Vec<usize> = acknowledged
        .iter()
        .map(|number| {
            *first_position
                .get(number)
                .unwrap_or_else(|| panic!("acknowledged record {number} was lost"))
        })
        .collect();
    assert!(
        positions.windows(2).all(|pair| pair[0] < pair[1]),
        "acknowledged records are out of order"
    );
    eprintln!(
        "{} records acknowledged across {rounds} leader kills, {} stored",
        acknowledged.len(),
        stored.len()
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn acknowledged_records_survive_repeated_leader_kills() {
    acknowledged_records_survive_leader_kills(3).await;
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "long soak: cargo test --test cluster -- --ignored"]
async fn acknowledged_records_survive_many_leader_kills() {
    acknowledged_records_survive_leader_kills(40).await;
}
