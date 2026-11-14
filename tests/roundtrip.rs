use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::{BufMut, Bytes, BytesMut};
use kafka_protocol::indexmap::IndexMap;
use kafka_protocol::messages::create_topics_request::CreatableTopic;
use kafka_protocol::messages::describe_configs_request::DescribeConfigsResource;
use kafka_protocol::messages::fetch_request::{FetchPartition, FetchTopic};
use kafka_protocol::messages::fetch_response::PartitionData;
use kafka_protocol::messages::join_group_request::JoinGroupRequestProtocol;
use kafka_protocol::messages::leave_group_request::MemberIdentity;
use kafka_protocol::messages::offset_commit_request::{
    OffsetCommitRequestPartition, OffsetCommitRequestTopic,
};
use kafka_protocol::messages::offset_fetch_request::{
    OffsetFetchRequestGroup, OffsetFetchRequestTopic, OffsetFetchRequestTopics,
};
use kafka_protocol::messages::produce_request::{PartitionProduceData, TopicProduceData};
use kafka_protocol::messages::sync_group_request::SyncGroupRequestAssignment;
use kafka_protocol::messages::{
    ApiVersionsRequest, CreateTopicsRequest, DescribeConfigsRequest, DescribeGroupsRequest,
    FetchRequest, FindCoordinatorRequest, GroupId, HeartbeatRequest, JoinGroupRequest,
    JoinGroupResponse, LeaveGroupRequest, ListGroupsRequest, MetadataRequest, OffsetCommitRequest,
    OffsetFetchRequest, ProduceRequest, ProduceResponse, RequestHeader, ResponseHeader,
    SyncGroupRequest, SyncGroupResponse, TopicName,
};
use kafka_protocol::protocol::{Decodable, Encodable, HeaderVersion, Request, StrBytes};
use kafka_protocol::records::{
    Compression, Record, RecordBatchDecoder, RecordBatchEncoder, RecordEncodeOptions, TimestampType,
};
use redcrab::{Broker, BrokerConfig};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::task::JoinHandle;
use uuid::Uuid;

struct Client {
    stream: TcpStream,
    correlation_id: i32,
}

impl Client {
    async fn connect(port: u16) -> Client {
        Client {
            stream: TcpStream::connect(("127.0.0.1", port)).await.unwrap(),
            correlation_id: 0,
        }
    }

    async fn call<R: Request>(&mut self, request: &R, version: i16) -> R::Response {
        self.correlation_id += 1;
        let header = RequestHeader::default()
            .with_request_api_key(R::KEY)
            .with_request_api_version(version)
            .with_correlation_id(self.correlation_id)
            .with_client_id(Some(StrBytes::from_static_str("roundtrip-test")));
        let mut frame = BytesMut::new();
        frame.put_i32(0);
        header
            .encode(&mut frame, R::header_version(version))
            .unwrap();
        request.encode(&mut frame, version).unwrap();
        let len = i32::try_from(frame.len() - 4).unwrap();
        frame[..4].copy_from_slice(&len.to_be_bytes());
        self.stream.write_all(&frame).await.unwrap();

        let len = self.stream.read_i32().await.unwrap();
        let mut reply = vec![0; usize::try_from(len).unwrap()];
        self.stream.read_exact(&mut reply).await.unwrap();
        let mut reply = Bytes::from(reply);
        let header =
            ResponseHeader::decode(&mut reply, R::Response::header_version(version)).unwrap();
        assert_eq!(header.correlation_id, self.correlation_id);
        R::Response::decode(&mut reply, version).unwrap()
    }
}

fn start(dir: &Path) -> (u16, JoinHandle<()>) {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    listener.set_nonblocking(true).unwrap();
    let port = listener.local_addr().unwrap().port();
    let broker = Broker::open(BrokerConfig {
        node_id: 0,
        advertised_host: "127.0.0.1".into(),
        advertised_port: port,
        data_dir: dir.to_path_buf(),
    })
    .unwrap();
    let listener = TcpListener::from_std(listener).unwrap();
    (
        port,
        tokio::spawn(redcrab::serve(listener, Arc::new(broker))),
    )
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

fn topic_name(name: &'static str) -> TopicName {
    TopicName(StrBytes::from_static_str(name))
}

async fn create_topic(client: &mut Client, name: &'static str, partitions: i32) -> Uuid {
    let request = CreateTopicsRequest::default()
        .with_topics(vec![
            CreatableTopic::default()
                .with_name(topic_name(name))
                .with_num_partitions(partitions)
                .with_replication_factor(-1),
        ])
        .with_timeout_ms(5000);
    let created = client.call(&request, 7).await;
    assert_eq!(created.topics[0].error_code, 0);
    assert_eq!(created.topics[0].num_partitions, partitions);
    created.topics[0].topic_id
}

async fn produce_by_id(
    client: &mut Client,
    topic_id: Uuid,
    partition: i32,
    records: Bytes,
) -> ProduceResponse {
    let request = ProduceRequest::default()
        .with_acks(-1)
        .with_timeout_ms(5000)
        .with_topic_data(vec![
            TopicProduceData::default()
                .with_topic_id(topic_id)
                .with_partition_data(vec![
                    PartitionProduceData::default()
                        .with_index(partition)
                        .with_records(Some(records)),
                ]),
        ]);
    client.call(&request, 13).await
}

fn fetch_by_id(topic_id: Uuid, partition: i32, offset: i64, max_wait_ms: i32) -> FetchRequest {
    FetchRequest::default()
        .with_max_wait_ms(max_wait_ms)
        .with_min_bytes(1)
        .with_max_bytes(1 << 20)
        .with_topics(vec![
            FetchTopic::default()
                .with_topic_id(topic_id)
                .with_partitions(vec![
                    FetchPartition::default()
                        .with_partition(partition)
                        .with_fetch_offset(offset)
                        .with_partition_max_bytes(1 << 20),
                ]),
        ])
}

fn values(partition: &PartitionData) -> Vec<(i64, String)> {
    let mut records = partition.records.clone().unwrap_or_default();
    RecordBatchDecoder::decode_all(&mut records)
        .unwrap()
        .into_iter()
        .flat_map(|set| set.records)
        .map(|r| {
            (
                r.offset,
                String::from_utf8(r.value.unwrap().to_vec()).unwrap(),
            )
        })
        .collect()
}

#[tokio::test(flavor = "multi_thread")]
async fn produce_fetch_roundtrip_by_id_and_name() {
    let dir = tempfile::tempdir().unwrap();
    let (port, _server) = start(dir.path());
    let mut client = Client::connect(port).await;
    assert_eq!(
        client
            .call(&ApiVersionsRequest::default(), 3)
            .await
            .error_code,
        0
    );
    let topic_id = create_topic(&mut client, "events", 2).await;

    let two_batches = [batch(&["a", "b"]), batch(&["c"])].concat();
    let produced = produce_by_id(&mut client, topic_id, 1, Bytes::from(two_batches)).await;
    let partition = &produced.responses[0].partition_responses[0];
    assert_eq!((partition.error_code, partition.base_offset), (0, 0));

    let by_name = ProduceRequest::default()
        .with_acks(1)
        .with_timeout_ms(5000)
        .with_topic_data(vec![
            TopicProduceData::default()
                .with_name(topic_name("events"))
                .with_partition_data(vec![
                    PartitionProduceData::default()
                        .with_index(1)
                        .with_records(Some(batch(&["d"]))),
                ]),
        ]);
    let produced = client.call(&by_name, 9).await;
    let partition = &produced.responses[0].partition_responses[0];
    assert_eq!((partition.error_code, partition.base_offset), (0, 3));

    let fetched = client.call(&fetch_by_id(topic_id, 1, 1, 0), 18).await;
    let partition = &fetched.responses[0].partitions[0];
    assert_eq!((partition.error_code, partition.high_watermark), (0, 4));
    let expected = [(0, "a"), (1, "b"), (2, "c"), (3, "d")].map(|(o, v)| (o, v.to_owned()));
    assert_eq!(values(partition), expected);

    let fetched = client.call(&fetch_by_id(topic_id, 1, 9, 0), 18).await;
    assert_eq!(
        fetched.responses[0].partitions[0].error_code, 1,
        "offset past the end is OFFSET_OUT_OF_RANGE"
    );
    let fetched = client.call(&fetch_by_id(Uuid::new_v4(), 0, 0, 0), 18).await;
    assert_eq!(
        fetched.responses[0].partitions[0].error_code, 100,
        "unknown topic id is UNKNOWN_TOPIC_ID"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn fetch_long_poll_wakes_on_new_data() {
    let dir = tempfile::tempdir().unwrap();
    let (port, _server) = start(dir.path());
    let mut producer = Client::connect(port).await;
    let topic_id = create_topic(&mut producer, "waits", 1).await;

    let mut consumer = Client::connect(port).await;
    let started = Instant::now();
    let waiting = tokio::spawn(async move {
        consumer
            .call(&fetch_by_id(topic_id, 0, 0, 10_000), 18)
            .await
    });
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert!(
        !waiting.is_finished(),
        "fetch with min_bytes=1 must wait for data"
    );
    produce_by_id(&mut producer, topic_id, 0, batch(&["late"])).await;

    let fetched = waiting.await.unwrap();
    assert!(
        started.elapsed() < Duration::from_secs(5),
        "fetch was not woken by the produce"
    );
    assert_eq!(
        values(&fetched.responses[0].partitions[0]),
        [(0, "late".to_owned())]
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn restart_keeps_topics_and_data() {
    let dir = tempfile::tempdir().unwrap();
    let (port, server) = start(dir.path());
    let mut client = Client::connect(port).await;
    let topic_id = create_topic(&mut client, "durable", 1).await;
    produce_by_id(&mut client, topic_id, 0, batch(&["survives"])).await;
    drop(client);
    server.abort();

    let (port, _server) = start(dir.path());
    let mut client = Client::connect(port).await;
    let fetched = client.call(&fetch_by_id(topic_id, 0, 0, 0), 18).await;
    assert_eq!(
        values(&fetched.responses[0].partitions[0]),
        [(0, "survives".to_owned())]
    );
}

const MEMBER_ID_REQUIRED: i16 = 79;
const REBALANCE_IN_PROGRESS: i16 = 27;
const UNKNOWN_MEMBER_ID: i16 = 25;
const UNKNOWN_TOPIC_OR_PARTITION: i16 = 3;

fn group_id(name: &'static str) -> GroupId {
    GroupId(StrBytes::from_static_str(name))
}

fn join_request(
    group: &'static str,
    member_id: &StrBytes,
    session_timeout_ms: i32,
) -> JoinGroupRequest {
    JoinGroupRequest::default()
        .with_group_id(group_id(group))
        .with_session_timeout_ms(session_timeout_ms)
        .with_rebalance_timeout_ms(10_000)
        .with_member_id(member_id.clone())
        .with_protocol_type(StrBytes::from_static_str("consumer"))
        .with_protocols(vec![
            JoinGroupRequestProtocol::default()
                .with_name(StrBytes::from_static_str("range"))
                .with_metadata(Bytes::from_static(b"subscription")),
        ])
}

/// Joins `group` as a new member the way v4+ clients do: get a member id, then join with it.
async fn join_new(
    client: &mut Client,
    group: &'static str,
    session_timeout_ms: i32,
) -> JoinGroupResponse {
    let required = client
        .call(
            &join_request(group, &StrBytes::default(), session_timeout_ms),
            9,
        )
        .await;
    assert_eq!(required.error_code, MEMBER_ID_REQUIRED);
    client
        .call(
            &join_request(group, &required.member_id, session_timeout_ms),
            9,
        )
        .await
}

async fn sync(
    client: &mut Client,
    group: &'static str,
    joined: &JoinGroupResponse,
    assignments: &[(&StrBytes, &'static [u8])],
) -> SyncGroupResponse {
    let assignments = assignments
        .iter()
        .map(|(member, assignment)| {
            SyncGroupRequestAssignment::default()
                .with_member_id((*member).clone())
                .with_assignment(Bytes::from_static(assignment))
        })
        .collect();
    let request = SyncGroupRequest::default()
        .with_group_id(group_id(group))
        .with_generation_id(joined.generation_id)
        .with_member_id(joined.member_id.clone())
        .with_protocol_type(Some(StrBytes::from_static_str("consumer")))
        .with_protocol_name(joined.protocol_name.clone())
        .with_assignments(assignments);
    client.call(&request, 5).await
}

async fn heartbeat(client: &mut Client, group: &'static str, joined: &JoinGroupResponse) -> i16 {
    let request = HeartbeatRequest::default()
        .with_group_id(group_id(group))
        .with_generation_id(joined.generation_id)
        .with_member_id(joined.member_id.clone());
    client.call(&request, 4).await.error_code
}

fn commit_request(
    group: &'static str,
    joined: Option<&JoinGroupResponse>,
    topic: &'static str,
    offsets: &[(i32, i64)],
) -> OffsetCommitRequest {
    let partitions = offsets
        .iter()
        .map(|&(partition, offset)| {
            OffsetCommitRequestPartition::default()
                .with_partition_index(partition)
                .with_committed_offset(offset)
                .with_committed_metadata(Some(StrBytes::from_static_str("meta")))
        })
        .collect();
    OffsetCommitRequest::default()
        .with_group_id(group_id(group))
        .with_generation_id_or_member_epoch(joined.map_or(-1, |j| j.generation_id))
        .with_member_id(joined.map(|j| j.member_id.clone()).unwrap_or_default())
        .with_topics(vec![
            OffsetCommitRequestTopic::default()
                .with_name(topic_name(topic))
                .with_partitions(partitions),
        ])
}

/// Committed offsets of partitions 0 and 1 of `topic`, read with `OffsetFetch` v9.
async fn fetch_offsets(
    client: &mut Client,
    group: &'static str,
    topic: &'static str,
) -> Vec<(i32, i64)> {
    let request = OffsetFetchRequest::default().with_groups(vec![
        OffsetFetchRequestGroup::default()
            .with_group_id(group_id(group))
            .with_topics(Some(vec![
                OffsetFetchRequestTopics::default()
                    .with_name(topic_name(topic))
                    .with_partition_indexes(vec![0, 1]),
            ])),
    ]);
    let fetched = client.call(&request, 9).await;
    assert_eq!(fetched.groups[0].error_code, 0);
    fetched.groups[0].topics[0]
        .partitions
        .iter()
        .map(|p| (p.partition_index, p.committed_offset))
        .collect()
}

#[tokio::test(flavor = "multi_thread")]
async fn consumer_group_joins_syncs_commits_and_leaves() {
    let dir = tempfile::tempdir().unwrap();
    let (port, _server) = start(dir.path());
    let mut client = Client::connect(port).await;
    create_topic(&mut client, "orders", 2).await;

    let found = client
        .call(
            &FindCoordinatorRequest::default()
                .with_coordinator_keys(vec![StrBytes::from_static_str("g")]),
            6,
        )
        .await;
    let coordinator = &found.coordinators[0];
    assert_eq!((coordinator.error_code, coordinator.node_id.0), (0, 0));
    assert_eq!(
        (coordinator.host.as_str(), coordinator.port),
        ("127.0.0.1", i32::from(port))
    );

    let joined = join_new(&mut client, "g", 10_000).await;
    assert_eq!((joined.error_code, joined.generation_id), (0, 1));
    assert_eq!(joined.leader, joined.member_id);
    assert_eq!(joined.protocol_name.as_deref(), Some("range"));
    assert_eq!(joined.members.len(), 1);
    let synced = sync(
        &mut client,
        "g",
        &joined,
        &[(&joined.member_id, b"orders-0-1")],
    )
    .await;
    assert_eq!(
        (synced.error_code, &synced.assignment[..]),
        (0, &b"orders-0-1"[..])
    );
    assert_eq!(heartbeat(&mut client, "g", &joined).await, 0);

    let committed = client
        .call(&commit_request("g", Some(&joined), "orders", &[(0, 42)]), 9)
        .await;
    assert_eq!(committed.topics[0].partitions[0].error_code, 0);
    let unknown = client
        .call(&commit_request("g", Some(&joined), "orders", &[(7, 1)]), 9)
        .await;
    assert_eq!(
        unknown.topics[0].partitions[0].error_code,
        UNKNOWN_TOPIC_OR_PARTITION
    );
    assert_eq!(
        fetch_offsets(&mut client, "g", "orders").await,
        [(0, 42), (1, -1)]
    );

    let described = client
        .call(
            &DescribeGroupsRequest::default().with_groups(vec![group_id("g")]),
            6,
        )
        .await;
    let group = &described.groups[0];
    assert_eq!(
        (group.group_state.as_str(), group.protocol_data.as_str()),
        ("Stable", "range")
    );
    assert_eq!(group.members[0].client_id.as_str(), "roundtrip-test");
    assert_eq!(&group.members[0].member_assignment[..], b"orders-0-1");

    let left = client
        .call(
            &LeaveGroupRequest::default()
                .with_group_id(group_id("g"))
                .with_members(vec![
                    MemberIdentity::default().with_member_id(joined.member_id.clone()),
                ]),
            5,
        )
        .await;
    assert_eq!((left.error_code, left.members[0].error_code), (0, 0));
    assert_eq!(
        heartbeat(&mut client, "g", &joined).await,
        UNKNOWN_MEMBER_ID
    );
    let described = client
        .call(
            &DescribeGroupsRequest::default().with_groups(vec![group_id("g"), group_id("nope")]),
            6,
        )
        .await;
    assert_eq!(described.groups[0].group_state.as_str(), "Empty");
    assert_eq!(
        described.groups[1].error_code, 69,
        "unknown group is GROUP_ID_NOT_FOUND"
    );
    let listed = client.call(&ListGroupsRequest::default(), 5).await;
    assert_eq!(listed.groups.len(), 1);
    assert_eq!(listed.groups[0].group_state.as_str(), "Empty");
}

#[tokio::test(flavor = "multi_thread")]
async fn consumer_group_rebalances_on_join_and_session_expiry() {
    let dir = tempfile::tempdir().unwrap();
    let (port, _server) = start(dir.path());
    let mut a = Client::connect(port).await;
    let a_joined = join_new(&mut a, "g", 10_000).await;
    sync(&mut a, "g", &a_joined, &[(&a_joined.member_id, b"all")]).await;

    let mut b = Client::connect(port).await;
    let b_joining = tokio::spawn(async move {
        let joined = join_new(&mut b, "g", 500).await;
        (b, joined)
    });
    let mut heartbeats = 0;
    while heartbeat(&mut a, "g", &a_joined).await != REBALANCE_IN_PROGRESS {
        heartbeats += 1;
        assert!(heartbeats < 100, "the new member never started a rebalance");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let a_joined = a
        .call(&join_request("g", &a_joined.member_id, 10_000), 9)
        .await;
    let (mut b, b_joined) = b_joining.await.unwrap();
    assert_eq!((a_joined.generation_id, b_joined.generation_id), (2, 2));
    assert_eq!(a_joined.members.len(), 2);
    assert!(
        b_joined.members.is_empty(),
        "only the leader gets the member list"
    );

    let b_syncing = tokio::spawn(async move { sync(&mut b, "g", &b_joined, &[]).await });
    let (a_id, b_id) = (
        a_joined.member_id.clone(),
        a_joined
            .members
            .iter()
            .find(|m| m.member_id != a_joined.member_id)
            .unwrap()
            .member_id
            .clone(),
    );
    let synced = sync(&mut a, "g", &a_joined, &[(&a_id, b"p0"), (&b_id, b"p1")]).await;
    assert_eq!(&synced.assignment[..], b"p0");
    assert_eq!(&b_syncing.await.unwrap().assignment[..], b"p1");

    let started = Instant::now();
    while heartbeat(&mut a, "g", &a_joined).await != REBALANCE_IN_PROGRESS {
        assert!(
            started.elapsed() < Duration::from_secs(5),
            "silent member was never expired"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(
        started.elapsed() >= Duration::from_millis(400),
        "expired before its 500 ms session"
    );
    let a_rejoined = a.call(&join_request("g", &a_id, 10_000), 9).await;
    assert_eq!((a_rejoined.generation_id, a_rejoined.members.len()), (3, 1));
}

#[tokio::test(flavor = "multi_thread")]
async fn committed_offsets_survive_restart() {
    let dir = tempfile::tempdir().unwrap();
    let (port, server) = start(dir.path());
    let mut client = Client::connect(port).await;
    create_topic(&mut client, "kept", 2).await;
    let committed = client
        .call(&commit_request("idle", None, "kept", &[(0, 5), (1, 9)]), 9)
        .await;
    assert!(
        committed.topics[0]
            .partitions
            .iter()
            .all(|p| p.error_code == 0)
    );
    drop(client);
    server.abort();

    let (port, _server) = start(dir.path());
    let mut client = Client::connect(port).await;
    assert_eq!(
        fetch_offsets(&mut client, "idle", "kept").await,
        [(0, 5), (1, 9)]
    );
    let all = client
        .call(
            &OffsetFetchRequest::default()
                .with_group_id(group_id("idle"))
                .with_topics(None),
            7,
        )
        .await;
    let offsets: Vec<_> = all.topics[0]
        .partitions
        .iter()
        .map(|p| (p.partition_index, p.committed_offset))
        .collect();
    assert_eq!(
        (all.topics[0].name.as_str(), offsets),
        ("kept", vec![(0, 5), (1, 9)])
    );
    assert_eq!(
        all.topics[0].partitions[0].metadata.as_deref(),
        Some("meta")
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn group_apis_answer_at_oldest_versions() {
    let dir = tempfile::tempdir().unwrap();
    let (port, _server) = start(dir.path());
    let mut client = Client::connect(port).await;
    create_topic(&mut client, "old", 1).await;

    let found = client
        .call(
            &FindCoordinatorRequest::default().with_key(StrBytes::from_static_str("g")),
            0,
        )
        .await;
    assert_eq!((found.error_code, found.port), (0, i32::from(port)));
    let refused = client
        .call(
            &FindCoordinatorRequest::default()
                .with_key(StrBytes::from_static_str("txn"))
                .with_key_type(1),
            3,
        )
        .await;
    assert_ne!(
        refused.error_code, 0,
        "transaction coordinators are not offered"
    );

    let joined = client
        .call(&join_request("g", &StrBytes::default(), 10_000), 0)
        .await;
    assert_eq!((joined.error_code, joined.generation_id), (0, 1));
    let synced = client
        .call(
            &SyncGroupRequest::default()
                .with_group_id(group_id("g"))
                .with_generation_id(1)
                .with_member_id(joined.member_id.clone())
                .with_assignments(vec![
                    SyncGroupRequestAssignment::default()
                        .with_member_id(joined.member_id.clone())
                        .with_assignment(Bytes::from_static(b"a")),
                ]),
            0,
        )
        .await;
    assert_eq!(synced.error_code, 0);
    let beat = HeartbeatRequest::default()
        .with_group_id(group_id("g"))
        .with_generation_id(1)
        .with_member_id(joined.member_id.clone());
    assert_eq!(client.call(&beat, 0).await.error_code, 0);
    let committed = client
        .call(&commit_request("g", Some(&joined), "old", &[(0, 3)]), 2)
        .await;
    assert_eq!(committed.topics[0].partitions[0].error_code, 0);
    let fetched = client
        .call(
            &OffsetFetchRequest::default()
                .with_group_id(group_id("g"))
                .with_topics(Some(vec![
                    OffsetFetchRequestTopic::default()
                        .with_name(topic_name("old"))
                        .with_partition_indexes(vec![0]),
                ])),
            1,
        )
        .await;
    assert_eq!(fetched.topics[0].partitions[0].committed_offset, 3);
    let described = client
        .call(
            &DescribeGroupsRequest::default().with_groups(vec![group_id("g"), group_id("nope")]),
            0,
        )
        .await;
    assert_eq!(described.groups[0].group_state.as_str(), "Stable");
    assert_eq!(described.groups[1].group_state.as_str(), "Dead");
    assert_eq!(
        client
            .call(&ListGroupsRequest::default(), 0)
            .await
            .groups
            .len(),
        1
    );
    let left = client
        .call(
            &LeaveGroupRequest::default()
                .with_group_id(group_id("g"))
                .with_member_id(joined.member_id.clone()),
            0,
        )
        .await;
    assert_eq!(left.error_code, 0);
}

/// A Metadata request whose topics array claims more elements than any frame could hold: a compact
/// count of 2^32 - 2 at v12, and a count of `i32::MAX` at v1. Each must close only its own connection.
#[tokio::test(flavor = "multi_thread")]
async fn absurd_array_count_closes_connection_and_broker_stays_up() {
    let dir = tempfile::tempdir().unwrap();
    let (port, _server) = start(dir.path());
    let crafted: [(i16, &[u8]); 2] = [
        (12, &[0xff, 0xff, 0xff, 0xff, 0x0f]),
        (1, &i32::MAX.to_be_bytes()),
    ];
    for (version, topics_count) in crafted {
        let mut frame = BytesMut::new();
        frame.put_i32(0);
        RequestHeader::default()
            .with_request_api_key(MetadataRequest::KEY)
            .with_request_api_version(version)
            .with_correlation_id(1)
            .encode(&mut frame, MetadataRequest::header_version(version))
            .unwrap();
        frame.put_slice(topics_count);
        let len = i32::try_from(frame.len() - 4).unwrap();
        frame[..4].copy_from_slice(&len.to_be_bytes());

        let mut stream = TcpStream::connect(("127.0.0.1", port)).await.unwrap();
        stream.write_all(&frame).await.unwrap();
        let mut reply = Vec::new();
        let read = tokio::time::timeout(Duration::from_secs(5), stream.read_to_end(&mut reply))
            .await
            .expect("broker must close the connection");
        assert!(
            matches!(read, Ok(0) | Err(_)),
            "Metadata v{version} with an absurd array count got a reply: {reply:?}"
        );
    }
    let versions = Client::connect(port)
        .await
        .call(&ApiVersionsRequest::default(), 3)
        .await;
    assert_eq!(versions.error_code, 0);
}

#[tokio::test(flavor = "multi_thread")]
async fn describe_configs_answers_known_topics_and_this_broker() {
    let dir = tempfile::tempdir().unwrap();
    let (port, _server) = start(dir.path());
    let mut client = Client::connect(port).await;
    create_topic(&mut client, "configured", 1).await;
    let resource = |resource_type, name| {
        DescribeConfigsResource::default()
            .with_resource_type(resource_type)
            .with_resource_name(StrBytes::from_static_str(name))
    };
    let request = DescribeConfigsRequest::default().with_resources(vec![
        resource(2, "configured"),
        resource(2, "missing"),
        resource(4, "0"),
    ]);
    for version in [1, 4] {
        let codes: Vec<_> = client
            .call(&request, version)
            .await
            .results
            .iter()
            .map(|r| r.error_code)
            .collect();
        assert_eq!(codes, [0, UNKNOWN_TOPIC_OR_PARTITION, 0]);
    }
}
