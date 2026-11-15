//! Request handlers, one per implemented Kafka API, plus the advertised version table.

use std::future::Future;
use std::io;
use std::ops::Range;
use std::sync::Arc;
use std::time::Duration;

use bytes::{BufMut, Bytes, BytesMut};
use kafka_protocol::ResponseError;
use kafka_protocol::messages::api_versions_response::ApiVersion;
use kafka_protocol::messages::create_topics_request::CreatableTopic;
use kafka_protocol::messages::create_topics_response::CreatableTopicResult;
use kafka_protocol::messages::delete_topics_response::DeletableTopicResult;
use kafka_protocol::messages::describe_configs_response::DescribeConfigsResult;
use kafka_protocol::messages::describe_groups_response::DescribedGroup;
use kafka_protocol::messages::fetch_response::{
    EpochEndOffset, FetchableTopicResponse, LeaderIdAndEpoch, PartitionData, SnapshotId,
};
use kafka_protocol::messages::find_coordinator_response::Coordinator as CoordinatorNode;
use kafka_protocol::messages::leave_group_response::MemberResponse;
use kafka_protocol::messages::list_offsets_response::{
    ListOffsetsPartitionResponse, ListOffsetsTopicResponse,
};
use kafka_protocol::messages::metadata_request::MetadataRequestTopic;
use kafka_protocol::messages::metadata_response::{
    MetadataResponseBroker, MetadataResponsePartition, MetadataResponseTopic,
};
use kafka_protocol::messages::offset_commit_response::{
    OffsetCommitResponsePartition, OffsetCommitResponseTopic,
};
use kafka_protocol::messages::offset_fetch_response::{
    OffsetFetchResponseGroup, OffsetFetchResponsePartition, OffsetFetchResponsePartitions,
    OffsetFetchResponseTopic, OffsetFetchResponseTopics,
};
use kafka_protocol::messages::offset_for_leader_epoch_response::{
    EpochEndOffset as LeaderEpochEndOffset, OffsetForLeaderTopicResult,
};
use kafka_protocol::messages::produce_response::{PartitionProduceResponse, TopicProduceResponse};
use kafka_protocol::messages::{
    AddOffsetsToTxnResponse, ApiKey, ApiVersionsRequest, ApiVersionsResponse, BrokerId,
    ConsumerGroupHeartbeatResponse, CreateTopicsRequest, CreateTopicsResponse, DeleteTopicsRequest,
    DeleteTopicsResponse, DescribeClusterResponse, DescribeConfigsRequest, DescribeConfigsResponse,
    DescribeGroupsRequest, DescribeGroupsResponse, EndTxnResponse, FetchRequest, FetchResponse,
    FindCoordinatorRequest, FindCoordinatorResponse, GetTelemetrySubscriptionsResponse, GroupId,
    HeartbeatRequest, HeartbeatResponse, InitProducerIdRequest, InitProducerIdResponse,
    JoinGroupRequest, JoinGroupResponse, LeaveGroupRequest, LeaveGroupResponse, ListGroupsRequest,
    ListGroupsResponse, ListOffsetsRequest, ListOffsetsResponse, MetadataRequest, MetadataResponse,
    OffsetCommitRequest, OffsetCommitResponse, OffsetFetchRequest, OffsetFetchResponse,
    OffsetForLeaderEpochRequest, OffsetForLeaderEpochResponse, ProduceRequest, ProduceResponse,
    ProducerId, PushTelemetryResponse, ResponseKind, SaslAuthenticateResponse,
    SaslHandshakeResponse, SyncGroupRequest, SyncGroupResponse, TopicName,
};
use kafka_protocol::protocol::{Message, Request, StrBytes, VersionRange};
use tokio::sync::mpsc;
use tokio::time::{Instant, sleep_until, timeout_at};
use tracing::warn;
use uuid::Uuid;

use crate::broker::{Broker, check_leader_epoch};
use crate::cluster::{Cluster, Refusal, Topic, TopicRef, TopicSpec};
use crate::group::{self, ClientInfo, Committed};
use crate::replica::Replica;
use crate::storage::Extent;

const CLUSTER_ID: &str = "redcrab";
/// Largest record batch accepted, so that replicating one stays within a transport frame.
pub const MAX_BATCH_BYTES: usize = 8 << 20;
/// Logs keep everything from offset 0; see the ponytail in `raft`.
const LOG_START_OFFSET: i64 = 0;
const BATCH_HEADER_LEN: usize = 61;
const EARLIEST_TIMESTAMP: i64 = -2;
const LATEST_TIMESTAMP: i64 = -1;
const GROUP_KEY_TYPE: i8 = 0;
const TOPIC_RESOURCE: i8 = 2;
const BROKER_RESOURCE: i8 = 4;
const MAX_OFFSET_METADATA_BYTES: usize = 4096;

/// Version range this broker implements for `key`, or `None` if the API is not implemented.
pub fn implemented(key: ApiKey) -> Option<VersionRange> {
    Some(match key {
        ApiKey::Produce => both::<ProduceRequest>(),
        ApiKey::Fetch => both::<FetchRequest>(),
        // ponytail: ListOffsets capped at v6, so max-timestamp / earliest-local / latest-tiered lookups are unsupported.
        ApiKey::ListOffsets => {
            both::<ListOffsetsRequest>().intersect(&VersionRange { min: 0, max: 6 })
        }
        ApiKey::Metadata => both::<MetadataRequest>(),
        ApiKey::ApiVersions => both::<ApiVersionsRequest>(),
        ApiKey::CreateTopics => both::<CreateTopicsRequest>(),
        ApiKey::DeleteTopics => both::<DeleteTopicsRequest>(),
        ApiKey::InitProducerId => both::<InitProducerIdRequest>(),
        ApiKey::FindCoordinator => both::<FindCoordinatorRequest>(),
        ApiKey::JoinGroup => both::<JoinGroupRequest>(),
        ApiKey::SyncGroup => both::<SyncGroupRequest>(),
        ApiKey::Heartbeat => both::<HeartbeatRequest>(),
        ApiKey::LeaveGroup => both::<LeaveGroupRequest>(),
        ApiKey::OffsetCommit => both::<OffsetCommitRequest>(),
        ApiKey::OffsetFetch => both::<OffsetFetchRequest>(),
        ApiKey::DescribeGroups => both::<DescribeGroupsRequest>(),
        ApiKey::ListGroups => both::<ListGroupsRequest>(),
        ApiKey::DescribeConfigs => both::<DescribeConfigsRequest>(),
        ApiKey::OffsetForLeaderEpoch => both::<OffsetForLeaderEpochRequest>(),
        _ => return None,
    })
}

fn both<R: Request>() -> VersionRange {
    R::VERSIONS.intersect(&R::Response::VERSIONS)
}

/// Whether `version` of `key` is implemented.
pub fn supports(key: ApiKey, version: i16) -> bool {
    implemented(key).is_some_and(|r| (r.min..=r.max).contains(&version))
}

/// The `ApiVersions` response listing every implemented API.
pub fn api_versions(error_code: i16) -> ApiVersionsResponse {
    let api_keys = ApiKey::iter()
        .filter_map(|key| {
            implemented(key).map(|r| {
                ApiVersion::default()
                    .with_api_key(key as i16)
                    .with_min_version(r.min)
                    .with_max_version(r.max)
            })
        })
        .collect();
    ApiVersionsResponse::default()
        .with_error_code(error_code)
        .with_api_keys(api_keys)
}

/// `UNSUPPORTED_VERSION` reply, and the version to encode it at, for an API or version this broker
/// does not implement. `None` when the reply has no top-level error code to carry it.
pub fn unsupported(key: ApiKey, version: i16) -> Option<(i16, ResponseKind)> {
    let code = ResponseError::UnsupportedVersion.code();
    if key == ApiKey::ApiVersions {
        return Some((0, api_versions(code).into()));
    }
    let crate_versions = key.valid_versions();
    if !(crate_versions.min..=crate_versions.max).contains(&version) {
        return None;
    }
    let response: ResponseKind = match key {
        ApiKey::ConsumerGroupHeartbeat => ConsumerGroupHeartbeatResponse::default()
            .with_error_code(code)
            .into(),
        ApiKey::SaslHandshake => SaslHandshakeResponse::default()
            .with_error_code(code)
            .into(),
        ApiKey::SaslAuthenticate => SaslAuthenticateResponse::default()
            .with_error_code(code)
            .into(),
        ApiKey::DescribeCluster => DescribeClusterResponse::default()
            .with_error_code(code)
            .into(),
        ApiKey::GetTelemetrySubscriptions => GetTelemetrySubscriptionsResponse::default()
            .with_error_code(code)
            .into(),
        ApiKey::PushTelemetry => PushTelemetryResponse::default()
            .with_error_code(code)
            .into(),
        ApiKey::AddOffsetsToTxn => AddOffsetsToTxnResponse::default()
            .with_error_code(code)
            .into(),
        ApiKey::EndTxn => EndTxnResponse::default().with_error_code(code).into(),
        _ => return None,
    };
    Some((version, response))
}

/// Kafka topic name rules: 1-249 chars of `[a-zA-Z0-9._-]`, not `.` or `..`.
pub fn valid_topic_name(name: &str) -> bool {
    !name.is_empty()
        && name.len() <= 249
        && name != "."
        && name != ".."
        && name
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'.' | b'_' | b'-'))
}

/// Answers a `Metadata` request. No topic is ever auto-created.
pub fn metadata(broker: &Broker, version: i16, request: &MetadataRequest) -> MetadataResponse {
    let cluster = broker.cluster();
    let topics = match &request.topics {
        Some(wanted) if !(wanted.is_empty() && version == 0) => wanted
            .iter()
            .map(|t| requested_topic(cluster, version, t))
            .collect(),
        _ => cluster
            .topics()
            .iter()
            .map(|t| topic_metadata(cluster, t))
            .collect(),
    };
    let brokers = cluster
        .peers
        .iter()
        .map(|peer| {
            MetadataResponseBroker::default()
                .with_node_id(BrokerId(peer.id))
                .with_host(StrBytes::from_string(peer.host.clone()))
                .with_port(i32::from(peer.port))
        })
        .collect();
    MetadataResponse::default()
        .with_brokers(brokers)
        .with_cluster_id(Some(StrBytes::from_static_str(CLUSTER_ID)))
        .with_controller_id(BrokerId(cluster.controller().unwrap_or(-1)))
        .with_topics(topics)
}

fn requested_topic(
    cluster: &Cluster,
    version: i16,
    wanted: &MetadataRequestTopic,
) -> MetadataResponseTopic {
    let found = match &wanted.name {
        Some(name) => cluster.topic(name),
        None => cluster.topic_by_id(wanted.topic_id),
    };
    if let Some(topic) = found {
        return topic_metadata(cluster, &topic);
    }
    let error = match &wanted.name {
        Some(name) if !valid_topic_name(name) => ResponseError::InvalidTopicException,
        Some(_) => ResponseError::UnknownTopicOrPartition,
        None => ResponseError::UnknownTopicId,
    };
    MetadataResponseTopic::default()
        .with_error_code(error.code())
        .with_name(
            wanted
                .name
                .clone()
                .or_else(|| (version < 12).then(TopicName::default)),
        )
        .with_topic_id(wanted.topic_id)
}

fn topic_metadata(cluster: &Cluster, topic: &Topic) -> MetadataResponseTopic {
    let partitions = (0..)
        .zip(&topic.replicas)
        .map(|(index, replicas)| {
            let nodes: Vec<BrokerId> = replicas.iter().map(|&id| BrokerId(id)).collect();
            let partition = MetadataResponsePartition::default()
                .with_partition_index(index)
                .with_replica_nodes(nodes.clone())
                // ponytail: every replica is reported in sync; tracking which followers are caught up is the upgrade.
                .with_isr_nodes(nodes);
            match topic.group(index).and_then(|key| cluster.leader(key)) {
                Some((leader, epoch)) => partition
                    .with_leader_id(BrokerId(leader))
                    .with_leader_epoch(epoch),
                None => partition
                    .with_error_code(ResponseError::LeaderNotAvailable.code())
                    .with_leader_id(BrokerId(-1))
                    .with_leader_epoch(-1),
            }
        })
        .collect();
    MetadataResponseTopic::default()
        .with_name(Some(topic_name(&topic.name)))
        .with_topic_id(topic.id)
        .with_partitions(partitions)
}

/// Answers a `CreateTopics` request. The controller creates each topic in the replicated metadata
/// log; the answer comes once this broker has applied it and, within the request's timeout, every
/// new partition has a leader.
pub async fn create_topics(broker: &Broker, request: CreateTopicsRequest) -> CreateTopicsResponse {
    let wait = millis(request.timeout_ms);
    let mut results = Vec::with_capacity(request.topics.len());
    for wanted in &request.topics {
        let result = CreatableTopicResult::default().with_name(wanted.name.clone());
        let created = match topic_spec(wanted) {
            Ok(spec) if request.validate_only => broker.cluster().validate_topic(&spec),
            Ok(spec) => broker.cluster().create_topic(&spec, wait).await,
            Err(refusal) => Err(refusal),
        };
        results.push(match created {
            Ok(created) => result
                .with_topic_id(created.id)
                .with_num_partitions(created.partitions)
                .with_replication_factor(created.replication_factor)
                .with_configs(Some(Vec::new())),
            Err(refusal) => result
                .with_error_code(refusal.code)
                .with_error_message(refusal.message.map(StrBytes::from_string)),
        });
    }
    CreateTopicsResponse::default().with_topics(results)
}

fn topic_spec(wanted: &CreatableTopic) -> Result<TopicSpec, Refusal> {
    let mut assignments: Vec<_> = wanted.assignments.iter().collect();
    assignments.sort_by_key(|a| a.partition_index);
    if (0..)
        .zip(&assignments)
        .any(|(index, a)| a.partition_index != index)
    {
        return Err(Refusal {
            code: ResponseError::InvalidReplicaAssignment.code(),
            message: Some("assignments must name partitions 0 to n-1 once each".into()),
        });
    }
    Ok(TopicSpec {
        name: wanted.name.to_string(),
        partitions: wanted.num_partitions,
        replication_factor: wanted.replication_factor,
        assignments: assignments
            .iter()
            .map(|a| a.broker_ids.iter().map(|id| id.0).collect())
            .collect(),
    })
}

fn millis(ms: i32) -> Duration {
    Duration::from_millis(u64::try_from(ms).unwrap_or(0))
}

/// Answers a `DeleteTopics` request once each deletion is applied on this broker.
pub async fn delete_topics(
    broker: &Broker,
    version: i16,
    request: DeleteTopicsRequest,
) -> DeleteTopicsResponse {
    let wait = millis(request.timeout_ms);
    let wanted: Vec<_> = if version >= 6 {
        request
            .topics
            .into_iter()
            .map(|t| (t.name, t.topic_id))
            .collect()
    } else {
        request
            .topic_names
            .into_iter()
            .map(|name| (Some(name), Uuid::nil()))
            .collect()
    };
    let mut responses = Vec::with_capacity(wanted.len());
    for (name, id) in wanted {
        let target = match &name {
            Some(name) => TopicRef::Name(name.to_string()),
            None => TopicRef::Id(id),
        };
        let mut result = DeletableTopicResult::default()
            .with_name(name.clone())
            .with_topic_id(id);
        match broker.cluster().delete_topic(target, wait).await {
            Ok((id, deleted)) => {
                result.name = Some(topic_name(&deleted));
                result.topic_id = id;
            }
            Err(refusal) => {
                result.error_code = refusal.code;
                result.error_message = refusal.message.map(StrBytes::from_string);
            }
        }
        responses.push(result);
    }
    DeleteTopicsResponse::default().with_responses(responses)
}

/// Starts a `Produce`: every batch is proposed to its partition's leader before this returns, so
/// request order is append order. The future resolves once each proposal is committed on a majority
/// of its replicas, has failed, or the request's timeout has passed; immediately with `None` for
/// acks=0, which gets no response.
pub fn produce(
    broker: &Broker,
    version: i16,
    request: ProduceRequest,
) -> impl Future<Output = Option<ProduceResponse>> + Send + use<> {
    let acks = request.acks;
    let deadline = Instant::now() + millis(request.timeout_ms);
    let mut responses = Vec::with_capacity(request.topic_data.len());
    let mut pending = Vec::new();
    for (t, wanted) in request.topic_data.into_iter().enumerate() {
        let (topic, unknown) = if version >= 13 {
            (
                broker.cluster().topic_by_id(wanted.topic_id),
                ResponseError::UnknownTopicId,
            )
        } else {
            (
                broker.cluster().topic(&wanted.name),
                ResponseError::UnknownTopicOrPartition,
            )
        };
        let mut partitions = Vec::with_capacity(wanted.partition_data.len());
        for (p, data) in wanted.partition_data.into_iter().enumerate() {
            let mut response = PartitionProduceResponse::default().with_index(data.index);
            let leader = topic
                .as_ref()
                .ok_or(unknown.code())
                .and_then(|topic| broker.leader_replica(topic, data.index));
            let records = data.records.unwrap_or_default();
            match (leader, split_batches(&records)) {
                _ if !matches!(acks, -1..=1) => {
                    response.error_code = ResponseError::InvalidRequiredAcks.code();
                }
                (Err(code), _) | (Ok(_), Err(code)) => response.error_code = code,
                (Ok(_), Ok(batches)) if batches.iter().any(|b| b.len() > MAX_BATCH_BYTES) => {
                    response.error_code = ResponseError::MessageTooLarge.code();
                }
                (Ok(leader), Ok(batches)) => {
                    response.log_start_offset = LOG_START_OFFSET;
                    pending.push((t, p, leader.propose(records.to_vec(), batches)));
                }
            }
            partitions.push(response);
        }
        responses.push(
            TopicProduceResponse::default()
                .with_name(wanted.name)
                .with_topic_id(wanted.topic_id)
                .with_partition_responses(partitions),
        );
    }
    async move {
        if acks == 0 {
            return None;
        }
        // ponytail: acks=1 waits for a majority like acks=all; acknowledging on the leader's own flush is the upgrade.
        for (t, p, proposed) in pending {
            let response = &mut responses[t].partition_responses[p];
            match timeout_at(deadline, proposed).await {
                Ok(Ok(Ok(base_offset))) => response.base_offset = base_offset,
                Ok(Ok(Err(code))) => response.error_code = code,
                Ok(Err(_)) => response.error_code = ResponseError::NotLeaderOrFollower.code(),
                Err(_) => response.error_code = ResponseError::RequestTimedOut.code(),
            }
        }
        Some(ProduceResponse::default().with_responses(responses))
    }
}

/// Splits a produce payload into its back-to-back v2 record batches, checking framing and CRC-32C.
///
/// # Errors
/// Returns the Kafka error code to report for the partition.
pub fn split_batches(records: &[u8]) -> Result<Vec<Range<usize>>, i16> {
    let corrupt = ResponseError::CorruptMessage.code();
    if records.is_empty() {
        return Err(corrupt);
    }
    let mut batches = Vec::new();
    let mut pos = 0;
    while pos < records.len() {
        let rest = &records[pos..];
        if rest.len() < BATCH_HEADER_LEN {
            return Err(corrupt);
        }
        let batch_length = i32::from_be_bytes([rest[8], rest[9], rest[10], rest[11]]);
        let len = usize::try_from(batch_length).map_or(0, |n| n + 12);
        if len < BATCH_HEADER_LEN || len > rest.len() {
            return Err(corrupt);
        }
        if rest[16] != 2 {
            return Err(ResponseError::UnsupportedForMessageFormat.code());
        }
        let crc = u32::from_be_bytes([rest[17], rest[18], rest[19], rest[20]]);
        let last_offset_delta = i32::from_be_bytes([rest[23], rest[24], rest[25], rest[26]]);
        if crc32c::crc32c(&rest[21..len]) != crc || last_offset_delta < 0 {
            return Err(corrupt);
        }
        batches.push(pos..pos + len);
        pos += len;
    }
    Ok(batches)
}

struct FetchTarget {
    topic: usize,
    index: i32,
    replica: Option<Arc<Replica>>,
    error: i16,
    offset: i64,
    max_bytes: usize,
}

impl FetchTarget {
    /// Whether the partition has records past the fetch offset, or an answer that cannot change.
    fn ready(&self) -> bool {
        self.error != 0
            || self.replica.as_ref().is_some_and(|r| {
                self.offset < r.high_watermark()
                    || !(LOG_START_OFFSET..=r.log_end_offset()).contains(&self.offset)
            })
    }
}

/// Answers a sessionless `Fetch` on partition leaders, waiting up to `max_wait_ms` for `min_bytes`
/// of newly committed data.
pub async fn fetch(broker: Arc<Broker>, version: i16, request: FetchRequest) -> FetchBody {
    if request.session_id != 0 {
        let response =
            FetchResponse::default().with_error_code(ResponseError::FetchSessionIdNotFound.code());
        return FetchBody::new(version, &response, Vec::new());
    }
    let cluster = broker.cluster();
    let mut topics = Vec::with_capacity(request.topics.len());
    let mut targets = Vec::new();
    for (t, wanted) in request.topics.into_iter().enumerate() {
        let (topic, unknown) = if version >= 13 {
            (
                cluster.topic_by_id(wanted.topic_id),
                ResponseError::UnknownTopicId,
            )
        } else {
            (
                cluster.topic(&wanted.topic),
                ResponseError::UnknownTopicOrPartition,
            )
        };
        for p in &wanted.partitions {
            let replica = topic
                .as_ref()
                .ok_or(unknown.code())
                .and_then(|topic| broker.leader_replica(topic, p.partition))
                .and_then(|replica| {
                    check_leader_epoch(replica.view(), p.current_leader_epoch).map(|()| replica)
                });
            targets.push(FetchTarget {
                topic: t,
                index: p.partition,
                error: replica.as_ref().err().copied().unwrap_or(0),
                replica: replica.ok(),
                offset: p.fetch_offset,
                max_bytes: usize::try_from(p.partition_max_bytes).unwrap_or(0),
            });
        }
        topics.push(
            FetchableTopicResponse::default()
                .with_topic(wanted.topic)
                .with_topic_id(wanted.topic_id),
        );
    }
    let max_bytes = usize::try_from(request.max_bytes).unwrap_or(0);
    let min_bytes = usize::try_from(request.min_bytes).unwrap_or(0);
    let mut deadline =
        Instant::now() + Duration::from_millis(u64::try_from(request.max_wait_ms).unwrap_or(0));
    let targets = Arc::new(targets);
    let mut flushes = cluster.commits();
    loop {
        flushes.borrow_and_update();
        let expired = Instant::now() >= deadline;
        if expired || min_bytes == 0 || targets.iter().any(FetchTarget::ready) {
            let locating = Arc::clone(&targets);
            let found =
                tokio::task::spawn_blocking(move || locate_partitions(&locating, max_bytes))
                    .await
                    .unwrap_or_else(|_| {
                        let code = ResponseError::KafkaStorageError.code();
                        targets
                            .iter()
                            .map(|t| (partition_data(t, code), Vec::new()))
                            .collect()
                    });
            let bytes: usize = found.iter().flat_map(|(_, e)| e).map(Extent::size).sum();
            if expired || bytes >= min_bytes || found.iter().any(|(p, _)| p.error_code != 0) {
                let mut extents = Vec::with_capacity(found.len());
                for (target, (data, found)) in targets.iter().zip(found) {
                    topics[target.topic].partitions.push(data);
                    extents.push(found);
                }
                let response = FetchResponse::default().with_responses(topics);
                return FetchBody::new(version, &response, extents);
            }
        }
        tokio::select! {
            changed = flushes.changed() => if changed.is_err() { deadline = Instant::now() },
            () = sleep_until(deadline) => {}
        }
    }
}

fn partition_data(target: &FetchTarget, code: i16) -> PartitionData {
    PartitionData::default()
        .with_partition_index(target.index)
        .with_error_code(code)
}

/// Locates the records of every target, honouring partition and response byte limits; only the first
/// non-empty partition may exceed them, by a single batch, so the consumer always makes progress.
fn locate_partitions(
    targets: &[FetchTarget],
    max_bytes: usize,
) -> Vec<(PartitionData, Vec<Extent>)> {
    let mut remaining = max_bytes;
    let mut first = true;
    targets
        .iter()
        .map(|target| {
            let Some(replica) = &target.replica else {
                return (partition_data(target, target.error), Vec::new());
            };
            let limit = target.max_bytes.min(remaining);
            let high_watermark = replica.high_watermark();
            let mut data = partition_data(target, 0);
            let mut extents = Vec::new();
            if !(LOG_START_OFFSET..=replica.log_end_offset()).contains(&target.offset) {
                data.error_code = ResponseError::OffsetOutOfRange.code();
            } else if target.offset < high_watermark && (limit > 0 || first) {
                match replica.locate(target.offset, limit, first, high_watermark) {
                    Ok(found) => extents = found,
                    Err(code) => data.error_code = code,
                }
            }
            let len: usize = extents.iter().map(Extent::size).sum();
            if len > 0 {
                first = false;
                remaining = remaining.saturating_sub(len);
            }
            let data = data
                .with_high_watermark(high_watermark)
                .with_last_stable_offset(high_watermark)
                .with_log_start_offset(LOG_START_OFFSET);
            (data, extents)
        })
        .collect()
}

/// The body of a `Fetch` response, written while its records are still being read: `len` bytes,
/// arriving in order on `chunks`, so the first partitions are on the wire while later ones are read.
pub struct FetchBody {
    /// Length of the whole body.
    pub len: usize,
    /// The body in order; an error means it cannot be completed.
    pub chunks: mpsc::UnboundedReceiver<io::Result<Bytes>>,
}

impl FetchBody {
    /// Starts reading `extents`, the records of each partition of `response` in order.
    fn new(version: i16, response: &FetchResponse, extents: Vec<Vec<Extent>>) -> FetchBody {
        let (sender, chunks) = mpsc::unbounded_channel();
        let lengths: Vec<_> = extents
            .iter()
            .map(|e| Some(e.iter().map(Extent::size).sum()))
            .collect();
        let mut head = BytesMut::new();
        match encode_fetch(response, version, &lengths, &mut head) {
            Ok(positions) => {
                let records: Vec<_> = positions
                    .into_iter()
                    .zip(extents)
                    .filter(|(_, extents)| !extents.is_empty())
                    .collect();
                let len = head.len() + lengths.into_iter().flatten().sum::<usize>();
                tokio::task::spawn_blocking(move || {
                    read_body(&head.freeze(), &records, len, &sender);
                });
                FetchBody { len, chunks }
            }
            Err(e) => {
                let _ = sender.send(Err(e));
                FetchBody { len: 0, chunks }
            }
        }
    }
}

/// Sends `head` with the records read into it at their positions, `len` bytes in all, cut after each
/// record set so each goes out as soon as it is read.
fn read_body(
    head: &Bytes,
    records: &[(usize, Vec<Extent>)],
    len: usize,
    chunks: &mpsc::UnboundedSender<io::Result<Bytes>>,
) {
    let mut from = records.first().map_or(head.len(), |(at, _)| *at);
    if chunks.send(Ok(head.slice(..from))).is_err() {
        return;
    }
    // ponytail: one fresh buffer per fetch; a pool would spare musl an mmap and page faults per fetch.
    let mut buffer = BytesMut::with_capacity(len - from);
    for (i, (_, extents)) in records.iter().enumerate() {
        for extent in extents {
            if let Err(e) = extent.read_into(&mut buffer) {
                warn!("reading fetched records failed: {e}");
                let _ = chunks.send(Err(e));
                return;
            }
        }
        let to = records.get(i + 1).map_or(head.len(), |(at, _)| *at);
        buffer.extend_from_slice(&head[from..to]);
        from = to;
        if chunks.send(Ok(buffer.split().freeze())).is_err() {
            return;
        }
    }
}

/// Encodes `response` as `FetchResponse::encode` does, except that each partition's records field
/// holds only the length prefix from `lengths` (one per partition, in order; `None` for null). Returns
/// where in `head` each partition's record bytes belong.
///
/// # Errors
/// Fails for a field this does not encode (tagged fields, aborted transactions) or a length that does
/// not fit the wire format.
fn encode_fetch(
    response: &FetchResponse,
    version: i16,
    lengths: &[Option<usize>],
    head: &mut BytesMut,
) -> io::Result<Vec<usize>> {
    let unsupported = || io::Error::new(io::ErrorKind::InvalidData, "unsupported Fetch response");
    let compact = version >= 12;
    let plain = (4..=18).contains(&version)
        && (version >= 7 || response.session_id == 0)
        && response.node_endpoints.is_empty()
        && response.unknown_tagged_fields.is_empty();
    if !plain {
        return Err(unsupported());
    }
    let mut lengths = lengths.iter();
    let mut positions = Vec::with_capacity(lengths.len());
    head.put_i32(response.throttle_time_ms);
    if version >= 7 {
        head.put_i16(response.error_code);
        head.put_i32(response.session_id);
    }
    put_len(head, Some(response.responses.len()), compact)?;
    for topic in &response.responses {
        if !topic.unknown_tagged_fields.is_empty() {
            return Err(unsupported());
        }
        if version >= 13 {
            head.put_slice(topic.topic_id.as_bytes());
        } else if compact {
            put_len(head, Some(topic.topic.len()), compact)?;
            head.put_slice(topic.topic.as_bytes());
        } else {
            head.put_i16(i16::try_from(topic.topic.len()).map_err(|_| unsupported())?);
            head.put_slice(topic.topic.as_bytes());
        }
        put_len(head, Some(topic.partitions.len()), compact)?;
        for partition in &topic.partitions {
            let plain = partition.diverging_epoch == EpochEndOffset::default()
                && partition.current_leader == LeaderIdAndEpoch::default()
                && partition.snapshot_id == SnapshotId::default()
                && partition.unknown_tagged_fields.is_empty()
                && partition
                    .aborted_transactions
                    .as_ref()
                    .is_none_or(Vec::is_empty)
                && (version >= 11 || partition.preferred_read_replica.0 == -1);
            if !plain {
                return Err(unsupported());
            }
            head.put_i32(partition.partition_index);
            head.put_i16(partition.error_code);
            head.put_i64(partition.high_watermark);
            head.put_i64(partition.last_stable_offset);
            if version >= 5 {
                head.put_i64(partition.log_start_offset);
            }
            let aborted = partition.aborted_transactions.as_ref().map(Vec::len);
            put_len(head, aborted, compact)?;
            if version >= 11 {
                head.put_i32(partition.preferred_read_replica.0);
            }
            put_len(head, *lengths.next().ok_or_else(unsupported)?, compact)?;
            positions.push(head.len());
            if compact {
                head.put_u8(0);
            }
        }
        if compact {
            head.put_u8(0);
        }
    }
    if compact {
        head.put_u8(0);
    }
    Ok(positions)
}

/// Length prefix of a nullable field: an unsigned varint of `len + 1` in flexible versions, otherwise
/// an `i32` with `-1` for null, as arrays and bytes use.
fn put_len(head: &mut BytesMut, len: Option<usize>, compact: bool) -> io::Result<()> {
    let too_long = || io::Error::new(io::ErrorKind::InvalidData, "field too long to encode");
    if compact {
        let mut value = match len {
            Some(len) => u32::try_from(len)
                .ok()
                .and_then(|len| len.checked_add(1))
                .ok_or_else(too_long)?,
            None => 0,
        };
        while value >= 0x80 {
            head.put_u8(u8::try_from(value & 0x7f).map_err(|_| too_long())? | 0x80);
            value >>= 7;
        }
        head.put_u8(u8::try_from(value).map_err(|_| too_long())?);
    } else {
        head.put_i32(len.map_or(Ok(-1), i32::try_from).map_err(|_| too_long())?);
    }
    Ok(())
}

/// Answers `ListOffsets` on partition leaders for the earliest, latest (high watermark) and
/// timestamp lookups.
pub async fn list_offsets(broker: Arc<Broker>, request: ListOffsetsRequest) -> ListOffsetsResponse {
    let topics = tokio::task::spawn_blocking(move || {
        request
            .topics
            .into_iter()
            .map(|wanted| {
                let topic = broker.cluster().topic(&wanted.name);
                let partitions = wanted
                    .partitions
                    .iter()
                    .map(|p| {
                        let mut response = ListOffsetsPartitionResponse::default()
                            .with_partition_index(p.partition_index)
                            .with_timestamp(-1)
                            .with_offset(-1)
                            .with_leader_epoch(-1);
                        let leader = topic
                            .as_ref()
                            .ok_or(ResponseError::UnknownTopicOrPartition.code())
                            .and_then(|t| broker.leader_replica(t, p.partition_index))
                            .and_then(|replica| {
                                check_leader_epoch(replica.view(), p.current_leader_epoch)
                                    .map(|()| replica)
                            });
                        match leader {
                            Err(code) => response.error_code = code,
                            Ok(replica) => list_offset(&replica, p.timestamp, &mut response),
                        }
                        response
                    })
                    .collect();
                ListOffsetsTopicResponse::default()
                    .with_name(wanted.name)
                    .with_partitions(partitions)
            })
            .collect()
    })
    .await
    .unwrap_or_default();
    ListOffsetsResponse::default().with_topics(topics)
}

fn list_offset(replica: &Replica, timestamp: i64, response: &mut ListOffsetsPartitionResponse) {
    match timestamp {
        EARLIEST_TIMESTAMP => {
            response.offset = LOG_START_OFFSET;
            response.leader_epoch = replica.epoch_at(LOG_START_OFFSET).unwrap_or(-1);
        }
        // A new leader's high watermark is relearned; until an entry of its own term commits it can
        // be lower than one already handed out, and a consumer resetting to latest would rewind.
        LATEST_TIMESTAMP if !replica.view().ready => {
            response.error_code = ResponseError::OffsetNotAvailable.code();
        }
        LATEST_TIMESTAMP => {
            let high_watermark = replica.high_watermark();
            response.offset = high_watermark;
            response.leader_epoch = replica.epoch_at(high_watermark - 1).unwrap_or(-1);
        }
        timestamp if timestamp >= 0 => {
            if let Some(offset) = replica.offset_for_timestamp(timestamp) {
                response.offset = offset;
                response.leader_epoch = replica.epoch_at(offset).unwrap_or(-1);
                // ponytail: echoes the requested timestamp, not the matched record's; needs a record-level lookup.
                response.timestamp = timestamp;
            }
        }
        _ => response.error_code = ResponseError::InvalidRequest.code(),
    }
}

/// Answers `OffsetsForLeaderEpoch` on partition leaders: for each requested epoch, the largest
/// epoch in the log at or below it and where that epoch ends.
pub async fn offsets_for_leader_epoch(
    broker: Arc<Broker>,
    request: OffsetForLeaderEpochRequest,
) -> OffsetForLeaderEpochResponse {
    let topics = tokio::task::spawn_blocking(move || {
        request
            .topics
            .into_iter()
            .map(|wanted| {
                let topic = broker.cluster().topic(&wanted.topic);
                let partitions = wanted
                    .partitions
                    .iter()
                    .map(|p| {
                        let mut result = LeaderEpochEndOffset::default()
                            .with_partition(p.partition)
                            .with_leader_epoch(-1)
                            .with_end_offset(-1);
                        let leader = topic
                            .as_ref()
                            .ok_or(ResponseError::UnknownTopicOrPartition.code())
                            .and_then(|t| broker.leader_replica(t, p.partition))
                            .and_then(|replica| {
                                check_leader_epoch(replica.view(), p.current_leader_epoch)
                                    .map(|()| replica)
                            });
                        match leader {
                            Err(code) => result.error_code = code,
                            Ok(replica) => {
                                if let Some((epoch, end)) = replica.epoch_end(p.leader_epoch) {
                                    result.leader_epoch = epoch;
                                    result.end_offset = end;
                                }
                            }
                        }
                        result
                    })
                    .collect();
                OffsetForLeaderTopicResult::default()
                    .with_topic(wanted.topic)
                    .with_partitions(partitions)
            })
            .collect()
    })
    .await
    .unwrap_or_default();
    OffsetForLeaderEpochResponse::default().with_topics(topics)
}

/// Hands out a fresh producer id so the default idempotent producer can start.
pub fn init_producer_id(
    broker: &Broker,
    request: &InitProducerIdRequest,
) -> InitProducerIdResponse {
    if request.transactional_id.is_some() {
        return InitProducerIdResponse::default()
            .with_error_code(ResponseError::InvalidRequest.code())
            .with_producer_id(ProducerId(-1))
            .with_producer_epoch(-1);
    }
    // ponytail: no sequence-number de-duplication yet (later milestone); retried batches may be appended twice.
    InitProducerIdResponse::default()
        .with_producer_id(ProducerId(broker.next_producer_id()))
        .with_producer_epoch(0)
}

/// Answers `FindCoordinator`: the controller coordinates every group; transactions are not
/// supported.
pub fn find_coordinator(
    broker: &Broker,
    version: i16,
    request: &FindCoordinatorRequest,
) -> FindCoordinatorResponse {
    let cluster = broker.cluster();
    let coordinator = cluster.controller().and_then(|id| cluster.peer(id));
    let node = BrokerId(coordinator.map_or(-1, |peer| peer.id));
    let host = StrBytes::from_string(
        coordinator
            .map(|peer| peer.host.clone())
            .unwrap_or_default(),
    );
    let port = coordinator.map_or(-1, |peer| i32::from(peer.port));
    let refused = if request.key_type != GROUP_KEY_TYPE {
        Some((
            ResponseError::InvalidRequest.code(),
            Some(StrBytes::from_static_str(
                "only consumer group coordinators exist",
            )),
        ))
    } else if coordinator.is_none() {
        Some((
            ResponseError::CoordinatorNotAvailable.code(),
            Some(StrBytes::from_static_str("no controller is elected yet")),
        ))
    } else {
        None
    };
    if version < 4 {
        let response = FindCoordinatorResponse::default();
        return match refused {
            None => response.with_node_id(node).with_host(host).with_port(port),
            Some((code, message)) => response
                .with_error_code(code)
                .with_error_message(message)
                .with_node_id(BrokerId(-1)),
        };
    }
    let coordinators = request
        .coordinator_keys
        .iter()
        .map(|key| {
            let coordinator = CoordinatorNode::default().with_key(key.clone());
            match &refused {
                None => coordinator
                    .with_node_id(node)
                    .with_host(host.clone())
                    .with_port(port),
                Some((code, message)) => coordinator
                    .with_error_code(*code)
                    .with_error_message(message.clone())
                    .with_node_id(BrokerId(-1)),
            }
        })
        .collect();
    FindCoordinatorResponse::default().with_coordinators(coordinators)
}

/// The error a member whose join or sync went unanswered gets: the coordinator moved, or it was
/// removed from the group.
fn unanswered(broker: &Broker) -> i16 {
    broker
        .cluster()
        .coordinator_check()
        .err()
        .unwrap_or(ResponseError::UnknownMemberId.code())
}

/// Answers `JoinGroup` once the group's join phase completes. The request is dropped before the
/// wait, which can last a rebalance timeout: decoded, it can be a hundred times its frame.
pub async fn join_group(
    broker: &Broker,
    version: i16,
    request: JoinGroupRequest,
    client: ClientInfo,
) -> JoinGroupResponse {
    if let Err(code) = broker.cluster().coordinator_check() {
        return group::join_error(code, request.member_id, version);
    }
    let reply = broker
        .groups()
        .join(&request, version, client, Instant::now());
    let member_id = request.member_id.clone();
    drop(request);
    reply
        .resolve(|| group::join_error(unanswered(broker), member_id, version))
        .await
}

/// Answers `SyncGroup`; followers wait for the leader's assignment, without keeping their request.
pub async fn sync_group(broker: &Broker, request: SyncGroupRequest) -> SyncGroupResponse {
    if let Err(code) = broker.cluster().coordinator_check() {
        return group::sync_error(code);
    }
    let reply = broker.groups().sync(&request, Instant::now());
    drop(request);
    reply
        .resolve(|| group::sync_error(unanswered(broker)))
        .await
}

/// Answers `Heartbeat`.
pub fn heartbeat(broker: &Broker, request: &HeartbeatRequest) -> HeartbeatResponse {
    let code = match broker.cluster().coordinator_check() {
        Ok(()) => broker.groups().heartbeat(request, Instant::now()),
        Err(code) => code,
    };
    HeartbeatResponse::default().with_error_code(code)
}

/// Answers `LeaveGroup`: one member before v3, a batch of members from v3.
pub fn leave_group(
    broker: &Broker,
    version: i16,
    request: &LeaveGroupRequest,
) -> LeaveGroupResponse {
    if let Err(code) = broker.cluster().coordinator_check() {
        return LeaveGroupResponse::default().with_error_code(code);
    }
    let ids: Vec<StrBytes> = if version >= 3 {
        request
            .members
            .iter()
            .map(|m| m.member_id.clone())
            .collect()
    } else {
        vec![request.member_id.clone()]
    };
    let codes = broker
        .groups()
        .leave(&request.group_id, &ids, Instant::now());
    if version < 3 {
        return LeaveGroupResponse::default()
            .with_error_code(codes.first().copied().unwrap_or_default());
    }
    let members = request
        .members
        .iter()
        .zip(codes)
        .map(|(member, code)| {
            MemberResponse::default()
                .with_member_id(member.member_id.clone())
                .with_group_instance_id(member.group_instance_id.clone())
                .with_error_code(code)
        })
        .collect();
    LeaveGroupResponse::default().with_members(members)
}

/// Answers `OffsetCommit` on the coordinator; offsets are committed to the replicated metadata log
/// before the response is sent.
pub async fn offset_commit(broker: &Broker, request: OffsetCommitRequest) -> OffsetCommitResponse {
    let coordinating = broker.cluster().coordinator_check();
    let mut topics = Vec::with_capacity(request.topics.len());
    let mut offsets = Vec::new();
    let mut accepted = Vec::new();
    for (t, wanted) in request.topics.iter().enumerate() {
        let topic = broker.cluster().topic(&wanted.name);
        let mut partitions = Vec::with_capacity(wanted.partitions.len());
        for (p, partition) in wanted.partitions.iter().enumerate() {
            let index = partition.partition_index;
            let mut response = OffsetCommitResponsePartition::default().with_partition_index(index);
            if let Err(code) = coordinating {
                response.error_code = code;
            } else if topic.as_ref().is_none_or(|t| t.group(index).is_none()) {
                response.error_code = ResponseError::UnknownTopicOrPartition.code();
            } else if partition
                .committed_metadata
                .as_ref()
                .is_some_and(|m| m.len() > MAX_OFFSET_METADATA_BYTES)
            {
                response.error_code = ResponseError::OffsetMetadataTooLarge.code();
            } else {
                offsets.push((
                    (wanted.name.0.clone(), index),
                    Committed {
                        offset: partition.committed_offset,
                        leader_epoch: partition.committed_leader_epoch,
                        metadata: partition.committed_metadata.clone(),
                    },
                ));
                accepted.push((t, p));
            }
            partitions.push(response);
        }
        topics.push(
            OffsetCommitResponseTopic::default()
                .with_name(wanted.name.clone())
                .with_partitions(partitions),
        );
    }
    if accepted.is_empty() {
        return OffsetCommitResponse::default().with_topics(topics);
    }
    let checked = broker.groups().check_commit(
        &request.group_id,
        &request.member_id,
        request.generation_id_or_member_epoch,
    );
    let code = match checked {
        Ok(()) => match broker
            .cluster()
            .commit_offsets(&request.group_id, offsets)
            .await
        {
            Ok(()) => 0,
            Err(code) => code,
        },
        Err(code) => code,
    };
    for (t, p) in accepted {
        topics[t].partitions[p].error_code = code;
    }
    OffsetCommitResponse::default().with_topics(topics)
}

type WantedOffsets = Option<Vec<(TopicName, Vec<i32>)>>;
type FoundOffsets = Vec<(TopicName, Vec<(i32, Option<Committed>)>)>;

/// Answers `OffsetFetch` on the coordinator; partitions without a committed offset report -1.
pub fn offset_fetch(
    broker: &Broker,
    version: i16,
    request: OffsetFetchRequest,
) -> OffsetFetchResponse {
    if let Err(code) = broker.cluster().coordinator_check() {
        return offset_fetch_refused(version, request, code);
    }
    if version < 8 {
        let wanted = request.topics.map(|topics| {
            topics
                .into_iter()
                .map(|t| (t.name, t.partition_indexes))
                .collect()
        });
        let topics = committed_offsets(broker, &request.group_id, wanted)
            .into_iter()
            .map(|(name, partitions)| {
                let partitions = partitions
                    .into_iter()
                    .map(|(index, committed)| {
                        let (offset, epoch, metadata) = offset_or_unset(committed);
                        OffsetFetchResponsePartition::default()
                            .with_partition_index(index)
                            .with_committed_offset(offset)
                            .with_committed_leader_epoch(epoch)
                            .with_metadata(metadata)
                    })
                    .collect();
                OffsetFetchResponseTopic::default()
                    .with_name(name)
                    .with_partitions(partitions)
            })
            .collect();
        return OffsetFetchResponse::default().with_topics(topics);
    }
    let mut groups = Vec::with_capacity(request.groups.len());
    for wanted_group in request.groups {
        let wanted = wanted_group.topics.map(|topics| {
            topics
                .into_iter()
                .map(|t| (t.name, t.partition_indexes))
                .collect()
        });
        let topics = committed_offsets(broker, &wanted_group.group_id, wanted)
            .into_iter()
            .map(|(name, partitions)| {
                let partitions = partitions
                    .into_iter()
                    .map(|(index, committed)| {
                        let (offset, epoch, metadata) = offset_or_unset(committed);
                        OffsetFetchResponsePartitions::default()
                            .with_partition_index(index)
                            .with_committed_offset(offset)
                            .with_committed_leader_epoch(epoch)
                            .with_metadata(metadata)
                    })
                    .collect();
                OffsetFetchResponseTopics::default()
                    .with_name(name)
                    .with_partitions(partitions)
            })
            .collect();
        groups.push(
            OffsetFetchResponseGroup::default()
                .with_group_id(wanted_group.group_id)
                .with_topics(topics),
        );
    }
    OffsetFetchResponse::default().with_groups(groups)
}

/// `OffsetFetch` answered with `code` everywhere it can carry one.
fn offset_fetch_refused(
    version: i16,
    request: OffsetFetchRequest,
    code: i16,
) -> OffsetFetchResponse {
    if version >= 8 {
        let groups = request
            .groups
            .into_iter()
            .map(|g| {
                OffsetFetchResponseGroup::default()
                    .with_group_id(g.group_id)
                    .with_error_code(code)
            })
            .collect();
        return OffsetFetchResponse::default().with_groups(groups);
    }
    let topics = request
        .topics
        .unwrap_or_default()
        .into_iter()
        .map(|t| {
            let partitions = t
                .partition_indexes
                .iter()
                .map(|&index| {
                    OffsetFetchResponsePartition::default()
                        .with_partition_index(index)
                        .with_committed_offset(-1)
                        .with_error_code(code)
                })
                .collect();
            OffsetFetchResponseTopic::default()
                .with_name(t.name)
                .with_partitions(partitions)
        })
        .collect();
    OffsetFetchResponse::default()
        .with_error_code(code)
        .with_topics(topics)
}

/// The group's committed offsets for `wanted` partitions, or for every committed partition.
fn committed_offsets(broker: &Broker, group: &GroupId, wanted: WantedOffsets) -> FoundOffsets {
    let offsets = broker.cluster().committed(group);
    if let Some(topics) = wanted {
        return topics
            .into_iter()
            .map(|(name, partitions)| {
                let found = partitions
                    .into_iter()
                    .map(|p| (p, offsets.get(&(name.0.clone(), p)).cloned()))
                    .collect();
                (name, found)
            })
            .collect();
    }
    let mut found: FoundOffsets = Vec::new();
    for ((topic, partition), committed) in offsets {
        match found.last_mut() {
            Some((name, partitions)) if name.0 == topic => {
                partitions.push((partition, Some(committed)));
            }
            _ => found.push((TopicName(topic), vec![(partition, Some(committed))])),
        }
    }
    found
}

fn offset_or_unset(committed: Option<Committed>) -> (i64, i32, Option<StrBytes>) {
    committed.map_or((-1, -1, Some(StrBytes::default())), |c| {
        (c.offset, c.leader_epoch, c.metadata)
    })
}

/// Answers `DescribeGroups`.
pub fn describe_groups(
    broker: &Broker,
    version: i16,
    request: &DescribeGroupsRequest,
) -> DescribeGroupsResponse {
    let coordinating = broker.cluster().coordinator_check();
    let groups = request
        .groups
        .iter()
        .map(|id| {
            if let Err(code) = coordinating {
                return DescribedGroup::default()
                    .with_group_id(id.clone())
                    .with_error_code(code);
            }
            let has_offsets = broker.cluster().has_offsets(id);
            broker
                .groups()
                .describe(id, has_offsets)
                .unwrap_or_else(|| {
                    let dead = DescribedGroup::default()
                        .with_group_id(id.clone())
                        .with_group_state(StrBytes::from_static_str("Dead"));
                    if version >= 6 {
                        dead.with_error_code(ResponseError::GroupIdNotFound.code())
                            .with_error_message(Some(StrBytes::from_static_str(
                                "group does not exist",
                            )))
                    } else {
                        dead
                    }
                })
        })
        .collect();
    DescribeGroupsResponse::default().with_groups(groups)
}

/// Answers `ListGroups`, honouring the state and type filters. Clients ask every broker; only the
/// coordinator has groups to list.
pub fn list_groups(broker: &Broker, request: &ListGroupsRequest) -> ListGroupsResponse {
    match broker.cluster().coordinator_check() {
        Ok(()) => {}
        Err(code) if code == ResponseError::NotCoordinator.code() => {
            return ListGroupsResponse::default();
        }
        Err(code) => return ListGroupsResponse::default().with_error_code(code),
    }
    let wanted = |filter: &[StrBytes], value: &StrBytes| {
        filter.is_empty() || filter.iter().any(|f| f.eq_ignore_ascii_case(value))
    };
    let groups = broker
        .groups()
        .list(&broker.cluster().groups_with_offsets())
        .into_iter()
        .filter(|g| {
            wanted(&request.states_filter, &g.group_state)
                && wanted(&request.types_filter, &g.group_type)
        })
        .collect();
    ListGroupsResponse::default().with_groups(groups)
}

/// Answers `DescribeConfigs` for topics and this broker.
pub fn describe_configs(
    broker: &Broker,
    request: &DescribeConfigsRequest,
) -> DescribeConfigsResponse {
    let results = request
        .resources
        .iter()
        .map(|resource| {
            let result = DescribeConfigsResult::default()
                .with_resource_type(resource.resource_type)
                .with_resource_name(resource.resource_name.clone());
            let error = match resource.resource_type {
                TOPIC_RESOURCE if broker.cluster().topic(&resource.resource_name).is_none() => {
                    Some(ResponseError::UnknownTopicOrPartition)
                }
                // ponytail: no topic or broker configs exist yet, so every entry list is empty.
                TOPIC_RESOURCE | BROKER_RESOURCE => None,
                _ => Some(ResponseError::InvalidRequest),
            };
            match error {
                Some(error) => result.with_error_code(error.code()),
                None => result.with_configs(Vec::new()),
            }
        })
        .collect();
    DescribeConfigsResponse::default().with_results(results)
}

fn topic_name(name: &str) -> TopicName {
    TopicName(StrBytes::from_string(name.to_owned()))
}

#[cfg(test)]
pub(crate) mod tests {
    use kafka_protocol::indexmap::IndexMap;
    use kafka_protocol::messages::fetch_response::AbortedTransaction;
    use kafka_protocol::protocol::Encodable;
    use kafka_protocol::records::{
        Compression, Record, RecordBatchEncoder, RecordEncodeOptions, TimestampType,
    };

    use super::*;

    /// One uncompressed v2 batch holding `values`.
    pub(crate) fn batch(values: &[&str]) -> Vec<u8> {
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
                timestamp: 1_700_000_000_000 + i64::from(i),
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
        buf.to_vec()
    }

    #[test]
    fn split_batches_finds_back_to_back_batches() {
        let first = batch(&["a", "b"]);
        let second = batch(&["c"]);
        let records = [first.clone(), second.clone()].concat();
        assert_eq!(
            split_batches(&records),
            Ok(vec![
                0..first.len(),
                first.len()..first.len() + second.len()
            ])
        );
    }

    #[test]
    fn split_batches_rejects_truncated_batch() {
        let records = batch(&["a"]);
        assert_eq!(
            split_batches(&records[..records.len() - 1]),
            Err(ResponseError::CorruptMessage.code())
        );
        assert_eq!(
            split_batches(&records[..20]),
            Err(ResponseError::CorruptMessage.code())
        );
    }

    #[test]
    fn split_batches_rejects_bad_crc() {
        let mut records = batch(&["a"]);
        *records.last_mut().unwrap() ^= 0xff;
        assert_eq!(
            split_batches(&records),
            Err(ResponseError::CorruptMessage.code())
        );
    }

    #[test]
    fn split_batches_rejects_old_magic() {
        let mut records = batch(&["a"]);
        records[16] = 1;
        assert_eq!(
            split_batches(&records),
            Err(ResponseError::UnsupportedForMessageFormat.code())
        );
    }

    #[test]
    fn split_batches_rejects_empty_and_negative_lengths() {
        assert_eq!(
            split_batches(&[]),
            Err(ResponseError::CorruptMessage.code())
        );
        let mut records = batch(&["a"]);
        records[8..12].copy_from_slice(&(-5_i32).to_be_bytes());
        assert_eq!(
            split_batches(&records),
            Err(ResponseError::CorruptMessage.code())
        );
    }

    #[test]
    fn valid_topic_name_follows_kafka_rules() {
        assert!(valid_topic_name("orders.v1_eu-west"));
        assert!(!valid_topic_name(""));
        assert!(!valid_topic_name(".."));
        assert!(!valid_topic_name("has space"));
        assert!(!valid_topic_name("../etc"));
        assert!(!valid_topic_name(&"x".repeat(250)));
    }

    #[test]
    fn api_versions_advertises_only_implemented_apis() {
        let keys: Vec<i16> = api_versions(0).api_keys.iter().map(|k| k.api_key).collect();
        assert_eq!(
            keys,
            [
                0, 1, 2, 3, 8, 9, 10, 11, 12, 13, 14, 15, 16, 18, 19, 20, 22, 23, 32
            ]
        );
        assert!(supports(ApiKey::OffsetForLeaderEpoch, 4));
        assert!(supports(ApiKey::Fetch, 18));
        assert!(!supports(ApiKey::ListOffsets, 7));
        assert!(supports(ApiKey::FindCoordinator, 6));
        assert!(!supports(ApiKey::OffsetCommit, 10));
    }

    #[test]
    fn unsupported_replies_with_error_code_when_possible() {
        let code = ResponseError::UnsupportedVersion.code();
        let Some((0, ResponseKind::ApiVersions(reply))) = unsupported(ApiKey::ApiVersions, 9)
        else {
            panic!("ApiVersions must always get a v0 reply");
        };
        assert_eq!(reply.error_code, code);
        assert!(
            matches!(unsupported(ApiKey::ConsumerGroupHeartbeat, 0), Some((0, ResponseKind::ConsumerGroupHeartbeat(r))) if r.error_code == code)
        );
        assert!(unsupported(ApiKey::AlterConfigs, 0).is_none());
        assert!(unsupported(ApiKey::ConsumerGroupHeartbeat, 99).is_none());
    }

    fn fetch_response(version: i16) -> FetchResponse {
        let partition = |index, records: Option<Bytes>| {
            PartitionData::default()
                .with_partition_index(index)
                .with_high_watermark(1000 + i64::from(index))
                .with_last_stable_offset(900)
                .with_log_start_offset(3)
                .with_records(records)
        };
        let topic = |name: &'static str, id, partitions| {
            FetchableTopicResponse::default()
                .with_topic(TopicName(StrBytes::from_static_str(name)))
                .with_topic_id(Uuid::from_u128(id))
                .with_partitions(partitions)
        };
        FetchResponse::default()
            .with_throttle_time_ms(7)
            .with_error_code(if version >= 7 { 3 } else { 0 })
            .with_responses(vec![
                topic(
                    "orders",
                    1,
                    vec![
                        partition(0, Some(Bytes::from(vec![1; 300]))),
                        partition(1, Some(Bytes::new())),
                        partition(2, None),
                        partition(3, Some(Bytes::new())).with_error_code(1),
                        partition(4, Some(Bytes::from(vec![2; 70_000]))),
                    ],
                ),
                topic("empty", 2, Vec::new()),
                topic(
                    "events",
                    3,
                    vec![partition(0, Some(Bytes::from(vec![3; 5])))],
                ),
            ])
    }

    #[test]
    fn encode_fetch_matches_generic_encoding_at_every_version() {
        for version in 4..=18 {
            let response = fetch_response(version);
            let records: Vec<_> = response
                .responses
                .iter()
                .flat_map(|t| &t.partitions)
                .map(|p| p.records.clone())
                .collect();
            let lengths: Vec<_> = records.iter().map(|r| r.as_ref().map(Bytes::len)).collect();
            let mut head = BytesMut::new();
            let positions = encode_fetch(&response, version, &lengths, &mut head).unwrap();

            let (mut split, mut from) = (Vec::new(), 0);
            for (at, records) in positions.into_iter().zip(&records) {
                split.extend_from_slice(&head[from..at]);
                split.extend_from_slice(records.as_deref().unwrap_or_default());
                from = at;
            }
            split.extend_from_slice(&head[from..]);
            let mut generic = BytesMut::new();
            response.encode(&mut generic, version).unwrap();
            assert_eq!(split, generic, "Fetch v{version}");
        }
    }

    #[test]
    fn encode_fetch_refuses_fields_it_leaves_out() {
        let mut response = fetch_response(12);
        response.responses[0].partitions[0].aborted_transactions =
            Some(vec![AbortedTransaction::default()]);
        let lengths = [Some(0); 6];
        assert!(encode_fetch(&response, 12, &lengths, &mut BytesMut::new()).is_err());
        assert!(
            encode_fetch(&fetch_response(12), 12, &lengths[..5], &mut BytesMut::new()).is_err()
        );
    }
}
