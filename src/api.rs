//! Request handlers, one per implemented Kafka API, plus the advertised version table.

use std::future::Future;
use std::ops::Range;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use kafka_protocol::ResponseError;
use kafka_protocol::messages::api_versions_response::ApiVersion;
use kafka_protocol::messages::create_topics_request::CreatableTopic;
use kafka_protocol::messages::create_topics_response::CreatableTopicResult;
use kafka_protocol::messages::delete_topics_response::DeletableTopicResult;
use kafka_protocol::messages::describe_configs_response::DescribeConfigsResult;
use kafka_protocol::messages::describe_groups_response::DescribedGroup;
use kafka_protocol::messages::fetch_response::{FetchableTopicResponse, PartitionData};
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
    ProduceRequest, ProduceResponse, ProducerId, PushTelemetryResponse, ResponseKind,
    SaslAuthenticateResponse, SaslHandshakeResponse, SyncGroupRequest, SyncGroupResponse,
    TopicName,
};
use kafka_protocol::protocol::{Message, Request, StrBytes, VersionRange};
use tokio::time::{Instant, sleep_until};
use uuid::Uuid;

use crate::broker::{Broker, Partition, Topic};
use crate::group::{self, ClientInfo, Committed};

const CLUSTER_ID: &str = "redcrab-single-node";
const MAX_PARTITIONS: i32 = 10_000;
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
    let topics = match &request.topics {
        Some(wanted) if !(wanted.is_empty() && version == 0) => wanted
            .iter()
            .map(|t| requested_topic(broker, version, t))
            .collect(),
        _ => broker
            .topics()
            .iter()
            .map(|t| topic_metadata(broker, t))
            .collect(),
    };
    let config = &broker.config;
    let node = BrokerId(config.node_id);
    MetadataResponse::default()
        .with_brokers(vec![
            MetadataResponseBroker::default()
                .with_node_id(node)
                .with_host(StrBytes::from_string(config.advertised_host.clone()))
                .with_port(i32::from(config.advertised_port)),
        ])
        .with_cluster_id(Some(StrBytes::from_static_str(CLUSTER_ID)))
        .with_controller_id(node)
        .with_topics(topics)
}

fn requested_topic(
    broker: &Broker,
    version: i16,
    wanted: &MetadataRequestTopic,
) -> MetadataResponseTopic {
    let found = match &wanted.name {
        Some(name) => broker.topic(name),
        None => broker.topic_by_id(wanted.topic_id),
    };
    if let Some(topic) = found {
        return topic_metadata(broker, &topic);
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

fn topic_metadata(broker: &Broker, topic: &Topic) -> MetadataResponseTopic {
    let node = BrokerId(broker.config.node_id);
    let partitions = (0..)
        .zip(&topic.partitions)
        .map(|(index, _)| {
            MetadataResponsePartition::default()
                .with_partition_index(index)
                .with_leader_id(node)
                .with_leader_epoch(0)
                .with_replica_nodes(vec![node])
                .with_isr_nodes(vec![node])
        })
        .collect();
    MetadataResponseTopic::default()
        .with_name(Some(topic_name(&topic.name)))
        .with_topic_id(topic.id)
        .with_partitions(partitions)
}

/// Answers a `CreateTopics` request; topics are durable in the data dir before the response is sent.
pub async fn create_topics(broker: &Broker, request: CreateTopicsRequest) -> CreateTopicsResponse {
    let mut results = Vec::with_capacity(request.topics.len());
    for wanted in &request.topics {
        let result = CreatableTopicResult::default().with_name(wanted.name.clone());
        results.push(
            match create_topic(broker, wanted, request.validate_only).await {
                Ok((id, partitions)) => result
                    .with_topic_id(id)
                    .with_num_partitions(partitions)
                    .with_replication_factor(1)
                    .with_configs(Some(Vec::new())),
                Err((code, message)) => result
                    .with_error_code(code)
                    .with_error_message(message.map(StrBytes::from_static_str)),
            },
        );
    }
    CreateTopicsResponse::default().with_topics(results)
}

async fn create_topic(
    broker: &Broker,
    wanted: &CreatableTopic,
    validate_only: bool,
) -> Result<(Uuid, i32), (i16, Option<&'static str>)> {
    let partitions = match (wanted.num_partitions, wanted.assignments.len()) {
        (-1, 0) => 1,
        (-1, n) => i32::try_from(n).unwrap_or(i32::MAX),
        (n, _) => n,
    };
    let only_this_node = [BrokerId(broker.config.node_id)];
    if !valid_topic_name(&wanted.name) {
        return Err((
            ResponseError::InvalidTopicException.code(),
            Some("topic names are 1-249 characters of [a-zA-Z0-9._-]"),
        ));
    }
    if !(1..=MAX_PARTITIONS).contains(&partitions) {
        return Err((
            ResponseError::InvalidPartitions.code(),
            Some("partition count must be between 1 and 10000"),
        ));
    }
    if !matches!(wanted.replication_factor, -1 | 1) {
        return Err((
            ResponseError::InvalidReplicationFactor.code(),
            Some("single-node broker: replication factor must be 1"),
        ));
    }
    if !wanted.assignments.is_empty()
        && (wanted.num_partitions != -1
            || wanted
                .assignments
                .iter()
                .any(|a| a.broker_ids != only_this_node))
    {
        return Err((
            ResponseError::InvalidReplicaAssignment.code(),
            Some("replicas can only be assigned to this node"),
        ));
    }
    if validate_only {
        return match broker.topic(&wanted.name) {
            Some(_) => Err((ResponseError::TopicAlreadyExists.code(), None)),
            None => Ok((Uuid::nil(), partitions)),
        };
    }
    let topic = broker
        .create_topic(wanted.name.to_string(), partitions)
        .await
        .map_err(|code| (code, None))?;
    Ok((topic.id, partitions))
}

/// Answers a `DeleteTopics` request.
pub async fn delete_topics(
    broker: &Broker,
    version: i16,
    request: DeleteTopicsRequest,
) -> DeleteTopicsResponse {
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
        let found = match &name {
            Some(name) => broker.topic(name),
            None => broker.topic_by_id(id),
        };
        let mut result = DeletableTopicResult::default()
            .with_name(name.clone())
            .with_topic_id(id);
        match found {
            Some(topic) => match broker.delete_topic(&topic).await {
                Ok(()) => {
                    result.name = Some(topic_name(&topic.name));
                    result.topic_id = topic.id;
                }
                Err(code) => result.error_code = code,
            },
            None if name.is_some() => {
                result.error_code = ResponseError::UnknownTopicOrPartition.code();
            }
            None => result.error_code = ResponseError::UnknownTopicId.code(),
        }
        responses.push(result);
    }
    DeleteTopicsResponse::default().with_responses(responses)
}

/// Starts a `Produce`: every batch is queued for append before this returns, so request order is
/// append order. The future resolves once all touched partitions are flushed, or immediately with
/// `None` for acks=0, which gets no response.
pub fn produce(
    broker: &Broker,
    version: i16,
    request: ProduceRequest,
) -> impl Future<Output = Option<ProduceResponse>> + Send + use<> {
    let acks = request.acks;
    let mut responses = Vec::with_capacity(request.topic_data.len());
    let mut pending = Vec::new();
    for (t, wanted) in request.topic_data.into_iter().enumerate() {
        let (topic, unknown) = if version >= 13 {
            (
                broker.topic_by_id(wanted.topic_id),
                ResponseError::UnknownTopicId,
            )
        } else {
            (
                broker.topic(&wanted.name),
                ResponseError::UnknownTopicOrPartition,
            )
        };
        let mut partitions = Vec::with_capacity(wanted.partition_data.len());
        for (p, data) in wanted.partition_data.into_iter().enumerate() {
            let mut response = PartitionProduceResponse::default().with_index(data.index);
            let partition = topic.as_ref().and_then(|topic| topic.partition(data.index));
            let records = data.records.unwrap_or_default();
            match (partition, split_batches(&records)) {
                _ if !matches!(acks, -1..=1) => {
                    response.error_code = ResponseError::InvalidRequiredAcks.code();
                }
                (None, _) => response.error_code = unknown.code(),
                (Some(_), Err(code)) => response.error_code = code,
                (Some(partition), Ok(batches)) => {
                    response.log_start_offset = partition.log_start_offset();
                    pending.push((t, p, partition.append(records.to_vec(), batches)));
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
        for (t, p, appended) in pending {
            let response = &mut responses[t].partition_responses[p];
            match appended.await {
                Ok(Ok(base_offset)) => response.base_offset = base_offset,
                Ok(Err(code)) => response.error_code = code,
                Err(_) => response.error_code = ResponseError::KafkaStorageError.code(),
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
    partition: Option<Arc<Partition>>,
    error: i16,
    offset: i64,
    max_bytes: usize,
}

impl FetchTarget {
    fn ready(&self) -> bool {
        self.error != 0
            || self
                .partition
                .as_ref()
                .is_some_and(|p| p.high_watermark() != self.offset)
    }
}

/// Answers a sessionless `Fetch`, waiting up to `max_wait_ms` for `min_bytes` of new flushed data.
pub async fn fetch(broker: Arc<Broker>, version: i16, request: FetchRequest) -> FetchResponse {
    if request.session_id != 0 {
        return FetchResponse::default()
            .with_error_code(ResponseError::FetchSessionIdNotFound.code());
    }
    let mut topics = Vec::with_capacity(request.topics.len());
    let mut targets = Vec::new();
    for (t, wanted) in request.topics.into_iter().enumerate() {
        let (topic, unknown) = if version >= 13 {
            (
                broker.topic_by_id(wanted.topic_id),
                ResponseError::UnknownTopicId,
            )
        } else {
            (
                broker.topic(&wanted.topic),
                ResponseError::UnknownTopicOrPartition,
            )
        };
        for p in &wanted.partitions {
            let partition = topic
                .as_ref()
                .and_then(|topic| topic.partition(p.partition))
                .cloned();
            targets.push(FetchTarget {
                topic: t,
                index: p.partition,
                error: if partition.is_some() {
                    0
                } else {
                    unknown.code()
                },
                partition,
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
    let mut flushes = broker.flushes();
    loop {
        flushes.borrow_and_update();
        let expired = Instant::now() >= deadline;
        if expired || min_bytes == 0 || targets.iter().any(FetchTarget::ready) {
            let reading = Arc::clone(&targets);
            let partitions =
                tokio::task::spawn_blocking(move || read_partitions(&reading, max_bytes))
                    .await
                    .unwrap_or_else(|_| {
                        targets
                            .iter()
                            .map(|t| partition_data(t, ResponseError::KafkaStorageError.code()))
                            .collect()
                    });
            let bytes: usize = partitions
                .iter()
                .filter_map(|p| p.records.as_ref())
                .map(Bytes::len)
                .sum();
            if expired || bytes >= min_bytes || partitions.iter().any(|p| p.error_code != 0) {
                for (target, data) in targets.iter().zip(partitions) {
                    topics[target.topic].partitions.push(data);
                }
                return FetchResponse::default().with_responses(topics);
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

/// Reads every target, honouring partition and response byte limits; only the first non-empty
/// partition may exceed them, by a single batch, so the consumer always makes progress.
fn read_partitions(targets: &[FetchTarget], max_bytes: usize) -> Vec<PartitionData> {
    let mut remaining = max_bytes;
    let mut first = true;
    targets
        .iter()
        .map(|target| {
            let Some(partition) = &target.partition else {
                return partition_data(target, target.error);
            };
            let limit = target.max_bytes.min(remaining);
            let mut data = partition_data(target, 0);
            if limit > 0 || first {
                match partition.read(target.offset, limit) {
                    Ok(records) if !records.is_empty() && (first || records.len() <= limit) => {
                        first = false;
                        remaining = remaining.saturating_sub(records.len());
                        data.records = Some(Bytes::from(records));
                    }
                    Ok(_) => {}
                    Err(code) => data.error_code = code,
                }
            }
            let high_watermark = partition.high_watermark();
            data.with_high_watermark(high_watermark)
                .with_last_stable_offset(high_watermark)
                .with_log_start_offset(partition.log_start_offset())
        })
        .collect()
}

/// Answers `ListOffsets` for the earliest, latest (high watermark) and timestamp lookups.
pub async fn list_offsets(broker: Arc<Broker>, request: ListOffsetsRequest) -> ListOffsetsResponse {
    let topics = tokio::task::spawn_blocking(move || {
        request
            .topics
            .into_iter()
            .map(|wanted| {
                let topic = broker.topic(&wanted.name);
                let partitions = wanted
                    .partitions
                    .iter()
                    .map(|p| {
                        let mut response = ListOffsetsPartitionResponse::default()
                            .with_partition_index(p.partition_index)
                            .with_timestamp(-1)
                            .with_offset(-1);
                        match topic.as_ref().and_then(|t| t.partition(p.partition_index)) {
                            None => {
                                response.error_code = ResponseError::UnknownTopicOrPartition.code();
                            }
                            Some(partition) => match p.timestamp {
                                EARLIEST_TIMESTAMP => {
                                    response.offset = partition.log_start_offset();
                                }
                                LATEST_TIMESTAMP => response.offset = partition.high_watermark(),
                                timestamp if timestamp >= 0 => {
                                    if let Some(offset) = partition.offset_for_timestamp(timestamp)
                                    {
                                        response.offset = offset;
                                        // ponytail: echoes the requested timestamp, not the matched record's; needs a record-level lookup.
                                        response.timestamp = timestamp;
                                    }
                                }
                                _ => response.error_code = ResponseError::InvalidRequest.code(),
                            },
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

/// Answers `FindCoordinator`: this node coordinates every group; transactions are not supported.
pub fn find_coordinator(
    broker: &Broker,
    version: i16,
    request: &FindCoordinatorRequest,
) -> FindCoordinatorResponse {
    let config = &broker.config;
    let node = BrokerId(config.node_id);
    let host = StrBytes::from_string(config.advertised_host.clone());
    let port = i32::from(config.advertised_port);
    let refused = (request.key_type != GROUP_KEY_TYPE).then(|| {
        (
            ResponseError::InvalidRequest.code(),
            Some(StrBytes::from_static_str(
                "only consumer group coordinators exist",
            )),
        )
    });
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

/// Answers `JoinGroup` once the group's join phase completes.
pub async fn join_group(
    broker: &Broker,
    version: i16,
    request: JoinGroupRequest,
    client: ClientInfo,
) -> JoinGroupResponse {
    let reply = broker
        .groups()
        .join(&request, version, client, Instant::now());
    reply
        .resolve(|| {
            group::join_error(
                ResponseError::UnknownMemberId.code(),
                request.member_id,
                version,
            )
        })
        .await
}

/// Answers `SyncGroup`; followers wait for the leader's assignment.
pub async fn sync_group(broker: &Broker, request: &SyncGroupRequest) -> SyncGroupResponse {
    broker
        .groups()
        .sync(request, Instant::now())
        .resolve(|| group::sync_error(ResponseError::UnknownMemberId.code()))
        .await
}

/// Answers `Heartbeat`.
pub fn heartbeat(broker: &Broker, request: &HeartbeatRequest) -> HeartbeatResponse {
    HeartbeatResponse::default().with_error_code(broker.groups().heartbeat(request, Instant::now()))
}

/// Answers `LeaveGroup`: one member before v3, a batch of members from v3.
pub fn leave_group(
    broker: &Broker,
    version: i16,
    request: &LeaveGroupRequest,
) -> LeaveGroupResponse {
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

/// Answers `OffsetCommit`; offsets are durable before the response is sent.
pub async fn offset_commit(broker: &Broker, request: OffsetCommitRequest) -> OffsetCommitResponse {
    let mut topics = Vec::with_capacity(request.topics.len());
    let mut offsets = Vec::new();
    let mut accepted = Vec::new();
    for (t, wanted) in request.topics.iter().enumerate() {
        let topic = broker.topic(&wanted.name);
        let mut partitions = Vec::with_capacity(wanted.partitions.len());
        for (p, partition) in wanted.partitions.iter().enumerate() {
            let index = partition.partition_index;
            let mut response = OffsetCommitResponsePartition::default().with_partition_index(index);
            if topic.as_ref().is_none_or(|t| t.partition(index).is_none()) {
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
    let code = broker
        .groups()
        .commit(
            &request.group_id,
            &request.member_id,
            request.generation_id_or_member_epoch,
            offsets,
        )
        .await;
    for (t, p) in accepted {
        topics[t].partitions[p].error_code = code;
    }
    OffsetCommitResponse::default().with_topics(topics)
}

type WantedOffsets = Option<Vec<(TopicName, Vec<i32>)>>;
type FoundOffsets = Vec<(TopicName, Vec<(i32, Option<Committed>)>)>;

/// Answers `OffsetFetch`; partitions without a committed offset report offset -1.
pub async fn offset_fetch(
    broker: &Broker,
    version: i16,
    request: OffsetFetchRequest,
) -> OffsetFetchResponse {
    if version < 8 {
        let wanted = request.topics.map(|topics| {
            topics
                .into_iter()
                .map(|t| (t.name, t.partition_indexes))
                .collect()
        });
        let topics = committed_offsets(broker, &request.group_id, wanted)
            .await
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
            .await
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

/// The group's committed offsets for `wanted` partitions, or for every committed partition.
async fn committed_offsets(
    broker: &Broker,
    group: &GroupId,
    wanted: WantedOffsets,
) -> FoundOffsets {
    let offsets = broker.groups().committed(group).await;
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
    let groups = request
        .groups
        .iter()
        .map(|id| {
            broker.groups().describe(id).unwrap_or_else(|| {
                let dead = DescribedGroup::default()
                    .with_group_id(id.clone())
                    .with_group_state(StrBytes::from_static_str("Dead"));
                if version >= 6 {
                    dead.with_error_code(ResponseError::GroupIdNotFound.code())
                        .with_error_message(Some(StrBytes::from_static_str("group does not exist")))
                } else {
                    dead
                }
            })
        })
        .collect();
    DescribeGroupsResponse::default().with_groups(groups)
}

/// Answers `ListGroups`, honouring the state and type filters.
pub fn list_groups(broker: &Broker, request: &ListGroupsRequest) -> ListGroupsResponse {
    let wanted = |filter: &[StrBytes], value: &StrBytes| {
        filter.is_empty() || filter.iter().any(|f| f.eq_ignore_ascii_case(value))
    };
    let groups = broker
        .groups()
        .list()
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
                TOPIC_RESOURCE if broker.topic(&resource.resource_name).is_none() => {
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
    use bytes::BytesMut;
    use kafka_protocol::indexmap::IndexMap;
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
                0, 1, 2, 3, 8, 9, 10, 11, 12, 13, 14, 15, 16, 18, 19, 20, 22, 32
            ]
        );
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
}
