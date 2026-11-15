//! TCP accept loop, request framing and in-order response writing.
//!
//! kafka-protocol sizes an array's allocation from the count the client sent, up to one element
//! per byte left in the frame, and keeps each struct's unknown tagged fields in a map of its own;
//! decoding a crafted request can take a few hundred times its size. So request headers and
//! `Produce`, the one request that legitimately carries megabytes, are parsed here without
//! allocating from counts. Every other request is limited to [`MAX_REQUEST_BYTES`] before its body
//! is read, waits in its connection's queue as raw bytes, and is decoded only once it is its turn
//! to be handled, while at most [`DECODE_BUDGET_BYTES`] of frames decode at once, broker-wide.

use std::fmt::Display;
use std::future::{Future, ready};
use std::io::{self, IoSlice};
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use bytes::{Buf, BufMut, Bytes, BytesMut};
use kafka_protocol::messages::produce_request::{PartitionProduceData, TopicProduceData};
use kafka_protocol::messages::{
    ApiKey, FetchRequest, ProduceRequest, RequestKind, ResponseHeader, ResponseKind, TopicName,
    TransactionalId,
};
use kafka_protocol::protocol::{Decodable, Encodable, StrBytes};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{Semaphore, mpsc, watch};
use tokio::task::JoinSet;
use tokio::time::{Instant, MissedTickBehavior};
use tracing::{debug, info, warn};
use uuid::Uuid;

use crate::api;
use crate::broker::Broker;
use crate::group::ClientInfo;

/// Largest request frame accepted, which only a `Produce` may use; a larger size prefix closes the
/// connection before anything is allocated.
pub const MAX_FRAME_BYTES: usize = 100 * 1024 * 1024;
/// Largest frame of any other request, checked against its API key before the body is read.
// ponytail: fixed at 1 MiB, about three times a Fetch of 10,000 partitions; a larger request (huge group subscriptions, assignments or fetches) closes its connection. Decoders that allocate per element read, not per count, would lift it.
pub const MAX_REQUEST_BYTES: usize = 1 << 20;
const MIN_FRAME_BYTES: usize = 8;
// ponytail: in-flight requests are bounded per connection by count and frame size, not by a broker-wide memory budget, and connections are not limited.
const MAX_IN_FLIGHT: usize = 32;
/// Frame bytes of non-`Produce` requests that may be decoding at once, broker-wide.
// ponytail: decoding a crafted 1 MiB request still makes kafka-protocol request up to ~220 MiB (arrays sized from counts, ~120 B per element and three levels deep), and a valid one can decode to ~110 MiB (a tagged-field map per struct); this budget serializes only the large ones.
const DECODE_BUDGET_BYTES: usize = MAX_REQUEST_BYTES;
/// Most topic and partition entries one `Produce` may carry.
// ponytail: a fixed 65,536, so a crafted Produce decodes to at most ~12 MiB of entries; a Produce for more partitions than that closes its connection.
const MAX_PRODUCE_ENTRIES: usize = 1 << 16;
/// How long a shutdown waits for requests already read to be answered.
// ponytail: shutdown hands no leadership over first, so partitions this node led wait one election timeout (1.5-3 s) for a new leader.
const DRAIN_TIMEOUT: Duration = Duration::from_secs(5);
/// How often group session and rebalance timeouts are checked.
const GROUP_TICK: Duration = Duration::from_millis(100);

type ResponseFuture = Pin<Box<dyn Future<Output = io::Result<Option<ResponseKind>>> + Send>>;
type FetchFuture = Pin<Box<dyn Future<Output = io::Result<api::FetchBody>> + Send>>;
type Handled = Pin<Box<dyn Future<Output = Option<ResponseKind>> + Send>>;

/// A pending response: most are encoded whole once ready, fetches are written while still being
/// read. Either fails for a request that does not decode.
enum Response {
    Message(ResponseFuture),
    Fetch(FetchFuture),
}

struct InFlight {
    api_key: ApiKey,
    version: i16,
    correlation_id: i32,
    response: Response,
}

/// Accepts connections, serving each one on its own task, and enforces group timeouts until
/// `shutdown` completes. Then it stops accepting and reading requests, and waits up to
/// [`DRAIN_TIMEOUT`] for the requests already read to be answered before closing every connection.
pub async fn serve(listener: TcpListener, broker: Arc<Broker>, shutdown: impl Future<Output = ()>) {
    let (stop, stopping) = watch::channel(false);
    let decoding = Arc::new(Semaphore::new(DECODE_BUDGET_BYTES));
    let mut connections = JoinSet::new();
    let mut ticks = tokio::time::interval(GROUP_TICK);
    ticks.set_missed_tick_behavior(MissedTickBehavior::Delay);
    tokio::pin!(shutdown);
    loop {
        tokio::select! {
            () = &mut shutdown => break,
            accepted = listener.accept() => match accepted {
                Ok((stream, peer)) => {
                    debug!(%peer, "connection accepted");
                    connections.spawn(connection(
                        stream,
                        Arc::clone(&broker),
                        Arc::clone(&decoding),
                        stopping.clone(),
                    ));
                }
                Err(e) => {
                    warn!("accept failed: {e}");
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
            },
            _ = ticks.tick() => broker.groups().expire(Instant::now()),
            Some(_) = connections.join_next(), if !connections.is_empty() => {}
        }
    }
    drop(listener);
    let _ = stop.send(true);
    info!(
        connections = connections.len(),
        "stopped accepting; answering the requests already read"
    );
    let drained = tokio::time::timeout(DRAIN_TIMEOUT, async {
        while connections.join_next().await.is_some() {}
    })
    .await;
    if drained.is_err() {
        warn!(
            connections = connections.len(),
            "closing connections still busy after {DRAIN_TIMEOUT:?}"
        );
        connections.shutdown().await;
    }
}

async fn connection(
    stream: TcpStream,
    broker: Arc<Broker>,
    decoding: Arc<Semaphore>,
    mut stopping: watch::Receiver<bool>,
) {
    let peer = stream.peer_addr().ok();
    let _ = stream.set_nodelay(true);
    let (reader, writer) = stream.into_split();
    let (in_flight, queue) = mpsc::channel(MAX_IN_FLIGHT);
    let mut writing = tokio::spawn(write_responses(writer, queue));
    let written = tokio::select! {
        read = read_requests(reader, &broker, peer, in_flight, &decoding) => {
            if let Err(e) = read {
                warn!(?peer, "closing connection: {e}");
            }
            None
        }
        _ = stopping.wait_for(|&stop| stop) => {
            debug!(?peer, "shutting down: reading no more requests");
            None
        }
        written = &mut writing => Some(written),
    };
    let written = match written {
        Some(written) => written,
        None => writing.await,
    };
    match written {
        Ok(Err(e)) if e.kind() == io::ErrorKind::InvalidData => {
            warn!(?peer, "closing connection: {e}");
        }
        Ok(Err(e)) => debug!(?peer, "write side closed: {e}"),
        _ => {}
    }
}

async fn read_requests(
    mut reader: OwnedReadHalf,
    broker: &Arc<Broker>,
    peer: Option<SocketAddr>,
    in_flight: mpsc::Sender<InFlight>,
    decoding: &Arc<Semaphore>,
) -> io::Result<()> {
    while let Some(frame) = read_frame(&mut reader).await? {
        if in_flight
            .send(dispatch(broker, peer, frame, decoding)?)
            .await
            .is_err()
        {
            break;
        }
    }
    Ok(())
}

/// Writes responses strictly in request order; produce futures complete when their flush does.
/// Fails, closing the connection, on a request that does not decode.
async fn write_responses(
    mut writer: OwnedWriteHalf,
    mut queue: mpsc::Receiver<InFlight>,
) -> io::Result<()> {
    while let Some(request) = queue.recv().await {
        match request.response {
            Response::Message(response) => {
                if let Some(response) = response.await? {
                    let frame = encode_response(
                        request.api_key,
                        request.version,
                        request.correlation_id,
                        &response,
                    )?;
                    writer.write_all(&frame).await?;
                }
            }
            Response::Fetch(body) => {
                let body = body.await?;
                write_fetch(&mut writer, request.version, request.correlation_id, body).await?;
            }
        }
    }
    Ok(())
}

/// Writes a `Fetch` response frame, sending each chunk of its body as soon as it has been read.
async fn write_fetch<W: AsyncWrite + Unpin>(
    writer: &mut W,
    version: i16,
    correlation_id: i32,
    mut body: api::FetchBody,
) -> io::Result<()> {
    let mut header = BytesMut::new();
    header.put_i32(0);
    ResponseHeader::default()
        .with_correlation_id(correlation_id)
        .encode(&mut header, ApiKey::Fetch.response_header_version(version))
        .map_err(invalid)?;
    let len = i32::try_from(header.len() - 4 + body.len).map_err(invalid)?;
    header[..4].copy_from_slice(&len.to_be_bytes());

    let mut left = body.len;
    let mut header = Some(header);
    while let Some(chunk) = body.chunks.recv().await {
        let chunk = chunk?;
        left = left
            .checked_sub(chunk.len())
            .ok_or_else(|| invalid("fetch body longer than announced"))?;
        let mut slices = [
            IoSlice::new(header.as_deref().unwrap_or_default()),
            IoSlice::new(&chunk),
        ];
        let mut slices = &mut slices[..];
        IoSlice::advance_slices(&mut slices, 0);
        while !slices.is_empty() {
            let written = writer.write_vectored(slices).await?;
            if written == 0 {
                return Err(io::ErrorKind::WriteZero.into());
            }
            IoSlice::advance_slices(&mut slices, written);
        }
        header = None;
    }
    if left != 0 || header.is_some() {
        return Err(invalid("fetch body ended early"));
    }
    Ok(())
}

/// Reads one size-prefixed request frame; `None` on a clean end of stream. A frame over the limit
/// of its API key fails before its body is read, and memory grows with the bytes actually
/// received, so a lying size prefix cannot force a large allocation.
async fn read_frame<R: AsyncRead + Unpin>(reader: &mut R) -> io::Result<Option<Bytes>> {
    let mut prefix = [0; 4];
    match reader.read_exact(&mut prefix).await {
        Ok(_) => {}
        Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => return Ok(None),
        Err(e) => return Err(e),
    }
    let len = frame_len(prefix)?;
    let mut frame = Vec::with_capacity(len.min(64 * 1024));
    frame.resize(2, 0);
    reader.read_exact(&mut frame).await?;
    let limit = if api_key(&frame) == ApiKey::Produce as i16 {
        MAX_FRAME_BYTES
    } else {
        MAX_REQUEST_BYTES
    };
    if len > limit {
        return Err(invalid(format!(
            "a {len}-byte request with API key {} is over its {limit}-byte limit",
            api_key(&frame)
        )));
    }
    (&mut *reader)
        .take((len - frame.len()) as u64)
        .read_to_end(&mut frame)
        .await?;
    if frame.len() < len {
        return Err(io::ErrorKind::UnexpectedEof.into());
    }
    Ok(Some(Bytes::from(frame)))
}

fn api_key(frame: &[u8]) -> i16 {
    i16::from_be_bytes([frame[0], frame[1]])
}

fn frame_len(prefix: [u8; 4]) -> io::Result<usize> {
    let len = i32::from_be_bytes(prefix);
    usize::try_from(len)
        .ok()
        .filter(|len| (MIN_FRAME_BYTES..=MAX_FRAME_BYTES).contains(len))
        .ok_or_else(|| {
            invalid(format!(
                "frame size {len} outside {MIN_FRAME_BYTES}..={MAX_FRAME_BYTES}"
            ))
        })
}

/// Reads one request's header and returns its response future. Handlers run only when the writer
/// polls that future, so a connection's requests are handled in order, as Kafka does, and all but
/// `Produce` are decoded only then; produce is decoded and queues its appends here, so that request
/// order is append order.
fn dispatch(
    broker: &Arc<Broker>,
    peer: Option<SocketAddr>,
    frame: Bytes,
    decoding: &Arc<Semaphore>,
) -> io::Result<InFlight> {
    let key = api_key(&frame);
    let version = i16::from_be_bytes([frame[2], frame[3]]);
    let correlation_id = i32::from_be_bytes([frame[4], frame[5], frame[6], frame[7]]);
    let api_key = ApiKey::try_from(key).map_err(|()| invalid(format!("unknown api key {key}")))?;
    if !api::supports(api_key, version) {
        if api_key == ApiKey::ApiVersions {
            debug!(
                version,
                "ApiVersions newer than supported, replying with v0 so the client downgrades"
            );
        } else {
            warn!(?api_key, version, "unsupported request");
        }
        let (version, response) = api::unsupported(api_key, version)
            .ok_or_else(|| invalid(format!("{api_key:?} v{version} is not supported")))?;
        return Ok(InFlight {
            api_key,
            version,
            correlation_id,
            response: Response::Message(Box::pin(ready(Ok(Some(response))))),
        });
    }
    let mut body = frame;
    let client_id = client_id(&mut body, api_key, version)
        .map_err(|e| invalid(format!("bad request header: {e}")))?;
    debug!(?api_key, version, correlation_id, client = ?client_id, "request");
    let malformed = move |e: io::Error| invalid(format!("malformed {api_key:?} v{version}: {e}"));
    let broker = Arc::clone(broker);
    let decoding = Arc::clone(decoding);
    let response = match api_key {
        ApiKey::Produce => {
            let request = decode_produce(&mut body, version).map_err(malformed)?;
            let produced = api::produce(&broker, version, request);
            Response::Message(Box::pin(async move { Ok(produced.await.map(Into::into)) }))
        }
        ApiKey::Fetch => Response::Fetch(Box::pin(async move {
            let request = decode(&decoding, body, |body| FetchRequest::decode(body, version))
                .await
                .map_err(malformed)?;
            Ok(api::fetch(broker, version, request).await)
        })),
        _ => Response::Message(Box::pin(async move {
            let request = decode(&decoding, body, |body| {
                RequestKind::decode(api_key, body, version)
            })
            .await
            .map_err(malformed)?;
            let client = || ClientInfo::new(client_id, peer);
            let response = respond(broker, version, request, client)
                .ok_or_else(|| invalid(format!("no handler for {api_key:?}")))?;
            Ok(response.await)
        })),
    };
    Ok(InFlight {
        api_key,
        version,
        correlation_id,
        response,
    })
}

/// Decodes `body` with kafka-protocol once the broker-wide budget of frame bytes being decoded has
/// room for it.
async fn decode<T, E: Display>(
    decoding: &Semaphore,
    mut body: Bytes,
    decode: impl FnOnce(&mut Bytes) -> Result<T, E>,
) -> io::Result<T> {
    let bytes = u32::try_from(body.len()).map_err(invalid)?;
    let _decoding = decoding.acquire_many(bytes).await.map_err(invalid)?;
    decode(&mut body).map_err(invalid)
}

/// Reads a request header up to its body, returning its client id; unknown tagged fields are
/// skipped rather than kept.
fn client_id(frame: &mut Bytes, api_key: ApiKey, version: i16) -> io::Result<Option<StrBytes>> {
    frame.advance(8);
    let client_id = string(frame, false)?;
    if api_key.request_header_version(version) >= 2 {
        skip_tagged_fields(frame)?;
    }
    Ok(client_id)
}

/// A `Produce` body, collected entry by entry as it is read rather than into arrays sized from the
/// counts the client sent, with unknown tagged fields skipped and at most [`MAX_PRODUCE_ENTRIES`]
/// topic and partition entries.
fn decode_produce(body: &mut Bytes, version: i16) -> io::Result<ProduceRequest> {
    let flexible = version >= 9;
    let transactional_id = string(body, flexible)?;
    let acks = body.try_get_i16().map_err(invalid)?;
    let timeout_ms = body.try_get_i32().map_err(invalid)?;
    let mut entries = 0;
    let mut entry = || {
        entries += 1;
        if entries > MAX_PRODUCE_ENTRIES {
            return Err(invalid(format!(
                "more than {MAX_PRODUCE_ENTRIES} topic and partition entries"
            )));
        }
        Ok(())
    };
    let mut topic_data = Vec::new();
    for _ in 0..count(body, flexible)? {
        entry()?;
        let name = if version < 13 {
            string(body, flexible)?.ok_or_else(|| invalid("null topic name"))?
        } else {
            StrBytes::default()
        };
        let mut topic_id = [0; 16];
        if version >= 13 {
            body.try_copy_to_slice(&mut topic_id).map_err(invalid)?;
        }
        let mut partition_data = Vec::new();
        for _ in 0..count(body, flexible)? {
            entry()?;
            let index = body.try_get_i32().map_err(invalid)?;
            let records = if flexible {
                compact(body)?
            } else {
                let len = body.try_get_i32().map_err(invalid)?;
                (len != -1)
                    .then(|| take(body, usize::try_from(len).map_err(invalid)?))
                    .transpose()?
            };
            if flexible {
                skip_tagged_fields(body)?;
            }
            partition_data.push(
                PartitionProduceData::default()
                    .with_index(index)
                    .with_records(records),
            );
        }
        if flexible {
            skip_tagged_fields(body)?;
        }
        topic_data.push(
            TopicProduceData::default()
                .with_name(TopicName(name))
                .with_topic_id(Uuid::from_bytes(topic_id))
                .with_partition_data(partition_data),
        );
    }
    if flexible {
        skip_tagged_fields(body)?;
    }
    Ok(ProduceRequest::default()
        .with_transactional_id(transactional_id.map(TransactionalId))
        .with_acks(acks)
        .with_timeout_ms(timeout_ms)
        .with_topic_data(topic_data))
}

/// A nullable string: a compact length + 1 when `flexible`, otherwise an `i16` length, -1 for null.
fn string(buf: &mut Bytes, flexible: bool) -> io::Result<Option<StrBytes>> {
    let bytes = if flexible {
        compact(buf)?
    } else {
        let len = buf.try_get_i16().map_err(invalid)?;
        (len != -1)
            .then(|| take(buf, usize::try_from(len).map_err(invalid)?))
            .transpose()?
    };
    bytes
        .map(|bytes| StrBytes::from_utf8(bytes).map_err(invalid))
        .transpose()
}

/// Nullable compact bytes: an unsigned varint length + 1, 0 for null.
fn compact(buf: &mut Bytes) -> io::Result<Option<Bytes>> {
    uvarint(buf)?
        .checked_sub(1)
        .map(|len| take(buf, usize::try_from(len).map_err(invalid)?))
        .transpose()
}

/// The element count of a non-null array.
fn count(buf: &mut Bytes, flexible: bool) -> io::Result<u32> {
    let count = if flexible {
        uvarint(buf)?.checked_sub(1)
    } else {
        u32::try_from(buf.try_get_i32().map_err(invalid)?).ok()
    };
    count.ok_or_else(|| invalid("null or negative array"))
}

fn skip_tagged_fields(buf: &mut Bytes) -> io::Result<()> {
    for _ in 0..uvarint(buf)? {
        uvarint(buf)?;
        let len = uvarint(buf)?;
        take(buf, usize::try_from(len).map_err(invalid)?)?;
    }
    Ok(())
}

fn uvarint(buf: &mut Bytes) -> io::Result<u32> {
    let mut value = 0;
    for shift in (0..35).step_by(7) {
        let byte = buf.try_get_u8().map_err(invalid)?;
        value |= u32::from(byte & 0x7f) << shift;
        if byte < 0x80 {
            return Ok(value);
        }
    }
    Err(invalid("varint longer than 5 bytes"))
}

fn take(buf: &mut Bytes, len: usize) -> io::Result<Bytes> {
    if len > buf.len() {
        return Err(invalid(format!(
            "a length of {len} runs past the {} bytes left",
            buf.len()
        )));
    }
    Ok(buf.split_to(len))
}

/// The response future of a non-fetch request, or `None` for an API without a handler.
fn respond(
    broker: Arc<Broker>,
    version: i16,
    request: RequestKind,
    client: impl FnOnce() -> ClientInfo,
) -> Option<Handled> {
    Some(match request {
        RequestKind::ApiVersions(_) => Box::pin(ready(Some(api::api_versions(0).into()))),
        RequestKind::Metadata(request) => {
            Box::pin(async move { Some(api::metadata(&broker, version, &request).into()) })
        }
        RequestKind::CreateTopics(request) => {
            Box::pin(async move { Some(api::create_topics(&broker, request).await.into()) })
        }
        RequestKind::DeleteTopics(request) => {
            Box::pin(
                async move { Some(api::delete_topics(&broker, version, request).await.into()) },
            )
        }
        RequestKind::ListOffsets(request) => {
            Box::pin(async move { Some(api::list_offsets(broker, request).await.into()) })
        }
        RequestKind::OffsetForLeaderEpoch(request) => {
            Box::pin(
                async move { Some(api::offsets_for_leader_epoch(broker, request).await.into()) },
            )
        }
        RequestKind::InitProducerId(request) => {
            Box::pin(async move { Some(api::init_producer_id(&broker, &request).into()) })
        }
        RequestKind::FindCoordinator(request) => {
            Box::pin(async move { Some(api::find_coordinator(&broker, version, &request).into()) })
        }
        RequestKind::JoinGroup(request) => {
            let client = client();
            Box::pin(async move {
                Some(
                    api::join_group(&broker, version, request, client)
                        .await
                        .into(),
                )
            })
        }
        RequestKind::SyncGroup(request) => {
            Box::pin(async move { Some(api::sync_group(&broker, request).await.into()) })
        }
        RequestKind::Heartbeat(request) => {
            Box::pin(async move { Some(api::heartbeat(&broker, &request).into()) })
        }
        RequestKind::LeaveGroup(request) => {
            Box::pin(async move { Some(api::leave_group(&broker, version, &request).into()) })
        }
        RequestKind::OffsetCommit(request) => {
            Box::pin(async move { Some(api::offset_commit(&broker, request).await.into()) })
        }
        RequestKind::OffsetFetch(request) => {
            Box::pin(async move { Some(api::offset_fetch(&broker, version, request).into()) })
        }
        RequestKind::DescribeGroups(request) => {
            Box::pin(async move { Some(api::describe_groups(&broker, version, &request).into()) })
        }
        RequestKind::ListGroups(request) => {
            Box::pin(async move { Some(api::list_groups(&broker, &request).into()) })
        }
        RequestKind::DescribeConfigs(request) => {
            Box::pin(async move { Some(api::describe_configs(&broker, &request).into()) })
        }
        _ => return None,
    })
}

fn encode_response(
    api_key: ApiKey,
    version: i16,
    correlation_id: i32,
    response: &ResponseKind,
) -> io::Result<BytesMut> {
    let mut frame = BytesMut::new();
    frame.put_i32(0);
    ResponseHeader::default()
        .with_correlation_id(correlation_id)
        .encode(&mut frame, api_key.response_header_version(version))
        .map_err(invalid)?;
    response.encode(&mut frame, version).map_err(invalid)?;
    let len = i32::try_from(frame.len() - 4).map_err(invalid)?;
    frame[..4].copy_from_slice(&len.to_be_bytes());
    Ok(frame)
}

fn invalid(error: impl Display) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, error.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// What `write_fetch` sends of a body announced as `len` bytes and made of `chunks`, through a
    /// pipe small enough to force partial writes.
    async fn fetch_frame(len: usize, chunks: Vec<io::Result<Bytes>>) -> (io::Result<()>, Vec<u8>) {
        let (sender, receiver) = mpsc::unbounded_channel();
        for chunk in chunks {
            sender.send(chunk).unwrap();
        }
        drop(sender);
        let (mut writer, mut reader) = tokio::io::duplex(4096);
        let reading = tokio::spawn(async move {
            let mut out = Vec::new();
            reader.read_to_end(&mut out).await.unwrap();
            out
        });
        let body = api::FetchBody {
            len,
            chunks: receiver,
        };
        let written = write_fetch(&mut writer, 12, 9, body).await;
        drop(writer);
        (written, reading.await.unwrap())
    }

    #[tokio::test]
    async fn write_fetch_frames_the_body_as_it_arrives() {
        let chunks = vec![
            Ok(Bytes::from(vec![1; 10])),
            Ok(Bytes::from(vec![2; 10_000])),
            Ok(Bytes::from(vec![3; 3])),
        ];
        let (written, frame) = fetch_frame(10_013, chunks).await;
        written.unwrap();
        let header = [&10_018_i32.to_be_bytes()[..], &9_i32.to_be_bytes(), &[0]].concat();
        let body = [vec![1; 10], vec![2; 10_000], vec![3; 3]].concat();
        assert_eq!(frame, [header, body].concat());
    }

    #[tokio::test]
    async fn write_fetch_fails_on_a_body_that_breaks_its_length() {
        let short = vec![Ok(Bytes::from(vec![1; 10]))];
        assert!(fetch_frame(11, short).await.0.is_err());
        let long = vec![Ok(Bytes::from(vec![1; 10])), Ok(Bytes::from(vec![2; 2]))];
        assert!(fetch_frame(11, long).await.0.is_err());
        let failed = vec![
            Ok(Bytes::from(vec![1; 10])),
            Err(io::ErrorKind::Other.into()),
        ];
        assert!(fetch_frame(20, failed).await.0.is_err());
    }

    #[test]
    fn frame_len_accepts_only_sane_sizes() {
        assert_eq!(frame_len(8_i32.to_be_bytes()).unwrap(), 8);
        assert_eq!(
            frame_len(i32::try_from(MAX_FRAME_BYTES).unwrap().to_be_bytes()).unwrap(),
            MAX_FRAME_BYTES
        );
        assert!(frame_len(7_i32.to_be_bytes()).is_err());
        assert!(frame_len((-1_i32).to_be_bytes()).is_err());
        assert!(frame_len(i32::try_from(MAX_FRAME_BYTES + 1).unwrap().to_be_bytes()).is_err());
    }

    #[tokio::test]
    async fn read_frame_splits_stream_into_frames() {
        let stream = [
            &10_i32.to_be_bytes()[..],
            b"0123456789",
            &8_i32.to_be_bytes(),
            b"abcdefgh",
        ]
        .concat();
        let mut reader = stream.as_slice();
        assert_eq!(
            read_frame(&mut reader).await.unwrap().unwrap(),
            &b"0123456789"[..]
        );
        assert_eq!(
            read_frame(&mut reader).await.unwrap().unwrap(),
            &b"abcdefgh"[..]
        );
        assert!(read_frame(&mut reader).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn read_frame_rejects_truncated_and_oversized_frames() {
        let truncated = [&10_i32.to_be_bytes()[..], b"01234"].concat();
        let err = read_frame(&mut truncated.as_slice()).await.unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::UnexpectedEof);
        let oversized = i32::MAX.to_be_bytes();
        let err = read_frame(&mut oversized.as_slice()).await.unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
    }

    #[tokio::test]
    async fn read_frame_refuses_an_oversized_request_before_its_body_but_not_a_produce() {
        let len = i32::try_from(MAX_REQUEST_BYTES + 1).unwrap().to_be_bytes();
        let metadata = [&len[..], &(ApiKey::Metadata as i16).to_be_bytes()].concat();
        let err = read_frame(&mut metadata.as_slice()).await.unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidData, "{err}");
        let produce = [
            &len[..],
            &(ApiKey::Produce as i16).to_be_bytes(),
            &vec![7; MAX_REQUEST_BYTES - 1],
        ]
        .concat();
        let frame = read_frame(&mut produce.as_slice()).await.unwrap().unwrap();
        assert_eq!(frame.len(), MAX_REQUEST_BYTES + 1);
    }

    #[test]
    fn client_id_reads_both_header_versions_and_leaves_the_body() {
        use kafka_protocol::messages::RequestHeader;
        for version in [1, 12] {
            let header = RequestHeader::default()
                .with_request_api_key(ApiKey::Metadata as i16)
                .with_request_api_version(version)
                .with_correlation_id(9)
                .with_client_id(Some(StrBytes::from_static_str("me")));
            let header_version = ApiKey::Metadata.request_header_version(version);
            let header = if header_version >= 2 {
                header.with_unknown_tagged_fields([(4, Bytes::from_static(b"tag"))].into())
            } else {
                header
            };
            let mut frame = BytesMut::new();
            header.encode(&mut frame, header_version).unwrap();
            frame.put_slice(b"body");
            let mut frame = frame.freeze();
            let found = client_id(&mut frame, ApiKey::Metadata, version).unwrap();
            assert_eq!(found.as_deref(), Some("me"));
            assert_eq!(frame, &b"body"[..]);
        }
    }

    fn untagged(mut request: ProduceRequest) -> ProduceRequest {
        request.unknown_tagged_fields.clear();
        for topic in &mut request.topic_data {
            topic.unknown_tagged_fields.clear();
            for partition in &mut topic.partition_data {
                partition.unknown_tagged_fields.clear();
            }
        }
        request
    }

    #[test]
    fn decode_produce_agrees_with_kafka_protocol_at_every_version() {
        use kafka_protocol::protocol::Decodable;
        let tags = || [(5, Bytes::from_static(b"x"))].into();
        for version in 3..=13 {
            let partition = |index, records| {
                let partition = PartitionProduceData::default()
                    .with_index(index)
                    .with_records(records);
                if version >= 9 {
                    partition.with_unknown_tagged_fields(tags())
                } else {
                    partition
                }
            };
            let topic = |name, id, partitions| {
                let topic = TopicProduceData::default()
                    .with_name(TopicName(StrBytes::from_static_str(name)))
                    .with_topic_id(Uuid::from_u128(id))
                    .with_partition_data(partitions);
                if version >= 9 {
                    topic.with_unknown_tagged_fields(tags())
                } else {
                    topic
                }
            };
            let request = ProduceRequest::default()
                .with_transactional_id(Some(TransactionalId(StrBytes::from_static_str("tx"))))
                .with_acks(-1)
                .with_timeout_ms(1500)
                .with_topic_data(vec![
                    topic(
                        "a",
                        7,
                        vec![
                            partition(0, Some(Bytes::from_static(b"batch"))),
                            partition(3, None),
                        ],
                    ),
                    topic("b", 8, Vec::new()),
                ]);
            let request = if version >= 9 {
                request.with_unknown_tagged_fields(tags())
            } else {
                request
            };
            let mut encoded = BytesMut::new();
            request.encode(&mut encoded, version).unwrap();
            let encoded = encoded.freeze();
            let expected = ProduceRequest::decode(&mut encoded.clone(), version).unwrap();
            let mut body = encoded.clone();
            let decoded = decode_produce(&mut body, version).unwrap();
            assert_eq!(decoded, untagged(expected), "v{version}");
            assert!(body.is_empty(), "v{version}");
            for cut in 0..encoded.len() {
                assert!(
                    decode_produce(&mut encoded.slice(..cut), version).is_err(),
                    "v{version} cut at {cut}"
                );
            }
        }
    }

    #[test]
    fn decode_produce_refuses_absurd_counts_and_too_many_entries() {
        let mut flexible = BytesMut::new();
        flexible.put_slice(&[0, 0xff, 0xff, 0, 0, 0, 0]);
        flexible.put_slice(&[0xff, 0xff, 0xff, 0xff, 0x0f]);
        assert!(decode_produce(&mut flexible.freeze(), 9).is_err());
        let mut classic = BytesMut::new();
        classic.put_i16(-1);
        classic.put_i16(-1);
        classic.put_i32(0);
        classic.put_i32(i32::MAX);
        assert!(decode_produce(&mut classic.freeze(), 3).is_err());

        let mut crowded = BytesMut::new();
        crowded.put_slice(&[0, 0xff, 0xff, 0, 0, 0, 0, 2, 1]);
        crowded.put_slice(&[0x81, 0x80, 0x04]);
        for index in 0..=i32::try_from(MAX_PRODUCE_ENTRIES).unwrap() {
            crowded.put_i32(index);
            crowded.put_slice(&[0, 0]);
        }
        crowded.put_slice(&[0, 0]);
        let err = decode_produce(&mut crowded.freeze(), 9).unwrap_err();
        assert!(err.to_string().contains("entries"), "{err}");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn dispatch_closes_on_garbage_without_panicking() {
        let dir = tempfile::tempdir().unwrap();
        let broker = Broker::start(crate::broker::tests::config(dir.path()), None)
            .await
            .unwrap();
        let decoding = Arc::new(Semaphore::new(DECODE_BUDGET_BYTES));
        for frame in [
            &b"\x03\xe7\x00\x00\x00\x00\x00\x01"[..],
            b"\x00\x03\x00\x0c\x00\x00\x00\x01\xff\xff\x00\xff\xff\xff\xff\x0f",
            b"\x00\x12\x00\x03\x00\x00\x00\x01",
        ] {
            let failed = match dispatch(&broker, None, Bytes::copy_from_slice(frame), &decoding) {
                Err(_) => true,
                Ok(InFlight {
                    response: Response::Message(response),
                    ..
                }) => response.await.is_err(),
                Ok(InFlight {
                    response: Response::Fetch(body),
                    ..
                }) => body.await.is_err(),
            };
            assert!(failed, "{frame:?}");
        }
    }
}
