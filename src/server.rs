//! TCP accept loop, request framing and in-order response writing.

use std::fmt::Display;
use std::future::{Future, ready};
use std::io::{self, IoSlice};
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use bytes::{BufMut, Bytes, BytesMut};
use kafka_protocol::messages::{ApiKey, RequestKind, ResponseHeader, ResponseKind};
use kafka_protocol::protocol::{Encodable, decode_request_header_from_buffer};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;
use tokio::time::{Instant, MissedTickBehavior};
use tracing::{debug, warn};

use crate::api;
use crate::broker::Broker;
use crate::group::ClientInfo;

/// Largest request frame accepted; a larger size prefix closes the connection before anything is allocated.
pub const MAX_FRAME_BYTES: usize = 100 * 1024 * 1024;
const MIN_FRAME_BYTES: usize = 8;
// ponytail: in-flight requests are bounded per connection by count, not bytes; no broker-wide memory budget yet.
const MAX_IN_FLIGHT: usize = 32;
/// How often group session and rebalance timeouts are checked.
const GROUP_TICK: Duration = Duration::from_millis(100);

type ResponseFuture = Pin<Box<dyn Future<Output = Option<ResponseKind>> + Send>>;
type FetchFuture = Pin<Box<dyn Future<Output = api::FetchBody> + Send>>;

/// A pending response: most are encoded whole once ready, fetches are written while still being read.
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

/// Accepts connections forever, serving each one on its own task, and enforces group timeouts.
pub async fn serve(listener: TcpListener, broker: Arc<Broker>) {
    let mut ticks = tokio::time::interval(GROUP_TICK);
    ticks.set_missed_tick_behavior(MissedTickBehavior::Delay);
    loop {
        tokio::select! {
            accepted = listener.accept() => match accepted {
                Ok((stream, peer)) => {
                    debug!(%peer, "connection accepted");
                    tokio::spawn(connection(stream, Arc::clone(&broker)));
                }
                Err(e) => {
                    warn!("accept failed: {e}");
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
            },
            _ = ticks.tick() => broker.groups().expire(Instant::now()),
        }
    }
}

async fn connection(stream: TcpStream, broker: Arc<Broker>) {
    let peer = stream.peer_addr().ok();
    let _ = stream.set_nodelay(true);
    let (reader, writer) = stream.into_split();
    let (in_flight, queue) = mpsc::channel(MAX_IN_FLIGHT);
    let writing = tokio::spawn(write_responses(writer, queue));
    if let Err(e) = read_requests(reader, &broker, peer, in_flight).await {
        warn!(?peer, "closing connection: {e}");
    }
    if let Ok(Err(e)) = writing.await {
        debug!(?peer, "write side closed: {e}");
    }
}

async fn read_requests(
    mut reader: OwnedReadHalf,
    broker: &Arc<Broker>,
    peer: Option<SocketAddr>,
    in_flight: mpsc::Sender<InFlight>,
) -> io::Result<()> {
    while let Some(frame) = read_frame(&mut reader).await? {
        if in_flight
            .send(dispatch(broker, peer, frame)?)
            .await
            .is_err()
        {
            break;
        }
    }
    Ok(())
}

/// Writes responses strictly in request order; produce futures complete when their flush does.
async fn write_responses(
    mut writer: OwnedWriteHalf,
    mut queue: mpsc::Receiver<InFlight>,
) -> io::Result<()> {
    while let Some(request) = queue.recv().await {
        match request.response {
            Response::Message(response) => {
                if let Some(response) = response.await {
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
                let body = body.await;
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

/// Reads one size-prefixed request frame; `None` on a clean end of stream. Memory grows with the
/// bytes actually received, so a lying size prefix cannot force a large allocation.
async fn read_frame<R: AsyncRead + Unpin>(reader: &mut R) -> io::Result<Option<Bytes>> {
    let mut prefix = [0; 4];
    match reader.read_exact(&mut prefix).await {
        Ok(_) => {}
        Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => return Ok(None),
        Err(e) => return Err(e),
    }
    let len = frame_len(prefix)?;
    let mut frame = Vec::with_capacity(len.min(64 * 1024));
    (&mut *reader)
        .take(len as u64)
        .read_to_end(&mut frame)
        .await?;
    if frame.len() < len {
        return Err(io::ErrorKind::UnexpectedEof.into());
    }
    Ok(Some(Bytes::from(frame)))
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

/// Decodes one request and returns its response future. Most handlers run only when the writer
/// polls that future, so a connection's requests are handled in order, as Kafka does; produce
/// queues its appends here so that request order is append order.
fn dispatch(broker: &Arc<Broker>, peer: Option<SocketAddr>, frame: Bytes) -> io::Result<InFlight> {
    let key = i16::from_be_bytes([frame[0], frame[1]]);
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
            response: Response::Message(Box::pin(ready(Some(response)))),
        });
    }
    let mut body = frame;
    let header = decode_request_header_from_buffer(&mut body)
        .map_err(|e| invalid(format!("bad request header: {e}")))?;
    debug!(?api_key, version, correlation_id, client = ?header.client_id, "request");
    // ponytail: kafka-protocol is pinned to an unreleased commit (#200) that rejects array counts above the bytes
    // left in the frame, but it still reserves count * element size (up to 120 B) before decoding; on Linux a
    // crafted frame whose reservation exceeds RAM + swap still aborts the process (27 MiB did on a 2 GiB host).
    let request = RequestKind::decode(api_key, &mut body, version)
        .map_err(|e| invalid(format!("malformed {api_key:?} v{version}: {e}")))?;
    let broker = Arc::clone(broker);
    let response = match request {
        RequestKind::Fetch(request) => {
            Response::Fetch(Box::pin(api::fetch(broker, version, request)))
        }
        request => {
            let client = || ClientInfo::new(header.client_id, peer);
            Response::Message(
                respond(broker, version, request, client)
                    .ok_or_else(|| invalid(format!("no handler for {api_key:?}")))?,
            )
        }
    };
    Ok(InFlight {
        api_key,
        version,
        correlation_id,
        response,
    })
}

/// The response future of a non-fetch request, or `None` for an API without a handler.
fn respond(
    broker: Arc<Broker>,
    version: i16,
    request: RequestKind,
    client: impl FnOnce() -> ClientInfo,
) -> Option<ResponseFuture> {
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
        RequestKind::Produce(request) => {
            let produced = api::produce(&broker, version, request);
            Box::pin(async move { produced.await.map(Into::into) })
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
            Box::pin(async move { Some(api::sync_group(&broker, &request).await.into()) })
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

    #[tokio::test(flavor = "multi_thread")]
    async fn dispatch_closes_on_garbage_without_panicking() {
        let dir = tempfile::tempdir().unwrap();
        let broker = Broker::start(crate::broker::tests::config(dir.path()), None)
            .await
            .unwrap();
        for frame in [
            &b"\x03\xe7\x00\x00\x00\x00\x00\x01"[..],
            b"\x00\x03\x00\x0c\x00\x00\x00\x01\xff\xff\x00\xff\xff\xff\xff\x0f",
            b"\x00\x12\x00\x03\x00\x00\x00\x01",
        ] {
            assert!(dispatch(&broker, None, Bytes::copy_from_slice(frame)).is_err());
        }
    }
}
