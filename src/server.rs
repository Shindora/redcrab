//! TCP accept loop, request framing and in-order response writing.

use std::fmt::Display;
use std::future::{Future, ready};
use std::io;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use bytes::{BufMut, Bytes, BytesMut};
use kafka_protocol::messages::{ApiKey, RequestKind, ResponseHeader, ResponseKind};
use kafka_protocol::protocol::{Encodable, decode_request_header_from_buffer};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWriteExt};
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

struct InFlight {
    api_key: ApiKey,
    version: i16,
    correlation_id: i32,
    response: ResponseFuture,
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
        if let Some(response) = request.response.await {
            let frame = encode_response(
                request.api_key,
                request.version,
                request.correlation_id,
                &response,
            )?;
            writer.write_all(&frame).await?;
        }
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
            response: Box::pin(ready(Some(response))),
        });
    }
    let mut body = frame;
    let header = decode_request_header_from_buffer(&mut body)
        .map_err(|e| invalid(format!("bad request header: {e}")))?;
    debug!(?api_key, version, correlation_id, client = ?header.client_id, "request");
    // ponytail: kafka-protocol 0.18 pre-allocates arrays from the untrusted length prefix; on hosts that refuse
    // overcommit (Linux default) a hostile count aborts the process. Fixed upstream after 0.18 (#200), unreleased.
    let request = RequestKind::decode(api_key, &mut body, version)
        .map_err(|e| invalid(format!("malformed {api_key:?} v{version}: {e}")))?;
    let broker = Arc::clone(broker);
    let response: ResponseFuture = match request {
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
        RequestKind::Fetch(request) => {
            Box::pin(async move { Some(api::fetch(broker, version, request).await.into()) })
        }
        RequestKind::ListOffsets(request) => {
            Box::pin(async move { Some(api::list_offsets(broker, request).await.into()) })
        }
        RequestKind::InitProducerId(request) => {
            Box::pin(async move { Some(api::init_producer_id(&broker, &request).into()) })
        }
        RequestKind::FindCoordinator(request) => {
            Box::pin(async move { Some(api::find_coordinator(&broker, version, &request).into()) })
        }
        RequestKind::JoinGroup(request) => {
            let client = ClientInfo::new(header.client_id, peer);
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
            Box::pin(async move { Some(api::offset_fetch(&broker, version, request).await.into()) })
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
        _ => return Err(invalid(format!("no handler for {api_key:?}"))),
    };
    Ok(InFlight {
        api_key,
        version,
        correlation_id,
        response,
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
    async fn dispatch_closes_on_garbage_without_panicking() {
        let dir = tempfile::tempdir().unwrap();
        let broker = Arc::new(Broker::open(crate::broker::tests::config(dir.path())).unwrap());
        for frame in [
            &b"\x03\xe7\x00\x00\x00\x00\x00\x01"[..],
            b"\x00\x03\x00\x0c\x00\x00\x00\x01\xff\xff\x00\xff\xff\xff\xff\x0f",
            b"\x00\x12\x00\x03\x00\x00\x00\x01",
        ] {
            assert!(dispatch(&broker, None, Bytes::copy_from_slice(frame)).is_err());
        }
    }
}
