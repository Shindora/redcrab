//! Internal transport between the brokers of a cluster.
//!
//! Each node dials every peer once and writes everything it has for that peer on that
//! connection: Raft messages of every group, leader hints, and forwarded admin requests and their
//! responses. Connections accepted from peers are only read. Raft messages are fire-and-forget:
//! every queue is bounded and drops what does not fit, and a peer that is down misses what is sent
//! to it, which the Raft core tolerates. Everything queued for a peer when its sender wakes goes out
//! in one frame and one write, so the heartbeats of many groups share a syscall.
//!
//! Wire format, big-endian. A frame is `len: u32` and `len` bytes of items back to back; an empty
//! frame is a keepalive. The dialer's first frame is a hello: `magic: u32, version: u16, from: i32,
//! fingerprint: u64`, the fingerprint naming the cluster's voters. Each item starts with a tag byte;
//! a group is `topic: uuid (16 bytes), partition: i32`:
//!
//! - 1 vote request: group, `term: i32, pre_vote: u8, last_epoch: i32, log_end: i64`
//! - 2 vote response: group, `term: i32, pre_vote: u8, granted: u8`
//! - 3 append request: group, `term: i32, prev_offset: i64, prev_epoch: i32, commit_offset: i64,
//!   len: u32` and `len` bytes of record batches as stored in the leader's log
//! - 4 append response: group, `term: i32, outcome: u8` and for outcome 0 (accepted)
//!   `matched_end: i64, flushed_end: i64`, for 1 (mismatch) `epoch: i32, end_offset: i64`, for 2
//!   (stale term) nothing
//! - 5 leader hint: group, `term: i32, leader: i32`
//! - 6 forwarded request and 7 its response: `correlation: u64, len: u32` and `len` bytes
//!
//! Everything received is validated before use: frame lengths are bounded, tags and flags must be
//! known, and an append request's batches must be whole, well-framed and pass their CRC. The
//! listener is for trusted peers only: there is no authentication or encryption, and a peer that
//! sends well-formed but unsafe Raft messages (two leaders in one term) makes the core panic.

// ponytail: peers are neither authenticated nor encrypted; mutual TLS on the RPC listener is the upgrade.

use std::collections::{HashMap, VecDeque};
use std::io::{self, Read, Write};
use std::net::{Shutdown, SocketAddr, TcpListener, TcpStream, ToSocketAddrs};
use std::ops::Range;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex, MutexGuard, PoisonError};
use std::thread;
use std::time::Duration;

use bytes::{Buf, BufMut, Bytes, BytesMut};
use tracing::{debug, info, warn};
use uuid::Uuid;

use crate::raft::{
    AppendOutcome, AppendRequest, AppendResponse, BatchInfo, Message, NodeId, VoteRequest,
    VoteResponse,
};
use crate::replica::GroupKey;
use crate::storage::batch_frames;

/// Largest frame sent or accepted.
pub const MAX_FRAME_BYTES: usize = 32 << 20;
const MAGIC: u32 = 0x7263_7270;
const VERSION: u16 = 1;
const QUEUE_BYTES: usize = 64 << 20;
const QUEUE_ITEMS: usize = 65_536;
/// Bytes counted for an item besides its payload; at least its encoded header.
const ITEM_OVERHEAD: usize = 64;
const KEEPALIVE: Duration = Duration::from_secs(1);
const READ_TIMEOUT: Duration = Duration::from_secs(5);
const WRITE_TIMEOUT: Duration = Duration::from_secs(5);
const CONNECT_TIMEOUT: Duration = Duration::from_secs(1);
const BACKOFF_MIN: Duration = Duration::from_millis(50);
const BACKOFF_MAX: Duration = Duration::from_secs(1);

const VOTE_REQUEST: u8 = 1;
const VOTE_RESPONSE: u8 = 2;
const APPEND_REQUEST: u8 = 3;
const APPEND_RESPONSE: u8 = 4;
const HINT: u8 = 5;
const REQUEST: u8 = 6;
const RESPONSE: u8 = 7;
const ACCEPTED: u8 = 0;
const MISMATCH: u8 = 1;
const STALE_TERM: u8 = 2;

const BATCH_CRC_AT: usize = 17;
const BATCH_CRC_START: usize = 21;

/// Something received that cannot be decoded.
#[derive(Debug, thiserror::Error)]
pub enum WireError {
    /// The bytes break the wire format.
    #[error("malformed frame: {0}")]
    Malformed(&'static str),
    /// Reading the connection failed.
    #[error(transparent)]
    Io(#[from] io::Error),
}

/// Something to send to a peer.
#[derive(Debug, Clone)]
pub enum Item {
    /// A Raft message. An append request's batches travel as `payload`; its `batches` field is
    /// not sent, the receiver rebuilds it from the payload.
    Raft {
        /// The group.
        group: GroupKey,
        /// The message.
        message: Message,
        /// Record batches of an append request, empty otherwise.
        payload: Bytes,
    },
    /// The sender leads `group` in `term`, for nodes that are not its replicas.
    Hint {
        /// The group.
        group: GroupKey,
        /// Its term.
        term: i32,
        /// Its leader.
        leader: NodeId,
    },
    /// An admin request forwarded to the controller.
    Request {
        /// Matches the response to the request.
        correlation: u64,
        /// The request.
        body: Bytes,
    },
    /// The controller's answer to a forwarded request.
    Response {
        /// The request's correlation.
        correlation: u64,
        /// The answer.
        body: Bytes,
    },
}

impl Item {
    fn size(&self) -> usize {
        ITEM_OVERHEAD
            + match self {
                Item::Raft { payload, .. } => payload.len(),
                Item::Hint { .. } => 0,
                Item::Request { body, .. } | Item::Response { body, .. } => body.len(),
            }
    }

    fn encode(&self, out: &mut BytesMut) {
        match self {
            Item::Raft {
                group,
                message,
                payload,
            } => encode_raft(out, *group, message, payload),
            Item::Hint {
                group,
                term,
                leader,
            } => {
                out.put_u8(HINT);
                put_group(out, *group);
                out.put_i32(*term);
                out.put_i32(*leader);
            }
            Item::Request { correlation, body } => put_correlated(out, REQUEST, *correlation, body),
            Item::Response { correlation, body } => {
                put_correlated(out, RESPONSE, *correlation, body);
            }
        }
    }
}

/// Something received from a peer.
#[derive(Debug)]
pub enum Inbound {
    /// A Raft message.
    Raft(Received),
    /// The sender leads `group` in `term`.
    Hint {
        /// The group.
        group: GroupKey,
        /// Its term.
        term: i32,
        /// Its leader.
        leader: NodeId,
    },
    /// A forwarded admin request.
    Request {
        /// To be echoed in the response.
        correlation: u64,
        /// The request.
        body: Bytes,
    },
    /// The answer to a request this node forwarded.
    Response {
        /// The request's correlation.
        correlation: u64,
        /// The answer.
        body: Bytes,
    },
}

/// A Raft message received, with the bytes of an append request's batches.
#[derive(Debug)]
pub struct Received {
    /// The group.
    pub group: GroupKey,
    /// The message, an append request with its batches filled in from `payload`.
    pub message: Message,
    /// The batches of an append request, back to back.
    pub payload: Bytes,
    /// Where each batch of the append request lies in `payload`, in order.
    pub ranges: Vec<Range<usize>>,
}

/// Where received items go.
pub trait Inbox: Send + Sync {
    /// Handle `items` received from peer `from`, in the order received.
    fn deliver(&self, from: NodeId, items: Vec<Inbound>);
}

/// The sending side of the transport, plus the listener once started.
pub struct Transport {
    links: HashMap<NodeId, Arc<Link>>,
    stop: Arc<AtomicBool>,
    inbound: Arc<Mutex<HashMap<u64, TcpStream>>>,
    listening: Mutex<Option<SocketAddr>>,
}

impl Transport {
    /// Starts one sender thread per peer in `peers` (id and `host:port` of its RPC listener,
    /// resolved again on every connection attempt).
    #[must_use]
    pub fn start(me: NodeId, fingerprint: u64, peers: &[(NodeId, String)]) -> Transport {
        let stop = Arc::new(AtomicBool::new(false));
        let hello = hello_frame(me, fingerprint);
        let links = peers
            .iter()
            .map(|(peer, address)| {
                let link = Arc::new(Link::default());
                let (sending, stop, hello, peer, address) = (
                    Arc::clone(&link),
                    Arc::clone(&stop),
                    hello.clone(),
                    *peer,
                    address.clone(),
                );
                thread::Builder::new()
                    .name(format!("redcrab-send-{peer}"))
                    .spawn(move || run_sender(&sending, &hello, peer, &address, &stop))
                    .map_err(|e| warn!(peer, "cannot start sender thread: {e}"))
                    .ok();
                (peer, link)
            })
            .collect();
        Transport {
            links,
            stop,
            inbound: Arc::new(Mutex::new(HashMap::new())),
            listening: Mutex::new(None),
        }
    }

    /// Accepts peer connections on `listener` on a thread of its own, reading each on a thread of
    /// its own and handing what arrives to `inbox`.
    ///
    /// # Errors
    ///
    /// Fails when the listener's address cannot be read or the thread cannot be started.
    pub fn listen(
        &self,
        listener: TcpListener,
        me: NodeId,
        fingerprint: u64,
        inbox: Arc<dyn Inbox>,
    ) -> io::Result<()> {
        let mut address = listener.local_addr()?;
        if address.ip().is_unspecified() {
            address.set_ip(if address.is_ipv4() {
                [127, 0, 0, 1].into()
            } else {
                std::net::Ipv6Addr::LOCALHOST.into()
            });
        }
        *lock(&self.listening) = Some(address);
        let peers: Vec<NodeId> = self.links.keys().copied().collect();
        let (stop, inbound) = (Arc::clone(&self.stop), Arc::clone(&self.inbound));
        thread::Builder::new()
            .name("redcrab-accept".into())
            .spawn(move || {
                run_acceptor(&listener, me, fingerprint, &peers, &inbox, &stop, &inbound);
            })?;
        Ok(())
    }

    /// Queues `item` for peer `to`; `false` when it was dropped because the peer is unknown or its
    /// queue is full.
    pub fn send(&self, to: NodeId, item: Item) -> bool {
        self.links.get(&to).is_some_and(|link| link.push(item))
    }

    /// Stops every thread of the transport and closes its connections.
    pub fn shutdown(&self) {
        self.stop.store(true, Ordering::Release);
        for link in self.links.values() {
            link.ready.notify_all();
        }
        for (_, stream) in lock(&self.inbound).drain() {
            let _ = stream.shutdown(Shutdown::Both);
        }
        if let Some(address) = *lock(&self.listening) {
            let _ = TcpStream::connect_timeout(&address, CONNECT_TIMEOUT);
        }
    }
}

#[derive(Default)]
struct Link {
    queue: Mutex<Queue>,
    ready: Condvar,
}

#[derive(Default)]
struct Queue {
    items: VecDeque<Item>,
    bytes: usize,
}

impl Link {
    fn push(&self, item: Item) -> bool {
        let size = item.size();
        let mut queue = lock(&self.queue);
        if queue.items.len() >= QUEUE_ITEMS || queue.bytes + size > QUEUE_BYTES {
            return false;
        }
        queue.bytes += size;
        queue.items.push_back(item);
        drop(queue);
        self.ready.notify_one();
        true
    }

    /// Waits up to `wait` for something to send, then takes what fits in one frame.
    fn take(&self, wait: Duration) -> Vec<Item> {
        let mut queue = lock(&self.queue);
        if queue.items.is_empty() {
            queue = self
                .ready
                .wait_timeout(queue, wait)
                .map_or_else(|e| e.into_inner().0, |(queue, _)| queue);
        }
        let mut taken = Vec::new();
        let mut bytes = 0;
        while let Some(size) = queue.items.front().map(Item::size) {
            if !taken.is_empty() && bytes + size > MAX_FRAME_BYTES {
                break;
            }
            bytes += size;
            queue.bytes -= size;
            taken.extend(queue.items.pop_front());
        }
        taken
    }

    fn clear(&self) {
        let mut queue = lock(&self.queue);
        queue.items.clear();
        queue.bytes = 0;
    }
}

fn run_sender(link: &Link, hello: &[u8], peer: NodeId, address: &str, stop: &AtomicBool) {
    let mut backoff = BACKOFF_MIN;
    let mut connection: Option<TcpStream> = None;
    let mut out = BytesMut::new();
    let mut complained = false;
    while !stop.load(Ordering::Acquire) {
        let Some(stream) = connection.as_mut() else {
            match connect(address, hello) {
                Ok(stream) => {
                    info!(peer, address, "connected to peer");
                    connection = Some(stream);
                    backoff = BACKOFF_MIN;
                    complained = false;
                }
                Err(e) => {
                    if complained {
                        debug!(peer, address, "cannot reach peer: {e}");
                    } else {
                        warn!(peer, address, "cannot reach peer: {e}");
                        complained = true;
                    }
                    link.clear();
                    thread::sleep(backoff);
                    backoff = (backoff * 2).min(BACKOFF_MAX);
                }
            }
            continue;
        };
        let items = link.take(KEEPALIVE);
        out.clear();
        encode_frame(&mut out, &items);
        if let Err(e) = stream.write_all(&out) {
            warn!(peer, "lost connection to peer: {e}");
            connection = None;
        }
    }
}

fn connect(address: &str, hello: &[u8]) -> io::Result<TcpStream> {
    let mut failure = io::Error::new(io::ErrorKind::NotFound, "address resolves to nothing");
    for socket in address.to_socket_addrs()? {
        match TcpStream::connect_timeout(&socket, CONNECT_TIMEOUT) {
            Ok(mut stream) => {
                stream.set_nodelay(true)?;
                stream.set_write_timeout(Some(WRITE_TIMEOUT))?;
                // A peer whose address vanished never resets the connection and writes keep landing
                // in the socket buffer; failing on unacknowledged data makes the sender redial.
                // ponytail: only Linux has TCP_USER_TIMEOUT; elsewhere the kernel's retransmission timeout (minutes) applies.
                #[cfg(target_os = "linux")]
                socket2::SockRef::from(&stream).set_tcp_user_timeout(Some(WRITE_TIMEOUT))?;
                stream.write_all(hello)?;
                return Ok(stream);
            }
            Err(e) => failure = e,
        }
    }
    Err(failure)
}

fn run_acceptor(
    listener: &TcpListener,
    me: NodeId,
    fingerprint: u64,
    peers: &[NodeId],
    inbox: &Arc<dyn Inbox>,
    stop: &Arc<AtomicBool>,
    inbound: &Arc<Mutex<HashMap<u64, TcpStream>>>,
) {
    let next_id = AtomicU64::new(0);
    for accepted in listener.incoming() {
        if stop.load(Ordering::Acquire) {
            return;
        }
        let stream = match accepted {
            Ok(stream) => stream,
            Err(e) => {
                warn!("accepting a peer connection failed: {e}");
                thread::sleep(BACKOFF_MIN);
                continue;
            }
        };
        let id = next_id.fetch_add(1, Ordering::Relaxed);
        if let Ok(clone) = stream.try_clone() {
            lock(inbound).insert(id, clone);
        }
        let (peers, inbox, stop, inbound) = (
            peers.to_vec(),
            Arc::clone(inbox),
            Arc::clone(stop),
            Arc::clone(inbound),
        );
        let spawned = thread::Builder::new()
            .name("redcrab-receive".into())
            .spawn(move || {
                let peer = stream.peer_addr().ok();
                if let Err(e) = receive(stream, me, fingerprint, &peers, &*inbox, &stop) {
                    warn!(?peer, "closing peer connection: {e}");
                }
                lock(&inbound).remove(&id);
            });
        if let Err(e) = spawned {
            warn!("cannot start a receiver thread: {e}");
        }
    }
}

fn receive(
    mut stream: TcpStream,
    me: NodeId,
    fingerprint: u64,
    peers: &[NodeId],
    inbox: &dyn Inbox,
    stop: &AtomicBool,
) -> Result<(), WireError> {
    stream.set_read_timeout(Some(READ_TIMEOUT))?;
    stream.set_nodelay(true)?;
    let hello = read_frame(&mut stream)?.ok_or(WireError::Malformed("closed before hello"))?;
    let from = parse_hello(hello, fingerprint)?;
    if from == me || !peers.contains(&from) {
        return Err(WireError::Malformed("hello from a node that is not a peer"));
    }
    debug!(peer = from, "peer connection accepted");
    while let Some(frame) = read_frame(&mut stream)? {
        if stop.load(Ordering::Acquire) {
            break;
        }
        if !frame.is_empty() {
            inbox.deliver(from, decode_items(frame)?);
        }
    }
    Ok(())
}

/// Reads one frame; `None` on a clean end of stream. Memory grows with the bytes received, so a
/// lying length cannot force a large allocation.
fn read_frame(stream: &mut impl Read) -> Result<Option<Bytes>, WireError> {
    let mut prefix = [0; 4];
    match stream.read_exact(&mut prefix) {
        Ok(()) => {}
        Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => return Ok(None),
        Err(e) => return Err(e.into()),
    }
    let len = usize::try_from(u32::from_be_bytes(prefix))
        .ok()
        .filter(|len| *len <= MAX_FRAME_BYTES)
        .ok_or(WireError::Malformed("frame longer than the limit"))?;
    let mut frame = Vec::with_capacity(len.min(1 << 20));
    stream.take(len as u64).read_to_end(&mut frame)?;
    if frame.len() < len {
        return Err(io::Error::from(io::ErrorKind::UnexpectedEof).into());
    }
    Ok(Some(Bytes::from(frame)))
}

fn hello_frame(me: NodeId, fingerprint: u64) -> Vec<u8> {
    let mut frame = BytesMut::new();
    frame.put_u32(18);
    frame.put_u32(MAGIC);
    frame.put_u16(VERSION);
    frame.put_i32(me);
    frame.put_u64(fingerprint);
    frame.to_vec()
}

fn parse_hello(hello: Bytes, fingerprint: u64) -> Result<NodeId, WireError> {
    let mut reader = Reader::new(hello);
    if reader.u32()? != MAGIC || reader.u16()? != VERSION {
        return Err(WireError::Malformed("not a redcrab peer of this version"));
    }
    let from = reader.i32()?;
    if reader.u64()? != fingerprint {
        return Err(WireError::Malformed(
            "peer was started with another list of cluster nodes",
        ));
    }
    reader.finish()?;
    Ok(from)
}

fn encode_frame(out: &mut BytesMut, items: &[Item]) {
    let start = out.len();
    out.put_u32(0);
    for item in items {
        item.encode(out);
    }
    let len = u32::try_from(out.len() - start - 4).expect("frames are bounded by MAX_FRAME_BYTES");
    out[start..start + 4].copy_from_slice(&len.to_be_bytes());
}

fn encode_raft(out: &mut BytesMut, group: GroupKey, message: &Message, payload: &Bytes) {
    match message {
        Message::VoteRequest(request) => {
            out.put_u8(VOTE_REQUEST);
            put_group(out, group);
            out.put_i32(request.term);
            out.put_u8(u8::from(request.pre_vote));
            out.put_i32(request.last_epoch);
            out.put_i64(request.log_end);
        }
        Message::VoteResponse(response) => {
            out.put_u8(VOTE_RESPONSE);
            put_group(out, group);
            out.put_i32(response.term);
            out.put_u8(u8::from(response.pre_vote));
            out.put_u8(u8::from(response.granted));
        }
        Message::AppendRequest(request) => {
            out.put_u8(APPEND_REQUEST);
            put_group(out, group);
            out.put_i32(request.term);
            out.put_i64(request.prev_offset);
            out.put_i32(request.prev_epoch);
            out.put_i64(request.commit_offset);
            out.put_u32(u32::try_from(payload.len()).expect("payloads are bounded by frames"));
            out.put_slice(payload);
        }
        Message::AppendResponse(response) => {
            out.put_u8(APPEND_RESPONSE);
            put_group(out, group);
            out.put_i32(response.term);
            match response.outcome {
                AppendOutcome::Accepted {
                    matched_end,
                    flushed_end,
                } => {
                    out.put_u8(ACCEPTED);
                    out.put_i64(matched_end);
                    out.put_i64(flushed_end);
                }
                AppendOutcome::Mismatch { epoch, end_offset } => {
                    out.put_u8(MISMATCH);
                    out.put_i32(epoch);
                    out.put_i64(end_offset);
                }
                AppendOutcome::StaleTerm => out.put_u8(STALE_TERM),
            }
        }
    }
}

fn put_group(out: &mut BytesMut, group: GroupKey) {
    out.put_slice(group.topic.as_bytes());
    out.put_i32(group.partition);
}

fn put_correlated(out: &mut BytesMut, tag: u8, correlation: u64, body: &Bytes) {
    out.put_u8(tag);
    out.put_u64(correlation);
    out.put_u32(u32::try_from(body.len()).expect("bodies are bounded by frames"));
    out.put_slice(body);
}

/// Decodes the items of one frame.
///
/// # Errors
///
/// Returns [`WireError::Malformed`] for anything that breaks the wire format.
pub fn decode_items(frame: Bytes) -> Result<Vec<Inbound>, WireError> {
    let mut reader = Reader::new(frame);
    let mut items = Vec::new();
    while reader.remaining() > 0 {
        let tag = reader.u8()?;
        let item = match tag {
            VOTE_REQUEST | VOTE_RESPONSE | APPEND_REQUEST | APPEND_RESPONSE => {
                Inbound::Raft(decode_raft(tag, &mut reader)?)
            }
            HINT => Inbound::Hint {
                group: reader.group()?,
                term: reader.i32()?,
                leader: reader.i32()?,
            },
            REQUEST | RESPONSE => {
                let correlation = reader.u64()?;
                let len = reader.len32()?;
                let body = reader.bytes(len)?;
                if tag == REQUEST {
                    Inbound::Request { correlation, body }
                } else {
                    Inbound::Response { correlation, body }
                }
            }
            _ => return Err(WireError::Malformed("unknown item tag")),
        };
        items.push(item);
    }
    Ok(items)
}

fn decode_raft(tag: u8, reader: &mut Reader) -> Result<Received, WireError> {
    let group = reader.group()?;
    let term = reader.i32()?;
    let (mut payload, mut ranges) = (Bytes::new(), Vec::new());
    let message = match tag {
        VOTE_REQUEST => Message::VoteRequest(VoteRequest {
            term,
            pre_vote: reader.flag()?,
            last_epoch: reader.i32()?,
            log_end: reader.i64()?,
        }),
        VOTE_RESPONSE => Message::VoteResponse(VoteResponse {
            term,
            pre_vote: reader.flag()?,
            granted: reader.flag()?,
        }),
        APPEND_REQUEST => {
            let prev_offset = reader.i64()?;
            let prev_epoch = reader.i32()?;
            let commit_offset = reader.i64()?;
            let len = reader.len32()?;
            payload = reader.bytes(len)?;
            let batches;
            (batches, ranges) = decode_batches(&payload)?;
            Message::AppendRequest(AppendRequest {
                term,
                prev_offset,
                prev_epoch,
                commit_offset,
                batches,
            })
        }
        _ => {
            let outcome = match reader.u8()? {
                ACCEPTED => AppendOutcome::Accepted {
                    matched_end: reader.i64()?,
                    flushed_end: reader.i64()?,
                },
                MISMATCH => AppendOutcome::Mismatch {
                    epoch: reader.i32()?,
                    end_offset: reader.i64()?,
                },
                STALE_TERM => AppendOutcome::StaleTerm,
                _ => return Err(WireError::Malformed("unknown append outcome")),
            };
            Message::AppendResponse(AppendResponse { term, outcome })
        }
    };
    Ok(Received {
        group,
        message,
        payload,
        ranges,
    })
}

/// The header facts and position of each batch in `payload`, which must hold whole, well-framed
/// v2 batches with valid CRCs.
fn decode_batches(payload: &[u8]) -> Result<(Vec<BatchInfo>, Vec<Range<usize>>), WireError> {
    let (mut batches, mut ranges, mut at) = (Vec::new(), Vec::new(), 0);
    for frame in batch_frames(payload) {
        let frame = frame.map_err(|_| WireError::Malformed("bad record batch framing"))?;
        let batch = &payload[at..at + frame.len];
        let crc = u32::from_be_bytes(
            batch[BATCH_CRC_AT..BATCH_CRC_START]
                .try_into()
                .map_err(|_| WireError::Malformed("short record batch"))?,
        );
        if crc32c::crc32c(&batch[BATCH_CRC_START..]) != crc {
            return Err(WireError::Malformed("record batch fails its CRC"));
        }
        batches.push(BatchInfo {
            base_offset: frame.base_offset,
            record_count: i32::try_from(frame.record_count)
                .map_err(|_| WireError::Malformed("record batch spans too many offsets"))?,
            epoch: frame.epoch,
        });
        ranges.push(at..at + frame.len);
        at += frame.len;
    }
    Ok((batches, ranges))
}

/// Bounds-checked big-endian reads from a received buffer.
pub(crate) struct Reader(Bytes);

impl Reader {
    pub(crate) fn new(bytes: Bytes) -> Reader {
        Reader(bytes)
    }

    pub(crate) fn remaining(&self) -> usize {
        self.0.remaining()
    }

    fn need(&self, len: usize) -> Result<(), WireError> {
        if self.0.remaining() < len {
            return Err(WireError::Malformed("truncated"));
        }
        Ok(())
    }

    pub(crate) fn u8(&mut self) -> Result<u8, WireError> {
        self.need(1)?;
        Ok(self.0.get_u8())
    }

    pub(crate) fn u16(&mut self) -> Result<u16, WireError> {
        self.need(2)?;
        Ok(self.0.get_u16())
    }

    pub(crate) fn u32(&mut self) -> Result<u32, WireError> {
        self.need(4)?;
        Ok(self.0.get_u32())
    }

    pub(crate) fn u64(&mut self) -> Result<u64, WireError> {
        self.need(8)?;
        Ok(self.0.get_u64())
    }

    pub(crate) fn i16(&mut self) -> Result<i16, WireError> {
        self.need(2)?;
        Ok(self.0.get_i16())
    }

    pub(crate) fn i32(&mut self) -> Result<i32, WireError> {
        self.need(4)?;
        Ok(self.0.get_i32())
    }

    pub(crate) fn i64(&mut self) -> Result<i64, WireError> {
        self.need(8)?;
        Ok(self.0.get_i64())
    }

    pub(crate) fn flag(&mut self) -> Result<bool, WireError> {
        match self.u8()? {
            0 => Ok(false),
            1 => Ok(true),
            _ => Err(WireError::Malformed("flag is neither 0 nor 1")),
        }
    }

    pub(crate) fn uuid(&mut self) -> Result<Uuid, WireError> {
        self.need(16)?;
        let mut bytes = [0; 16];
        self.0.copy_to_slice(&mut bytes);
        Ok(Uuid::from_bytes(bytes))
    }

    fn group(&mut self) -> Result<GroupKey, WireError> {
        Ok(GroupKey {
            topic: self.uuid()?,
            partition: self.i32()?,
        })
    }

    /// A `u32` length, as `usize`.
    pub(crate) fn len32(&mut self) -> Result<usize, WireError> {
        usize::try_from(self.u32()?).map_err(|_| WireError::Malformed("length overflows"))
    }

    pub(crate) fn bytes(&mut self, len: usize) -> Result<Bytes, WireError> {
        self.need(len)?;
        Ok(self.0.split_to(len))
    }

    /// A string of `len: u16` UTF-8 bytes.
    pub(crate) fn str16(&mut self) -> Result<String, WireError> {
        let len = usize::from(self.u16()?);
        let bytes = self.bytes(len)?;
        String::from_utf8(bytes.to_vec()).map_err(|_| WireError::Malformed("string is not UTF-8"))
    }

    /// Fails unless everything was read.
    pub(crate) fn finish(&self) -> Result<(), WireError> {
        if self.0.has_remaining() {
            return Err(WireError::Malformed("trailing bytes"));
        }
        Ok(())
    }
}

/// Writes `text` as `len: u16` and its bytes.
///
/// # Errors
///
/// Fails when `text` is longer than `u16::MAX` bytes.
pub(crate) fn put_str16(out: &mut BytesMut, text: &str) -> Result<(), WireError> {
    let len = u16::try_from(text.len()).map_err(|_| WireError::Malformed("string too long"))?;
    out.put_u16(len);
    out.put_slice(text.as_bytes());
    Ok(())
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

#[cfg(test)]
mod tests {
    use std::sync::mpsc;
    use std::time::Instant;

    use super::*;
    use crate::storage::noop_control_batch;

    fn group() -> GroupKey {
        GroupKey {
            topic: Uuid::from_u128(7),
            partition: 3,
        }
    }

    /// Two stamped batches, as a leader's log holds them, starting at offset 10.
    fn payload() -> Bytes {
        let mut first = noop_control_batch(4, 1000);
        first[..8].copy_from_slice(&10_i64.to_be_bytes());
        let mut second = noop_control_batch(5, 1001);
        second[..8].copy_from_slice(&11_i64.to_be_bytes());
        Bytes::from([first, second].concat())
    }

    fn roundtrip(items: &[Item]) -> Vec<Inbound> {
        let mut out = BytesMut::new();
        encode_frame(&mut out, items);
        let frame = read_frame(&mut &out[..]).unwrap().unwrap();
        decode_items(frame).unwrap()
    }

    #[test]
    fn small_raft_messages_survive_encoding() {
        let messages = [
            Message::VoteRequest(VoteRequest {
                term: 3,
                pre_vote: true,
                last_epoch: 2,
                log_end: 77,
            }),
            Message::VoteResponse(VoteResponse {
                term: 3,
                pre_vote: false,
                granted: true,
            }),
            Message::AppendResponse(AppendResponse {
                term: 9,
                outcome: AppendOutcome::Accepted {
                    matched_end: 40,
                    flushed_end: 30,
                },
            }),
            Message::AppendResponse(AppendResponse {
                term: 9,
                outcome: AppendOutcome::Mismatch {
                    epoch: 2,
                    end_offset: 12,
                },
            }),
            Message::AppendResponse(AppendResponse {
                term: 9,
                outcome: AppendOutcome::StaleTerm,
            }),
        ];
        let items: Vec<Item> = messages
            .iter()
            .map(|message| Item::Raft {
                group: group(),
                message: message.clone(),
                payload: Bytes::new(),
            })
            .collect();
        let decoded = roundtrip(&items);
        assert_eq!(decoded.len(), items.len());
        for (message, inbound) in messages.iter().zip(&decoded) {
            let Inbound::Raft(received) = inbound else {
                panic!("expected a Raft message, got {inbound:?}");
            };
            assert_eq!((&received.message, received.group), (message, group()));
        }
    }

    #[test]
    fn appends_hints_and_forwards_survive_encoding() {
        let append = AppendRequest {
            term: 5,
            prev_offset: 10,
            prev_epoch: 4,
            commit_offset: 9,
            batches: Vec::new(),
        };
        let items = [
            Item::Raft {
                group: group(),
                message: Message::AppendRequest(append.clone()),
                payload: payload(),
            },
            Item::Hint {
                group: group(),
                term: 6,
                leader: 2,
            },
            Item::Request {
                correlation: 41,
                body: Bytes::from_static(b"create"),
            },
            Item::Response {
                correlation: 41,
                body: Bytes::from_static(b"done"),
            },
        ];
        let decoded = roundtrip(&items);
        let Inbound::Raft(received) = &decoded[0] else {
            panic!("expected the append request");
        };
        let batches = vec![
            BatchInfo {
                base_offset: 10,
                record_count: 1,
                epoch: 4,
            },
            BatchInfo {
                base_offset: 11,
                record_count: 1,
                epoch: 5,
            },
        ];
        let expected = AppendRequest { batches, ..append };
        assert_eq!(received.message, Message::AppendRequest(expected));
        assert_eq!(received.payload, payload());
        assert_eq!(received.ranges, [0..72, 72..144]);
        assert!(matches!(
            decoded[1],
            Inbound::Hint {
                term: 6,
                leader: 2,
                ..
            }
        ));
        assert!(
            matches!(&decoded[2], Inbound::Request { correlation: 41, body } if body == "create")
        );
        assert!(
            matches!(&decoded[3], Inbound::Response { correlation: 41, body } if body == "done")
        );
    }

    #[test]
    fn decode_rejects_malformed_items_without_panicking() {
        let mut valid = BytesMut::new();
        Item::Raft {
            group: group(),
            message: Message::AppendRequest(AppendRequest {
                term: 5,
                prev_offset: 10,
                prev_epoch: 4,
                commit_offset: 9,
                batches: Vec::new(),
            }),
            payload: payload(),
        }
        .encode(&mut valid);
        for cut in 0..valid.len() {
            assert_eq!(
                decode_items(Bytes::copy_from_slice(&valid[..cut])).is_ok(),
                cut == 0
            );
        }
        let mut corrupt = valid.to_vec();
        *corrupt.last_mut().unwrap() ^= 1;
        assert!(decode_items(Bytes::from(corrupt)).is_err(), "bad CRC");
        let mut framing = valid.to_vec();
        framing[57..61].copy_from_slice(&1_i32.to_be_bytes());
        assert!(
            decode_items(Bytes::from(framing)).is_err(),
            "bad batch length"
        );
        assert!(
            decode_items(Bytes::from_static(&[99])).is_err(),
            "unknown tag"
        );
        let mut flag = BytesMut::new();
        flag.put_u8(VOTE_RESPONSE);
        put_group(&mut flag, group());
        flag.put_i32(1);
        flag.put_u8(2);
        flag.put_u8(0);
        assert!(decode_items(flag.freeze()).is_err(), "flag of 2");
        let mut outcome = BytesMut::new();
        outcome.put_u8(APPEND_RESPONSE);
        put_group(&mut outcome, group());
        outcome.put_i32(1);
        outcome.put_u8(7);
        assert!(decode_items(outcome.freeze()).is_err(), "unknown outcome");
    }

    #[test]
    fn read_frame_bounds_lengths_and_hello_checks_the_cluster() {
        let huge = u32::try_from(MAX_FRAME_BYTES + 1).unwrap().to_be_bytes();
        assert!(read_frame(&mut &huge[..]).is_err());
        let short = [0, 0, 0, 9, 1, 2];
        assert!(read_frame(&mut &short[..]).is_err());
        assert!(read_frame(&mut &[][..]).unwrap().is_none());

        let hello = hello_frame(3, 99);
        let frame = read_frame(&mut &hello[..]).unwrap().unwrap();
        assert_eq!(parse_hello(frame.clone(), 99).unwrap(), 3);
        assert!(parse_hello(frame, 98).is_err(), "another cluster");
    }

    struct Collect(Mutex<mpsc::Sender<(NodeId, Vec<Inbound>)>>);

    impl Inbox for Collect {
        fn deliver(&self, from: NodeId, items: Vec<Inbound>) {
            let _ = lock(&self.0).send((from, items));
        }
    }

    #[test]
    fn peers_deliver_over_tcp_and_strangers_are_refused() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap().to_string();
        let (sender, received) = mpsc::channel();
        let receiving = Transport::start(2, 7, &[(1, "127.0.0.1:1".into())]);
        receiving
            .listen(listener, 2, 7, Arc::new(Collect(Mutex::new(sender))))
            .unwrap();

        let sending = Transport::start(1, 7, &[(2, address.clone())]);
        let hint = |term| Item::Hint {
            group: group(),
            term,
            leader: 1,
        };
        assert!(sending.send(2, hint(1)));
        assert!(!sending.send(3, hint(1)), "no such peer");
        let started = Instant::now();
        let (from, items) = loop {
            if let Ok(got) = received.recv_timeout(Duration::from_millis(100)) {
                break got;
            }
            assert!(
                started.elapsed() < Duration::from_secs(10),
                "nothing arrived"
            );
            sending.send(2, hint(1));
        };
        assert_eq!(from, 1);
        assert!(matches!(items[0], Inbound::Hint { term: 1, .. }));

        let stranger = Transport::start(5, 7, &[(2, address.clone())]);
        let other_cluster = Transport::start(1, 8, &[(2, address)]);
        for _ in 0..5 {
            stranger.send(2, hint(9));
            other_cluster.send(2, hint(9));
            thread::sleep(Duration::from_millis(50));
        }
        while let Ok((_, items)) = received.try_recv() {
            assert!(
                items
                    .iter()
                    .all(|item| matches!(item, Inbound::Hint { term: 1, .. })),
                "a stranger got through: {items:?}"
            );
        }
        for transport in [sending, receiving, stranger, other_cluster] {
            transport.shutdown();
        }
    }
}
