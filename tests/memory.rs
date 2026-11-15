//! What crafted requests make an in-process broker allocate, measured by a counting global
//! allocator. This file holds one test so that nothing else allocates meanwhile.
#![expect(unsafe_code, reason = "a counting global allocator")]

use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use bytes::{BufMut, BytesMut};
use kafka_protocol::messages::{
    ApiVersionsRequest, LeaveGroupRequest, OffsetFetchRequest, ProduceRequest, RequestHeader,
};
use kafka_protocol::protocol::{Encodable, Request, StrBytes};
use redcrab::{Broker, BrokerConfig};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

struct Counting;

static CURRENT: AtomicUsize = AtomicUsize::new(0);
static PEAK: AtomicUsize = AtomicUsize::new(0);

unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let ptr = unsafe { System.alloc(layout) };
        if !ptr.is_null() {
            let now = CURRENT.fetch_add(layout.size(), Ordering::Relaxed) + layout.size();
            PEAK.fetch_max(now, Ordering::Relaxed);
        }
        ptr
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) };
        CURRENT.fetch_sub(layout.size(), Ordering::Relaxed);
    }
}

#[global_allocator]
static ALLOCATOR: Counting = Counting;

const MIB: usize = 1 << 20;

/// A request frame: size prefix, a header of `R` at `version`, then `body`.
fn frame<R: Request>(version: i16, body: &[u8]) -> Vec<u8> {
    let mut frame = BytesMut::new();
    frame.put_i32(0);
    RequestHeader::default()
        .with_request_api_key(R::KEY)
        .with_request_api_version(version)
        .with_correlation_id(1)
        .with_client_id(Some(StrBytes::from_static_str("memory-test")))
        .encode(&mut frame, R::header_version(version))
        .unwrap();
    frame.put_slice(body);
    let len = i32::try_from(frame.len() - 4).unwrap();
    frame[..4].copy_from_slice(&len.to_be_bytes());
    frame.to_vec()
}

/// Bytes allocated at the peak, beyond what was allocated before, while the broker takes `frame`
/// and either answers it or closes the connection; and whether it answered.
async fn peak_while_taking(port: u16, frame: &[u8]) -> (usize, bool) {
    let mut stream = TcpStream::connect(("127.0.0.1", port)).await.unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;
    let before = CURRENT.load(Ordering::Relaxed);
    PEAK.store(before, Ordering::Relaxed);
    let _ = stream.write_all(frame).await;
    let len = tokio::time::timeout(Duration::from_secs(10), stream.read_i32())
        .await
        .expect("the broker must answer or close the connection");
    let peak = PEAK.load(Ordering::Relaxed).saturating_sub(before);
    if let Ok(len) = len {
        let mut reply = vec![0; usize::try_from(len).unwrap()];
        stream.read_exact(&mut reply).await.unwrap();
    }
    (peak, len.is_ok())
}

#[tokio::test(flavor = "multi_thread")]
async fn crafted_requests_cannot_make_the_broker_allocate_much_more_than_their_limit() {
    let dir = tempfile::tempdir().unwrap();
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    listener.set_nonblocking(true).unwrap();
    let port = listener.local_addr().unwrap().port();
    let config = BrokerConfig {
        node_id: 0,
        advertised_host: "127.0.0.1".into(),
        advertised_port: port,
        data_dir: dir.path().to_path_buf(),
        peers: Vec::new(),
    };
    let broker = Broker::start(config, None).await.unwrap();
    let listener = TcpListener::from_std(listener).unwrap();
    tokio::spawn(redcrab::serve(
        listener,
        Arc::clone(&broker),
        std::future::pending(),
    ));

    let huge = 27 * MIB;
    let body = MIB - 64;
    let leave_group = |body: usize| {
        let mut leave = BytesMut::new();
        leave.put_i16(0);
        leave.put_i32(i32::try_from(body - 6).unwrap());
        leave.resize(body, 0xff);
        frame::<LeaveGroupRequest>(3, &leave)
    };
    let uvarint = |out: &mut BytesMut, mut value: usize| {
        while value >= 0x80 {
            out.put_u8(u8::try_from(value & 0x7f).unwrap() | 0x80);
            value >>= 7;
        }
        out.put_u8(u8::try_from(value).unwrap());
    };
    let mut nested = BytesMut::new();
    uvarint(&mut nested, body - 2);
    nested.put_u8(1);
    uvarint(&mut nested, body - 6);
    nested.put_u8(1);
    uvarint(&mut nested, body - 10);
    nested.resize(body, 0);
    let offset_fetch = frame::<OffsetFetchRequest>(8, &nested);

    let mut tagged = BytesMut::new();
    tagged.put_u8(1);
    let members = (body - 8) / 6;
    uvarint(&mut tagged, members + 1);
    for _ in 0..members {
        tagged.put_slice(&[1, 0, 0, 1, 0, 0]);
    }
    tagged.put_u8(0);
    let leave_group_tagged = frame::<LeaveGroupRequest>(5, &tagged);

    let mut produce = BytesMut::new();
    produce.put_slice(&[0, 0xff, 0xff, 0, 0, 0, 0]);
    uvarint(&mut produce, huge);
    produce.resize(huge - 64, 0);
    let produce_huge = frame::<ProduceRequest>(9, &produce);

    for (what, frame, bound, answered) in [
        ("LeaveGroup of 27 MiB", &leave_group(huge), MIB, false),
        ("LeaveGroup of 1 MiB", &leave_group(body), 256 * MIB, false),
        ("OffsetFetch of 1 MiB", &offset_fetch, 256 * MIB, false),
        (
            "valid LeaveGroup of 1 MiB, a tagged field per member",
            &leave_group_tagged,
            160 * MIB,
            true,
        ),
        ("Produce of 27 MiB", &produce_huge, 3 * huge, false),
    ] {
        let (peak, replied) = peak_while_taking(port, frame).await;
        eprintln!("{what}: {} KiB at the peak", peak >> 10);
        assert_eq!(replied, answered, "{what}");
        assert!(peak < bound, "{what} took {peak} bytes");
    }

    let mut client = TcpStream::connect(("127.0.0.1", port)).await.unwrap();
    let versions = frame::<ApiVersionsRequest>(3, &[1, 1, 0]);
    client.write_all(&versions).await.unwrap();
    let len = client.read_i32().await.unwrap();
    assert!(len > 0, "the broker still answers");
}
