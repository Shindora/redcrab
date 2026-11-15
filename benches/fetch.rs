//! Consumer throughput load generator, run with `cargo bench --bench fetch`.
//!
//! Starts the release broker as a child process on localhost, produces 1 KiB records in
//! producer-sized batches across 100 partitions, then reads everything back the way the Java
//! consumer does: sessionless full fetch requests over every partition, `max_bytes` 16 MiB,
//! `partition_max_bytes` 1 MiB, `min_bytes` 1, `max_wait_ms` 500, one request in flight, offsets
//! advanced from the returned batches.
//!
//! Knobs (environment):
//! - `FETCH_BENCH_MIB`: data to produce (3072).
//! - `FETCH_BENCH_PASSES`: read passes (3).
//! - `FETCH_BENCH_RECORDS_PER_BATCH`: 126, about the Java producer's 128 KiB `batch.size`.
//! - `FETCH_BENCH_PARTITIONS`: 100.
//! - `FETCH_BENCH_RCVBUF`: the consumer socket's receive buffer, 65536 as the Java consumer's
//!   `receive.buffer.bytes` default; 0 leaves the OS default.
//! - `FETCH_BENCH_DIR`: keep the data dir there, and skip producing when it already holds the topic.
//! - `FETCH_BENCH_BROKER`: run another broker binary than this crate's.
//! - `FETCH_BENCH_LIVE_MIBPS`: keep producing at this rate while passes after the first re-read the
//!   first pass's range, as when a consumer drains a backlog under live traffic.

use std::env;
use std::net::TcpListener as StdTcpListener;
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use bytes::{BufMut, Bytes, BytesMut};
use kafka_protocol::indexmap::IndexMap;
use kafka_protocol::messages::create_topics_request::CreatableTopic;
use kafka_protocol::messages::fetch_request::{FetchPartition, FetchTopic};
use kafka_protocol::messages::metadata_request::MetadataRequestTopic;
use kafka_protocol::messages::produce_request::{PartitionProduceData, TopicProduceData};
use kafka_protocol::messages::{
    CreateTopicsRequest, FetchRequest, MetadataRequest, ProduceRequest, RequestHeader,
    ResponseHeader, TopicName,
};
use kafka_protocol::protocol::{Decodable, Encodable, HeaderVersion, Request, StrBytes};
use kafka_protocol::records::{
    Compression, Record, RecordBatchEncoder, RecordEncodeOptions, TimestampType,
};
use tempfile::TempDir;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpSocket, TcpStream};
use tokio::time::MissedTickBehavior;
use uuid::Uuid;

const TOPIC: &str = "fetch-bench";
const RECORD_BYTES: usize = 1024;
const FETCH_MAX_BYTES: usize = 16 << 20;
const PARTITION_MAX_BYTES: usize = 1 << 20;
const MAX_WAIT_MS: i32 = 500;
const FETCH_VERSION: i16 = 17;
const PRODUCE_VERSION: i16 = 13;
const PRODUCERS: usize = 4;
const BATCHES_PER_PRODUCE: usize = 8;
const TOPIC_ALREADY_EXISTS: i16 = 36;
const BATCH_HEADER_LEN: usize = 61;

struct Settings {
    total_bytes: usize,
    passes: usize,
    records_per_batch: usize,
    partitions: usize,
    receive_buffer: Option<u32>,
    keep_dir: Option<PathBuf>,
    live_bytes_per_second: usize,
}

impl Settings {
    fn from_env() -> Settings {
        let number = |name: &str, default: usize| {
            env::var(name)
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(default)
        };
        Settings {
            total_bytes: number("FETCH_BENCH_MIB", 3072) << 20,
            passes: number("FETCH_BENCH_PASSES", 3),
            records_per_batch: number("FETCH_BENCH_RECORDS_PER_BATCH", 126),
            partitions: number("FETCH_BENCH_PARTITIONS", 100),
            receive_buffer: u32::try_from(number("FETCH_BENCH_RCVBUF", 65536))
                .ok()
                .filter(|&bytes| bytes > 0),
            keep_dir: env::var_os("FETCH_BENCH_DIR").map(PathBuf::from),
            live_bytes_per_second: number("FETCH_BENCH_LIVE_MIBPS", 0) << 20,
        }
    }
}

struct BrokerProcess {
    child: Child,
    port: u16,
    _temp: Option<TempDir>,
}

impl BrokerProcess {
    fn start(keep_dir: Option<&PathBuf>) -> BrokerProcess {
        let temp = keep_dir.is_none().then(|| tempfile::tempdir().unwrap());
        let dir = keep_dir
            .cloned()
            .unwrap_or_else(|| temp.as_ref().unwrap().path().to_path_buf());
        let port = StdTcpListener::bind("127.0.0.1:0")
            .unwrap()
            .local_addr()
            .unwrap()
            .port();
        let broker = env::var_os("FETCH_BENCH_BROKER").map_or_else(
            || PathBuf::from(env!("CARGO_BIN_EXE_redcrab")),
            PathBuf::from,
        );
        let child = Command::new(broker)
            .arg("--listen")
            .arg(format!("127.0.0.1:{port}"))
            .arg("--data-dir")
            .arg(&dir)
            .env("RUST_LOG", "warn")
            .stdout(Stdio::null())
            .spawn()
            .unwrap();
        println!(
            "broker pid {} on port {port}, data in {}",
            child.id(),
            dir.display()
        );
        BrokerProcess {
            child,
            port,
            _temp: temp,
        }
    }
}

impl Drop for BrokerProcess {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

struct Client {
    stream: TcpStream,
    correlation_id: i32,
    buffer: BytesMut,
}

impl Client {
    /// Connects with the OS default receive buffer, or a fixed one as the Java client sets from
    /// `receive.buffer.bytes`.
    async fn connect(port: u16, receive_buffer: Option<u32>) -> Client {
        let started = Instant::now();
        loop {
            let socket = TcpSocket::new_v4().unwrap();
            if let Some(bytes) = receive_buffer {
                socket.set_recv_buffer_size(bytes).unwrap();
            }
            match socket.connect(([127, 0, 0, 1], port).into()).await {
                Ok(stream) => {
                    stream.set_nodelay(true).unwrap();
                    return Client {
                        stream,
                        correlation_id: 0,
                        buffer: BytesMut::new(),
                    };
                }
                Err(e) if started.elapsed() < Duration::from_secs(10) => {
                    let _ = e;
                    tokio::time::sleep(Duration::from_millis(20)).await;
                }
                Err(e) => panic!("broker did not start listening: {e}"),
            }
        }
    }

    async fn call<R: Request>(&mut self, request: &R, version: i16) -> R::Response {
        self.correlation_id += 1;
        let mut frame = BytesMut::new();
        frame.put_i32(0);
        RequestHeader::default()
            .with_request_api_key(R::KEY)
            .with_request_api_version(version)
            .with_correlation_id(self.correlation_id)
            .with_client_id(Some(StrBytes::from_static_str("fetch-bench")))
            .encode(&mut frame, R::header_version(version))
            .unwrap();
        request.encode(&mut frame, version).unwrap();
        let len = i32::try_from(frame.len() - 4).unwrap();
        frame[..4].copy_from_slice(&len.to_be_bytes());
        self.stream.write_all(&frame).await.unwrap();

        let len = usize::try_from(self.stream.read_i32().await.unwrap()).unwrap();
        self.buffer.clear();
        self.buffer.reserve(len);
        while self.buffer.len() < len {
            let wanted = (len - self.buffer.len()) as u64;
            let read = (&mut self.stream)
                .take(wanted)
                .read_buf(&mut self.buffer)
                .await
                .unwrap();
            assert_ne!(read, 0, "broker closed the connection mid-response");
        }
        let mut reply = self.buffer.split().freeze();
        let header =
            ResponseHeader::decode(&mut reply, R::Response::header_version(version)).unwrap();
        assert_eq!(header.correlation_id, self.correlation_id);
        R::Response::decode(&mut reply, version).unwrap()
    }
}

fn record_batch(records: usize) -> Bytes {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis();
    let timestamp = i64::try_from(now).unwrap();
    let value: Bytes = (0..RECORD_BYTES)
        .map(|at| u8::try_from(at % 251).unwrap())
        .collect();
    let records: Vec<Record> = (0..records)
        .map(|i| {
            let i = i32::try_from(i).unwrap();
            Record {
                transactional: false,
                control: false,
                delete_horizon: false,
                partition_leader_epoch: -1,
                producer_id: -1,
                producer_epoch: -1,
                timestamp_type: TimestampType::Creation,
                offset: i64::from(i),
                sequence: i,
                timestamp,
                key: None,
                value: Some(value.clone()),
                headers: IndexMap::new(),
            }
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

/// Creates the topic, or finds it when a kept data dir already holds it; returns its id and
/// whether it was just created.
async fn ensure_topic(client: &mut Client, partitions: usize) -> (Uuid, bool) {
    let name = TopicName(StrBytes::from_static_str(TOPIC));
    let request = CreateTopicsRequest::default()
        .with_topics(vec![
            CreatableTopic::default()
                .with_name(name.clone())
                .with_num_partitions(i32::try_from(partitions).unwrap())
                .with_replication_factor(1),
        ])
        .with_timeout_ms(30_000);
    let created = &client.call(&request, 7).await.topics[0];
    assert!(
        matches!(created.error_code, 0 | TOPIC_ALREADY_EXISTS),
        "create topic failed: {}",
        created.error_code
    );
    let metadata = client
        .call(
            &MetadataRequest::default().with_topics(Some(vec![
                MetadataRequestTopic::default().with_name(Some(name)),
            ])),
            12,
        )
        .await;
    let topic = &metadata.topics[0];
    assert_eq!(topic.error_code, 0);
    assert_eq!(
        topic.partitions.len(),
        partitions,
        "kept topic has another layout"
    );
    (topic.topic_id, created.error_code == 0)
}

async fn produce(port: u16, topic_id: Uuid, settings: &Settings) {
    let batch = record_batch(settings.records_per_batch);
    let per_partition = settings
        .total_bytes
        .div_ceil(settings.partitions * batch.len());
    println!(
        "producing {per_partition} batches of {} bytes to each of {} partitions",
        batch.len(),
        settings.partitions
    );
    let started = Instant::now();
    let mut producers = Vec::new();
    for producer in 0..PRODUCERS {
        let partitions: Vec<i32> = (0..settings.partitions)
            .filter(|p| p % PRODUCERS == producer)
            .map(|p| i32::try_from(p).unwrap())
            .collect();
        let batch = batch.clone();
        producers.push(tokio::spawn(async move {
            let mut client = Client::connect(port, None).await;
            let mut sent = 0;
            while sent < per_partition {
                let count = BATCHES_PER_PRODUCE.min(per_partition - sent);
                let records = Bytes::from(batch.repeat(count));
                let data = partitions
                    .iter()
                    .map(|&p| {
                        PartitionProduceData::default()
                            .with_index(p)
                            .with_records(Some(records.clone()))
                    })
                    .collect();
                let request = ProduceRequest::default()
                    .with_acks(1)
                    .with_timeout_ms(30_000)
                    .with_topic_data(vec![
                        TopicProduceData::default()
                            .with_topic_id(topic_id)
                            .with_partition_data(data),
                    ]);
                let response = client.call(&request, PRODUCE_VERSION).await;
                for partition in &response.responses[0].partition_responses {
                    assert_eq!(partition.error_code, 0, "produce failed");
                }
                sent += count;
            }
        }));
    }
    for producer in producers {
        producer.await.unwrap();
    }
    let bytes = per_partition * settings.partitions * batch.len();
    println!(
        "produced {:.0} MiB in {:.1} s",
        mib(bytes),
        started.elapsed().as_secs_f64()
    );
}

/// Produces one batch to every partition per request at `bytes_per_second` until `stop` is set.
async fn produce_live(
    port: u16,
    topic_id: Uuid,
    settings: &Settings,
    stop: Arc<AtomicBool>,
) -> usize {
    let batch = record_batch(settings.records_per_batch);
    let per_request = batch.len() * settings.partitions;
    let period = Duration::from_micros(
        u64::try_from(per_request * 1_000_000 / settings.live_bytes_per_second).unwrap(),
    );
    let mut ticks = tokio::time::interval(period);
    ticks.set_missed_tick_behavior(MissedTickBehavior::Delay);
    let mut client = Client::connect(port, None).await;
    let mut produced = 0;
    while !stop.load(Ordering::Relaxed) {
        ticks.tick().await;
        let data = (0..settings.partitions)
            .map(|p| {
                PartitionProduceData::default()
                    .with_index(i32::try_from(p).unwrap())
                    .with_records(Some(batch.clone()))
            })
            .collect();
        let request = ProduceRequest::default()
            .with_acks(1)
            .with_timeout_ms(30_000)
            .with_topic_data(vec![
                TopicProduceData::default()
                    .with_topic_id(topic_id)
                    .with_partition_data(data),
            ]);
        let response = client.call(&request, PRODUCE_VERSION).await;
        assert!(
            response.responses[0]
                .partition_responses
                .iter()
                .all(|p| p.error_code == 0)
        );
        produced += per_request;
    }
    produced
}

/// A full fetch request over every partition, or only over those still short of `until`.
fn fetch_request(topic_id: Uuid, offsets: &[i64], until: Option<&[i64]>) -> FetchRequest {
    let partitions = (0..)
        .zip(offsets)
        .zip(0..)
        .filter(|&(_, at): &(_, usize)| until.is_none_or(|until| offsets[at] < until[at]))
        .map(|((index, &offset), _)| {
            FetchPartition::default()
                .with_partition(index)
                .with_fetch_offset(offset)
                .with_partition_max_bytes(i32::try_from(PARTITION_MAX_BYTES).unwrap())
        })
        .collect();
    FetchRequest::default()
        .with_max_wait_ms(MAX_WAIT_MS)
        .with_min_bytes(1)
        .with_max_bytes(i32::try_from(FETCH_MAX_BYTES).unwrap())
        .with_topics(vec![
            FetchTopic::default()
                .with_topic_id(topic_id)
                .with_partitions(partitions),
        ])
}

/// `bytes` in MiB, to the KiB.
fn mib(bytes: usize) -> f64 {
    f64::from(u32::try_from(bytes >> 10).unwrap()) / 1024.0
}

fn be_i32(bytes: &[u8], at: usize) -> i32 {
    i32::from_be_bytes(bytes[at..at + 4].try_into().unwrap())
}

/// Walks whole batches starting at `next`, returning the offset after them and the record count.
fn advance(records: &[u8], mut next: i64) -> (i64, u64) {
    let (mut at, mut count) = (0, 0);
    while at < records.len() {
        let batch = &records[at..];
        assert!(batch.len() >= BATCH_HEADER_LEN, "partial batch header");
        let base = i64::from_be_bytes(batch[..8].try_into().unwrap());
        let len = usize::try_from(be_i32(batch, 8)).unwrap() + 12;
        let last = base + i64::from(be_i32(batch, 23));
        assert!(len <= batch.len(), "partial batch");
        assert!(
            base <= next && next <= last,
            "batch {base}..={last} skips offset {next}"
        );
        next = last + 1;
        count += u64::try_from(be_i32(batch, 57)).unwrap();
        at += len;
    }
    (next, count)
}

struct Pass {
    bytes: usize,
    records: u64,
    fetches: usize,
    elapsed: Duration,
    slowest: Duration,
}

/// Reads every partition from offset 0 up to `until`, or up to its high watermark.
async fn read_everything(
    port: u16,
    topic_id: Uuid,
    settings: &Settings,
    until: Option<&[i64]>,
) -> (Pass, Vec<i64>) {
    let partitions = settings.partitions;
    let mut client = Client::connect(port, settings.receive_buffer).await;
    let mut offsets = vec![0; partitions];
    let mut ends = until.map_or_else(|| vec![-1; partitions], <[i64]>::to_vec);
    let (mut bytes, mut records, mut fetches) = (0, 0, 0);
    let mut slowest = Duration::ZERO;
    let started = Instant::now();
    while fetches == 0 || offsets.iter().zip(&ends).any(|(offset, end)| offset < end) {
        let sent = Instant::now();
        let response = client
            .call(&fetch_request(topic_id, &offsets, until), FETCH_VERSION)
            .await;
        slowest = slowest.max(sent.elapsed());
        fetches += 1;
        assert_eq!(response.error_code, 0);
        let (mut response_bytes, mut first) = (0, true);
        for partition in &response.responses[0].partitions {
            assert_eq!(partition.error_code, 0, "fetch failed");
            let index = usize::try_from(partition.partition_index).unwrap();
            if until.is_none() {
                ends[index] = partition.high_watermark;
            }
            let Some(data) = partition.records.as_ref().filter(|r| !r.is_empty()) else {
                continue;
            };
            if offsets[index] >= ends[index] {
                continue;
            }
            assert!(
                first || data.len() <= PARTITION_MAX_BYTES,
                "partition {index} exceeded partition_max_bytes"
            );
            first = false;
            let (next, count) = advance(data, offsets[index]);
            offsets[index] = next;
            response_bytes += data.len();
            records += count;
        }
        assert!(
            response_bytes <= FETCH_MAX_BYTES.max(PARTITION_MAX_BYTES),
            "response of {response_bytes} bytes exceeded max_bytes"
        );
        bytes += response_bytes;
    }
    let pass = Pass {
        bytes,
        records,
        fetches,
        elapsed: started.elapsed(),
        slowest,
    };
    (pass, offsets)
}

async fn run(settings: &Arc<Settings>, port: u16) {
    let mut admin = Client::connect(port, None).await;
    let (topic_id, created) = ensure_topic(&mut admin, settings.partitions).await;
    if created {
        produce(port, topic_id, settings).await;
    } else {
        println!("reusing the topic already in the data dir");
    }
    let (mut rates, mut until, mut live) = (Vec::new(), None, None);
    for pass in 1..=settings.passes {
        let (result, ends) = read_everything(port, topic_id, settings, until.as_deref()).await;
        if settings.live_bytes_per_second > 0 && live.is_none() {
            let stop = Arc::new(AtomicBool::new(false));
            let producing = tokio::spawn({
                let (stop, settings) = (Arc::clone(&stop), Arc::clone(settings));
                async move { produce_live(port, topic_id, &settings, stop).await }
            });
            live = Some((stop, producing));
            until = Some(ends);
        }
        let seconds = result.elapsed.as_secs_f64();
        let mib = mib(result.bytes);
        let rate = mib / seconds;
        let fetches = f64::from(u32::try_from(result.fetches).unwrap());
        println!(
            "pass {pass}: {mib:.0} MiB, {} records in {seconds:.2} s = {rate:.0} MiB/s; {} fetches \
             ({:.0} per GiB), {:.2} MiB and {:.2} ms per fetch, slowest {:.1} ms",
            result.records,
            result.fetches,
            fetches / (mib / 1024.0),
            mib / fetches,
            seconds * 1000.0 / fetches,
            result.slowest.as_secs_f64() * 1000.0,
        );
        if pass > 1 || live.is_none() {
            rates.push(rate);
        }
    }
    if let Some((stop, producing)) = live {
        stop.store(true, Ordering::Relaxed);
        let produced = producing.await.unwrap();
        println!("produced {:.0} MiB while reading", mib(produced));
    }
    rates.sort_by(f64::total_cmp);
    if let Some(median) = rates.get(rates.len() / 2) {
        println!("median fetch throughput: {median:.0} MiB/s");
    }
}

fn main() {
    let settings = Arc::new(Settings::from_env());
    let broker = BrokerProcess::start(settings.keep_dir.as_ref());
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap()
        .block_on(run(&settings, broker.port));
}
