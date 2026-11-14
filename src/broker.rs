//! Broker state: the topic registry persisted in the data dir, partition logs and the group-commit write path.

use std::collections::HashMap;
use std::fs::{self, File};
use std::io::{self, Write};
use std::mem;
use std::ops::Range;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError, RwLock};
use std::time::{SystemTime, UNIX_EPOCH};

use kafka_protocol::ResponseError;
use tokio::sync::{oneshot, watch};
use tracing::{debug, error, info};
use uuid::Uuid;

use crate::group::Coordinator;
use crate::storage::{Log, LogConfig, StorageError};

const TOPICS_FILE: &str = "topics";
const GROUPS_DIR: &str = "groups";

/// Static settings of one broker process.
#[derive(Debug, Clone)]
pub struct BrokerConfig {
    /// Node id reported in metadata.
    pub node_id: i32,
    /// Host clients are told to connect to.
    pub advertised_host: String,
    /// Port clients are told to connect to.
    pub advertised_port: u16,
    /// Directory holding topic metadata and partition logs.
    pub data_dir: PathBuf,
}

/// Shared state of a single-node broker.
pub struct Broker {
    /// Settings this broker was started with.
    pub config: BrokerConfig,
    topics: RwLock<Topics>,
    admin: tokio::sync::Mutex<()>,
    // ponytail: one broker-wide flush signal wakes every waiting fetch; per-partition wakeups once long-polls are many.
    flushes: Arc<watch::Sender<u64>>,
    next_producer_id: AtomicI64,
    groups: Coordinator,
}

#[derive(Default)]
struct Topics {
    by_name: HashMap<String, Arc<Topic>>,
    by_id: HashMap<Uuid, Arc<Topic>>,
}

/// A topic and its partitions.
pub struct Topic {
    /// Topic name.
    pub name: String,
    /// Stable topic id.
    pub id: Uuid,
    /// Partitions indexed by partition number.
    pub partitions: Vec<Arc<Partition>>,
}

impl Topic {
    /// Partition `index`, if it exists.
    #[must_use]
    pub fn partition(&self, index: i32) -> Option<&Arc<Partition>> {
        usize::try_from(index)
            .ok()
            .and_then(|i| self.partitions.get(i))
    }
}

/// One partition: its log plus the queue of appends waiting for the next group commit.
pub struct Partition {
    /// Only locked from blocking threads; tokio's mutex is FIFO-fair, so fetch reads are not starved
    /// by back-to-back commit rounds holding it across fsync.
    log: tokio::sync::Mutex<Log>,
    queue: Mutex<CommitQueue>,
    high_watermark: AtomicI64,
    log_start: AtomicI64,
    flushes: Arc<watch::Sender<u64>>,
}

#[derive(Default)]
struct CommitQueue {
    pending: Vec<Append>,
    running: bool,
}

struct Append {
    records: Vec<u8>,
    batches: Vec<Range<usize>>,
    reply: oneshot::Sender<Result<i64, i16>>,
}

/// Outcome of an append: the first assigned offset, or a Kafka error code.
pub type AppendResult = oneshot::Receiver<Result<i64, i16>>;

impl Broker {
    /// Loads topic metadata from `config.data_dir` and opens every partition log. Blocking.
    ///
    /// # Errors
    /// Fails when the data dir cannot be read or a log cannot be opened.
    pub fn open(config: BrokerConfig) -> Result<Broker, StorageError> {
        fs::create_dir_all(&config.data_dir)?;
        let flushes = Arc::new(watch::channel(0).0);
        let mut topics = Topics::default();
        for (id, partitions, name) in load_topics(&config.data_dir)? {
            let topic = open_topic(&config.data_dir, name, id, partitions, &flushes)?;
            info!(topic = %topic.name, id = %topic.id, partitions, "loaded topic");
            topics.insert(Arc::new(topic));
        }
        let groups = Coordinator::open(config.data_dir.join(GROUPS_DIR))?;
        let millis = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(0, |d| d.as_millis());
        Ok(Broker {
            config,
            topics: RwLock::new(topics),
            admin: tokio::sync::Mutex::new(()),
            flushes,
            // ponytail: producer ids are seeded from wall-clock millis, not persisted; a clock step back across restart can reuse ids.
            next_producer_id: AtomicI64::new(i64::try_from(millis).unwrap_or(0)),
            groups,
        })
    }

    /// The consumer-group coordinator; this node coordinates every group.
    pub(crate) fn groups(&self) -> &Coordinator {
        &self.groups
    }

    /// Topic by name.
    pub fn topic(&self, name: &str) -> Option<Arc<Topic>> {
        read(&self.topics).by_name.get(name).cloned()
    }

    /// Topic by id.
    pub fn topic_by_id(&self, id: Uuid) -> Option<Arc<Topic>> {
        read(&self.topics).by_id.get(&id).cloned()
    }

    /// Every topic, sorted by name.
    pub fn topics(&self) -> Vec<Arc<Topic>> {
        let mut all: Vec<_> = read(&self.topics).by_name.values().cloned().collect();
        all.sort_by(|a, b| a.name.cmp(&b.name));
        all
    }

    /// Receiver that changes after every successful flush on any partition.
    pub fn flushes(&self) -> watch::Receiver<u64> {
        self.flushes.subscribe()
    }

    /// Hands out a producer id never returned before by this process.
    pub fn next_producer_id(&self) -> i64 {
        self.next_producer_id.fetch_add(1, Ordering::Relaxed)
    }

    /// Creates a topic, its partition logs and persists the registry before returning.
    ///
    /// # Errors
    /// Returns the Kafka error code to report for this topic.
    pub async fn create_topic(&self, name: String, partitions: i32) -> Result<Arc<Topic>, i16> {
        let _admin = self.admin.lock().await;
        if self.topic(&name).is_some() {
            return Err(ResponseError::TopicAlreadyExists.code());
        }
        let dir = self.config.data_dir.clone();
        let flushes = Arc::clone(&self.flushes);
        let mut listing = self.listing();
        let topic = tokio::task::spawn_blocking(move || {
            let topic = open_topic(&dir, name, Uuid::new_v4(), partitions, &flushes)?;
            listing.push((topic.id, partitions, topic.name.clone()));
            save_topics(&dir, &listing)?;
            Ok::<_, StorageError>(Arc::new(topic))
        })
        .await
        .map_err(|e| storage_failure(&e))?
        .map_err(|e| storage_failure(&e))?;
        write(&self.topics).insert(Arc::clone(&topic));
        info!(topic = %topic.name, id = %topic.id, partitions, "created topic");
        Ok(topic)
    }

    /// Deletes a topic from the registry and removes its data.
    ///
    /// # Errors
    /// Returns the Kafka error code to report for this topic.
    pub async fn delete_topic(&self, topic: &Topic) -> Result<(), i16> {
        let _admin = self.admin.lock().await;
        if self.topic_by_id(topic.id).is_none() {
            return Err(ResponseError::UnknownTopicId.code());
        }
        let dir = self.config.data_dir.clone();
        let id = topic.id;
        let listing: Vec<_> = self.listing().into_iter().filter(|t| t.0 != id).collect();
        tokio::task::spawn_blocking(move || save_topics(&dir, &listing))
            .await
            .map_err(|e| storage_failure(&e))?
            .map_err(|e| storage_failure(&e))?;
        {
            let mut topics = write(&self.topics);
            topics.by_id.remove(&id);
            topics.by_name.remove(&topic.name);
        }
        let topic_dir = self.config.data_dir.join(id.to_string());
        match tokio::task::spawn_blocking(move || fs::remove_dir_all(topic_dir)).await {
            Ok(Ok(())) => {}
            Ok(Err(e)) => error!(%id, "removing topic data failed: {e}"),
            Err(e) => error!(%id, "removing topic data failed: {e}"),
        }
        info!(topic = %topic.name, %id, "deleted topic");
        Ok(())
    }

    fn listing(&self) -> Vec<(Uuid, i32, String)> {
        read(&self.topics)
            .by_name
            .values()
            .map(|t| {
                (
                    t.id,
                    i32::try_from(t.partitions.len()).unwrap_or(i32::MAX),
                    t.name.clone(),
                )
            })
            .collect()
    }
}

impl Topics {
    fn insert(&mut self, topic: Arc<Topic>) {
        self.by_id.insert(topic.id, Arc::clone(&topic));
        self.by_name.insert(topic.name.clone(), topic);
    }
}

impl Partition {
    fn new(log: Log, flushes: Arc<watch::Sender<u64>>) -> Partition {
        Partition {
            high_watermark: AtomicI64::new(log.end_offset()),
            log_start: AtomicI64::new(log.start_offset()),
            log: tokio::sync::Mutex::new(log),
            queue: Mutex::new(CommitQueue::default()),
            flushes,
        }
    }

    /// Offset after the last flushed batch; consumers never see past it.
    pub fn high_watermark(&self) -> i64 {
        self.high_watermark.load(Ordering::Acquire)
    }

    /// First offset in the log.
    pub fn log_start_offset(&self) -> i64 {
        self.log_start.load(Ordering::Acquire)
    }

    /// Queues `records` (already split into `batches`) for the next group commit. The receiver resolves
    /// with the first assigned offset once the data is flushed. Appends queued while a flush is in flight
    /// are written and flushed together by the next round.
    pub fn append(self: &Arc<Self>, records: Vec<u8>, batches: Vec<Range<usize>>) -> AppendResult {
        let (reply, result) = oneshot::channel();
        let start = {
            let mut queue = lock(&self.queue);
            queue.pending.push(Append {
                records,
                batches,
                reply,
            });
            !mem::replace(&mut queue.running, true)
        };
        if start {
            let partition = Arc::clone(self);
            tokio::task::spawn_blocking(move || partition.commit_rounds());
        }
        result
    }

    fn commit_rounds(&self) {
        loop {
            let mut group = {
                let mut queue = lock(&self.queue);
                if queue.pending.is_empty() {
                    queue.running = false;
                    return;
                }
                mem::take(&mut queue.pending)
            };
            let mut log = self.log.blocking_lock();
            let results: Vec<_> = group.iter_mut().map(|a| append_all(&mut log, a)).collect();
            let flushed = log.flush();
            let end = log.end_offset();
            drop(log);
            match flushed {
                Ok(()) => {
                    self.high_watermark.store(end, Ordering::Release);
                    self.flushes.send_modify(|n| *n += 1);
                    debug!(appends = group.len(), end, "group commit flushed");
                    for (append, result) in group.into_iter().zip(results) {
                        let _ = append.reply.send(result);
                    }
                }
                Err(e) => {
                    error!("flush failed: {e}");
                    let code = error_code(&e);
                    for append in group {
                        let _ = append.reply.send(Err(code));
                    }
                }
            }
        }
    }

    /// Whole batches from `offset` below the high watermark, up to `max_bytes` but at least one. Blocking.
    ///
    /// # Errors
    /// Returns the Kafka error code for an out-of-range offset or a storage failure.
    pub fn read(&self, offset: i64, max_bytes: usize) -> Result<Vec<u8>, i16> {
        let log = self.log.blocking_lock();
        let high_watermark = self.high_watermark();
        let mut data = log.read(offset, max_bytes).map_err(|e| error_code(&e))?;
        data.truncate(batches_below(&data, high_watermark));
        Ok(data)
    }

    /// Offset of the first batch at or after `timestamp_ms`. Blocking.
    pub fn offset_for_timestamp(&self, timestamp_ms: i64) -> Option<i64> {
        let offset = self
            .log
            .blocking_lock()
            .offset_for_timestamp(timestamp_ms)?;
        (offset < self.high_watermark()).then_some(offset)
    }
}

fn append_all(log: &mut Log, append: &mut Append) -> Result<i64, i16> {
    let mut first = None;
    for range in &append.batches {
        let base = log
            .append(&mut append.records[range.clone()])
            .map_err(|e| error_code(&e))?;
        first.get_or_insert(base);
    }
    first.ok_or(ResponseError::CorruptMessage.code())
}

/// Length of the leading whole batches in `data` whose base offset is below `end`.
fn batches_below(data: &[u8], end: i64) -> usize {
    let mut pos = 0;
    while let Some(header) = data.get(pos..pos + 12) {
        let base = i64::from_be_bytes(header[..8].try_into().unwrap_or_default());
        let len = i32::from_be_bytes(header[8..12].try_into().unwrap_or_default());
        let next = usize::try_from(len).map_or(usize::MAX, |len| pos + 12 + len);
        if base >= end || next > data.len() {
            break;
        }
        pos = next;
    }
    pos
}

/// Kafka error code for a storage failure.
fn error_code(error: &StorageError) -> i16 {
    match error {
        StorageError::Io(_) => ResponseError::KafkaStorageError.code(),
        StorageError::InvalidBatch(_) => ResponseError::CorruptMessage.code(),
        StorageError::OffsetOutOfRange { .. } => ResponseError::OffsetOutOfRange.code(),
    }
}

fn storage_failure(error: &dyn std::fmt::Display) -> i16 {
    error!("storage operation failed: {error}");
    ResponseError::KafkaStorageError.code()
}

fn open_topic(
    data_dir: &Path,
    name: String,
    id: Uuid,
    partitions: i32,
    flushes: &Arc<watch::Sender<u64>>,
) -> Result<Topic, StorageError> {
    let partitions = (0..partitions)
        .map(|p| {
            let log = Log::open(
                &data_dir.join(id.to_string()).join(p.to_string()),
                LogConfig::default(),
            )?;
            Ok(Arc::new(Partition::new(log, Arc::clone(flushes))))
        })
        .collect::<Result<_, StorageError>>()?;
    Ok(Topic {
        name,
        id,
        partitions,
    })
}

fn load_topics(data_dir: &Path) -> io::Result<Vec<(Uuid, i32, String)>> {
    let text = match fs::read_to_string(data_dir.join(TOPICS_FILE)) {
        Ok(text) => text,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(e) => return Err(e),
    };
    text.lines()
        .map(|line| {
            let mut fields = line.splitn(3, '\t');
            let id = fields.next().and_then(|f| Uuid::parse_str(f).ok());
            let partitions = fields.next().and_then(|f| f.parse().ok());
            let name = fields.next().map(str::to_owned);
            match (id, partitions, name) {
                (Some(id), Some(partitions), Some(name)) => Ok((id, partitions, name)),
                _ => Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("bad line in topics file: {line:?}"),
                )),
            }
        })
        .collect()
}

fn save_topics(data_dir: &Path, topics: &[(Uuid, i32, String)]) -> io::Result<()> {
    let mut text = Vec::new();
    for (id, partitions, name) in topics {
        writeln!(text, "{id}\t{partitions}\t{name}")?;
    }
    replace_file(data_dir, TOPICS_FILE, &text)
}

/// Durably replaces `dir/name` with `contents`: written to a temp file, fsynced, renamed over the
/// old file and the directory fsynced, so a crash leaves either the old or the new contents.
pub(crate) fn replace_file(dir: &Path, name: &str, contents: &[u8]) -> io::Result<()> {
    let tmp = dir.join(format!("{name}.tmp"));
    let mut file = File::create(&tmp)?;
    file.write_all(contents)?;
    file.sync_all()?;
    fs::rename(&tmp, dir.join(name))?;
    File::open(dir)?.sync_all()
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

fn read<T>(lock: &RwLock<T>) -> std::sync::RwLockReadGuard<'_, T> {
    lock.read().unwrap_or_else(PoisonError::into_inner)
}

fn write<T>(lock: &RwLock<T>) -> std::sync::RwLockWriteGuard<'_, T> {
    lock.write().unwrap_or_else(PoisonError::into_inner)
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use crate::api::tests::batch;

    pub(crate) fn config(dir: &Path) -> BrokerConfig {
        BrokerConfig {
            node_id: 0,
            advertised_host: "127.0.0.1".into(),
            advertised_port: 9092,
            data_dir: dir.to_path_buf(),
        }
    }

    async fn read(partition: &Arc<Partition>, offset: i64, max_bytes: usize) -> Vec<u8> {
        let partition = Arc::clone(partition);
        tokio::task::spawn_blocking(move || partition.read(offset, max_bytes).unwrap())
            .await
            .unwrap()
    }

    fn whole(records: &[u8]) -> (Vec<u8>, Vec<Range<usize>>) {
        (
            records.to_vec(),
            std::iter::once(0..records.len()).collect(),
        )
    }

    #[tokio::test]
    async fn group_commit_flushes_queued_appends_together() {
        let dir = tempfile::tempdir().unwrap();
        let flushes = Arc::new(watch::channel(0).0);
        let partition = Arc::new(Partition::new(
            Log::open(dir.path(), LogConfig::default()).unwrap(),
            Arc::clone(&flushes),
        ));
        let (one, two) = (batch(&["a"]), batch(&["b", "c"]));

        let flush_in_flight = partition.log.try_lock().unwrap();
        let results: Vec<_> = [&one, &two, &one, &two]
            .into_iter()
            .map(|records| {
                let (records, batches) = whole(records);
                partition.append(records, batches)
            })
            .collect();
        assert_eq!(
            partition.high_watermark(),
            0,
            "nothing is visible before the flush"
        );
        drop(flush_in_flight);

        let mut offsets = Vec::new();
        for result in results {
            offsets.push(result.await.unwrap().unwrap());
        }
        assert_eq!(offsets, [0, 1, 3, 4]);
        assert_eq!(partition.high_watermark(), 6);
        assert!(
            *flushes.borrow() <= 2,
            "4 appends queued behind one busy round took {} flushes",
            *flushes.borrow()
        );
        assert_eq!(
            read(&partition, 0, usize::MAX).await.len(),
            2 * (one.len() + two.len())
        );
    }

    #[tokio::test]
    async fn group_commit_assigns_offsets_to_multi_batch_appends() {
        let dir = tempfile::tempdir().unwrap();
        let partition = Arc::new(Partition::new(
            Log::open(dir.path(), LogConfig::default()).unwrap(),
            Arc::new(watch::channel(0).0),
        ));
        let (one, two) = (batch(&["a"]), batch(&["b", "c"]));
        let records = [one.clone(), two.clone()].concat();
        let first = partition.append(
            records,
            vec![0..one.len(), one.len()..one.len() + two.len()],
        );
        let (records, batches) = whole(&one);
        let second = partition.append(records, batches);
        assert_eq!(first.await.unwrap(), Ok(0));
        assert_eq!(second.await.unwrap(), Ok(3));
        let stored = read(&partition, 1, 1).await;
        assert_eq!(
            &stored[..8],
            &1_i64.to_be_bytes(),
            "read starts at the batch holding offset 1"
        );
        assert_eq!(stored.len(), two.len(), "max_bytes stops after one batch");
    }

    #[test]
    fn batches_below_cuts_at_the_high_watermark() {
        let dir = tempfile::tempdir().unwrap();
        let mut log = Log::open(dir.path(), LogConfig::default()).unwrap();
        let (mut one, mut two) = (batch(&["a", "b"]), batch(&["c"]));
        log.append(&mut one).unwrap();
        log.append(&mut two).unwrap();
        let data = log.read(0, usize::MAX).unwrap();
        assert_eq!(batches_below(&data, 3), data.len());
        assert_eq!(batches_below(&data, 2), one.len());
        assert_eq!(batches_below(&data, 0), 0);
        assert_eq!(batches_below(&data[..data.len() - 1], 3), one.len());
    }

    #[tokio::test]
    async fn create_topic_survives_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let broker = Broker::open(config(dir.path())).unwrap();
        let created = broker.create_topic("orders".into(), 3).await.unwrap();
        assert_eq!(
            broker.create_topic("orders".into(), 1).await.err(),
            Some(ResponseError::TopicAlreadyExists.code())
        );
        let (records, batches) = whole(&batch(&["kept"]));
        created
            .partition(2)
            .unwrap()
            .append(records, batches)
            .await
            .unwrap()
            .unwrap();
        drop((created, broker));

        let reopened = Broker::open(config(dir.path())).unwrap();
        let topic = reopened.topic("orders").unwrap();
        assert_eq!(topic.partitions.len(), 3);
        assert_eq!(reopened.topic_by_id(topic.id).unwrap().name, "orders");
        assert_eq!(topic.partition(2).unwrap().high_watermark(), 1);

        reopened.delete_topic(&topic).await.unwrap();
        assert!(
            Broker::open(config(dir.path()))
                .unwrap()
                .topics()
                .is_empty()
        );
    }
}
