//! On-disk partition log.
//!
//! A log is a directory of segments. Each segment is a `.log` file holding Kafka v2 record
//! batches byte-for-byte as appended, and a sparse `.index` file of 16-byte entries
//! (relative offset u32, file position u32, max timestamp i64 of everything before that
//! position), all big-endian.
//!
//! Index entries reach the index file only after the log bytes they cover have been synced,
//! so every entry found on disk marks a point up to which the log is durable. Recovery
//! therefore verifies the active segment from its last such entry onwards. Segments are
//! fully synced before a new one is rolled, so only the active segment needs recovery.

use std::fmt;
use std::fs::{self, File, OpenOptions};
use std::io::{self, BufReader, Read, Seek, SeekFrom};
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};

const LOG_OVERHEAD: usize = 12;
const BATCH_HEADER_LEN: usize = 61;
const MIN_BATCH_LENGTH: i32 = 49;
const MAGIC_V2: i8 = 2;

const BATCH_LENGTH_AT: usize = 8;
const MAGIC_AT: usize = 16;
const CRC_AT: usize = 17;
const CRC_START: usize = 21;
const LAST_OFFSET_DELTA_AT: usize = 23;
const MAX_TIMESTAMP_AT: usize = 35;

const INDEX_ENTRY_LEN: usize = 16;
const LOG_SUFFIX: &str = ".log";
const INDEX_SUFFIX: &str = ".index";
const RECOVERY_BUFFER_BYTES: usize = 1 << 20;

/// Tuning knobs for a [`Log`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LogConfig {
    /// Roll to a new segment once the active one reaches this many bytes.
    pub segment_bytes: u64,
    /// Add a sparse index entry about every this many bytes of log.
    pub index_interval_bytes: u32,
}

impl Default for LogConfig {
    fn default() -> Self {
        Self {
            segment_bytes: 1 << 30,
            index_interval_bytes: 4096,
        }
    }
}

/// Errors returned by [`Log`].
#[derive(Debug, thiserror::Error)]
pub enum StorageError {
    /// A file system operation failed, or stored data is unreadable.
    #[error(transparent)]
    Io(#[from] io::Error),
    /// The bytes passed to [`Log::append`] are not one valid v2 record batch.
    #[error("invalid record batch: {0}")]
    InvalidBatch(String),
    /// The requested offset lies outside `[start, end]`.
    #[error("offset {requested} is out of range [{start}, {end}]")]
    OffsetOutOfRange {
        requested: i64,
        start: i64,
        end: i64,
    },
}

/// An append-only, segmented log of Kafka v2 record batches for one partition.
pub struct Log {
    dir: PathBuf,
    config: LogConfig,
    /// Never empty; the last segment is the active one.
    segments: Vec<Segment>,
    /// Set once a write or sync fails: the kernel may have dropped the dirty pages, so a later
    /// successful sync would not prove earlier appends are durable.
    failed: bool,
}

impl Log {
    /// Open or create the log in `dir`, recovering after a crash.
    ///
    /// The active segment is verified from its last durable index entry onwards and any torn
    /// or corrupt tail is truncated.
    ///
    /// # Errors
    ///
    /// Returns [`StorageError::Io`] if the directory or segment files cannot be created, read
    /// or truncated.
    pub fn open(dir: &Path, config: LogConfig) -> Result<Log, StorageError> {
        create_dir_durably(dir)?;
        let bases = segment_bases(dir)?;
        let segments = if let Some((&last, _)) = bases.split_last() {
            let mut segments = bases
                .windows(2)
                .map(|pair| Segment::open_sealed(dir, pair[0], pair[1]))
                .collect::<io::Result<Vec<_>>>()?;
            segments.push(Segment::recover(dir, last, config.index_interval_bytes)?);
            segments
        } else {
            let segment = Segment::create(dir, 0)?;
            sync_dir(dir)?;
            vec![segment]
        };

        Ok(Self {
            dir: dir.to_path_buf(),
            config,
            segments,
            failed: false,
        })
    }

    /// Append exactly one Kafka v2 record batch and return the base offset assigned to it.
    ///
    /// The offset is assigned by overwriting the batch's baseOffset field in place, so the
    /// stored bytes equal `batch` after the call. Not durable until [`Log::flush`].
    ///
    /// # Errors
    ///
    /// Returns [`StorageError::InvalidBatch`] if the length, magic, CRC or lastOffsetDelta is
    /// wrong, and [`StorageError::Io`] if the write or a segment roll fails. After an I/O
    /// failure the log refuses further appends and flushes until it is reopened.
    pub fn append(&mut self, batch: &mut [u8]) -> Result<i64, StorageError> {
        self.ensure_writable()?;
        let header = BatchHeader::validate(batch)?;
        if self.should_roll(&header) {
            self.roll().map_err(|error| self.fail(error))?;
        }

        let base_offset = self.end_offset();
        batch[..8].copy_from_slice(&base_offset.to_be_bytes());
        let index_interval_bytes = self.config.index_interval_bytes;
        let active = self.active_mut();
        if let Err(error) = active.log.write_all_at(batch, active.size) {
            return Err(self.fail(error));
        }
        active.record_batch(
            &BatchHeader {
                base_offset,
                ..header
            },
            index_interval_bytes,
        );
        Ok(base_offset)
    }

    /// Sync everything appended so far to stable storage.
    ///
    /// # Errors
    ///
    /// Returns [`StorageError::Io`] if syncing the log or writing the index fails. After an
    /// I/O failure the log refuses further appends and flushes until it is reopened.
    pub fn flush(&mut self) -> Result<(), StorageError> {
        self.ensure_writable()?;
        self.active_mut().flush().map_err(|error| self.fail(error))
    }

    /// Return whole batches, starting with the batch that contains `offset`, up to
    /// `max_bytes`, but always at least one batch if one exists.
    ///
    /// The bytes are returned exactly as stored. The result is empty when
    /// `offset == end_offset()`.
    ///
    /// # Errors
    ///
    /// Returns [`StorageError::OffsetOutOfRange`] when `offset < start_offset()` or
    /// `offset > end_offset()`, and [`StorageError::Io`] if reading fails.
    pub fn read(&self, offset: i64, max_bytes: usize) -> Result<Vec<u8>, StorageError> {
        let (start, end) = (self.start_offset(), self.end_offset());
        if offset < start || offset > end {
            return Err(StorageError::OffsetOutOfRange {
                requested: offset,
                start,
                end,
            });
        }

        let mut out = Vec::new();
        if offset == end {
            return Ok(out);
        }
        let first = self
            .segments
            .partition_point(|segment| segment.base_offset <= offset)
            - 1;
        let mut position = self.segments[first].locate(offset)?;
        for segment in &self.segments[first..] {
            let budget = max_bytes.saturating_sub(out.len());
            if budget == 0 && !out.is_empty() {
                break;
            }
            if !segment.read_batches(position, budget, &mut out)? {
                break;
            }
            position = 0;
        }
        Ok(out)
    }

    /// First offset still present in the log.
    #[must_use]
    pub fn start_offset(&self) -> i64 {
        self.segments[0].base_offset
    }

    /// Next offset that will be assigned.
    #[must_use]
    pub fn end_offset(&self) -> i64 {
        self.active().next_offset
    }

    /// Base offset of the first batch whose max timestamp is `>= timestamp_ms`, if any.
    ///
    /// An I/O error while searching yields `None`.
    #[must_use]
    pub fn offset_for_timestamp(&self, timestamp_ms: i64) -> Option<i64> {
        for segment in &self.segments {
            match segment.offset_for_timestamp(timestamp_ms) {
                Ok(None) => {}
                Ok(found) => return found,
                Err(_) => return None,
            }
        }
        None
    }

    fn ensure_writable(&self) -> Result<(), StorageError> {
        if self.failed {
            return Err(io::Error::other(
                "log failed after an earlier I/O error; reopen it to recover",
            )
            .into());
        }
        Ok(())
    }

    fn fail(&mut self, error: io::Error) -> StorageError {
        self.failed = true;
        error.into()
    }

    fn active(&self) -> &Segment {
        &self.segments[self.segments.len() - 1]
    }

    fn active_mut(&mut self) -> &mut Segment {
        let last = self.segments.len() - 1;
        &mut self.segments[last]
    }

    /// Roll once the active segment reaches `segment_bytes`, or earlier if the next batch's
    /// position or relative offset would no longer fit the u32 index fields.
    fn should_roll(&self, header: &BatchHeader) -> bool {
        let active = self.active();
        if active.size == 0 {
            return false;
        }
        let threshold = self.config.segment_bytes.min(u64::from(u32::MAX));
        let last_relative_offset =
            active.next_offset - active.base_offset + i64::from(header.last_offset_delta);
        active.size >= threshold || last_relative_offset > i64::from(u32::MAX)
    }

    fn roll(&mut self) -> io::Result<()> {
        self.active_mut().seal()?;
        let segment = Segment::create(&self.dir, self.end_offset())?;
        sync_dir(&self.dir)?;
        self.segments.push(segment);
        Ok(())
    }
}

impl fmt::Debug for Log {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Log")
            .field("dir", &self.dir)
            .field("config", &self.config)
            .field("segments", &self.segments.len())
            .field("start_offset", &self.start_offset())
            .field("end_offset", &self.end_offset())
            .field("failed", &self.failed)
            .finish()
    }
}

struct Segment {
    base_offset: i64,
    log: File,
    index: File,
    /// Bytes of valid batches at the start of `log`.
    size: u64,
    /// Offset following the last batch in this segment.
    next_offset: i64,
    entries: Vec<IndexEntry>,
    /// How many leading `entries` are already in the index file.
    durable_entries: usize,
    /// Append-side indexing state; only meaningful for the active segment.
    bytes_since_index: u64,
    max_timestamp: i64,
}

impl Segment {
    fn create(dir: &Path, base_offset: i64) -> io::Result<Self> {
        Ok(Self {
            base_offset,
            log: open_file(&segment_path(dir, base_offset, LOG_SUFFIX), true)?,
            index: open_file(&segment_path(dir, base_offset, INDEX_SUFFIX), true)?,
            size: 0,
            next_offset: base_offset,
            entries: Vec::new(),
            durable_entries: 0,
            bytes_since_index: 0,
            max_timestamp: i64::MIN,
        })
    }

    fn open_sealed(dir: &Path, base_offset: i64, next_offset: i64) -> io::Result<Self> {
        let log = open_file(&segment_path(dir, base_offset, LOG_SUFFIX), false)?;
        let index = open_file(&segment_path(dir, base_offset, INDEX_SUFFIX), false)?;
        let size = log.metadata()?.len();
        let entries = load_index(&index, size)?;
        Ok(Self {
            base_offset,
            log,
            index,
            size,
            next_offset,
            durable_entries: entries.len(),
            entries,
            bytes_since_index: 0,
            max_timestamp: i64::MIN,
        })
    }

    fn recover(dir: &Path, base_offset: i64, index_interval_bytes: u32) -> io::Result<Self> {
        let log = open_file(&segment_path(dir, base_offset, LOG_SUFFIX), false)?;
        let index = open_file(&segment_path(dir, base_offset, INDEX_SUFFIX), false)?;
        let file_len = log.metadata()?.len();
        let mut entries = load_index(&index, file_len)?;
        while let Some(entry) = entries.last() {
            if starts_batch(&log, file_len, base_offset, entry)? {
                break;
            }
            entries.pop();
        }
        index.set_len((entries.len() * INDEX_ENTRY_LEN) as u64)?;

        let (size, next_offset, max_timestamp) =
            entries.last().map_or((0, base_offset, i64::MIN), |entry| {
                (
                    u64::from(entry.position),
                    base_offset + i64::from(entry.relative_offset),
                    entry.max_timestamp,
                )
            });
        let mut segment = Self {
            base_offset,
            log,
            index,
            size,
            next_offset,
            durable_entries: entries.len(),
            entries,
            bytes_since_index: 0,
            max_timestamp,
        };
        segment.verify_tail(file_len, index_interval_bytes)?;
        if segment.size < file_len {
            segment.log.set_len(segment.size)?;
        }
        Ok(segment)
    }

    /// Advance over every intact batch after `size`, stopping at the first torn or corrupt one.
    fn verify_tail(&mut self, file_len: u64, index_interval_bytes: u32) -> io::Result<()> {
        let mut reader = BufReader::with_capacity(RECOVERY_BUFFER_BYTES, self.log.try_clone()?);
        reader.seek(SeekFrom::Start(self.size))?;
        let mut batch = Vec::new();
        while let Some(header) = next_valid_batch(
            &mut reader,
            file_len - self.size,
            self.next_offset,
            &mut batch,
        )? {
            self.record_batch(&header, index_interval_bytes);
        }
        Ok(())
    }

    /// Account for a batch just placed at `size`.
    fn record_batch(&mut self, header: &BatchHeader, index_interval_bytes: u32) {
        let position = self.size;
        let past_last_entry = self
            .entries
            .last()
            .is_none_or(|entry| u64::from(entry.position) < position);
        if past_last_entry
            && self.bytes_since_index >= u64::from(index_interval_bytes)
            && let (Ok(relative_offset), Ok(position)) = (
                u32::try_from(header.base_offset - self.base_offset),
                u32::try_from(position),
            )
        {
            self.entries.push(IndexEntry {
                relative_offset,
                position,
                max_timestamp: self.max_timestamp,
            });
            self.bytes_since_index = 0;
        }
        let size = header.size();
        self.bytes_since_index += size;
        self.size += size;
        self.next_offset = header.last_offset() + 1;
        self.max_timestamp = self.max_timestamp.max(header.max_timestamp);
    }

    fn flush(&mut self) -> io::Result<()> {
        // fdatasync on Linux. On macOS std already issues fcntl(F_FULLFSYNC) here, the stricter
        // option that also drains the drive's write cache, rather than a plain fsync.
        self.log.sync_data()?;
        if self.durable_entries == self.entries.len() {
            return Ok(());
        }
        let pending: Vec<u8> = self.entries[self.durable_entries..]
            .iter()
            .flat_map(|entry| entry.encode())
            .collect();
        self.index
            .write_all_at(&pending, (self.durable_entries * INDEX_ENTRY_LEN) as u64)?;
        self.durable_entries = self.entries.len();
        Ok(())
    }

    fn seal(&mut self) -> io::Result<()> {
        self.flush()?;
        self.index.sync_data()
    }

    fn header_at(&self, position: u64) -> io::Result<BatchHeader> {
        let header = read_header(&self.log, position)?;
        if header.batch_length < MIN_BATCH_LENGTH {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "corrupt batch at position {position} of segment {}",
                    self.base_offset
                ),
            ));
        }
        Ok(header)
    }

    /// Position of the first batch whose last offset is `>= offset`, or `size` if none.
    fn locate(&self, offset: i64) -> io::Result<u64> {
        let relative_offset = offset - self.base_offset;
        let floor = self
            .entries
            .partition_point(|entry| i64::from(entry.relative_offset) <= relative_offset);
        let mut position = self.entry_position(floor);
        while position < self.size {
            let header = self.header_at(position)?;
            if header.last_offset() >= offset {
                return Ok(position);
            }
            position += header.size();
        }
        Ok(self.size)
    }

    /// Append whole batches from `position` to `out` within `budget` bytes, taking at least
    /// one if `out` is empty. Returns whether the end of the segment was reached.
    fn read_batches(&self, position: u64, budget: usize, out: &mut Vec<u8>) -> io::Result<bool> {
        let available = self.size - position;
        if available == 0 {
            return Ok(true);
        }
        let start = out.len();
        let wanted = usize::try_from(available).map_or(budget, |available| available.min(budget));
        out.resize(start + wanted, 0);
        self.log.read_exact_at(&mut out[start..], position)?;

        let mut taken = whole_batches_len(&out[start..]);
        if taken == 0 && start == 0 {
            taken = usize::try_from(self.header_at(position)?.size()).map_err(io::Error::other)?;
            out.resize(start + taken, 0);
            self.log
                .read_exact_at(&mut out[start + wanted..], position + wanted as u64)?;
        }
        out.truncate(start + taken);
        Ok(position + taken as u64 == self.size)
    }

    fn offset_for_timestamp(&self, timestamp_ms: i64) -> io::Result<Option<i64>> {
        let floor = self
            .entries
            .partition_point(|entry| entry.max_timestamp < timestamp_ms);
        let mut position = self.entry_position(floor);
        while position < self.size {
            let header = self.header_at(position)?;
            if header.max_timestamp >= timestamp_ms {
                return Ok(Some(header.base_offset));
            }
            position += header.size();
        }
        Ok(None)
    }

    /// Position of the entry before `partition_point`, or the segment start.
    fn entry_position(&self, partition_point: usize) -> u64 {
        partition_point
            .checked_sub(1)
            .map_or(0, |at| u64::from(self.entries[at].position))
    }
}

#[derive(Debug, Clone, Copy)]
struct IndexEntry {
    relative_offset: u32,
    position: u32,
    /// Largest batch max timestamp stored before `position`.
    max_timestamp: i64,
}

impl IndexEntry {
    fn encode(self) -> [u8; INDEX_ENTRY_LEN] {
        let mut bytes = [0; INDEX_ENTRY_LEN];
        bytes[..4].copy_from_slice(&self.relative_offset.to_be_bytes());
        bytes[4..8].copy_from_slice(&self.position.to_be_bytes());
        bytes[8..].copy_from_slice(&self.max_timestamp.to_be_bytes());
        bytes
    }

    fn decode(bytes: &[u8]) -> Self {
        Self {
            relative_offset: u32::from_be_bytes(field(bytes, 0)),
            position: u32::from_be_bytes(field(bytes, 4)),
            max_timestamp: i64::from_be_bytes(field(bytes, 8)),
        }
    }

    fn follows(&self, previous: &Self) -> bool {
        self.relative_offset > previous.relative_offset
            && self.position > previous.position
            && self.max_timestamp >= previous.max_timestamp
    }
}

#[derive(Debug, Clone, Copy)]
struct BatchHeader {
    base_offset: i64,
    batch_length: i32,
    magic: i8,
    crc: u32,
    last_offset_delta: i32,
    max_timestamp: i64,
}

impl BatchHeader {
    /// Decode the fixed header; `bytes` must hold at least [`BATCH_HEADER_LEN`] bytes.
    fn parse(bytes: &[u8]) -> Self {
        Self {
            base_offset: i64::from_be_bytes(field(bytes, 0)),
            batch_length: i32::from_be_bytes(field(bytes, BATCH_LENGTH_AT)),
            magic: i8::from_be_bytes(field(bytes, MAGIC_AT)),
            crc: u32::from_be_bytes(field(bytes, CRC_AT)),
            last_offset_delta: i32::from_be_bytes(field(bytes, LAST_OFFSET_DELTA_AT)),
            max_timestamp: i64::from_be_bytes(field(bytes, MAX_TIMESTAMP_AT)),
        }
    }

    fn validate(batch: &[u8]) -> Result<Self, StorageError> {
        if batch.len() < BATCH_HEADER_LEN {
            return Err(StorageError::InvalidBatch(format!(
                "{} bytes is shorter than the {BATCH_HEADER_LEN}-byte batch header",
                batch.len()
            )));
        }
        let header = Self::parse(batch);
        let body_len = batch.len() - LOG_OVERHEAD;
        if usize::try_from(header.batch_length).ok() != Some(body_len) {
            return Err(StorageError::InvalidBatch(format!(
                "batchLength {} does not match the {body_len} bytes that follow it",
                header.batch_length
            )));
        }
        if header.magic != MAGIC_V2 {
            return Err(StorageError::InvalidBatch(format!(
                "magic {} is not {MAGIC_V2}",
                header.magic
            )));
        }
        let crc = crc32c::crc32c(&batch[CRC_START..]);
        if crc != header.crc {
            return Err(StorageError::InvalidBatch(format!(
                "crc {:#010x} does not match computed {crc:#010x}",
                header.crc
            )));
        }
        if header.last_offset_delta < 0 {
            return Err(StorageError::InvalidBatch(format!(
                "lastOffsetDelta {} is negative",
                header.last_offset_delta
            )));
        }
        Ok(header)
    }

    fn size(&self) -> u64 {
        LOG_OVERHEAD as u64 + u64::from(self.batch_length.unsigned_abs())
    }

    fn last_offset(&self) -> i64 {
        self.base_offset + i64::from(self.last_offset_delta)
    }
}

fn field<const N: usize>(bytes: &[u8], at: usize) -> [u8; N] {
    let mut out = [0; N];
    out.copy_from_slice(&bytes[at..at + N]);
    out
}

fn read_header(file: &File, position: u64) -> io::Result<BatchHeader> {
    let mut bytes = [0; BATCH_HEADER_LEN];
    file.read_exact_at(&mut bytes, position)?;
    Ok(BatchHeader::parse(&bytes))
}

fn starts_batch(
    log: &File,
    file_len: u64,
    base_offset: i64,
    entry: &IndexEntry,
) -> io::Result<bool> {
    let position = u64::from(entry.position);
    if position + BATCH_HEADER_LEN as u64 > file_len {
        return Ok(false);
    }
    Ok(read_header(log, position)?.base_offset == base_offset + i64::from(entry.relative_offset))
}

/// Read the next batch into `batch` if it is complete, intact and starts at `expected_offset`.
fn next_valid_batch(
    reader: &mut impl Read,
    remaining: u64,
    expected_offset: i64,
    batch: &mut Vec<u8>,
) -> io::Result<Option<BatchHeader>> {
    if remaining < BATCH_HEADER_LEN as u64 {
        return Ok(None);
    }
    batch.resize(LOG_OVERHEAD, 0);
    reader.read_exact(batch)?;
    let Ok(body_len) = usize::try_from(i32::from_be_bytes(field(batch, BATCH_LENGTH_AT))) else {
        return Ok(None);
    };
    if (LOG_OVERHEAD + body_len) as u64 > remaining {
        return Ok(None);
    }
    batch.resize(LOG_OVERHEAD + body_len, 0);
    reader.read_exact(&mut batch[LOG_OVERHEAD..])?;
    Ok(BatchHeader::validate(batch)
        .ok()
        .filter(|header| header.base_offset == expected_offset))
}

/// Length of the prefix of `bytes` made of complete batches.
fn whole_batches_len(bytes: &[u8]) -> usize {
    let mut end = 0;
    while let Some(length) = bytes.get(end + BATCH_LENGTH_AT..end + LOG_OVERHEAD) {
        let Ok(body_len) = usize::try_from(i32::from_be_bytes(field(length, 0))) else {
            break;
        };
        if end + LOG_OVERHEAD + body_len > bytes.len() {
            break;
        }
        end += LOG_OVERHEAD + body_len;
    }
    end
}

/// Leading index entries that are well-formed and point inside a log of `log_len` bytes.
fn load_index(index: &File, log_len: u64) -> io::Result<Vec<IndexEntry>> {
    let mut bytes = Vec::new();
    let mut reader = index;
    reader.read_to_end(&mut bytes)?;
    let mut entries: Vec<IndexEntry> = Vec::with_capacity(bytes.len() / INDEX_ENTRY_LEN);
    for chunk in bytes.as_chunks::<INDEX_ENTRY_LEN>().0 {
        let entry = IndexEntry::decode(chunk);
        let in_order = entries
            .last()
            .is_none_or(|previous| entry.follows(previous));
        if !in_order || u64::from(entry.position) >= log_len {
            break;
        }
        entries.push(entry);
    }
    Ok(entries)
}

fn segment_bases(dir: &Path) -> io::Result<Vec<i64>> {
    let mut bases = Vec::new();
    for entry in fs::read_dir(dir)? {
        let name = entry?.file_name();
        let base = name
            .to_str()
            .and_then(|name| name.strip_suffix(LOG_SUFFIX))
            .filter(|stem| stem.len() == 20 && stem.bytes().all(|byte| byte.is_ascii_digit()))
            .and_then(|stem| stem.parse().ok());
        if let Some(base) = base {
            bases.push(base);
        }
    }
    bases.sort_unstable();
    Ok(bases)
}

/// Create `dir` and any missing ancestors, syncing each new directory entry into its parent.
fn create_dir_durably(dir: &Path) -> io::Result<()> {
    let missing: Vec<&Path> = dir
        .ancestors()
        .take_while(|path| !path.as_os_str().is_empty() && !path.exists())
        .collect();
    fs::create_dir_all(dir)?;
    for path in missing {
        match path.parent() {
            Some(parent) if parent.as_os_str().is_empty() => sync_dir(Path::new("."))?,
            Some(parent) => sync_dir(parent)?,
            None => {}
        }
    }
    Ok(())
}

fn segment_path(dir: &Path, base_offset: i64, suffix: &str) -> PathBuf {
    dir.join(format!("{base_offset:020}{suffix}"))
}

fn open_file(path: &Path, truncate: bool) -> io::Result<File> {
    OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(truncate)
        .open(path)
}

fn sync_dir(dir: &Path) -> io::Result<()> {
    File::open(dir)?.sync_all()
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::path::{Path, PathBuf};

    use tempfile::TempDir;

    use super::*;

    const UNLIMITED: usize = usize::MAX;

    fn build_batch(record_count: i32, max_timestamp: i64, payload_len: usize) -> Vec<u8> {
        let mut batch = vec![0; BATCH_HEADER_LEN + payload_len];
        let batch_length = i32::try_from(batch.len() - LOG_OVERHEAD).unwrap();
        batch[8..12].copy_from_slice(&batch_length.to_be_bytes());
        batch[16] = 2;
        batch[23..27].copy_from_slice(&(record_count - 1).to_be_bytes());
        batch[27..35].copy_from_slice(&max_timestamp.to_be_bytes());
        batch[35..43].copy_from_slice(&max_timestamp.to_be_bytes());
        batch[43..51].copy_from_slice(&(-1_i64).to_be_bytes());
        batch[51..53].copy_from_slice(&(-1_i16).to_be_bytes());
        batch[53..57].copy_from_slice(&(-1_i32).to_be_bytes());
        batch[57..61].copy_from_slice(&record_count.to_be_bytes());
        for (at, byte) in batch[BATCH_HEADER_LEN..].iter_mut().enumerate() {
            *byte = u8::try_from(at % 251).unwrap();
        }
        let crc = crc32c::crc32c(&batch[CRC_START..]);
        batch[17..21].copy_from_slice(&crc.to_be_bytes());
        batch
    }

    /// 100-byte batches of one record each, timestamps 1000, 1001, ...
    fn uniform_batches(count: i64) -> Vec<Vec<u8>> {
        (0..count).map(|at| build_batch(1, 1000 + at, 39)).collect()
    }

    fn config(segment_bytes: u64, index_interval_bytes: u32) -> LogConfig {
        LogConfig {
            segment_bytes,
            index_interval_bytes,
        }
    }

    fn append_all(log: &mut Log, batches: &mut [Vec<u8>]) -> Vec<i64> {
        batches
            .iter_mut()
            .map(|batch| log.append(batch).unwrap())
            .collect()
    }

    fn segment_files(dir: &Path, suffix: &str) -> Vec<PathBuf> {
        let mut files: Vec<PathBuf> = fs::read_dir(dir)
            .unwrap()
            .map(|entry| entry.unwrap().path())
            .filter(|path| path.to_str().unwrap().ends_with(suffix))
            .collect();
        files.sort();
        files
    }

    fn last_log_file(dir: &Path) -> PathBuf {
        segment_files(dir, LOG_SUFFIX).pop().unwrap()
    }

    fn assert_invalid(log: &mut Log, mut batch: Vec<u8>) {
        let end = log.end_offset();
        assert!(matches!(
            log.append(&mut batch),
            Err(StorageError::InvalidBatch(_))
        ));
        assert_eq!(log.end_offset(), end);
    }

    /// Configurations that put index entries on every batch, some batches and no batch,
    /// with and without segment rolls.
    const RECOVERY_CONFIGS: [(u64, u32); 4] =
        [(1 << 20, 0), (1 << 20, 150), (1 << 20, 4096), (250, 0)];

    #[test]
    fn open_creates_missing_directories() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("topic").join("0");
        let mut log = Log::open(&path, LogConfig::default()).unwrap();

        assert_eq!(log.append(&mut build_batch(1, 10, 10)).unwrap(), 0);
        assert!(segment_path(&path, 0, LOG_SUFFIX).is_file());
    }

    #[test]
    fn append_assigns_consecutive_offsets() {
        let dir = TempDir::new().unwrap();
        let mut log = Log::open(dir.path(), LogConfig::default()).unwrap();
        let mut batches = vec![
            build_batch(3, 10, 20),
            build_batch(1, 11, 0),
            build_batch(5, 12, 7),
        ];

        assert_eq!(append_all(&mut log, &mut batches), [0, 3, 4]);
        assert_eq!((log.start_offset(), log.end_offset()), (0, 9));
    }

    #[test]
    fn append_rewrites_base_offset_in_place() {
        let dir = TempDir::new().unwrap();
        let mut log = Log::open(dir.path(), LogConfig::default()).unwrap();
        let mut first = build_batch(4, 10, 10);
        let mut second = build_batch(1, 10, 10);
        second[..8].copy_from_slice(&999_i64.to_be_bytes());

        log.append(&mut first).unwrap();
        log.append(&mut second).unwrap();

        assert_eq!(i64::from_be_bytes(second[..8].try_into().unwrap()), 4);
        assert_eq!(log.read(4, UNLIMITED).unwrap(), second);
    }

    #[test]
    fn append_rejects_bad_crc() {
        let dir = TempDir::new().unwrap();
        let mut log = Log::open(dir.path(), LogConfig::default()).unwrap();
        let mut batch = build_batch(1, 10, 10);
        batch[BATCH_HEADER_LEN + 3] ^= 0x40;
        assert_invalid(&mut log, batch);
    }

    #[test]
    fn append_rejects_bad_magic() {
        let dir = TempDir::new().unwrap();
        let mut log = Log::open(dir.path(), LogConfig::default()).unwrap();
        let mut batch = build_batch(1, 10, 10);
        batch[MAGIC_AT] = 1;
        assert_invalid(&mut log, batch);
    }

    #[test]
    fn append_rejects_wrong_length() {
        let dir = TempDir::new().unwrap();
        let mut log = Log::open(dir.path(), LogConfig::default()).unwrap();
        let mut trailing = build_batch(1, 10, 10);
        trailing.push(0);
        let mut truncated = build_batch(1, 10, 10);
        truncated.pop();

        assert_invalid(&mut log, trailing);
        assert_invalid(&mut log, truncated);
        assert_invalid(&mut log, build_batch(1, 10, 0)[..40].to_vec());
        assert_invalid(&mut log, Vec::new());
    }

    #[test]
    fn append_rejects_negative_last_offset_delta() {
        let dir = TempDir::new().unwrap();
        let mut log = Log::open(dir.path(), LogConfig::default()).unwrap();
        assert_invalid(&mut log, build_batch(0, 10, 10));
    }

    #[test]
    fn read_returns_batch_containing_mid_batch_offset() {
        let dir = TempDir::new().unwrap();
        let mut log = Log::open(dir.path(), LogConfig::default()).unwrap();
        let mut batches = vec![
            build_batch(3, 10, 20),
            build_batch(4, 11, 30),
            build_batch(2, 12, 40),
        ];
        append_all(&mut log, &mut batches);

        assert_eq!(
            log.read(5, UNLIMITED).unwrap(),
            [batches[1].clone(), batches[2].clone()].concat()
        );
        assert_eq!(log.read(3, 1).unwrap(), batches[1]);
        assert_eq!(log.read(6, 1).unwrap(), batches[1]);
        assert_eq!(log.read(8, UNLIMITED).unwrap(), batches[2]);
    }

    #[test]
    fn read_respects_max_bytes_but_returns_one_batch() {
        let dir = TempDir::new().unwrap();
        let mut log = Log::open(dir.path(), LogConfig::default()).unwrap();
        let mut batches = uniform_batches(4);
        append_all(&mut log, &mut batches);

        assert_eq!(log.read(0, 0).unwrap(), batches[0]);
        assert_eq!(log.read(0, 99).unwrap(), batches[0]);
        assert_eq!(log.read(0, 250).unwrap(), batches[..2].concat());
        assert_eq!(log.read(1, 300).unwrap(), batches[1..4].concat());
        assert_eq!(log.read(0, UNLIMITED).unwrap(), batches.concat());
    }

    #[test]
    fn read_at_end_offset_returns_empty() {
        let dir = TempDir::new().unwrap();
        let mut log = Log::open(dir.path(), LogConfig::default()).unwrap();
        assert_eq!(log.read(0, UNLIMITED).unwrap(), Vec::<u8>::new());

        log.append(&mut build_batch(3, 10, 10)).unwrap();
        assert_eq!(log.read(3, UNLIMITED).unwrap(), Vec::<u8>::new());
    }

    #[test]
    fn read_rejects_offsets_outside_log() {
        let dir = TempDir::new().unwrap();
        let mut log = Log::open(dir.path(), LogConfig::default()).unwrap();
        log.append(&mut build_batch(3, 10, 10)).unwrap();

        for requested in [-1, 4, i64::MAX] {
            assert!(matches!(
                log.read(requested, UNLIMITED),
                Err(StorageError::OffsetOutOfRange { requested: r, start: 0, end: 3 }) if r == requested
            ));
        }
    }

    #[test]
    fn read_rejects_offsets_before_start_offset() {
        let dir = TempDir::new().unwrap();
        fs::write(segment_path(dir.path(), 10, LOG_SUFFIX), b"").unwrap();
        let mut log = Log::open(dir.path(), LogConfig::default()).unwrap();
        assert_eq!((log.start_offset(), log.end_offset()), (10, 10));

        assert_eq!(log.append(&mut build_batch(2, 10, 10)).unwrap(), 10);
        assert!(matches!(
            log.read(9, UNLIMITED),
            Err(StorageError::OffsetOutOfRange {
                requested: 9,
                start: 10,
                end: 12
            })
        ));
    }

    #[test]
    fn segment_roll_spreads_batches_and_reads_span_segments() {
        for index_interval_bytes in [0, 150, 4096] {
            let dir = TempDir::new().unwrap();
            let mut log = Log::open(dir.path(), config(250, index_interval_bytes)).unwrap();
            let mut batches = uniform_batches(10);
            append_all(&mut log, &mut batches);

            let names: Vec<String> = segment_files(dir.path(), LOG_SUFFIX)
                .iter()
                .map(|path| path.file_name().unwrap().to_str().unwrap().to_owned())
                .collect();
            assert_eq!(
                names,
                [
                    "00000000000000000000.log",
                    "00000000000000000003.log",
                    "00000000000000000006.log",
                    "00000000000000000009.log"
                ]
            );
            assert_eq!(segment_files(dir.path(), INDEX_SUFFIX).len(), 4);
            assert_eq!(log.read(0, UNLIMITED).unwrap(), batches.concat());
            assert_eq!(log.read(2, 350).unwrap(), batches[2..5].concat());
            assert_eq!(log.read(5, 450).unwrap(), batches[5..9].concat());
            for (offset, batch) in (0..).zip(&batches) {
                assert_eq!(&log.read(offset, 0).unwrap(), batch);
            }
        }
    }

    #[test]
    fn reopen_after_clean_close_preserves_everything() {
        for (segment_bytes, index_interval_bytes) in RECOVERY_CONFIGS {
            let dir = TempDir::new().unwrap();
            let config = config(segment_bytes, index_interval_bytes);
            let mut batches = uniform_batches(10);
            {
                let mut log = Log::open(dir.path(), config).unwrap();
                append_all(&mut log, &mut batches);
                log.flush().unwrap();
            }

            let mut log = Log::open(dir.path(), config).unwrap();
            assert_eq!((log.start_offset(), log.end_offset()), (0, 10));
            assert_eq!(log.read(0, UNLIMITED).unwrap(), batches.concat());
            for (offset, batch) in (0..).zip(&batches) {
                assert_eq!(&log.read(offset, 0).unwrap(), batch);
            }
            assert_eq!(log.offset_for_timestamp(1004), Some(4));

            let mut next = build_batch(2, 2000, 39);
            assert_eq!(log.append(&mut next).unwrap(), 10);
            log.flush().unwrap();
            drop(log);

            let log = Log::open(dir.path(), config).unwrap();
            assert_eq!(log.end_offset(), 12);
            assert_eq!(log.read(10, UNLIMITED).unwrap(), next);
        }
    }

    #[test]
    fn reopen_after_truncated_tail_recovers_last_good_batch() {
        for (segment_bytes, index_interval_bytes) in RECOVERY_CONFIGS {
            for cut in [1, 10, 50, 99] {
                let dir = TempDir::new().unwrap();
                let config = config(segment_bytes, index_interval_bytes);
                let mut batches = uniform_batches(4);
                {
                    let mut log = Log::open(dir.path(), config).unwrap();
                    append_all(&mut log, &mut batches);
                    log.flush().unwrap();
                }
                let path = last_log_file(dir.path());
                let len = fs::metadata(&path).unwrap().len();
                fs::OpenOptions::new()
                    .write(true)
                    .open(&path)
                    .unwrap()
                    .set_len(len - cut)
                    .unwrap();

                let mut log = Log::open(dir.path(), config).unwrap();
                assert_eq!(log.end_offset(), 3);
                assert_eq!(log.read(0, UNLIMITED).unwrap(), batches[..3].concat());
                assert_eq!(fs::metadata(&path).unwrap().len(), len - 100);

                let mut next = build_batch(1, 3000, 39);
                assert_eq!(log.append(&mut next).unwrap(), 3);
                log.flush().unwrap();
                drop(log);

                let log = Log::open(dir.path(), config).unwrap();
                assert_eq!(log.end_offset(), 4);
                assert_eq!(log.read(3, UNLIMITED).unwrap(), next);
            }
        }
    }

    #[test]
    fn reopen_after_corrupt_tail_recovers_last_good_batch() {
        for (segment_bytes, index_interval_bytes) in RECOVERY_CONFIGS {
            for from_end in [1, 30, 70, 95, 100] {
                let dir = TempDir::new().unwrap();
                let config = config(segment_bytes, index_interval_bytes);
                let mut batches = uniform_batches(5);
                {
                    let mut log = Log::open(dir.path(), config).unwrap();
                    append_all(&mut log, &mut batches);
                    log.flush().unwrap();
                }
                let path = last_log_file(dir.path());
                let mut bytes = fs::read(&path).unwrap();
                let at = bytes.len() - from_end;
                bytes[at] ^= 0x01;
                fs::write(&path, &bytes).unwrap();

                let mut log = Log::open(dir.path(), config).unwrap();
                assert_eq!(log.end_offset(), 4, "flipped byte {from_end} from the end");
                assert_eq!(log.read(0, UNLIMITED).unwrap(), batches[..4].concat());

                let mut next = build_batch(1, 3000, 39);
                assert_eq!(log.append(&mut next).unwrap(), 4);
                assert_eq!(
                    log.read(3, UNLIMITED).unwrap(),
                    [batches[3].clone(), next].concat()
                );
            }
        }
    }

    #[test]
    fn reopen_keeps_flushed_batches_before_last_durable_entry() {
        let dir = TempDir::new().unwrap();
        let config = config(1 << 20, 0);
        let mut batches = uniform_batches(5);
        {
            let mut log = Log::open(dir.path(), config).unwrap();
            append_all(&mut log, &mut batches);
            log.flush().unwrap();
        }
        let path = last_log_file(dir.path());
        let mut bytes = fs::read(&path).unwrap();
        bytes[150] ^= 0x01;
        fs::write(&path, &bytes).unwrap();

        let log = Log::open(dir.path(), config).unwrap();
        assert_eq!(log.end_offset(), 5);
    }

    #[test]
    fn reopen_after_unflushed_appends_scans_from_last_durable_entry() {
        let dir = TempDir::new().unwrap();
        let config = config(1 << 20, 0);
        let mut batches = uniform_batches(5);
        {
            let mut log = Log::open(dir.path(), config).unwrap();
            append_all(&mut log, &mut batches[..3]);
            log.flush().unwrap();
            append_all(&mut log, &mut batches[3..]);
        }
        let index_path = segment_path(dir.path(), 0, INDEX_SUFFIX);
        assert_eq!(
            fs::metadata(&index_path).unwrap().len(),
            3 * INDEX_ENTRY_LEN as u64
        );

        let mut log = Log::open(dir.path(), config).unwrap();
        assert_eq!(log.end_offset(), 5);
        assert_eq!(log.active().entries.len(), 5);
        assert_eq!(log.read(3, UNLIMITED).unwrap(), batches[3..].concat());

        log.flush().unwrap();
        assert_eq!(
            fs::metadata(&index_path).unwrap().len(),
            5 * INDEX_ENTRY_LEN as u64
        );
    }

    #[test]
    fn reopen_ignores_damaged_or_missing_index() {
        let dir = TempDir::new().unwrap();
        let config = config(250, 0);
        let mut batches = uniform_batches(10);
        {
            let mut log = Log::open(dir.path(), config).unwrap();
            append_all(&mut log, &mut batches);
            log.flush().unwrap();
        }
        let indexes = segment_files(dir.path(), INDEX_SUFFIX);
        fs::remove_file(&indexes[0]).unwrap();
        let mut sealed = fs::read(&indexes[1]).unwrap();
        sealed.extend_from_slice(&[0; INDEX_ENTRY_LEN]);
        fs::write(&indexes[1], &sealed).unwrap();
        let mut active = fs::read(&indexes[3]).unwrap();
        active.extend_from_slice(&[0xff; INDEX_ENTRY_LEN + 5]);
        fs::write(&indexes[3], &active).unwrap();

        let log = Log::open(dir.path(), config).unwrap();
        assert_eq!(log.end_offset(), 10);
        for (offset, batch) in (0..).zip(&batches) {
            assert_eq!(&log.read(offset, 0).unwrap(), batch);
        }
        assert_eq!(log.offset_for_timestamp(1001), Some(1));
    }

    #[test]
    fn offset_for_timestamp_finds_first_batch_at_or_after_timestamp() {
        for (segment_bytes, index_interval_bytes) in RECOVERY_CONFIGS {
            let dir = TempDir::new().unwrap();
            let config = config(segment_bytes, index_interval_bytes);
            let mut batches: Vec<Vec<u8>> = [100, 300, 200, 400, 250]
                .into_iter()
                .map(|timestamp| build_batch(2, timestamp, 39))
                .collect();
            let mut log = Log::open(dir.path(), config).unwrap();
            assert_eq!(log.offset_for_timestamp(0), None);
            append_all(&mut log, &mut batches);
            log.flush().unwrap();

            let expected = [
                (i64::MIN, Some(0)),
                (100, Some(0)),
                (101, Some(2)),
                (250, Some(2)),
                (300, Some(2)),
                (301, Some(6)),
                (400, Some(6)),
                (401, None),
            ];
            for (timestamp, offset) in expected {
                assert_eq!(
                    log.offset_for_timestamp(timestamp),
                    offset,
                    "timestamp {timestamp}"
                );
            }
            drop(log);

            let log = Log::open(dir.path(), config).unwrap();
            for (timestamp, offset) in expected {
                assert_eq!(
                    log.offset_for_timestamp(timestamp),
                    offset,
                    "timestamp {timestamp} after reopen"
                );
            }
        }
    }

    #[test]
    fn failed_log_refuses_writes_until_reopened() {
        let dir = TempDir::new().unwrap();
        let mut batches = uniform_batches(2);
        let mut log = Log::open(dir.path(), LogConfig::default()).unwrap();
        append_all(&mut log, &mut batches);
        log.fail(io::Error::other("injected sync failure"));

        assert!(matches!(
            log.append(&mut build_batch(1, 10, 39)),
            Err(StorageError::Io(_))
        ));
        assert!(matches!(log.flush(), Err(StorageError::Io(_))));
        assert_eq!(log.end_offset(), 2);
        assert_eq!(log.read(0, UNLIMITED).unwrap(), batches.concat());
        drop(log);

        let mut log = Log::open(dir.path(), LogConfig::default()).unwrap();
        assert_eq!(log.read(0, UNLIMITED).unwrap(), batches.concat());
        assert_eq!(log.append(&mut build_batch(1, 10, 39)).unwrap(), 2);
        log.flush().unwrap();
    }

    #[test]
    fn log_is_send_and_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<Log>();
    }
}
