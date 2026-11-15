//! On-disk partition log.
//!
//! A log is a directory of segments and an epoch checkpoint. Each segment is a `.log` file holding
//! Kafka v2 record batches byte-for-byte as appended, and a sparse `.index` file of 16-byte entries
//! (relative offset u32, file position u32, max timestamp i64 of everything before that
//! position), all big-endian. Segment files are created under a temporary name and renamed into
//! place, so a file position, once given out in an [`Extent`], never holds other bytes later.
//!
//! Index entries reach the index file only after the log bytes they cover have been synced,
//! so every entry found on disk marks a point up to which the log is durable. Recovery
//! therefore verifies the active segment from its last such entry onwards, and syncs what it keeps.
//! Segments are fully synced before a new one is rolled, so only the active segment needs recovery.
//!
//! Each batch's partitionLeaderEpoch, its epoch, is stamped by the leader and never decreases
//! along the log. The `epochs` checkpoint lists where each epoch starts. It is replaced atomically
//! and is durable before any batch it describes can be: the first batch of an epoch is written only
//! once a checkpoint naming that epoch is synced, and a truncate rewrites the checkpoint after the
//! data it removes is gone for good. So after any crash the checkpoint describes the recovered log,
//! plus possibly epochs starting at or past its end; opening drops those and still checks every
//! other entry against the batches it names, rebuilding from the first one that disagrees.
//!
//! A truncate keeps every crash state a whole prefix of the log before it, never shorter than the
//! truncation point. In order, each step durable before the next: later segments are deleted
//! newest first with a directory sync after each; the segment holding the truncation point is cut
//! and synced, log then index; a fresh segment starting at the truncation point is renamed in and
//! the directory synced; the epoch checkpoint is rewritten. All of it is done when the truncate
//! returns, so later appends can never end up behind truncated bytes.

use std::fmt;
use std::fs::{self, File, OpenOptions};
use std::io::{self, BufReader, Read, Seek, SeekFrom, Write};
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use bytes::BytesMut;

const LOG_OVERHEAD: usize = 12;
const BATCH_HEADER_LEN: usize = 61;
const MIN_BATCH_LENGTH: i32 = 49;
const MAGIC_V2: i8 = 2;

const BATCH_LENGTH_AT: usize = 8;
const EPOCH_AT: usize = 12;
const MAGIC_AT: usize = 16;
const CRC_AT: usize = 17;
const CRC_START: usize = 21;
const LAST_OFFSET_DELTA_AT: usize = 23;
const MAX_TIMESTAMP_AT: usize = 35;

/// Attributes of a control batch: the control bit, uncompressed and not transactional.
const CONTROL_ATTRIBUTES: i16 = 1 << 5;
/// The one record of a no-op control batch: a control record key of version 0 and type 1 (COMMIT),
/// no value. Kafka's protocol documentation defines only types 0 (ABORT) and 1 (COMMIT) for regular
/// topics; COMMIT is the one a consumer skips without acting on it.
const CONTROL_RECORD: [u8; 11] = [
    0x14, // length 10, as a zigzag varint
    0x00, // attributes
    0x00, // timestampDelta
    0x00, // offsetDelta
    0x08, // key length 4
    0x00, 0x00, // key version
    0x00, 0x01, // key type
    0x01, // value length -1: null
    0x00, // header count
];

const INDEX_ENTRY_LEN: usize = 16;
const LOG_SUFFIX: &str = ".log";
const INDEX_SUFFIX: &str = ".index";
const TMP_SUFFIX: &str = ".tmp";
const EPOCHS_FILE: &str = "epochs";
const EPOCHS_HEADER: &str = "redcrab epochs 1";
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
    /// The bytes passed to an append are not one valid v2 record batch.
    #[error("invalid record batch: {0}")]
    InvalidBatch(String),
    /// The requested offset lies outside `[start, end]`.
    #[error("offset {requested} is out of range [{start}, {end}]")]
    OffsetOutOfRange {
        requested: i64,
        start: i64,
        end: i64,
    },
    /// A replicated batch does not start at the end offset.
    #[error("batch base offset {found} is not the end offset {expected}")]
    OffsetMismatch { expected: i64, found: i64 },
    /// A batch's epoch is lower than the last batch's.
    #[error("epoch {found} is lower than the last batch's epoch {last}")]
    EpochRegression { last: i32, found: i32 },
    /// A truncation offset lies inside a batch.
    #[error("offset {offset} is not where a batch starts")]
    NotBatchBoundary { offset: i64 },
}

/// Where an epoch's batches begin.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EpochStart {
    /// The partitionLeaderEpoch of the batches.
    pub epoch: i32,
    /// Base offset of the first batch with this epoch.
    pub start_offset: i64,
}

/// An append-only, segmented log of Kafka v2 record batches for one partition.
pub struct Log {
    dir: PathBuf,
    config: LogConfig,
    /// Never empty; the last segment is the active one.
    segments: Vec<Segment>,
    /// Where each epoch present starts, in log order, for every batch appended, durable or not.
    epochs: Vec<EpochStart>,
    /// Everything below this offset is synced.
    flushed_end: i64,
    /// Counts truncates, so that a flush begun before one claims nothing after it.
    truncations: u64,
    /// Set once a write or sync fails: the kernel may have dropped the dirty pages, so a later
    /// successful sync would not prove earlier appends are durable.
    failed: bool,
}

impl Log {
    /// Open or create the log in `dir`, recovering after a crash.
    ///
    /// The active segment is verified from its last durable index entry onwards, any torn or
    /// corrupt tail is truncated and the rest synced, so everything recovered is durable. The epoch
    /// checkpoint is checked against the recovered batches and rewritten if it disagrees.
    ///
    /// # Errors
    ///
    /// Returns [`StorageError::Io`] if the directory or segment files cannot be created, read,
    /// truncated or synced, or the checkpoint cannot be rewritten.
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

        let mut log = Self {
            dir: dir.to_path_buf(),
            config,
            segments,
            epochs: Vec::new(),
            flushed_end: 0,
            truncations: 0,
            failed: false,
        };
        log.flushed_end = log.end_offset();
        let saved = read_epochs(dir)?;
        log.epochs = log.reconcile_epochs(saved.clone())?;
        if log.epochs != saved {
            write_epochs(dir, &log.epochs)?;
        }
        Ok(log)
    }

    /// Append exactly one Kafka v2 record batch as the leader of `epoch` and return the base offset
    /// assigned to it.
    ///
    /// The offset and epoch are stamped by overwriting the batch's baseOffset and
    /// partitionLeaderEpoch fields in place, both outside the CRC, so the stored bytes equal `batch`
    /// after the call. Not durable until [`Log::flush`]. The first batch of a new epoch syncs the
    /// epoch checkpoint first.
    ///
    /// # Errors
    ///
    /// Returns [`StorageError::InvalidBatch`] if the length, magic, CRC or lastOffsetDelta is
    /// wrong, [`StorageError::EpochRegression`] if `epoch` is lower than the last batch's, and
    /// [`StorageError::Io`] if a write, a segment roll or the checkpoint fails. After an I/O
    /// failure the log refuses further writes until it is reopened.
    pub fn append(&mut self, batch: &mut [u8], epoch: i32) -> Result<i64, StorageError> {
        self.ensure_writable()?;
        let header = BatchHeader::validate(batch)?;
        self.check_epoch(epoch)?;
        let base_offset = self.end_offset();
        batch[..BATCH_LENGTH_AT].copy_from_slice(&base_offset.to_be_bytes());
        batch[EPOCH_AT..MAGIC_AT].copy_from_slice(&epoch.to_be_bytes());
        self.write(
            batch,
            &BatchHeader {
                base_offset,
                epoch,
                ..header
            },
        )?;
        Ok(base_offset)
    }

    /// Append exactly one Kafka v2 record batch as a follower, byte-for-byte as the leader stored
    /// it: its baseOffset and partitionLeaderEpoch are kept, not assigned. Not durable until
    /// [`Log::flush`]. The first batch of a new epoch syncs the epoch checkpoint first.
    ///
    /// # Errors
    ///
    /// Returns what [`Log::append`] returns, and [`StorageError::OffsetMismatch`] unless the batch
    /// starts exactly at [`Log::end_offset`]. A rejected batch leaves the log as it was.
    pub fn append_replicated(&mut self, batch: &[u8]) -> Result<(), StorageError> {
        self.ensure_writable()?;
        let header = BatchHeader::validate(batch)?;
        let expected = self.end_offset();
        if header.base_offset != expected {
            return Err(StorageError::OffsetMismatch {
                expected,
                found: header.base_offset,
            });
        }
        self.check_epoch(header.epoch)?;
        self.write(batch, &header)
    }

    /// Sync everything appended so far to stable storage.
    ///
    /// # Errors
    ///
    /// Returns [`StorageError::Io`] if syncing the log or writing the index fails. After an
    /// I/O failure the log refuses further writes until it is reopened.
    pub fn flush(&mut self) -> Result<(), StorageError> {
        let flush = self.begin_flush()?;
        let synced = flush.sync();
        self.end_flush(&flush, synced)
    }

    /// First half of [`Log::flush`], for syncing without holding the log: everything appended so
    /// far is durable once [`Flush::sync`] succeeds and [`Log::end_flush`] has run. Appends in
    /// between are allowed and the flush claims none of them; a truncate in between makes the
    /// flush claim nothing.
    ///
    /// # Errors
    ///
    /// Returns [`StorageError::Io`] if the log failed earlier.
    pub fn begin_flush(&self) -> Result<Flush, StorageError> {
        self.ensure_writable()?;
        let active = self.active();
        Ok(Flush {
            file: Arc::clone(&active.log),
            segment: active.base_offset,
            entries: active.entries.len(),
            end: self.end_offset(),
            truncations: self.truncations,
        })
    }

    /// Second half of [`Log::flush`], given the outcome of [`Flush::sync`].
    ///
    /// # Errors
    ///
    /// Returns [`StorageError::Io`] if the sync or writing the index failed. After an I/O
    /// failure the log refuses further writes until it is reopened.
    pub fn end_flush(&mut self, flush: &Flush, synced: io::Result<()>) -> Result<(), StorageError> {
        self.ensure_writable()?;
        if let Err(error) = synced {
            return Err(self.fail(error));
        }
        if flush.truncations != self.truncations {
            return Ok(());
        }
        let active = self.active_mut();
        if active.base_offset == flush.segment
            && let Err(error) = active.write_index(flush.entries)
        {
            return Err(self.fail(error));
        }
        self.flushed_end = self.flushed_end.max(flush.end);
        Ok(())
    }

    /// Discard every batch at or after `offset`, durably: once this returns no crash brings any of
    /// them back, and everything below `offset` is synced.
    ///
    /// `offset` must be where a batch starts, or the end offset, which makes this a no-op. Appends
    /// continue in a new segment file. The caller must never truncate below what it has committed;
    /// the log does not know that offset.
    ///
    /// # Errors
    ///
    /// Returns [`StorageError::OffsetOutOfRange`] outside `[start_offset(), end_offset()]`,
    /// [`StorageError::NotBatchBoundary`] inside a batch, and [`StorageError::Io`] if the log failed
    /// earlier or removing, cutting, creating or syncing a file fails. After an I/O failure the log
    /// refuses further writes until it is reopened.
    pub fn truncate(&mut self, offset: i64) -> Result<(), StorageError> {
        self.ensure_writable()?;
        let (start, end) = (self.start_offset(), self.end_offset());
        if offset < start || offset > end {
            return Err(StorageError::OffsetOutOfRange {
                requested: offset,
                start,
                end,
            });
        }
        if offset == end {
            return Ok(());
        }
        let keep = self.segment_containing(offset);
        let position = match self.segments[keep].locate(offset)? {
            Some((position, header)) if header.base_offset == offset => position,
            _ => return Err(StorageError::NotBatchBoundary { offset }),
        };
        self.truncations += 1;
        self.cut(keep, position, offset)
            .map_err(|error| self.fail(error))?;
        self.flushed_end = offset;
        Ok(())
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
        let mut out = BytesMut::new();
        for extent in self.locate(offset, max_bytes, true, self.end_offset())? {
            extent.read_into(&mut out)?;
        }
        Ok(out.into())
    }

    /// Locate whole batches, starting with the batch that contains `offset`, that start below
    /// offset `end` and fit in `max_bytes` together. The first batch is included even when it is
    /// larger than `max_bytes` if `at_least_one` is set.
    ///
    /// Only batch headers are read. The extents stay readable without the log, whatever is
    /// appended later; [`Extent`] says what a truncate does to them. The result is empty when
    /// `offset == end_offset()`.
    ///
    /// # Errors
    ///
    /// Returns [`StorageError::OffsetOutOfRange`] when `offset < start_offset()` or
    /// `offset > end_offset()`, and [`StorageError::Io`] if reading a header fails.
    pub fn locate(
        &self,
        offset: i64,
        max_bytes: usize,
        at_least_one: bool,
        end: i64,
    ) -> Result<Vec<Extent>, StorageError> {
        let (start, end_offset) = (self.start_offset(), self.end_offset());
        if offset < start || offset > end_offset {
            return Err(StorageError::OffsetOutOfRange {
                requested: offset,
                start,
                end: end_offset,
            });
        }

        let first = self.segment_containing(offset);
        let (mut budget, mut at_least_one) = (max_bytes, at_least_one);
        let mut extents = Vec::new();
        for (at, segment) in self.segments.iter().enumerate().skip(first) {
            let found = if at == first {
                segment.locate(offset)?
            } else {
                segment.batch_at(0)?
            };
            let Some((position, header)) = found else {
                continue;
            };
            let len = segment.extent_len(position, &header, budget, at_least_one, end)?;
            if len == 0 {
                break;
            }
            let size = usize::try_from(len).map_err(io::Error::other)?;
            extents.push(Extent {
                file: Arc::clone(&segment.log),
                position,
                len: size,
            });
            if position + len < segment.size {
                break;
            }
            budget = budget.saturating_sub(size);
            at_least_one = false;
        }
        Ok(extents)
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

    /// Offset below which everything is synced to stable storage; at most [`Log::end_offset`].
    #[must_use]
    pub fn flushed_end_offset(&self) -> i64 {
        self.flushed_end
    }

    /// Epoch of the last batch, if any.
    #[must_use]
    pub fn last_epoch(&self) -> Option<i32> {
        self.epochs.last().map(|start| start.epoch)
    }

    /// Where each epoch present in the log starts, in log order; the first starts at
    /// [`Log::start_offset`].
    #[must_use]
    pub fn epoch_starts(&self) -> &[EpochStart] {
        &self.epochs
    }

    /// Epoch of the batch containing `offset`, or of the last batch when `offset` is the end
    /// offset. `None` outside `[start_offset(), end_offset()]` or when the log is empty.
    #[must_use]
    pub fn epoch_at(&self, offset: i64) -> Option<i32> {
        if offset < self.start_offset() || offset > self.end_offset() {
            return None;
        }
        let next = self
            .epochs
            .partition_point(|start| start.start_offset <= offset);
        next.checked_sub(1).map(|at| self.epochs[at].epoch)
    }

    /// Kafka's `OffsetsForLeaderEpoch` answer for `epoch`: the largest epoch present that is
    /// `<= epoch`, and the end offset of `epoch`, which is where the next higher epoch present
    /// starts, or the end offset when there is none. `None` when no epoch `<= epoch` is present.
    #[must_use]
    pub fn epoch_end_offset(&self, epoch: i32) -> Option<(i32, i64)> {
        let next = self.epochs.partition_point(|start| start.epoch <= epoch);
        let found = self.epochs[..next].last()?;
        let end = self
            .epochs
            .get(next)
            .map_or(self.end_offset(), |start| start.start_offset);
        Some((found.epoch, end))
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

    fn check_epoch(&self, epoch: i32) -> Result<(), StorageError> {
        match self.last_epoch() {
            Some(last) if epoch < last => Err(StorageError::EpochRegression { last, found: epoch }),
            _ => Ok(()),
        }
    }

    /// Write a validated batch whose header holds its final offset and epoch.
    fn write(&mut self, batch: &[u8], header: &BatchHeader) -> Result<(), StorageError> {
        let opens_epoch = self.last_epoch() != Some(header.epoch);
        if opens_epoch {
            self.epochs.push(EpochStart {
                epoch: header.epoch,
                start_offset: header.base_offset,
            });
            // Synced before the batch is written, so no crash leaves a durable batch whose epoch the
            // checkpoint lacks; if the batch is lost instead, its entry lies at or past the
            // recovered end, which open drops.
            if let Err(error) = write_epochs(&self.dir, &self.epochs) {
                self.epochs.pop();
                return Err(self.fail(error));
            }
        }
        let written = self.write_bytes(batch, header);
        if written.is_err() && opens_epoch {
            self.epochs.pop();
        }
        written
    }

    fn write_bytes(&mut self, batch: &[u8], header: &BatchHeader) -> Result<(), StorageError> {
        if self.should_roll(header) {
            self.roll().map_err(|error| self.fail(error))?;
        }
        let index_interval_bytes = self.config.index_interval_bytes;
        let active = self.active_mut();
        if let Err(error) = active.log.write_all_at(batch, active.size) {
            return Err(self.fail(error));
        }
        active.record_batch(header, index_interval_bytes);
        Ok(())
    }

    /// The on-disk steps of [`Log::truncate`] in their crash-safe order; see the module docs.
    fn cut(&mut self, keep: usize, position: u64, offset: i64) -> io::Result<()> {
        // Newest first: a gap would make the sealed segment before it claim the missing offsets.
        while self.segments.len() > keep + 1 {
            if let Some(segment) = self.segments.pop() {
                segment.remove(&self.dir)?;
                sync_dir(&self.dir)?;
            }
        }
        // Before the fresh segment exists, since from then on this one is trusted as sealed.
        if position > 0 {
            self.segments[keep].cut(position, offset)?;
        }
        let fresh = Segment::create(&self.dir, offset)?;
        sync_dir(&self.dir)?;
        if position == 0 {
            self.segments.pop();
        }
        self.segments.push(fresh);
        let kept = self
            .epochs
            .partition_point(|start| start.start_offset < offset);
        // After the data: a crash before this leaves entries at or past the end, which open drops.
        // Before returning: a later append may reuse these offsets with an epoch the stale entries
        // would contradict.
        if kept < self.epochs.len() {
            self.epochs.truncate(kept);
            write_epochs(&self.dir, &self.epochs)?;
        }
        Ok(())
    }

    /// The epoch history of the recovered log, taken from `saved` as far as the batches confirm it.
    ///
    /// Entries at or past the end are dropped. An entry is confirmed when the batch at its start
    /// has its epoch and the batch before has the previous entry's; as epochs never decrease, two
    /// consecutive confirmed entries prove every batch between them has the first one's epoch, and
    /// the last entry is proven by the last batch. From the last confirmed entry on, the history is
    /// rebuilt from batch headers.
    fn reconcile_epochs(&self, mut saved: Vec<EpochStart>) -> io::Result<Vec<EpochStart>> {
        let (start, end) = (self.start_offset(), self.end_offset());
        saved.retain(|epoch| epoch.start_offset < end);
        if start == end {
            return Ok(Vec::new());
        }
        if let Some(first) = saved
            .partition_point(|epoch| epoch.start_offset <= start)
            .checked_sub(1)
        {
            saved.drain(..first);
            saved[0].start_offset = start;
        }
        let mut confirmed = 0;
        while confirmed < saved.len() && self.confirms(&saved, confirmed)? {
            confirmed += 1;
        }
        if confirmed == saved.len()
            && self.header_containing(end - 1)?.map(|header| header.epoch)
                == saved.last().map(|epoch| epoch.epoch)
        {
            return Ok(saved);
        }
        saved.truncate(confirmed);
        let from = saved.last().map_or(start, |epoch| epoch.start_offset);
        self.scan_epochs(from, &mut saved)?;
        Ok(saved)
    }

    fn confirms(&self, epochs: &[EpochStart], at: usize) -> io::Result<bool> {
        let entry = epochs[at];
        let starts_here = self
            .header_containing(entry.start_offset)?
            .is_some_and(|header| {
                header.base_offset == entry.start_offset && header.epoch == entry.epoch
            });
        let follows = match at.checked_sub(1) {
            None => entry.start_offset == self.start_offset(),
            Some(previous) => self
                .header_containing(entry.start_offset - 1)?
                .is_some_and(|header| header.epoch == epochs[previous].epoch),
        };
        Ok(starts_here && follows)
    }

    // ponytail: walks every batch header from the last confirmed epoch start, the whole log when the
    // checkpoint is missing, and again on every open of a log whose epochs go back (only one written
    // before the leader stamped them); bisecting over the sparse index would bound it by the epochs.
    fn scan_epochs(&self, from: i64, epochs: &mut Vec<EpochStart>) -> io::Result<()> {
        let first = self.segment_containing(from);
        for (at, segment) in self.segments.iter().enumerate().skip(first) {
            let mut position = if at == first {
                segment
                    .locate(from)?
                    .map_or(segment.size, |(position, _)| position)
            } else {
                0
            };
            while position < segment.size {
                let header = segment.header_at(position)?;
                if epochs.last().is_none_or(|last| header.epoch > last.epoch) {
                    epochs.push(EpochStart {
                        epoch: header.epoch,
                        start_offset: header.base_offset,
                    });
                }
                position += header.size();
            }
        }
        Ok(())
    }

    /// Header of the batch containing `offset`, if the log holds that offset.
    fn header_containing(&self, offset: i64) -> io::Result<Option<BatchHeader>> {
        if offset < self.start_offset() || offset >= self.end_offset() {
            return Ok(None);
        }
        let found = self.segments[self.segment_containing(offset)].locate(offset)?;
        Ok(found
            .map(|(_, header)| header)
            .filter(|header| header.base_offset <= offset))
    }

    /// Index of the last segment starting at or before `offset`, which must be `>= start_offset()`.
    fn segment_containing(&self, offset: i64) -> usize {
        self.segments
            .partition_point(|segment| segment.base_offset <= offset)
            - 1
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
        self.flushed_end = self.end_offset();
        let segment = Segment::create(&self.dir, self.end_offset())?;
        sync_dir(&self.dir)?;
        self.segments.push(segment);
        Ok(())
    }
}

#[cfg(feature = "failpoints")]
impl Log {
    /// Leaves on disk only what a power failure would: the active segment's log cut back to the
    /// flushed end, and its index emptied, since an active index is fsynced only when recovery
    /// trims it. Sealed segments were fsynced whole. Returns how many log bytes were cut.
    ///
    /// # Errors
    ///
    /// Returns the error of a failed read or truncation.
    // ponytail: one crash state, the harshest for the log; a torn last write, unsynced bytes that did reach the disk, and a lost directory entry are not simulated.
    pub fn lose_unsynced(&self) -> io::Result<u64> {
        let active = self.active();
        let synced = active
            .locate(self.flushed_end)?
            .map_or(active.size, |(position, _)| position);
        active.log.set_len(synced)?;
        active.index.set_len(0)?;
        Ok(active.size - synced)
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
            .field("flushed_end_offset", &self.flushed_end)
            .field("epochs", &self.epochs.len())
            .field("last_epoch", &self.last_epoch())
            .field("truncations", &self.truncations)
            .field("failed", &self.failed)
            .finish()
    }
}

/// A flush begun by [`Log::begin_flush`].
#[derive(Debug)]
pub struct Flush {
    file: Arc<File>,
    segment: i64,
    entries: usize,
    end: i64,
    truncations: u64,
}

impl Flush {
    /// Sync the active segment's log file. Blocking; the log need not be held.
    ///
    /// # Errors
    ///
    /// Returns the error of the failed sync, which [`Log::end_flush`] must be given.
    pub fn sync(&self) -> io::Result<()> {
        sync_log(&self.file)
    }
}

fn sync_log(file: &File) -> io::Result<()> {
    // fdatasync on Linux. On macOS std already issues fcntl(F_FULLFSYNC) here, the stricter
    // option that also drains the drive's write cache, rather than a plain fsync.
    file.sync_data()
}

/// Whole batches lying back to back in one segment file.
///
/// An extent holds its file open, so it stays readable after later appends, and after a truncate
/// deletes its segment. If a truncate cuts its segment short of the extent, reading fails with
/// [`io::ErrorKind::UnexpectedEof`]. Batches appended after a truncate always go to a new file, so
/// an extent never returns bytes other than those of the batches it was located on.
#[derive(Debug, Clone)]
pub struct Extent {
    file: Arc<File>,
    position: u64,
    len: usize,
}

impl Extent {
    /// Number of bytes.
    #[must_use]
    pub fn size(&self) -> usize {
        self.len
    }

    /// Append the bytes, exactly as stored, to `out`. Blocking; `out` is left as it was on error.
    ///
    /// # Errors
    ///
    /// Returns the error of the failed read.
    pub fn read_into(&self, out: &mut BytesMut) -> io::Result<()> {
        let start = out.len();
        out.resize(start + self.len, 0);
        let read = self.file.read_exact_at(&mut out[start..], self.position);
        if read.is_err() {
            out.truncate(start);
        }
        read
    }
}

/// Header fields of one record batch, as found by [`batch_frames`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BatchFrame {
    /// The batch's baseOffset.
    pub base_offset: i64,
    /// Offsets the batch spans, lastOffsetDelta + 1: its record count unless compacted.
    pub record_count: i64,
    /// The batch's partitionLeaderEpoch.
    pub epoch: i32,
    /// Bytes the batch takes, its 12-byte offset and length prefix included.
    pub len: usize,
}

/// Iterator returned by [`batch_frames`].
#[derive(Debug, Clone)]
pub struct BatchFrames<'a> {
    rest: &'a [u8],
}

impl Iterator for BatchFrames<'_> {
    type Item = Result<BatchFrame, StorageError>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.rest.is_empty() {
            return None;
        }
        match BatchHeader::frame(self.rest) {
            Ok((header, len)) => {
                self.rest = &self.rest[len..];
                Some(Ok(BatchFrame {
                    base_offset: header.base_offset,
                    record_count: i64::from(header.last_offset_delta) + 1,
                    epoch: header.epoch,
                    len,
                }))
            }
            Err(error) => {
                self.rest = &[];
                Some(Err(error))
            }
        }
    }
}

/// Walk back-to-back Kafka v2 record batches in `bytes`, as a follower receives them or
/// [`Extent`]s return them, yielding each batch's header fields.
///
/// Only headers are checked: length, magic and lastOffsetDelta. The CRC is not verified;
/// [`Log::append_replicated`] verifies it. A truncated or malformed frame yields
/// [`StorageError::InvalidBatch`] and ends the walk.
#[must_use]
pub fn batch_frames(bytes: &[u8]) -> BatchFrames<'_> {
    BatchFrames { rest: bytes }
}

/// Build a no-op control batch for a new leader to append at the start of its term: one control
/// record (key version 0, type 1, COMMIT; no value), occupying exactly one offset, stamped with
/// `epoch` and `timestamp_ms`, not transactional and with no producer. Its baseOffset is 0 until
/// appended. Not yet verified: that the Java consumer skips it.
#[must_use]
pub fn noop_control_batch(epoch: i32, timestamp_ms: i64) -> Vec<u8> {
    let len = BATCH_HEADER_LEN + CONTROL_RECORD.len();
    let batch_length = i32::try_from(len - LOG_OVERHEAD).unwrap_or(i32::MAX);
    let mut batch = Vec::with_capacity(len);
    batch.extend_from_slice(&0_i64.to_be_bytes()); // baseOffset
    batch.extend_from_slice(&batch_length.to_be_bytes());
    batch.extend_from_slice(&epoch.to_be_bytes());
    batch.extend_from_slice(&MAGIC_V2.to_be_bytes());
    batch.extend_from_slice(&[0; 4]); // crc, set below
    batch.extend_from_slice(&CONTROL_ATTRIBUTES.to_be_bytes());
    batch.extend_from_slice(&0_i32.to_be_bytes()); // lastOffsetDelta
    batch.extend_from_slice(&timestamp_ms.to_be_bytes()); // baseTimestamp
    batch.extend_from_slice(&timestamp_ms.to_be_bytes()); // maxTimestamp
    batch.extend_from_slice(&(-1_i64).to_be_bytes()); // producerId
    batch.extend_from_slice(&(-1_i16).to_be_bytes()); // producerEpoch
    batch.extend_from_slice(&(-1_i32).to_be_bytes()); // baseSequence
    batch.extend_from_slice(&1_i32.to_be_bytes()); // recordsCount
    batch.extend_from_slice(&CONTROL_RECORD);
    let crc = crc32c::crc32c(&batch[CRC_START..]);
    batch[CRC_AT..CRC_START].copy_from_slice(&crc.to_be_bytes());
    batch
}

struct Segment {
    base_offset: i64,
    log: Arc<File>,
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
    /// A new, empty segment in fresh files renamed over any of the same names, so a handle still
    /// open on an earlier file never sees this segment's bytes. The caller syncs the directory.
    fn create(dir: &Path, base_offset: i64) -> io::Result<Self> {
        let log = create_renamed(&segment_path(dir, base_offset, LOG_SUFFIX))?;
        let index = create_renamed(&segment_path(dir, base_offset, INDEX_SUFFIX))?;
        Ok(Self {
            base_offset,
            log: Arc::new(log),
            index,
            size: 0,
            next_offset: base_offset,
            entries: Vec::new(),
            durable_entries: 0,
            bytes_since_index: 0,
            max_timestamp: i64::MIN,
        })
    }

    fn open_sealed(dir: &Path, base_offset: i64, next_offset: i64) -> io::Result<Self> {
        let log = open_file(&segment_path(dir, base_offset, LOG_SUFFIX))?;
        let index = open_file(&segment_path(dir, base_offset, INDEX_SUFFIX))?;
        let size = log.metadata()?.len();
        let entries = load_index(&index, size)?;
        Ok(Self {
            base_offset,
            log: Arc::new(log),
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
        let log = open_file(&segment_path(dir, base_offset, LOG_SUFFIX))?;
        let index = open_file(&segment_path(dir, base_offset, INDEX_SUFFIX))?;
        let file_len = log.metadata()?.len();
        let mut entries = load_index(&index, file_len)?;
        while let Some(entry) = entries.last() {
            if starts_batch(&log, file_len, base_offset, entry)? {
                break;
            }
            entries.pop();
        }
        let index_len = (entries.len() * INDEX_ENTRY_LEN) as u64;
        if index.metadata()?.len() != index_len {
            index.set_len(index_len)?;
            index.sync_data()?;
        }

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
            log: Arc::new(log),
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
        // After a process crash the bytes past the last durable entry may be only in the page cache.
        if file_len > size {
            sync_log(&segment.log)?;
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
        sync_log(&self.log)?;
        self.write_index(self.entries.len())
    }

    /// Write the first `upto` index entries to the index file; the log bytes they cover must be
    /// synced already.
    fn write_index(&mut self, upto: usize) -> io::Result<()> {
        if self.durable_entries >= upto {
            return Ok(());
        }
        let pending: Vec<u8> = self.entries[self.durable_entries..upto]
            .iter()
            .flat_map(|entry| entry.encode())
            .collect();
        self.index
            .write_all_at(&pending, (self.durable_entries * INDEX_ENTRY_LEN) as u64)?;
        self.durable_entries = upto;
        Ok(())
    }

    fn seal(&mut self) -> io::Result<()> {
        self.flush()?;
        self.index.sync_data()
    }

    /// Cut the segment at `position`, where the batch at `offset` starts, and sync it. The log goes
    /// first, which also covers the entries still to be written; until the index is cut too, its
    /// entries past the cut point past the log's end, where loading ignores them.
    fn cut(&mut self, position: u64, offset: i64) -> io::Result<()> {
        self.log.set_len(position)?;
        let kept = self
            .entries
            .partition_point(|entry| u64::from(entry.position) < position);
        self.entries.truncate(kept);
        self.durable_entries = self.durable_entries.min(kept);
        self.size = position;
        self.next_offset = offset;
        sync_log(&self.log)?;
        self.write_index(kept)?;
        self.index.set_len((kept * INDEX_ENTRY_LEN) as u64)?;
        self.index.sync_data()
    }

    /// Delete the segment's files, the index first so a crash in between leaves no orphan index.
    /// The caller syncs the directory.
    fn remove(&self, dir: &Path) -> io::Result<()> {
        fs::remove_file(segment_path(dir, self.base_offset, INDEX_SUFFIX))?;
        fs::remove_file(segment_path(dir, self.base_offset, LOG_SUFFIX))
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

    /// Position and header of the first batch whose last offset is `>= offset`, if any.
    fn locate(&self, offset: i64) -> io::Result<Option<(u64, BatchHeader)>> {
        let relative_offset = offset - self.base_offset;
        let floor = self
            .entries
            .partition_point(|entry| i64::from(entry.relative_offset) <= relative_offset);
        let mut position = self.entry_position(floor);
        while position < self.size {
            let header = self.header_at(position)?;
            if header.last_offset() >= offset {
                return Ok(Some((position, header)));
            }
            position += header.size();
        }
        Ok(None)
    }

    /// Position and header of the batch at `position`, unless that is the end of the segment.
    fn batch_at(&self, position: u64) -> io::Result<Option<(u64, BatchHeader)>> {
        if position >= self.size {
            return Ok(None);
        }
        Ok(Some((position, self.header_at(position)?)))
    }

    /// Length of the whole batches from `position`, where the batch described by `first` starts,
    /// that start below offset `end` and fit in `budget` bytes. The first batch counts even when
    /// larger than `budget` if `at_least_one` is set.
    fn extent_len(
        &self,
        position: u64,
        first: &BatchHeader,
        budget: usize,
        at_least_one: bool,
        end: i64,
    ) -> io::Result<u64> {
        if first.base_offset >= end {
            return Ok(0);
        }
        let limit = position.saturating_add(u64::try_from(budget).unwrap_or(u64::MAX));
        let mut stop = position + first.size();
        if stop > limit {
            return Ok(if at_least_one { first.size() } else { 0 });
        }
        if self.size <= limit && self.next_offset <= end {
            return Ok(self.size - position);
        }
        // Index entries mark batch starts; jump to the last one that fits, then walk headers.
        let fitting = self.entries.partition_point(|entry| {
            u64::from(entry.position) <= limit
                && self.base_offset + i64::from(entry.relative_offset) <= end
        });
        if let Some(entry) = fitting.checked_sub(1).map(|at| self.entries[at]) {
            stop = stop.max(u64::from(entry.position));
        }
        while stop < self.size {
            let header = self.header_at(stop)?;
            if header.base_offset >= end || stop + header.size() > limit {
                break;
            }
            stop += header.size();
        }
        Ok(stop - position)
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
    epoch: i32,
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
            epoch: i32::from_be_bytes(field(bytes, EPOCH_AT)),
            magic: i8::from_be_bytes(field(bytes, MAGIC_AT)),
            crc: u32::from_be_bytes(field(bytes, CRC_AT)),
            last_offset_delta: i32::from_be_bytes(field(bytes, LAST_OFFSET_DELTA_AT)),
            max_timestamp: i64::from_be_bytes(field(bytes, MAX_TIMESTAMP_AT)),
        }
    }

    /// Header and length of the batch at the start of `bytes`, checking everything but the CRC.
    fn frame(bytes: &[u8]) -> Result<(Self, usize), StorageError> {
        if bytes.len() < BATCH_HEADER_LEN {
            return Err(StorageError::InvalidBatch(format!(
                "{} bytes is shorter than the {BATCH_HEADER_LEN}-byte batch header",
                bytes.len()
            )));
        }
        let header = Self::parse(bytes);
        if header.batch_length < MIN_BATCH_LENGTH {
            return Err(StorageError::InvalidBatch(format!(
                "batchLength {} is below the minimum {MIN_BATCH_LENGTH}",
                header.batch_length
            )));
        }
        let len = LOG_OVERHEAD + header.batch_length.unsigned_abs() as usize;
        if len > bytes.len() {
            return Err(StorageError::InvalidBatch(format!(
                "batchLength {} runs past the {} bytes that follow it",
                header.batch_length,
                bytes.len() - LOG_OVERHEAD
            )));
        }
        if header.magic != MAGIC_V2 {
            return Err(StorageError::InvalidBatch(format!(
                "magic {} is not {MAGIC_V2}",
                header.magic
            )));
        }
        if header.last_offset_delta < 0 {
            return Err(StorageError::InvalidBatch(format!(
                "lastOffsetDelta {} is negative",
                header.last_offset_delta
            )));
        }
        Ok((header, len))
    }

    fn validate(batch: &[u8]) -> Result<Self, StorageError> {
        let (header, len) = Self::frame(batch)?;
        if len != batch.len() {
            return Err(StorageError::InvalidBatch(format!(
                "batchLength {} does not match the {} bytes that follow it",
                header.batch_length,
                batch.len() - LOG_OVERHEAD
            )));
        }
        let crc = crc32c::crc32c(&batch[CRC_START..]);
        if crc != header.crc {
            return Err(StorageError::InvalidBatch(format!(
                "crc {:#010x} does not match computed {crc:#010x}",
                header.crc
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

/// The saved epoch history, empty when the checkpoint is missing or unreadable.
fn read_epochs(dir: &Path) -> io::Result<Vec<EpochStart>> {
    match fs::read(dir.join(EPOCHS_FILE)) {
        Ok(bytes) => Ok(parse_epochs(&bytes).unwrap_or_default()),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(Vec::new()),
        Err(error) => Err(error),
    }
}

fn parse_epochs(bytes: &[u8]) -> Option<Vec<EpochStart>> {
    let mut lines = std::str::from_utf8(bytes).ok()?.lines();
    if lines.next()? != EPOCHS_HEADER {
        return None;
    }
    let mut epochs: Vec<EpochStart> = Vec::new();
    for line in lines {
        let (epoch, start_offset) = line.split_once(' ')?;
        let entry = EpochStart {
            epoch: epoch.parse().ok()?,
            start_offset: start_offset.parse().ok()?,
        };
        if epochs.last().is_some_and(|last| {
            entry.epoch <= last.epoch || entry.start_offset <= last.start_offset
        }) {
            return None;
        }
        epochs.push(entry);
    }
    Some(epochs)
}

fn write_epochs(dir: &Path, epochs: &[EpochStart]) -> io::Result<()> {
    let text: String = std::iter::once(format!("{EPOCHS_HEADER}\n"))
        .chain(
            epochs
                .iter()
                .map(|start| format!("{} {}\n", start.epoch, start.start_offset)),
        )
        .collect();
    replace_file(dir, EPOCHS_FILE, text.as_bytes())
}

/// Durably replaces `dir/name` with `contents`: written to a temp file, fsynced, renamed over the
/// old file and the directory fsynced, so a crash leaves either the old or the new contents.
pub(crate) fn replace_file(dir: &Path, name: &str, contents: &[u8]) -> io::Result<()> {
    let tmp = dir.join(format!("{name}{TMP_SUFFIX}"));
    let mut file = File::create(&tmp)?;
    file.write_all(contents)?;
    file.sync_all()?;
    fs::rename(&tmp, dir.join(name))?;
    sync_dir(dir)
}

/// Segment base offsets in `dir`, sorted. Temporary files an interrupted rename left are removed.
fn segment_bases(dir: &Path) -> io::Result<Vec<i64>> {
    let mut bases = Vec::new();
    for entry in fs::read_dir(dir)? {
        let entry = entry?;
        let name = entry.file_name();
        let Some(name) = name.to_str() else {
            continue;
        };
        if name.ends_with(TMP_SUFFIX) {
            fs::remove_file(entry.path())?;
            continue;
        }
        let base = name
            .strip_suffix(LOG_SUFFIX)
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
pub(crate) fn create_dir_durably(dir: &Path) -> io::Result<()> {
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

fn open_file(path: &Path) -> io::Result<File> {
    OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(path)
}

/// A new empty file at `path`, created under a temporary name and renamed over whatever is there.
fn create_renamed(path: &Path) -> io::Result<File> {
    let mut tmp = path.as_os_str().to_owned();
    tmp.push(TMP_SUFFIX);
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(true)
        .open(&tmp)?;
    fs::rename(&tmp, path)?;
    Ok(file)
}

fn sync_dir(dir: &Path) -> io::Result<()> {
    File::open(dir)?.sync_all()
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::path::{Path, PathBuf};

    use bytes::Bytes;
    use kafka_protocol::records::RecordBatchDecoder;
    use tempfile::TempDir;

    use super::*;

    const UNLIMITED: usize = usize::MAX;

    fn build_batch(record_count: i32, max_timestamp: i64, payload_len: usize) -> Vec<u8> {
        let mut batch = vec![0; BATCH_HEADER_LEN + payload_len];
        let batch_length = i32::try_from(batch.len() - LOG_OVERHEAD).unwrap();
        batch[8..12].copy_from_slice(&batch_length.to_be_bytes());
        batch[12..16].copy_from_slice(&(-1_i32).to_be_bytes());
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

    /// `batch` as a leader stores it at `base_offset` in `epoch`.
    fn stamped(mut batch: Vec<u8>, base_offset: i64, epoch: i32) -> Vec<u8> {
        batch[..8].copy_from_slice(&base_offset.to_be_bytes());
        batch[EPOCH_AT..MAGIC_AT].copy_from_slice(&epoch.to_be_bytes());
        batch
    }

    fn config(segment_bytes: u64, index_interval_bytes: u32) -> LogConfig {
        LogConfig {
            segment_bytes,
            index_interval_bytes,
        }
    }

    fn append_all(log: &mut Log, batches: &mut [Vec<u8>]) -> Vec<i64> {
        append_in_epoch(log, batches, 0)
    }

    fn append_in_epoch(log: &mut Log, batches: &mut [Vec<u8>], epoch: i32) -> Vec<i64> {
        batches
            .iter_mut()
            .map(|batch| log.append(batch, epoch).unwrap())
            .collect()
    }

    fn read_all(log: &Log) -> Vec<u8> {
        log.read(log.start_offset(), UNLIMITED).unwrap()
    }

    fn read_extent(extent: &Extent) -> io::Result<Vec<u8>> {
        let mut out = BytesMut::new();
        extent.read_into(&mut out)?;
        Ok(out.to_vec())
    }

    fn starts(pairs: &[(i32, i64)]) -> Vec<EpochStart> {
        pairs
            .iter()
            .map(|&(epoch, start_offset)| EpochStart {
                epoch,
                start_offset,
            })
            .collect()
    }

    /// Epoch starts found by walking the stored batches, independently of the log's history.
    fn scanned_epochs(log: &Log) -> Vec<EpochStart> {
        let mut epochs: Vec<EpochStart> = Vec::new();
        for frame in batch_frames(&read_all(log)) {
            let frame = frame.unwrap();
            if epochs.last().is_none_or(|last| last.epoch != frame.epoch) {
                epochs.push(EpochStart {
                    epoch: frame.epoch,
                    start_offset: frame.base_offset,
                });
            }
        }
        epochs
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

    fn segment_names(dir: &Path) -> Vec<String> {
        segment_files(dir, LOG_SUFFIX)
            .iter()
            .map(|path| path.file_name().unwrap().to_str().unwrap().to_owned())
            .collect()
    }

    fn last_log_file(dir: &Path) -> PathBuf {
        segment_files(dir, LOG_SUFFIX).pop().unwrap()
    }

    fn copy_dir(from: &Path, to: &Path) {
        fs::create_dir_all(to).unwrap();
        for entry in fs::read_dir(from).unwrap() {
            let entry = entry.unwrap();
            fs::copy(entry.path(), to.join(entry.file_name())).unwrap();
        }
    }

    fn assert_invalid(log: &mut Log, mut batch: Vec<u8>) {
        let end = log.end_offset();
        assert!(matches!(
            log.append(&mut batch, 0),
            Err(StorageError::InvalidBatch(_))
        ));
        assert!(matches!(
            log.append_replicated(&stamped(batch, end, 0)),
            Err(StorageError::InvalidBatch(_))
        ));
        assert_eq!(log.end_offset(), end);
    }

    /// Configurations that put index entries on every batch, some batches and no batch,
    /// with and without segment rolls.
    const RECOVERY_CONFIGS: [(u64, u32); 4] =
        [(1 << 20, 0), (1 << 20, 150), (1 << 20, 4096), (250, 0)];

    /// Epoch starts of [`epoch_log`].
    const EPOCH_LOG: [(i32, i64); 3] = [(1, 0), (2, 5), (3, 9)];

    /// 12 flushed 100-byte batches in epochs 1 (offsets 0-4), 2 (5-8) and 3 (9-11); with
    /// `segment_bytes` 250 three to a segment, bases 0, 3, 6 and 9. Returns them as stored.
    fn epoch_log(dir: &Path, config: LogConfig) -> Vec<Vec<u8>> {
        let mut log = Log::open(dir, config).unwrap();
        let mut batches = uniform_batches(12);
        append_in_epoch(&mut log, &mut batches[..5], 1);
        append_in_epoch(&mut log, &mut batches[5..9], 2);
        append_in_epoch(&mut log, &mut batches[9..], 3);
        log.flush().unwrap();
        batches
    }

    #[test]
    fn open_creates_missing_directories() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("topic").join("0");
        let mut log = Log::open(&path, LogConfig::default()).unwrap();

        assert_eq!(log.append(&mut build_batch(1, 10, 10), 0).unwrap(), 0);
        assert!(segment_path(&path, 0, LOG_SUFFIX).is_file());
    }

    #[test]
    fn open_removes_leftover_temporary_files() {
        let dir = TempDir::new().unwrap();
        epoch_log(dir.path(), config(250, 0));
        for name in ["00000000000000000012.log.tmp", "epochs.tmp"] {
            fs::write(dir.path().join(name), b"partial").unwrap();
        }

        let log = Log::open(dir.path(), config(250, 0)).unwrap();
        assert_eq!(log.end_offset(), 12);
        assert_eq!(segment_files(dir.path(), TMP_SUFFIX), Vec::<PathBuf>::new());
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

        log.append(&mut first, 0).unwrap();
        log.append(&mut second, 0).unwrap();

        assert_eq!(i64::from_be_bytes(second[..8].try_into().unwrap()), 4);
        assert_eq!(log.read(4, UNLIMITED).unwrap(), second);
    }

    #[test]
    fn append_stamps_epoch_outside_crc() {
        let dir = TempDir::new().unwrap();
        let mut log = Log::open(dir.path(), LogConfig::default()).unwrap();
        let mut batch = build_batch(2, 10, 10);

        log.append(&mut batch, 7).unwrap();

        assert_eq!(i32::from_be_bytes(field(&batch, EPOCH_AT)), 7);
        assert_eq!(BatchHeader::validate(&batch).unwrap().epoch, 7);
        assert_eq!(read_all(&log), batch);
        assert_eq!(log.last_epoch(), Some(7));
    }

    #[test]
    fn append_rejects_decreasing_epoch() {
        let dir = TempDir::new().unwrap();
        let mut log = Log::open(dir.path(), LogConfig::default()).unwrap();
        log.append(&mut build_batch(1, 10, 10), 3).unwrap();

        assert!(matches!(
            log.append(&mut build_batch(1, 10, 10), 2),
            Err(StorageError::EpochRegression { last: 3, found: 2 })
        ));
        assert_eq!(log.end_offset(), 1);
        assert_eq!(log.append(&mut build_batch(1, 10, 10), 3).unwrap(), 1);
        assert_eq!(log.append(&mut build_batch(1, 10, 10), 5).unwrap(), 2);
        assert_eq!(log.epoch_starts(), starts(&[(3, 0), (5, 2)]));
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
        assert!(matches!(
            log.append(&mut [], 0),
            Err(StorageError::InvalidBatch(_))
        ));
    }

    #[test]
    fn append_rejects_negative_last_offset_delta() {
        let dir = TempDir::new().unwrap();
        let mut log = Log::open(dir.path(), LogConfig::default()).unwrap();
        assert_invalid(&mut log, build_batch(0, 10, 10));
    }

    #[test]
    fn append_replicated_stores_leader_bytes_unchanged() {
        let dir = TempDir::new().unwrap();
        let mut leader = Log::open(&dir.path().join("leader"), config(250, 0)).unwrap();
        let mut batches = uniform_batches(7);
        append_in_epoch(&mut leader, &mut batches[..3], 1);
        append_in_epoch(&mut leader, &mut batches[3..], 4);
        let bytes = read_all(&leader);

        let follower_dir = dir.path().join("follower");
        let mut follower = Log::open(&follower_dir, config(250, 0)).unwrap();
        let mut at = 0;
        for frame in batch_frames(&bytes) {
            let frame = frame.unwrap();
            follower
                .append_replicated(&bytes[at..at + frame.len])
                .unwrap();
            at += frame.len;
        }
        follower.flush().unwrap();

        assert_eq!(read_all(&follower), bytes);
        assert_eq!(follower.epoch_starts(), leader.epoch_starts());
        drop(follower);
        let follower = Log::open(&follower_dir, config(250, 0)).unwrap();
        assert_eq!(read_all(&follower), bytes);
        assert_eq!(follower.epoch_starts(), starts(&[(1, 0), (4, 3)]));
    }

    #[test]
    fn append_replicated_rejects_gaps_overlaps_and_decreasing_epochs() {
        let dir = TempDir::new().unwrap();
        let mut log = Log::open(dir.path(), LogConfig::default()).unwrap();
        let first = stamped(build_batch(3, 10, 10), 0, 2);
        log.append_replicated(&first).unwrap();
        let next = |base_offset, epoch| stamped(build_batch(1, 11, 10), base_offset, epoch);

        assert!(matches!(
            log.append_replicated(&next(4, 2)),
            Err(StorageError::OffsetMismatch {
                expected: 3,
                found: 4
            })
        ));
        assert!(matches!(
            log.append_replicated(&next(2, 2)),
            Err(StorageError::OffsetMismatch {
                expected: 3,
                found: 2
            })
        ));
        assert!(matches!(
            log.append_replicated(&next(3, 1)),
            Err(StorageError::EpochRegression { last: 2, found: 1 })
        ));
        assert_eq!((log.end_offset(), read_all(&log)), (3, first.clone()));

        log.append_replicated(&next(3, 2)).unwrap();
        log.append_replicated(&next(4, 5)).unwrap();
        assert_eq!(log.epoch_starts(), starts(&[(2, 0), (5, 4)]));
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
    fn locate_honours_end_offset_budget_and_at_least_one() {
        for (segment_bytes, index_interval_bytes) in RECOVERY_CONFIGS {
            let dir = TempDir::new().unwrap();
            let mut log =
                Log::open(dir.path(), config(segment_bytes, index_interval_bytes)).unwrap();
            let mut batches = uniform_batches(10);
            append_all(&mut log, &mut batches);
            let read = |offset, max_bytes, at_least_one, end| {
                let mut out = BytesMut::new();
                for extent in log.locate(offset, max_bytes, at_least_one, end).unwrap() {
                    extent.read_into(&mut out).unwrap();
                }
                out.to_vec()
            };

            assert_eq!(read(0, UNLIMITED, true, 4), batches[..4].concat());
            assert_eq!(read(3, 1000, true, 7), batches[3..7].concat());
            assert_eq!(read(2, 299, false, 10), batches[2..4].concat());
            assert_eq!(read(1, 800, false, 10), batches[1..9].concat());
            assert_eq!(read(2, 99, true, 10), batches[2]);
            assert_eq!(read(2, 99, false, 10), Vec::<u8>::new());
            assert_eq!(read(4, UNLIMITED, true, 4), Vec::<u8>::new());
            assert_eq!(read(10, UNLIMITED, true, 10), Vec::<u8>::new());
        }
    }

    #[test]
    fn located_batches_stay_readable_after_later_appends() {
        let dir = TempDir::new().unwrap();
        let mut log = Log::open(dir.path(), config(250, 0)).unwrap();
        let mut batches = uniform_batches(4);
        append_all(&mut log, &mut batches[..2]);
        let extents = log.locate(0, UNLIMITED, true, log.end_offset()).unwrap();
        append_all(&mut log, &mut batches[2..]);
        log.flush().unwrap();

        let mut out = BytesMut::new();
        for extent in &extents {
            extent.read_into(&mut out).unwrap();
        }
        assert_eq!(out, batches[..2].concat());
    }

    #[test]
    fn read_at_end_offset_returns_empty() {
        let dir = TempDir::new().unwrap();
        let mut log = Log::open(dir.path(), LogConfig::default()).unwrap();
        assert_eq!(log.read(0, UNLIMITED).unwrap(), Vec::<u8>::new());

        log.append(&mut build_batch(3, 10, 10), 0).unwrap();
        assert_eq!(log.read(3, UNLIMITED).unwrap(), Vec::<u8>::new());
    }

    #[test]
    fn read_rejects_offsets_outside_log() {
        let dir = TempDir::new().unwrap();
        let mut log = Log::open(dir.path(), LogConfig::default()).unwrap();
        log.append(&mut build_batch(3, 10, 10), 0).unwrap();

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

        assert_eq!(log.append(&mut build_batch(2, 10, 10), 0).unwrap(), 10);
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

            assert_eq!(
                segment_names(dir.path()),
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
            assert_eq!(log.append(&mut next, 0).unwrap(), 10);
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
                assert_eq!(log.append(&mut next, 0).unwrap(), 3);
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
                assert_eq!(log.append(&mut next, 0).unwrap(), 4);
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
            log.append(&mut build_batch(1, 10, 39), 0),
            Err(StorageError::Io(_))
        ));
        assert!(matches!(
            log.append_replicated(&stamped(build_batch(1, 10, 39), 2, 0)),
            Err(StorageError::Io(_))
        ));
        assert!(matches!(log.flush(), Err(StorageError::Io(_))));
        assert!(matches!(log.truncate(1), Err(StorageError::Io(_))));
        assert_eq!(log.end_offset(), 2);
        assert_eq!(log.read(0, UNLIMITED).unwrap(), batches.concat());
        drop(log);

        let mut log = Log::open(dir.path(), LogConfig::default()).unwrap();
        assert_eq!(log.read(0, UNLIMITED).unwrap(), batches.concat());
        assert_eq!(log.append(&mut build_batch(1, 10, 39), 0).unwrap(), 2);
        log.flush().unwrap();
        log.truncate(1).unwrap();
        assert_eq!(log.end_offset(), 1);
    }

    #[test]
    fn truncate_within_segment_keeps_earlier_batches() {
        for (segment_bytes, index_interval_bytes) in RECOVERY_CONFIGS {
            let dir = TempDir::new().unwrap();
            let config = config(segment_bytes, index_interval_bytes);
            let mut log = Log::open(dir.path(), config).unwrap();
            let mut batches = uniform_batches(10);
            append_all(&mut log, &mut batches);

            log.truncate(7).unwrap();

            assert_eq!((log.start_offset(), log.end_offset()), (0, 7));
            assert_eq!(read_all(&log), batches[..7].concat());
            assert_eq!(log.offset_for_timestamp(1006), Some(6));
            assert_eq!(log.offset_for_timestamp(1007), None);
            drop(log);
            let log = Log::open(dir.path(), config).unwrap();
            assert_eq!(log.end_offset(), 7);
            assert_eq!(read_all(&log), batches[..7].concat());
            for (offset, batch) in (0..7).zip(&batches) {
                assert_eq!(&log.read(offset, 0).unwrap(), batch);
            }
        }
    }

    #[test]
    fn truncate_at_segment_boundary_replaces_that_segment() {
        let dir = TempDir::new().unwrap();
        let mut log = Log::open(dir.path(), config(250, 0)).unwrap();
        let mut batches = uniform_batches(8);
        append_all(&mut log, &mut batches);

        log.truncate(6).unwrap();

        assert_eq!(log.end_offset(), 6);
        assert_eq!(
            segment_names(dir.path()),
            [
                "00000000000000000000.log",
                "00000000000000000003.log",
                "00000000000000000006.log"
            ]
        );
        assert_eq!(
            fs::metadata(segment_path(dir.path(), 6, LOG_SUFFIX))
                .unwrap()
                .len(),
            0
        );
        assert_eq!(read_all(&log), batches[..6].concat());
        drop(log);
        let log = Log::open(dir.path(), config(250, 0)).unwrap();
        assert_eq!(read_all(&log), batches[..6].concat());
    }

    #[test]
    fn truncate_across_several_segments_cuts_the_one_holding_the_offset() {
        let dir = TempDir::new().unwrap();
        let mut log = Log::open(dir.path(), config(250, 0)).unwrap();
        let mut batches = uniform_batches(11);
        append_all(&mut log, &mut batches);

        log.truncate(4).unwrap();

        assert_eq!(log.end_offset(), 4);
        assert_eq!(
            segment_names(dir.path()),
            [
                "00000000000000000000.log",
                "00000000000000000003.log",
                "00000000000000000004.log"
            ]
        );
        let sizes: Vec<u64> = segment_files(dir.path(), LOG_SUFFIX)
            .iter()
            .map(|path| fs::metadata(path).unwrap().len())
            .collect();
        assert_eq!(sizes, [300, 100, 0]);
        assert_eq!(
            fs::metadata(segment_path(dir.path(), 3, INDEX_SUFFIX))
                .unwrap()
                .len(),
            INDEX_ENTRY_LEN as u64
        );
        assert_eq!(read_all(&log), batches[..4].concat());
        drop(log);
        let log = Log::open(dir.path(), config(250, 0)).unwrap();
        assert_eq!(read_all(&log), batches[..4].concat());
    }

    #[test]
    fn truncate_to_start_empties_the_log_but_keeps_its_start() {
        for start in [0, 10] {
            let dir = TempDir::new().unwrap();
            fs::write(segment_path(dir.path(), start, LOG_SUFFIX), b"").unwrap();
            let mut log = Log::open(dir.path(), config(250, 0)).unwrap();
            append_in_epoch(&mut log, &mut uniform_batches(5), 2);

            log.truncate(start).unwrap();

            assert_eq!((log.start_offset(), log.end_offset()), (start, start));
            assert_eq!(log.last_epoch(), None);
            assert_eq!(segment_files(dir.path(), LOG_SUFFIX).len(), 1);
            drop(log);
            let mut log = Log::open(dir.path(), config(250, 0)).unwrap();
            assert_eq!((log.start_offset(), log.end_offset()), (start, start));
            assert_eq!(log.epoch_starts(), []);
            assert_eq!(log.append(&mut build_batch(1, 10, 10), 1).unwrap(), start);
        }
    }

    #[test]
    fn truncate_to_end_is_a_no_op() {
        let dir = TempDir::new().unwrap();
        let mut log = Log::open(dir.path(), config(250, 0)).unwrap();
        let mut batches = uniform_batches(5);
        append_all(&mut log, &mut batches);
        let names = segment_names(dir.path());

        log.truncate(5).unwrap();

        assert_eq!(log.end_offset(), 5);
        assert_eq!(log.flushed_end_offset(), 3);
        assert_eq!(segment_names(dir.path()), names);
        assert_eq!(read_all(&log), batches.concat());
    }

    #[test]
    fn truncate_rejects_offsets_inside_batches_or_outside_the_log() {
        let dir = TempDir::new().unwrap();
        fs::write(segment_path(dir.path(), 10, LOG_SUFFIX), b"").unwrap();
        let mut log = Log::open(dir.path(), LogConfig::default()).unwrap();
        log.append(&mut build_batch(3, 10, 10), 0).unwrap();
        log.append(&mut build_batch(2, 10, 10), 0).unwrap();

        for inside in [11, 12, 14] {
            assert!(matches!(
                log.truncate(inside),
                Err(StorageError::NotBatchBoundary { offset }) if offset == inside
            ));
        }
        for outside in [9, 16, i64::MIN] {
            assert!(matches!(
                log.truncate(outside),
                Err(StorageError::OffsetOutOfRange { requested, start: 10, end: 15 }) if requested == outside
            ));
        }
        assert_eq!(log.end_offset(), 15);
        log.truncate(13).unwrap();
        assert_eq!(log.end_offset(), 13);
    }

    #[test]
    fn truncate_then_append_new_epoch_survives_reopen() {
        for (segment_bytes, index_interval_bytes) in RECOVERY_CONFIGS {
            for offset in [0, 3, 4, 5, 9, 11] {
                let dir = TempDir::new().unwrap();
                let config = config(segment_bytes, index_interval_bytes);
                let old = epoch_log(dir.path(), config);
                let mut log = Log::open(dir.path(), config).unwrap();

                log.truncate(offset).unwrap();
                let mut fresh: Vec<Vec<u8>> =
                    (0..4).map(|at| build_batch(2, 5000 + at, 17)).collect();
                append_in_epoch(&mut log, &mut fresh, 6);
                log.flush().unwrap();
                drop(log);

                let log = Log::open(dir.path(), config).unwrap();
                let kept = usize::try_from(offset).unwrap();
                assert_eq!(
                    read_all(&log),
                    [old[..kept].concat(), fresh.concat()].concat(),
                    "truncated to {offset}"
                );
                for (at, batch) in (offset..).step_by(2).zip(&fresh) {
                    assert_eq!(&log.read(at + 1, 0).unwrap(), batch);
                }
                let mut expected: Vec<EpochStart> = starts(&EPOCH_LOG)
                    .into_iter()
                    .filter(|start| start.start_offset < offset)
                    .collect();
                expected.push(EpochStart {
                    epoch: 6,
                    start_offset: offset,
                });
                assert_eq!(log.epoch_starts(), expected, "truncated to {offset}");
                assert_eq!(log.end_offset(), offset + 8);
            }
        }
    }

    #[test]
    fn truncate_leaves_earlier_extents_their_own_bytes_or_eof() {
        let dir = TempDir::new().unwrap();
        let mut log = Log::open(dir.path(), config(250, 0)).unwrap();
        let mut batches = uniform_batches(11);
        append_all(&mut log, &mut batches);
        let extent =
            |log: &Log, offset| log.locate(offset, 0, true, log.end_offset()).unwrap()[0].clone();
        let whole = log.locate(0, UNLIMITED, true, 11).unwrap();
        let (third, fifth, seventh) = (extent(&log, 3), extent(&log, 5), extent(&log, 7));

        log.truncate(9).unwrap();
        log.truncate(4).unwrap();
        let mut fresh: Vec<Vec<u8>> = (0..9).map(|at| build_batch(1, 9000 + at, 61)).collect();
        append_in_epoch(&mut log, &mut fresh, 1);
        log.flush().unwrap();

        assert_eq!(read_extent(&whole[0]).unwrap(), batches[..3].concat());
        assert_eq!(read_extent(&third).unwrap(), batches[3]);
        for cut in [&whole[1], &fifth] {
            assert_eq!(
                read_extent(cut).unwrap_err().kind(),
                io::ErrorKind::UnexpectedEof
            );
        }
        assert_eq!(read_extent(&whole[2]).unwrap(), batches[6..9].concat());
        assert_eq!(read_extent(&seventh).unwrap(), batches[7]);
        assert_eq!(read_extent(&whole[3]).unwrap(), batches[9..].concat());
        assert_eq!(
            read_all(&log),
            [batches[..4].concat(), fresh.concat()].concat()
        );
    }

    /// Directories holding each on-disk state a crash during `truncate(offset)` of
    /// [`epoch_log`] can leave, built step by step from `before`, then `after` with a stale
    /// checkpoint or a stale index.
    fn truncate_crash_states(
        before: &Path,
        after: &Path,
        offset: i64,
        root: &Path,
    ) -> Vec<(String, PathBuf)> {
        let base = offset / 3 * 3;
        let position = u64::try_from(offset - base).unwrap() * 100;
        let progress = root.join("progress");
        copy_dir(before, &progress);
        let mut states = Vec::new();
        let mut snapshot = |name: &str| {
            let dir = root.join(format!("state-{}", states.len()));
            copy_dir(&progress, &dir);
            states.push((name.to_owned(), dir));
        };
        snapshot("before");
        let later: Vec<i64> = (base + 3..12).step_by(3).collect();
        for later in later.into_iter().rev() {
            fs::remove_file(segment_path(&progress, later, INDEX_SUFFIX)).unwrap();
            snapshot(&format!("index {later} removed"));
            fs::remove_file(segment_path(&progress, later, LOG_SUFFIX)).unwrap();
            snapshot(&format!("segment {later} removed"));
        }
        let (log_path, index_path) = (
            segment_path(&progress, base, LOG_SUFFIX),
            segment_path(&progress, base, INDEX_SUFFIX),
        );
        if position > 0 {
            let file = OpenOptions::new().write(true).open(&log_path).unwrap();
            file.set_len(position).unwrap();
            snapshot("log cut, index stale");
            let index = fs::read(&index_path).unwrap();
            let kept: Vec<u8> = index
                .chunks(INDEX_ENTRY_LEN)
                .filter(|entry| u64::from(IndexEntry::decode(entry).position) < position)
                .flatten()
                .copied()
                .collect();
            fs::write(&index_path, kept).unwrap();
            snapshot("index cut");
            fs::write(segment_path(&progress, offset, LOG_SUFFIX), b"").unwrap();
            snapshot("fresh log, no index");
            fs::write(segment_path(&progress, offset, INDEX_SUFFIX), b"").unwrap();
        } else {
            let index = fs::read(&index_path).unwrap();
            fs::write(&index_path, b"").unwrap();
            snapshot("index replaced, old log");
            fs::write(&log_path, b"").unwrap();
            snapshot("both replaced");
            fs::write(&index_path, index).unwrap();
            snapshot("log replaced, old index");
            fs::write(&index_path, b"").unwrap();
        }
        snapshot("fresh segment, stale checkpoint");
        fs::copy(after.join(EPOCHS_FILE), progress.join(EPOCHS_FILE)).unwrap();
        snapshot("checkpoint rewritten");
        let stale_index = format!("{base:020}{INDEX_SUFFIX}");
        for (name, stale) in [
            ("stale-checkpoint", EPOCHS_FILE),
            ("stale-index", &stale_index),
        ] {
            let dir = root.join(name);
            copy_dir(after, &dir);
            fs::copy(before.join(stale), dir.join(stale)).unwrap();
            states.push((format!("after, {name}"), dir));
        }
        states
    }

    /// Reopen a crash state of `truncate(offset)` of [`epoch_log`]: it must hold a prefix of the
    /// old log reaching at least `offset`, with a history matching its batches, and accept the
    /// truncate and a new epoch at `offset` that survive another reopen.
    fn check_crash_state(dir: &Path, config: LogConfig, offset: i64, old: &[Vec<u8>], name: &str) {
        let mut log = Log::open(dir, config).unwrap();
        let end = log.end_offset();
        assert!(end >= offset, "{name}: end {end} is below {offset}");
        let recovered = usize::try_from(end).unwrap();
        assert_eq!(read_all(&log), old[..recovered].concat(), "{name}");
        assert_eq!(log.epoch_starts(), scanned_epochs(&log), "{name}");
        assert_eq!(log.flushed_end_offset(), end, "{name}");

        log.truncate(offset).unwrap();
        let mut fresh: Vec<Vec<u8>> = (0..3).map(|at| build_batch(2, 7000 + at, 17)).collect();
        append_in_epoch(&mut log, &mut fresh, 4);
        log.flush().unwrap();
        drop(log);

        let log = Log::open(dir, config).unwrap();
        let kept = usize::try_from(offset).unwrap();
        let expected_bytes = [old[..kept].concat(), fresh.concat()].concat();
        assert_eq!(read_all(&log), expected_bytes, "{name}: after new appends");
        let mut expected: Vec<EpochStart> = starts(&EPOCH_LOG)
            .into_iter()
            .filter(|start| start.start_offset < offset)
            .collect();
        expected.push(EpochStart {
            epoch: 4,
            start_offset: offset,
        });
        assert_eq!(log.epoch_starts(), expected, "{name}: after new appends");
    }

    #[test]
    fn truncate_crash_states_reopen_to_a_prefix_and_accept_a_new_epoch() {
        for config in [config(250, 0), config(250, 4096)] {
            for offset in [0, 4, 5, 6, 7, 10] {
                let root = TempDir::new().unwrap();
                let before = root.path().join("before");
                let old = epoch_log(&before, config);
                let after = root.path().join("after");
                copy_dir(&before, &after);
                Log::open(&after, config).unwrap().truncate(offset).unwrap();

                for (name, state) in truncate_crash_states(&before, &after, offset, root.path()) {
                    let name = format!("truncate({offset}) {config:?}: {name}");
                    check_crash_state(&state, config, offset, &old, &name);
                }
            }
        }
    }

    #[test]
    fn epoch_history_answers_queries_across_epochs() {
        let dir = TempDir::new().unwrap();
        let mut log = Log::open(dir.path(), LogConfig::default()).unwrap();
        assert_eq!(
            (log.last_epoch(), log.epoch_at(0), log.epoch_end_offset(5)),
            (None, None, None)
        );
        for (records, epoch) in [(3, 1), (1, 1), (5, 3), (2, 7)] {
            log.append(&mut build_batch(records, 10, 10), epoch)
                .unwrap();
        }

        for log in [log, Log::open(dir.path(), LogConfig::default()).unwrap()] {
            assert_eq!(log.epoch_starts(), starts(&[(1, 0), (3, 4), (7, 9)]));
            assert_eq!(log.last_epoch(), Some(7));
            let at: Vec<Option<i32>> = [-1, 0, 2, 3, 4, 8, 9, 10, 11, 12]
                .into_iter()
                .map(|offset| log.epoch_at(offset))
                .collect();
            assert_eq!(
                at,
                [
                    None,
                    Some(1),
                    Some(1),
                    Some(1),
                    Some(3),
                    Some(3),
                    Some(7),
                    Some(7),
                    Some(7),
                    None
                ]
            );
            let ends: Vec<Option<(i32, i64)>> = [0, 1, 2, 3, 6, 7, 100]
                .into_iter()
                .map(|epoch| log.epoch_end_offset(epoch))
                .collect();
            assert_eq!(
                ends,
                [
                    None,
                    Some((1, 4)),
                    Some((1, 4)),
                    Some((3, 9)),
                    Some((3, 9)),
                    Some((7, 11)),
                    Some((7, 11))
                ]
            );
        }
    }

    #[test]
    fn epoch_history_after_truncate_mid_epoch_and_at_epoch_boundary() {
        let dir = TempDir::new().unwrap();
        epoch_log(dir.path(), config(250, 0));
        let mut log = Log::open(dir.path(), config(250, 0)).unwrap();

        log.truncate(7).unwrap();
        assert_eq!(log.epoch_starts(), starts(&[(1, 0), (2, 5)]));
        assert_eq!(log.epoch_end_offset(3), Some((2, 7)));
        assert_eq!(read_epochs(dir.path()).unwrap(), starts(&[(1, 0), (2, 5)]));

        log.truncate(5).unwrap();
        assert_eq!(log.epoch_starts(), starts(&[(1, 0)]));
        assert_eq!((log.last_epoch(), log.epoch_at(5)), (Some(1), Some(1)));
        assert_eq!(log.epoch_end_offset(2), Some((1, 5)));
        drop(log);

        let mut log = Log::open(dir.path(), config(250, 0)).unwrap();
        assert_eq!(log.epoch_starts(), starts(&[(1, 0)]));
        assert_eq!(read_epochs(dir.path()).unwrap(), starts(&[(1, 0)]));
        log.truncate(0).unwrap();
        assert_eq!(log.epoch_starts(), []);
        assert_eq!(read_epochs(dir.path()).unwrap(), []);
    }

    #[test]
    fn epoch_history_reopen_corrects_missing_behind_ahead_or_wrong_checkpoint() {
        let checkpoints: [(&str, &[u8]); 9] = [
            ("missing", b""),
            ("garbage", b"\xffnot a checkpoint"),
            ("behind by two", b"redcrab epochs 1\n1 0\n"),
            ("behind by one", b"redcrab epochs 1\n1 0\n2 5\n"),
            ("ahead", b"redcrab epochs 1\n1 0\n2 5\n3 9\n4 12\n9 40\n"),
            ("middle missing", b"redcrab epochs 1\n1 0\n3 9\n"),
            ("start moved", b"redcrab epochs 1\n1 0\n2 6\n3 9\n"),
            ("wrong epoch", b"redcrab epochs 1\n0 0\n2 5\n3 9\n"),
            ("starts late", b"redcrab epochs 1\n2 5\n3 9\n"),
        ];
        let dir = TempDir::new().unwrap();
        epoch_log(dir.path(), config(250, 0));
        for (name, checkpoint) in checkpoints {
            let path = dir.path().join(EPOCHS_FILE);
            if checkpoint.is_empty() {
                fs::remove_file(&path).unwrap();
            } else {
                fs::write(&path, checkpoint).unwrap();
            }

            let log = Log::open(dir.path(), config(250, 0)).unwrap();
            assert_eq!(log.epoch_starts(), starts(&EPOCH_LOG), "{name}");
            assert_eq!(log.end_offset(), 12, "{name}");
            assert_eq!(
                read_epochs(dir.path()).unwrap(),
                starts(&EPOCH_LOG),
                "{name}"
            );
        }
    }

    #[test]
    fn epoch_history_reopen_drops_epoch_whose_batch_was_lost() {
        let dir = TempDir::new().unwrap();
        epoch_log(dir.path(), config(250, 0));
        let mut log = Log::open(dir.path(), config(250, 0)).unwrap();
        log.append(&mut build_batch(1, 10, 39), 8).unwrap();
        assert_eq!(read_epochs(dir.path()).unwrap().last().unwrap().epoch, 8);
        drop(log);
        let active = last_log_file(dir.path());
        let len = fs::metadata(&active).unwrap().len();
        let file = OpenOptions::new().write(true).open(&active).unwrap();
        file.set_len(len - 100).unwrap();

        let log = Log::open(dir.path(), config(250, 0)).unwrap();
        assert_eq!(log.end_offset(), 12);
        assert_eq!(log.epoch_starts(), starts(&EPOCH_LOG));
        assert_eq!(read_epochs(dir.path()).unwrap(), starts(&EPOCH_LOG));
    }

    #[test]
    fn epoch_history_reopen_corrects_stale_checkpoint_after_truncate() {
        let dir = TempDir::new().unwrap();
        epoch_log(dir.path(), config(250, 0));
        let stale = fs::read(dir.path().join(EPOCHS_FILE)).unwrap();
        let mut log = Log::open(dir.path(), config(250, 0)).unwrap();
        log.truncate(5).unwrap();
        append_in_epoch(&mut log, &mut uniform_batches(6), 1);
        log.flush().unwrap();
        drop(log);
        fs::write(dir.path().join(EPOCHS_FILE), stale).unwrap();

        let log = Log::open(dir.path(), config(250, 0)).unwrap();
        assert_eq!(log.end_offset(), 11);
        assert_eq!(log.epoch_starts(), starts(&[(1, 0)]));
        assert_eq!(log.epoch_end_offset(3), Some((1, 11)));
    }

    #[test]
    fn flushed_end_offset_follows_flush_roll_truncate_and_reopen() {
        let dir = TempDir::new().unwrap();
        let mut log = Log::open(dir.path(), config(250, 0)).unwrap();
        assert_eq!(log.flushed_end_offset(), 0);
        let mut batches = uniform_batches(9);

        append_all(&mut log, &mut batches[..2]);
        assert_eq!((log.end_offset(), log.flushed_end_offset()), (2, 0));
        log.flush().unwrap();
        assert_eq!(log.flushed_end_offset(), 2);

        append_all(&mut log, &mut batches[2..4]);
        assert_eq!(
            (log.end_offset(), log.flushed_end_offset()),
            (4, 3),
            "rolled"
        );
        let flush = log.begin_flush().unwrap();
        log.end_flush(&flush, flush.sync()).unwrap();
        assert_eq!(log.flushed_end_offset(), 4);

        append_all(&mut log, &mut batches[4..6]);
        log.truncate(5).unwrap();
        assert_eq!((log.end_offset(), log.flushed_end_offset()), (5, 5));
        append_all(&mut log, &mut batches[6..]);
        assert_eq!((log.end_offset(), log.flushed_end_offset()), (8, 5));
        drop(log);

        let log = Log::open(dir.path(), config(250, 0)).unwrap();
        assert_eq!((log.end_offset(), log.flushed_end_offset()), (8, 8));
    }

    #[test]
    fn end_flush_begun_before_truncate_claims_nothing_after_it() {
        let dir = TempDir::new().unwrap();
        let mut log = Log::open(dir.path(), config(1 << 20, 0)).unwrap();
        append_all(&mut log, &mut uniform_batches(5));
        let flush = log.begin_flush().unwrap();

        log.truncate(2).unwrap();
        append_all(&mut log, &mut uniform_batches(1));
        log.end_flush(&flush, flush.sync()).unwrap();

        assert_eq!((log.end_offset(), log.flushed_end_offset()), (3, 2));
        log.flush().unwrap();
        assert_eq!(log.flushed_end_offset(), 3);
    }

    #[test]
    fn flush_claims_only_what_was_appended_before_it_began() {
        let dir = TempDir::new().unwrap();
        let mut log = Log::open(dir.path(), config(450, 0)).unwrap();
        let mut batches = uniform_batches(8);
        append_all(&mut log, &mut batches[..2]);

        let flush = log.begin_flush().unwrap();
        append_all(&mut log, &mut batches[2..6]);
        log.end_flush(&flush, flush.sync()).unwrap();
        assert_eq!(
            (log.end_offset(), log.flushed_end_offset()),
            (6, 5),
            "the roll synced the sealed segment"
        );

        let flush = log.begin_flush().unwrap();
        append_all(&mut log, &mut batches[6..]);
        log.end_flush(&flush, flush.sync()).unwrap();
        assert_eq!((log.end_offset(), log.flushed_end_offset()), (8, 6));
        log.flush().unwrap();
        drop(log);

        let log = Log::open(dir.path(), config(450, 0)).unwrap();
        assert_eq!((log.end_offset(), log.flushed_end_offset()), (8, 8));
        assert_eq!(log.read(0, UNLIMITED).unwrap().len(), 800);
    }

    #[test]
    fn batch_frames_walks_back_to_back_batches() {
        let batches = [
            stamped(build_batch(3, 10, 20), 100, 2),
            stamped(build_batch(1, 11, 0), 103, 2),
            stamped(build_batch(5, 12, 7), 104, 9),
        ];
        let frames: Vec<BatchFrame> = batch_frames(&batches.concat())
            .collect::<Result<_, _>>()
            .unwrap();

        assert_eq!(
            frames,
            [
                BatchFrame {
                    base_offset: 100,
                    record_count: 3,
                    epoch: 2,
                    len: 81
                },
                BatchFrame {
                    base_offset: 103,
                    record_count: 1,
                    epoch: 2,
                    len: 61
                },
                BatchFrame {
                    base_offset: 104,
                    record_count: 5,
                    epoch: 9,
                    len: 68
                },
            ]
        );
        assert_eq!(batch_frames(&[]).count(), 0);
    }

    #[test]
    fn batch_frames_rejects_truncated_or_malformed_frames_and_stops() {
        let good = build_batch(2, 10, 10);
        let mut bad_crc = good.clone();
        bad_crc[BATCH_HEADER_LEN] ^= 1;
        assert!(
            batch_frames(&bad_crc).all(|frame| frame.is_ok()),
            "CRC is not checked"
        );

        let mut short_length = good.clone();
        short_length[BATCH_LENGTH_AT..EPOCH_AT].copy_from_slice(&48_i32.to_be_bytes());
        let mut bad_magic = good.clone();
        bad_magic[MAGIC_AT] = 1;
        let negative_delta = build_batch(0, 10, 10);
        for broken in [
            &good[..BATCH_HEADER_LEN - 1],
            &good[..good.len() - 1],
            &short_length,
            &bad_magic,
            &negative_delta,
        ] {
            let bytes = [good.as_slice(), broken].concat();
            let mut frames = batch_frames(&bytes);
            assert!(frames.next().unwrap().is_ok());
            assert!(matches!(
                frames.next(),
                Some(Err(StorageError::InvalidBatch(_)))
            ));
            assert!(frames.next().is_none());
        }
        let bytes = [good.as_slice(), &bad_magic, &good].concat();
        let kinds: Vec<bool> = batch_frames(&bytes).map(|frame| frame.is_ok()).collect();
        assert_eq!(kinds, [true, false], "the walk ends at the first bad frame");
    }

    #[test]
    fn noop_control_batch_passes_validation_and_takes_one_offset() {
        let dir = TempDir::new().unwrap();
        let mut log = Log::open(dir.path(), LogConfig::default()).unwrap();
        log.append(&mut build_batch(2, 10, 10), 3).unwrap();
        let mut control = noop_control_batch(4, 1_700_000_000_000);
        BatchHeader::validate(&control).unwrap();

        assert_eq!(log.append(&mut control, 4).unwrap(), 2);
        assert_eq!((log.end_offset(), log.last_epoch()), (3, Some(4)));
        let frame = batch_frames(&control).next().unwrap().unwrap();
        assert_eq!(
            (frame.base_offset, frame.record_count, frame.epoch),
            (2, 1, 4)
        );

        let decoded = RecordBatchDecoder::decode(&mut Bytes::from(control.clone())).unwrap();
        let [record] = decoded.records.as_slice() else {
            panic!("expected one record, got {}", decoded.records.len());
        };
        assert!(record.control && !record.transactional);
        assert_eq!((record.offset, record.partition_leader_epoch), (2, 4));
        assert_eq!(
            (record.timestamp, record.producer_id),
            (1_700_000_000_000, -1)
        );
        assert_eq!(record.key.as_deref(), Some(&[0, 0, 0, 1][..]));
        assert_eq!(record.value, None);

        let mut follower = Log::open(&dir.path().join("follower"), LogConfig::default()).unwrap();
        follower
            .append_replicated(&noop_control_batch(1, 5))
            .unwrap();
        assert_eq!((follower.end_offset(), follower.last_epoch()), (1, Some(1)));
    }

    #[test]
    fn log_is_send_and_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<Log>();
    }
}
