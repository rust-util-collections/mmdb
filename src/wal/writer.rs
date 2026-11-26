//! WAL writer: appends records to a WAL file with block-based fragmentation.

use smallvec::SmallVec;
use std::fs::{File, OpenOptions};
use std::io::{self, BufWriter, IoSlice, Seek, SeekFrom, Write};
use std::path::Path;

use crate::error::{Result, ResultExt};
use crate::wal::record::*;

#[cfg(all(test, unix))]
type SyncObserver = Box<dyn FnMut(&File, u64)>;

#[cfg(all(test, unix))]
thread_local! {
    pub(crate) static OBSERVE_SYNC_DATA: std::cell::RefCell<Option<SyncObserver>> = const { std::cell::RefCell::new(None) };
}

#[cfg(test)]
thread_local! {
    pub(crate) static FAIL_NEXT_SYNC: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
    /// Makes the next `add_record` on this thread fail with this I/O kind.
    pub(crate) static FAIL_NEXT_APPEND: std::cell::Cell<Option<std::io::ErrorKind>> =
        const { std::cell::Cell::new(None) };
    /// Counts `WalWriter::new` file-creation + dir-sync calls on the current thread.
    pub(crate) static WAL_NEW_CALLS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

struct PartsCursor<'a> {
    parts: &'a [&'a [u8]],
    current: &'a [u8],
}

impl<'a> PartsCursor<'a> {
    fn chunks(&mut self, mut length: usize) -> impl Iterator<Item = &'a [u8]> + '_ {
        std::iter::from_fn(move || {
            if length == 0 {
                return None;
            }
            while self.current.is_empty() {
                let (part, rest) = self.parts.split_first()?;
                self.current = part;
                self.parts = rest;
            }
            let (chunk, rest) = self.current.split_at(length.min(self.current.len()));
            self.current = rest;
            length -= chunk.len();
            Some(chunk)
        })
    }
}

/// WAL writer. Appends records to a file, splitting across block boundaries.
pub struct WalWriter {
    writer: BufWriter<File>,
    /// Current offset within the current block.
    block_offset: usize,
    end_offset: u64,
    last_writeback: u64,
}

/// Buffer 2 full 32 KiB WAL blocks before issuing a syscall so multi-fragment
/// records and large write groups coalesce instead of bypassing an 8 KiB buffer.
const WAL_BUF_CAPACITY: usize = 2 * BLOCK_SIZE;

impl WalWriter {
    /// Release the file without letting BufWriter flush during a simulated crash.
    #[cfg(any(test, feature = "test-utils"))]
    pub(crate) fn discard_buffer(self) {
        drop(self.writer.into_parts());
    }

    /// Create a new WAL writer for the given file path.
    pub fn new(path: &Path) -> Result<Self> {
        #[cfg(test)]
        WAL_NEW_CALLS.with(|n| n.set(n.get() + 1));
        let file = OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(path)
            .ctx()?;
        Self::sync_parent_dir(path).ctx()?;
        Ok(Self {
            writer: BufWriter::with_capacity(WAL_BUF_CAPACITY, file),
            block_offset: 0,
            end_offset: 0,
            last_writeback: 0,
        })
    }

    /// Reopen a WAL file for appending, truncating it to `valid_len` first.
    ///
    /// After a crash, the file may contain a corrupt partial record at the tail.
    /// Use `WalReader::last_valid_offset()` to determine the safe truncation
    /// point, then call this to discard the corrupt tail before appending.
    pub fn open_append_truncated(path: &Path, valid_len: u64) -> Result<Self> {
        let mut file = OpenOptions::new().write(true).open(path).ctx()?;
        let discards_tail = file.metadata().ctx()?.len() > valid_len;
        file.set_len(valid_len).ctx()?;
        file.seek(SeekFrom::End(0)).ctx()?;
        let block_offset = valid_len as usize % BLOCK_SIZE;
        let mut writer = Self {
            writer: BufWriter::with_capacity(WAL_BUF_CAPACITY, file),
            block_offset,
            end_offset: valid_len,
            last_writeback: valid_len,
        };
        // Make the truncation durable before anything is appended. If the
        // next record's data reached disk while the size change was still
        // pending, a crash would leave the stale tail after that record, and
        // recovery must reject non-zero bytes after a failed record.
        if discards_tail {
            writer.sync().ctx()?;
        }
        Ok(writer)
    }

    /// Add a complete record (payload) to the WAL.
    ///
    /// The record may be split into multiple fragments across block boundaries.
    pub fn add_record(&mut self, payload: &[u8]) -> Result<()> {
        self.add_record_parts(&[payload])
    }

    /// Append one logical record from borrowed chunks. Each fragment CRC
    /// covers its type byte followed by exactly the bytes written, including
    /// fragments that cross a chunk boundary.
    pub fn add_record_parts(&mut self, parts: &[&[u8]]) -> Result<()> {
        #[cfg(test)]
        if let Some(kind) = FAIL_NEXT_APPEND.with(|fail| fail.take()) {
            return Err(std::io::Error::from(kind)).ctx();
        }
        let mut left = parts
            .iter()
            .try_fold(0usize, |n, p| n.checked_add(p.len()))
            .ok_or_else(|| crate::error::Error::invalid_argument("WAL record length overflow"))?;
        let mut cursor = PartsCursor {
            parts,
            current: &[],
        };
        let mut first = true;
        loop {
            let remaining = BLOCK_SIZE - self.block_offset;
            if remaining < HEADER_SIZE {
                self.writer
                    .write_all(&[0; HEADER_SIZE][..remaining])
                    .ctx()?;
                self.end_offset += remaining as u64;
                self.block_offset = 0;
                continue;
            }
            let length = left.min(remaining - HEADER_SIZE);
            let record_type = match (first, length == left) {
                (true, true) => RecordType::Full,
                (true, false) => RecordType::First,
                (false, true) => RecordType::Last,
                (false, false) => RecordType::Middle,
            };
            let mut hasher = crc32fast::Hasher::new();
            hasher.update(&[record_type as u8]);
            let mut header = [0; HEADER_SIZE];
            // A normal batch uses at most three slices: fragment header,
            // sequence/count header and body. Keep these descriptors on stack.
            let mut buffers = SmallVec::<[IoSlice<'_>; 3]>::new();
            buffers.push(IoSlice::new(&[]));
            for chunk in cursor.chunks(length) {
                hasher.update(chunk);
                buffers.push(IoSlice::new(chunk));
            }
            encode_header(&mut header, hasher.finalize(), length as u16, record_type);
            buffers[0] = IoSlice::new(&header);
            let mut unwritten = buffers.as_mut_slice();
            while !unwritten.is_empty() {
                match self.writer.write_vectored(unwritten) {
                    Ok(0) => return Err(io::Error::from(io::ErrorKind::WriteZero)).ctx(),
                    Ok(bytes) => IoSlice::advance_slices(&mut unwritten, bytes),
                    Err(e) if e.kind() == io::ErrorKind::Interrupted => continue,
                    Err(e) => return Err(e).ctx(),
                }
            }
            self.block_offset += HEADER_SIZE + length;
            self.end_offset += (HEADER_SIZE + length) as u64;
            left -= length;
            if left == 0 {
                return Ok(());
            }
            first = false;
        }
    }

    /// Flush and fsync the WAL file.
    pub fn sync(&mut self) -> Result<()> {
        self.writer.flush().ctx()?;
        #[cfg(test)]
        if FAIL_NEXT_SYNC.with(|fail| fail.replace(false)) {
            return Err(std::io::Error::other("injected WAL sync failure")).ctx();
        }
        self.writer.get_ref().sync_all().ctx()?;
        Ok(())
    }

    /// Commit appended data after the WAL directory entry is durable.
    pub fn sync_data(&mut self) -> Result<()> {
        self.writer.flush().ctx()?;
        #[cfg(test)]
        if FAIL_NEXT_SYNC.with(|fail| fail.replace(false)) {
            return Err(std::io::Error::other("injected WAL sync failure")).ctx();
        }
        self.writer.get_ref().sync_data().ctx()?;
        #[cfg(all(test, unix))]
        OBSERVE_SYNC_DATA.with(|hook| {
            if let Some(hook) = hook.borrow_mut().as_mut() {
                hook(self.writer.get_ref(), self.end_offset);
            }
        });
        self.last_writeback = self.end_offset;
        Ok(())
    }

    /// Start asynchronous writeback of completed batches before rotation.
    pub(crate) fn writeback_hint(&mut self, interval: u64) {
        let length = self.end_offset - self.last_writeback;
        if interval != 0 && length >= interval {
            #[cfg(target_os = "linux")]
            crate::db::hint_wal_writeback(self.writer.get_ref(), self.last_writeback, length);
            self.last_writeback = self.end_offset;
        }
    }

    /// Flush the WAL buffer (without fsync).
    pub fn flush(&mut self) -> Result<()> {
        self.writer.flush().ctx()?;
        Ok(())
    }

    fn sync_parent_dir(path: &Path) -> Result<()> {
        let parent = path
            .parent()
            .filter(|p| !p.as_os_str().is_empty())
            .unwrap_or_else(|| Path::new("."));
        File::open(parent).and_then(|dir| dir.sync_all()).ctx()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::wal::reader::WalReader;
    use std::result::Result as StdResult;

    #[test]
    fn record_parts_match_contiguous_v4_fragments() {
        let dir = tempfile::tempdir().unwrap();
        let flat = dir.path().join("flat.wal");
        let split = dir.path().join("split.wal");
        let header = [0x29; 12];
        let body = vec![0x61; BLOCK_SIZE * 2 + 13];
        let combined = [header.as_slice(), &body].concat();
        let mut a = WalWriter::new(&flat).unwrap();
        let mut b = WalWriter::new(&split).unwrap();
        // Every possible boundary inside the 12-byte batch header, plus
        // trailing space that cannot hold a complete fragment header.
        for leftover in 0..(HEADER_SIZE + header.len() + 1) {
            let pad = (BLOCK_SIZE * 2 - a.block_offset - HEADER_SIZE - leftover) % BLOCK_SIZE;
            let pad = vec![0; pad];
            a.add_record(&pad).unwrap();
            b.add_record(&pad).unwrap();
            a.add_record(&combined).unwrap();
            b.add_record_parts(&[&[], &header, &[], &body, &[]])
                .unwrap();
        }
        a.flush().unwrap();
        b.flush().unwrap();
        assert_eq!(std::fs::read(flat).unwrap(), std::fs::read(split).unwrap());
    }

    #[test]
    fn test_write_and_read_single_record() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.wal");

        {
            let mut writer = WalWriter::new(&path).unwrap();
            writer.add_record(b"hello world").unwrap();
            writer.sync().unwrap();
        }

        let mut reader = WalReader::new(&path).unwrap();
        let records: Vec<Vec<u8>> = reader.iter().collect::<StdResult<Vec<_>, _>>().unwrap();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0], b"hello world");
    }

    #[test]
    fn test_write_multiple_records() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.wal");

        {
            let mut writer = WalWriter::new(&path).unwrap();
            for i in 0..100 {
                let data = format!("record_{}", i);
                writer.add_record(data.as_bytes()).unwrap();
            }
            writer.sync().unwrap();
        }

        let mut reader = WalReader::new(&path).unwrap();
        let records: Vec<Vec<u8>> = reader.iter().collect::<StdResult<Vec<_>, _>>().unwrap();
        assert_eq!(records.len(), 100);
        for (i, rec) in records.iter().enumerate() {
            assert_eq!(rec, format!("record_{}", i).as_bytes());
        }
    }

    #[test]
    fn test_write_large_record_spanning_blocks() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.wal");

        // A record larger than BLOCK_SIZE
        let large_data = vec![0xAB_u8; BLOCK_SIZE * 3 + 100];

        {
            let mut writer = WalWriter::new(&path).unwrap();
            writer.add_record(&large_data).unwrap();
            writer.add_record(b"small").unwrap();
            writer.sync().unwrap();
        }

        let mut reader = WalReader::new(&path).unwrap();
        let records: Vec<Vec<u8>> = reader.iter().collect::<StdResult<Vec<_>, _>>().unwrap();
        assert_eq!(records.len(), 2);
        assert_eq!(records[0], large_data);
        assert_eq!(records[1], b"small");
    }

    #[test]
    fn test_empty_payload_record() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("empty_payload.wal");

        {
            let mut writer = WalWriter::new(&path).unwrap();
            writer.add_record(b"").unwrap();
            writer.add_record(b"after_empty").unwrap();
            writer.sync().unwrap();
        }

        let mut reader = WalReader::new(&path).unwrap();
        let records: Vec<Vec<u8>> = reader.iter().collect::<StdResult<Vec<_>, _>>().unwrap();
        assert_eq!(records.len(), 2);
        assert_eq!(records[0], b"");
        assert_eq!(records[1], b"after_empty");
    }

    #[test]
    fn test_exact_block_boundary() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("boundary.wal");

        // Fill exactly to BLOCK_SIZE: header (7 bytes) + payload
        // payload_size = BLOCK_SIZE - HEADER_SIZE
        let payload_size = BLOCK_SIZE - HEADER_SIZE;
        let payload = vec![0x42_u8; payload_size];

        {
            let mut writer = WalWriter::new(&path).unwrap();
            writer.add_record(&payload).unwrap();
            // This second record should start exactly at the next block boundary
            writer.add_record(b"next_block").unwrap();
            writer.sync().unwrap();
        }

        let mut reader = WalReader::new(&path).unwrap();
        let records: Vec<Vec<u8>> = reader.iter().collect::<StdResult<Vec<_>, _>>().unwrap();
        assert_eq!(records.len(), 2);
        assert_eq!(records[0], payload);
        assert_eq!(records[1], b"next_block");
    }

    #[test]
    fn test_multiple_sync_append() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("multi_sync.wal");

        {
            let mut writer = WalWriter::new(&path).unwrap();

            // First batch: write + sync
            writer.add_record(b"batch1_rec1").unwrap();
            writer.add_record(b"batch1_rec2").unwrap();
            writer.sync().unwrap();

            // Second batch: write + sync
            writer.add_record(b"batch2_rec1").unwrap();
            writer.sync().unwrap();

            // Third batch: write + flush (no fsync) + write + sync
            writer.add_record(b"batch3_rec1").unwrap();
            writer.flush().unwrap();
            writer.add_record(b"batch3_rec2").unwrap();
            writer.sync().unwrap();
        }

        let mut reader = WalReader::new(&path).unwrap();
        let records: Vec<Vec<u8>> = reader.iter().collect::<StdResult<Vec<_>, _>>().unwrap();
        assert_eq!(records.len(), 5);
        assert_eq!(records[0], b"batch1_rec1");
        assert_eq!(records[1], b"batch1_rec2");
        assert_eq!(records[2], b"batch2_rec1");
        assert_eq!(records[3], b"batch3_rec1");
        assert_eq!(records[4], b"batch3_rec2");
    }

    /// Regression: the torn-tail truncation was not synced before the next
    /// append. If the new record reached disk while the size change was still
    /// pending, a crash left the stale tail after it, which recovery rejects.
    #[test]
    fn test_open_append_truncated_syncs_discarded_tail() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("MANIFEST-000001");
        {
            let mut writer = WalWriter::new(&path).unwrap();
            writer.add_record(b"complete").unwrap();
            writer.sync().unwrap();
        }
        let valid_len = std::fs::metadata(&path).unwrap().len();
        {
            use std::io::Write;
            let mut f = OpenOptions::new().append(true).open(&path).unwrap();
            f.write_all(b"torn tail bytes").unwrap();
        }

        FAIL_NEXT_SYNC.with(|fail| fail.set(true));
        assert!(
            WalWriter::open_append_truncated(&path, valid_len).is_err(),
            "discarding a tail must sync before the writer is returned"
        );
        FAIL_NEXT_SYNC.with(|fail| fail.set(false));

        let mut writer = WalWriter::open_append_truncated(&path, valid_len).unwrap();
        assert_eq!(std::fs::metadata(&path).unwrap().len(), valid_len);
        writer.add_record(b"next").unwrap();
        writer.sync().unwrap();
        let mut reader = WalReader::new(&path).unwrap();
        let records: Vec<Vec<u8>> = reader.iter().collect::<StdResult<Vec<_>, _>>().unwrap();
        assert_eq!(records, vec![b"complete".to_vec(), b"next".to_vec()]);
    }
}
