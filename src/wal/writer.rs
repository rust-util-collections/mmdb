//! WAL writer: appends records to a WAL file with block-based fragmentation.

use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Seek, SeekFrom, Write};
use std::path::Path;

use ruc::*;

use crate::error::Result;
use crate::wal::record::*;

/// WAL writer. Appends records to a file, splitting across block boundaries.
pub struct WalWriter {
    writer: BufWriter<File>,
    /// Current offset within the current block.
    block_offset: usize,
}

impl WalWriter {
    /// Create a new WAL writer for the given file path.
    pub fn new(path: &Path) -> Result<Self> {
        let file = OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(path)
            .c(d!())?;
        Self::sync_parent_dir(path).c(d!())?;
        Ok(Self {
            writer: BufWriter::new(file),
            block_offset: 0,
        })
    }

    /// Reopen a WAL file for appending, truncating it to `valid_len` first.
    ///
    /// After a crash, the file may contain a corrupt partial record at the tail.
    /// Use `WalReader::last_valid_offset()` to determine the safe truncation
    /// point, then call this to discard the corrupt tail before appending.
    pub fn open_append_truncated(path: &Path, valid_len: u64) -> Result<Self> {
        let mut file = OpenOptions::new().write(true).open(path).c(d!())?;
        file.set_len(valid_len).c(d!())?;
        file.seek(SeekFrom::End(0)).c(d!())?;
        let block_offset = valid_len as usize % BLOCK_SIZE;
        Ok(Self {
            writer: BufWriter::new(file),
            block_offset,
        })
    }

    /// Add a complete record (payload) to the WAL.
    ///
    /// The record may be split into multiple fragments across block boundaries.
    pub fn add_record(&mut self, payload: &[u8]) -> Result<()> {
        let mut left = payload;
        let mut is_first = true;

        loop {
            let leftover = BLOCK_SIZE - self.block_offset;

            if leftover < HEADER_SIZE {
                // Not enough space for a header, fill remainder with zeros
                if leftover > 0 {
                    let zeros = vec![0u8; leftover];
                    self.writer.write_all(&zeros).c(d!())?;
                }
                self.block_offset = 0;
                continue;
            }

            let avail = leftover - HEADER_SIZE;
            let fragment_len = left.len().min(avail);
            let is_last = fragment_len == left.len();

            let record_type = match (is_first, is_last) {
                (true, true) => RecordType::Full,
                (true, false) => RecordType::First,
                (false, true) => RecordType::Last,
                (false, false) => RecordType::Middle,
            };

            let fragment = &left[..fragment_len];
            self.emit_fragment(record_type, fragment)?;

            left = &left[fragment_len..];
            is_first = false;

            if is_last {
                break;
            }
        }

        Ok(())
    }

    /// Flush and fsync the WAL file.
    pub fn sync(&mut self) -> Result<()> {
        self.writer.flush().c(d!())?;
        self.writer.get_ref().sync_all().c(d!())?;
        Ok(())
    }

    /// Flush the WAL buffer (without fsync).
    pub fn flush(&mut self) -> Result<()> {
        self.writer.flush().c(d!())?;
        Ok(())
    }

    fn emit_fragment(&mut self, record_type: RecordType, data: &[u8]) -> Result<()> {
        let length = data.len() as u16;
        // CRC covers type + data
        let mut hasher = crc32fast::Hasher::new();
        hasher.update(&[record_type as u8]);
        hasher.update(data);
        let checksum = hasher.finalize();

        let mut header = [0u8; HEADER_SIZE];
        encode_header(&mut header, checksum, length, record_type);

        self.writer.write_all(&header).c(d!())?;
        self.writer.write_all(data).c(d!())?;
        self.block_offset += HEADER_SIZE + data.len();

        Ok(())
    }

    fn sync_parent_dir(path: &Path) -> Result<()> {
        let parent = path.parent().unwrap_or_else(|| Path::new("."));
        File::open(parent).and_then(|dir| dir.sync_all()).c(d!())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::wal::reader::WalReader;
    use std::result::Result as StdResult;

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
}
