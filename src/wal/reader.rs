//! WAL reader: reads and reassembles records from a WAL file.

use std::fs::File;
use std::io::{BufReader, ErrorKind, Read, Seek};
use std::path::Path;

use crate::error::{Error, Result, ResultExt};
use crate::wal::record::*;

/// WAL reader. Reads records from a WAL file, handling fragmentation.
pub struct WalReader {
    reader: BufReader<File>,
    /// Current block offset for tracking position within blocks.
    block_offset: usize,
    /// Whether we've reached EOF.
    eof: bool,
    /// Byte position of the end of the last successfully read complete record.
    /// After iterating all records, this is the safe truncation point for
    /// append-after-crash (any bytes beyond this are corrupt/partial).
    last_valid_offset: u64,
}

impl WalReader {
    /// Open a WAL file for reading.
    pub fn new(path: &Path) -> Result<Self> {
        let file = File::open(path).ctx()?;
        Ok(Self {
            reader: BufReader::new(file),
            block_offset: 0,
            eof: false,
            last_valid_offset: 0,
        })
    }

    /// Byte position of the end of the last successfully read complete record.
    /// Use this as the truncation point when reopening for append after a crash.
    pub fn last_valid_offset(&self) -> u64 {
        self.last_valid_offset
    }

    /// True if every byte from the current read position to EOF is zero.
    ///
    /// After `read_record()` fails, this distinguishes a torn tail from
    /// mid-log corruption: a record torn by a crash is the last thing in the
    /// file, followed at most by block padding or a filesystem zero-extended
    /// tail. Non-zero bytes after the failed record mean data was written
    /// after it, so the failure is real corruption and the log suffix would
    /// be silently lost by prefix recovery.
    ///
    /// Consumes the remaining bytes; the reader is not usable for further
    /// record reads afterwards (`last_valid_offset` is unaffected).
    pub fn rest_is_zero_padding(&mut self) -> Result<bool> {
        let mut buf = [0u8; 4096];
        loop {
            match self.reader.read(&mut buf) {
                Ok(0) => return Ok(true),
                Ok(n) => {
                    if buf[..n].iter().any(|&b| b != 0) {
                        return Ok(false);
                    }
                }
                Err(e) if e.kind() == ErrorKind::Interrupted => continue,
                Err(e) => return Err(e).ctx(),
            }
        }
    }

    /// Return an iterator over all records in the WAL.
    #[cfg(test)]
    pub fn iter(&mut self) -> WalIterator<'_> {
        WalIterator { reader: self }
    }

    /// Read the next complete record.
    ///
    /// Returns `Ok(Some(data))` for a record, `Ok(None)` at EOF.
    pub fn read_record(&mut self) -> Result<Option<Vec<u8>>> {
        if self.eof {
            return Ok(None);
        }

        let mut result = Vec::new();
        let mut in_fragmented_record = false;

        loop {
            match self.read_physical_record()? {
                None => {
                    self.eof = true;
                    if in_fragmented_record {
                        tracing::warn!("WAL: partial record without end (truncated)");
                        return Ok(None);
                    }
                    return Ok(None);
                }
                Some((record_type, data)) => match record_type {
                    RecordType::Full => {
                        if in_fragmented_record {
                            return Err(Error::corruption(
                                "full record inside fragment".to_string(),
                            ));
                        }
                        self.last_valid_offset = self.reader.stream_position().ctx()?;
                        return Ok(Some(data));
                    }
                    RecordType::First => {
                        if in_fragmented_record {
                            return Err(Error::corruption(
                                "first record inside fragment".to_string(),
                            ));
                        }
                        in_fragmented_record = true;
                        result = data;
                    }
                    RecordType::Middle => {
                        if !in_fragmented_record {
                            return Err(Error::corruption(
                                "middle record without first".to_string(),
                            ));
                        }
                        result.extend_from_slice(&data);
                    }
                    RecordType::Last => {
                        if !in_fragmented_record {
                            return Err(Error::corruption("last record without first".to_string()));
                        }
                        result.extend_from_slice(&data);
                        self.last_valid_offset = self.reader.stream_position().ctx()?;
                        return Ok(Some(result));
                    }
                    RecordType::Zero => unreachable!("zero records are handled as padding"),
                },
            }
        }
    }

    /// Read a single physical record (fragment).
    /// Returns None at EOF.
    fn read_physical_record(&mut self) -> Result<Option<(RecordType, Vec<u8>)>> {
        loop {
            // Check if we need to skip to the next block
            let leftover = BLOCK_SIZE - self.block_offset;
            if leftover < HEADER_SIZE {
                // Skip the remaining bytes in this block
                if leftover > 0 {
                    let mut skip = vec![0u8; leftover];
                    match self.reader.read_exact(&mut skip) {
                        Ok(()) => {}
                        Err(e) if e.kind() == ErrorKind::UnexpectedEof => return Ok(None),
                        Err(e) => return Err(e).ctx(),
                    }
                }
                self.block_offset = 0;
                continue;
            }

            // Read the header
            let mut header_buf = [0u8; HEADER_SIZE];
            match self.reader.read_exact(&mut header_buf) {
                Ok(()) => {}
                Err(e) if e.kind() == ErrorKind::UnexpectedEof => return Ok(None),
                Err(e) => return Err(e).ctx(),
            }

            let (checksum, length, record_type) = decode_header(&header_buf);
            let record_type = match record_type {
                Some(rt) => rt,
                None => {
                    return Err(Error::corruption(format!(
                        "unknown WAL record type: {}",
                        header_buf[6]
                    )));
                }
            };
            let length = length as usize;

            // Validate that the record payload fits within the current block.
            let remaining = BLOCK_SIZE - self.block_offset - HEADER_SIZE;
            if length > remaining {
                return Err(Error::corruption(format!(
                    "WAL record length {} exceeds remaining block space {}",
                    length, remaining
                )));
            }

            // Read the data
            let mut data = vec![0u8; length];
            match self.reader.read_exact(&mut data) {
                Ok(()) => {}
                Err(e) if e.kind() == ErrorKind::UnexpectedEof => {
                    // Crash-truncated tail record: treat as clean end-of-data
                    return Ok(None);
                }
                Err(e) => return Err(e).ctx(),
            }

            self.block_offset += HEADER_SIZE + length;

            // All-zero physical headers are block padding. A decoded Zero record
            // with any non-zero header field is corruption and must not bypass CRC.
            if header_buf == [0u8; HEADER_SIZE] {
                continue;
            }
            if matches!(record_type, RecordType::Zero) {
                return Err(Error::corruption("non-padding WAL zero record".to_string()));
            }

            // Verify checksum
            let mut hasher = crc32fast::Hasher::new();
            hasher.update(&[record_type as u8]);
            hasher.update(&data);
            let expected_checksum = hasher.finalize();

            if checksum != expected_checksum {
                return Err(Error::corruption(format!(
                    "WAL checksum mismatch: expected {:#x}, got {:#x}",
                    expected_checksum, checksum
                )));
            }

            return Ok(Some((record_type, data)));
        }
    }
}

/// Iterator adapter over WAL records (test helper).
#[cfg(test)]
pub struct WalIterator<'a> {
    reader: &'a mut WalReader,
}

#[cfg(test)]
impl<'a> Iterator for WalIterator<'a> {
    type Item = Result<Vec<u8>>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.reader.read_record() {
            Ok(Some(data)) => Some(Ok(data)),
            Ok(None) => None,
            Err(e) => Some(Err(e)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::wal::writer::WalWriter;
    use std::io::SeekFrom;

    #[test]
    fn test_empty_wal() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("empty.wal");

        // Create empty file
        {
            let _writer = WalWriter::new(&path).unwrap();
        }

        let mut reader = WalReader::new(&path).unwrap();
        assert!(reader.read_record().unwrap().is_none());
    }

    #[test]
    fn test_checksum_verification() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("corrupt.wal");

        {
            let mut writer = WalWriter::new(&path).unwrap();
            writer.add_record(b"test data").unwrap();
            writer.sync().unwrap();
        }

        // Corrupt the data portion (after the 7-byte header)
        {
            use std::io::Write;
            let mut file = std::fs::OpenOptions::new().write(true).open(&path).unwrap();
            file.seek(SeekFrom::Start(HEADER_SIZE as u64)).unwrap();
            file.write_all(b"CORRUPTED").unwrap();
        }

        let mut reader = WalReader::new(&path).unwrap();
        let result = reader.read_record();
        assert!(result.is_err());
    }
}
