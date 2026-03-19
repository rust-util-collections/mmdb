//! WAL reader: reads and reassembles records from a WAL file.

use std::fs::File;
use std::io::{BufReader, Read, Seek, SeekFrom};
use std::path::Path;

use ruc::*;

use crate::error::{Error, Result};
use crate::wal::record::*;

/// WAL reader. Reads records from a WAL file, handling fragmentation.
pub struct WalReader {
    reader: BufReader<File>,
    /// Current block offset for tracking position within blocks.
    block_offset: usize,
    /// Whether we've reached EOF.
    eof: bool,
}

impl WalReader {
    /// Open a WAL file for reading.
    pub fn new(path: &Path) -> Result<Self> {
        let file = File::open(path).c(d!())?;
        Ok(Self {
            reader: BufReader::new(file),
            block_offset: 0,
            eof: false,
        })
    }

    /// Reset to the beginning of the file.
    pub fn reset(&mut self) -> Result<()> {
        self.reader.seek(SeekFrom::Start(0)).c(d!())?;
        self.block_offset = 0;
        self.eof = false;
        Ok(())
    }

    /// Return an iterator over all records in the WAL.
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
                        return Err(eg!(Error::Corruption(
                            "partial record without end".to_string()
                        )));
                    }
                    return Ok(None);
                }
                Some((record_type, data)) => match record_type {
                    RecordType::Full => {
                        if in_fragmented_record {
                            return Err(eg!(Error::Corruption(
                                "full record inside fragment".to_string(),
                            )));
                        }
                        return Ok(Some(data));
                    }
                    RecordType::First => {
                        if in_fragmented_record {
                            return Err(eg!(Error::Corruption(
                                "first record inside fragment".to_string(),
                            )));
                        }
                        in_fragmented_record = true;
                        result = data;
                    }
                    RecordType::Middle => {
                        if !in_fragmented_record {
                            return Err(eg!(Error::Corruption(
                                "middle record without first".to_string(),
                            )));
                        }
                        result.extend_from_slice(&data);
                    }
                    RecordType::Last => {
                        if !in_fragmented_record {
                            return Err(eg!(Error::Corruption(
                                "last record without first".to_string()
                            )));
                        }
                        result.extend_from_slice(&data);
                        return Ok(Some(result));
                    }
                    RecordType::Zero => {
                        // Skip zero/padding records
                    }
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
                        Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(None),
                        Err(e) => return Err(eg!(e)),
                    }
                }
                self.block_offset = 0;
                continue;
            }

            // Read the header
            let mut header_buf = [0u8; HEADER_SIZE];
            match self.reader.read_exact(&mut header_buf) {
                Ok(()) => {}
                Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(None),
                Err(e) => return Err(eg!(e)),
            }

            let (checksum, length, record_type) = decode_header(&header_buf);
            let record_type = match record_type {
                Some(rt) => rt,
                None => {
                    return Err(eg!(Error::Corruption(format!(
                        "unknown WAL record type: {}",
                        header_buf[6]
                    ))));
                }
            };
            let length = length as usize;

            // Read the data
            let mut data = vec![0u8; length];
            match self.reader.read_exact(&mut data) {
                Ok(()) => {}
                Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                    return Err(eg!(Error::Corruption("truncated record data".to_string())));
                }
                Err(e) => return Err(eg!(e)),
            }

            self.block_offset += HEADER_SIZE + length;

            // Verify checksum
            let mut hasher = crc32fast::Hasher::new();
            hasher.update(&[record_type as u8]);
            hasher.update(&data);
            let expected_checksum = hasher.finalize();

            if checksum != expected_checksum {
                return Err(eg!(Error::Corruption(format!(
                    "WAL checksum mismatch: expected {:#x}, got {:#x}",
                    expected_checksum, checksum
                ))));
            }

            if record_type == RecordType::Zero && length == 0 {
                continue; // skip padding
            }

            return Ok(Some((record_type, data)));
        }
    }
}

/// Iterator adapter over WAL records.
pub struct WalIterator<'a> {
    reader: &'a mut WalReader,
}

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
