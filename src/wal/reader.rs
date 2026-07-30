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
    /// Set when the most recent `read_record` failure is a structural short
    /// read (partial header/payload/trailer, Zero header, multi-fragment EOF).
    /// Checksum, type, and other semantic failures clear it — those must not
    /// be treated as torn tails even if the remaining file bytes are zero,
    /// because an untrusted length can already have consumed later records.
    last_error_is_truncation: bool,
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
            last_error_is_truncation: false,
        })
    }

    /// Byte position of the end of the last successfully read complete record.
    /// Use this as the truncation point when reopening for append after a crash.
    pub fn last_valid_offset(&self) -> u64 {
        self.last_valid_offset
    }

    /// True when the previous `read_record` error is a structural short-read
    /// that recovery may treat as a torn active log tail (subject to
    /// [`Self::rest_is_zero_padding`]). Checksum, type, and other semantic
    /// failures return false.
    pub fn last_error_is_truncation(&self) -> bool {
        self.last_error_is_truncation
    }

    /// True if every byte from the current read position to EOF is zero.
    ///
    /// After a **structural truncation** from `read_record()`, this
    /// distinguishes a torn tail from mid-log corruption: a record torn by a
    /// crash is the last thing in the file, followed at most by block padding
    /// or a filesystem zero-extended tail. Non-zero bytes after the failed
    /// record mean data was written after it, so the failure is real
    /// corruption and the log suffix would be silently lost by prefix recovery.
    ///
    /// Must not be used alone after checksum/type/length simulation of a full
    /// physical record: an untrusted length may already have skipped later
    /// valid records. Map that case with [`Self::last_error_is_truncation`].
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

    fn fail_truncation<T>(&mut self, msg: impl Into<String>) -> Result<T> {
        self.last_error_is_truncation = true;
        Err(Error::corruption(msg.into()))
    }

    fn fail_corruption<T>(&mut self, msg: impl Into<String>) -> Result<T> {
        self.last_error_is_truncation = false;
        Err(Error::corruption(msg.into()))
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
                        return self.fail_truncation("partial WAL record without end");
                    }
                    return Ok(None);
                }
                Some((record_type, data)) => match record_type {
                    RecordType::Full => {
                        if in_fragmented_record {
                            return self.fail_corruption("full record inside fragment");
                        }
                        self.last_valid_offset = self.reader.stream_position().ctx()?;
                        return Ok(Some(data));
                    }
                    RecordType::First => {
                        if in_fragmented_record {
                            return self.fail_corruption("first record inside fragment");
                        }
                        in_fragmented_record = true;
                        result = data;
                    }
                    RecordType::Middle => {
                        if !in_fragmented_record {
                            return self.fail_corruption("middle record without first");
                        }
                        result.extend_from_slice(&data);
                    }
                    RecordType::Last => {
                        if !in_fragmented_record {
                            return self.fail_corruption("last record without first");
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
                // Skip the trailer padding. A clean EOF at the trailer start is
                // a normal end of file; a partial trailer is a torn tail and is
                // surfaced like a partial header, so only the recoverable
                // active WAL tolerates it.
                if leftover > 0 {
                    let mut skip = [0u8; HEADER_SIZE];
                    let mut skipped = 0;
                    while skipped < leftover {
                        match self.reader.read(&mut skip[skipped..leftover]) {
                            Ok(0) if skipped == 0 => return Ok(None),
                            Ok(0) => {
                                return self.fail_truncation("truncated WAL block trailer");
                            }
                            Ok(n) => skipped += n,
                            Err(e) if e.kind() == ErrorKind::Interrupted => continue,
                            Err(e) => return Err(e).ctx(),
                        }
                    }
                }
                self.block_offset = 0;
                continue;
            }

            // Read the header. A clean EOF before the first header byte is normal;
            // a partial header is a torn tail and must be surfaced to recovery so
            // only the active WAL can tolerate it.
            let mut header_buf = [0u8; HEADER_SIZE];
            let mut header_read = 0;
            while header_read < HEADER_SIZE {
                match self.reader.read(&mut header_buf[header_read..]) {
                    Ok(0) if header_read == 0 => return Ok(None),
                    Ok(0) => {
                        return self.fail_truncation("truncated WAL record header");
                    }
                    Ok(n) => header_read += n,
                    Err(e) if e.kind() == ErrorKind::Interrupted => continue,
                    Err(e) => return Err(e).ctx(),
                }
            }

            // A full zero header is only valid in a zero-extended tail. Surface
            // it as truncation so recovery can accept it only after proving
            // that every remaining byte is also zero.
            if header_buf == [0u8; HEADER_SIZE] {
                return self.fail_truncation("zero WAL record before proven zero tail");
            }

            let (checksum, length, record_type) = decode_header(&header_buf);
            let record_type = match record_type {
                Some(rt) => rt,
                None => {
                    return self
                        .fail_corruption(format!("unknown WAL record type: {}", header_buf[6]));
                }
            };
            let length = length as usize;

            // Validate that the record payload fits within the current block.
            // An invalid length must fail closed: it is semantic corruption,
            // not a short read of a real physical record.
            let remaining = BLOCK_SIZE - self.block_offset - HEADER_SIZE;
            if length > remaining {
                return self.fail_corruption(format!(
                    "WAL record length {} exceeds remaining block space {}",
                    length, remaining
                ));
            }

            // Read the data
            let mut data = vec![0u8; length];
            match self.reader.read_exact(&mut data) {
                Ok(()) => {}
                Err(e) if e.kind() == ErrorKind::UnexpectedEof => {
                    return self.fail_truncation("truncated WAL record payload");
                }
                Err(e) => return Err(e).ctx(),
            }

            self.block_offset += HEADER_SIZE + length;

            if matches!(record_type, RecordType::Zero) {
                return self.fail_corruption("non-padding WAL zero record");
            }

            // Verify checksum. A mismatch after a full payload read is only a
            // torn-tail candidate when the payload ends in bytes that were
            // never written — i.e. it has a non-empty all-zero suffix. That is
            // exactly the shape a crash mid-append leaves behind (written
            // prefix + filesystem zero-extension), and recovery still has to
            // prove every byte to EOF is zero before truncating.
            //
            // Without the zero-suffix requirement, an untrusted length that
            // swallowed later *valid* records would also reach EOF and look
            // like a clean tail, silently dropping committed data. Those
            // swallowed records carry their own non-zero headers/payloads, so
            // the declared payload does not end in zeros and this fails closed.
            let mut hasher = crc32fast::Hasher::new();
            hasher.update(&[record_type as u8]);
            hasher.update(&data);
            let expected_checksum = hasher.finalize();

            if checksum != expected_checksum {
                let msg = format!(
                    "WAL checksum mismatch: expected {:#x}, got {:#x}",
                    expected_checksum, checksum
                );
                return if data.last().is_some_and(|&b| b == 0) {
                    self.fail_truncation(msg)
                } else {
                    self.fail_corruption(msg)
                };
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
    use std::{fs::OpenOptions, io::SeekFrom};

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
            let mut file = OpenOptions::new().write(true).open(&path).unwrap();
            file.seek(SeekFrom::Start(HEADER_SIZE as u64)).unwrap();
            file.write_all(b"CORRUPTED").unwrap();
        }

        let mut reader = WalReader::new(&path).unwrap();
        let result = reader.read_record();
        assert!(result.is_err());
    }

    #[test]
    fn test_block_trailer_truncation() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("trailer.wal");

        // First record ends 6 bytes before the block boundary (< HEADER_SIZE),
        // so the writer emits 6 bytes of trailer padding before record two.
        let trailer_len = 6u64;
        let payload = vec![0x42_u8; BLOCK_SIZE - HEADER_SIZE - trailer_len as usize];
        {
            let mut writer = WalWriter::new(&path).unwrap();
            writer.add_record(&payload).unwrap();
            writer.add_record(b"next_block").unwrap();
            writer.sync().unwrap();
        }
        let trailer_start = (BLOCK_SIZE as u64) - trailer_len;

        // Truncated mid-trailer: committed records in the next block were
        // lost, so this must surface as corruption, not clean EOF.
        let file = OpenOptions::new().write(true).open(&path).unwrap();
        file.set_len(trailer_start + 3).unwrap();
        drop(file);
        let mut reader = WalReader::new(&path).unwrap();
        assert_eq!(reader.read_record().unwrap().unwrap(), payload);
        assert!(reader.read_record().is_err());

        // Truncated exactly at the trailer start: a legitimate end of file
        // (the writer only pads when it is about to append another record).
        let file = OpenOptions::new().write(true).open(&path).unwrap();
        file.set_len(trailer_start).unwrap();
        drop(file);
        let mut reader = WalReader::new(&path).unwrap();
        assert_eq!(reader.read_record().unwrap().unwrap(), payload);
        assert!(reader.read_record().unwrap().is_none());
    }

    #[test]
    fn test_enlarged_length_through_later_records_is_not_truncation() {
        // Three same-block records. Inflating the middle length through the
        // third consumes later payload bytes before the checksum fails; that
        // must be semantic corruption, not a torn-tail candidate.
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("enlarged_len.wal");
        {
            let mut writer = WalWriter::new(&path).unwrap();
            writer.add_record(b"rec-a").unwrap();
            writer.add_record(b"rec-b").unwrap();
            writer.add_record(b"rec-c").unwrap();
            writer.sync().unwrap();
        }

        let mut bytes = std::fs::read(&path).unwrap();
        let physical_len = |offset: usize| {
            HEADER_SIZE + u16::from_le_bytes([bytes[offset + 4], bytes[offset + 5]]) as usize
        };
        let a_len = physical_len(0);
        let b_off = a_len;
        let b_len = physical_len(b_off);
        let c_off = b_off + b_len;
        let c_len = physical_len(c_off);
        assert!(
            c_off + c_len <= bytes.len(),
            "expected three complete same-block records"
        );

        // Length of B is enlarged so the declared payload covers through C.
        let enlarged = (c_off + c_len - b_off - HEADER_SIZE) as u16;
        bytes[b_off + 4..b_off + 6].copy_from_slice(&enlarged.to_le_bytes());
        std::fs::write(&path, &bytes).unwrap();

        let mut reader = WalReader::new(&path).unwrap();
        assert_eq!(reader.read_record().unwrap().unwrap(), b"rec-a");
        let err = reader.read_record().unwrap_err();
        assert!(
            err.to_string().contains("checksum mismatch"),
            "enlarged length should fail at checksum, got: {err}"
        );
        assert!(
            !reader.last_error_is_truncation(),
            "checksum after full untrusted-length read must not be a torn tail"
        );
        // With the pre-fix policy, rest_is_zero_padding alone would return
        // true (EOF after swallowing C) and hide the third record.
        assert!(reader.rest_is_zero_padding().unwrap());
    }

    /// Regression: a crash mid-append can leave a record whose header and
    /// leading payload bytes landed while the tail was zero-extended by the
    /// filesystem. The declared length is still valid and the type byte is
    /// still valid, so the failure surfaces at the checksum — but it is a
    /// genuine torn active tail and recovery must be allowed to truncate it.
    /// Classifying every post-payload checksum failure as semantic corruption
    /// made such a WAL unopenable, so a normal crash could refuse to reopen.
    #[test]
    fn test_zero_extended_payload_tail_is_truncation_candidate() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("zero_extended_tail.wal");
        {
            let mut writer = WalWriter::new(&path).unwrap();
            writer.add_record(b"complete-one").unwrap();
            writer.add_record(b"second-record-payload").unwrap();
            writer.sync().unwrap();
        }

        // Zero the tail of the second record's payload, leaving its header and
        // the payload's first bytes intact — the filesystem zero-extension a
        // crash mid-append produces.
        let mut bytes = std::fs::read(&path).unwrap();
        let first_len = HEADER_SIZE + u16::from_le_bytes([bytes[4], bytes[5]]) as usize;
        let second_payload_start = first_len + HEADER_SIZE;
        for b in bytes[second_payload_start + 4..].iter_mut() {
            *b = 0;
        }
        std::fs::write(&path, &bytes).unwrap();

        let mut reader = WalReader::new(&path).unwrap();
        assert_eq!(reader.read_record().unwrap().unwrap(), b"complete-one");
        let err = reader.read_record().unwrap_err();
        assert!(err.to_string().contains("checksum mismatch"), "got {err}");
        assert!(
            reader.last_error_is_truncation(),
            "a zero-extended payload tail must stay a torn-tail candidate"
        );
        assert!(reader.rest_is_zero_padding().unwrap());
    }

    #[test]
    fn test_truncated_payload_is_truncation_candidate() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("torn_payload.wal");
        {
            let mut writer = WalWriter::new(&path).unwrap();
            writer.add_record(b"complete").unwrap();
            writer.add_record(b"will-be-torn").unwrap();
            writer.sync().unwrap();
        }
        let len = std::fs::metadata(&path).unwrap().len();
        assert!(len > 4);
        let file = OpenOptions::new().write(true).open(&path).unwrap();
        file.set_len(len - 4).unwrap();
        drop(file);

        let mut reader = WalReader::new(&path).unwrap();
        assert_eq!(reader.read_record().unwrap().unwrap(), b"complete");
        assert!(reader.read_record().is_err());
        assert!(reader.last_error_is_truncation());
        assert!(reader.rest_is_zero_padding().unwrap());
    }
}
