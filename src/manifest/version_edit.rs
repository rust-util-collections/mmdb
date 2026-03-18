//! VersionEdit: a delta applied to a Version to produce the next Version.

use crate::error::{Error, Result};
use crate::types::SequenceNumber;

/// Metadata for a file in a particular level.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FileMetaData {
    pub number: u64,
    pub file_size: u64,
    pub smallest_key: Vec<u8>,
    pub largest_key: Vec<u8>,
    /// Whether this SST file contains any range deletion entries.
    pub has_range_deletions: bool,
}

/// A VersionEdit describes a set of changes to apply to a Version.
#[derive(Debug, Clone, Default)]
pub struct VersionEdit {
    /// New log (WAL) file number.
    pub log_number: Option<u64>,
    /// Next file number to allocate.
    pub next_file_number: Option<u64>,
    /// Last sequence number used.
    pub last_sequence: Option<SequenceNumber>,
    /// Files to add: (level, metadata).
    pub new_files: Vec<(u32, FileMetaData)>,
    /// Files to delete: (level, file_number).
    pub deleted_files: Vec<(u32, u64)>,
}

impl VersionEdit {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn set_log_number(&mut self, num: u64) {
        self.log_number = Some(num);
    }

    pub fn set_next_file_number(&mut self, num: u64) {
        self.next_file_number = Some(num);
    }

    pub fn set_last_sequence(&mut self, seq: SequenceNumber) {
        self.last_sequence = Some(seq);
    }

    pub fn add_file(&mut self, level: u32, meta: FileMetaData) {
        self.new_files.push((level, meta));
    }

    pub fn delete_file(&mut self, level: u32, file_number: u64) {
        self.deleted_files.push((level, file_number));
    }

    /// Encode to bytes for MANIFEST file storage.
    ///
    /// Format (tag-length-value):
    /// Tag byte:
    ///   1 = log_number(u64 LE)
    ///   2 = next_file_number(u64 LE)
    ///   3 = last_sequence(u64 LE)
    ///   4 = new_file (legacy): level(u32 LE) + number(u64 LE) + file_size(u64 LE)
    ///       + smallest_key_len(u32 LE) + smallest_key
    ///       + largest_key_len(u32 LE) + largest_key
    ///   5 = deleted_file: level(u32 LE) + number(u64 LE)
    ///   6 = new_file_v2: same as 4 + has_range_deletions(u8)
    pub fn encode(&self) -> Vec<u8> {
        let mut buf = Vec::new();

        if let Some(n) = self.log_number {
            buf.push(1);
            buf.extend_from_slice(&n.to_le_bytes());
        }
        if let Some(n) = self.next_file_number {
            buf.push(2);
            buf.extend_from_slice(&n.to_le_bytes());
        }
        if let Some(s) = self.last_sequence {
            buf.push(3);
            buf.extend_from_slice(&s.to_le_bytes());
        }
        for (level, meta) in &self.new_files {
            buf.push(6); // v2 format with has_range_deletions
            buf.extend_from_slice(&level.to_le_bytes());
            buf.extend_from_slice(&meta.number.to_le_bytes());
            buf.extend_from_slice(&meta.file_size.to_le_bytes());
            buf.extend_from_slice(&(meta.smallest_key.len() as u32).to_le_bytes());
            buf.extend_from_slice(&meta.smallest_key);
            buf.extend_from_slice(&(meta.largest_key.len() as u32).to_le_bytes());
            buf.extend_from_slice(&meta.largest_key);
            buf.push(meta.has_range_deletions as u8);
        }
        for (level, number) in &self.deleted_files {
            buf.push(5);
            buf.extend_from_slice(&level.to_le_bytes());
            buf.extend_from_slice(&number.to_le_bytes());
        }

        buf
    }

    /// Decode from bytes.
    pub fn decode(data: &[u8]) -> Result<Self> {
        let mut edit = Self::new();
        let mut pos = 0;

        while pos < data.len() {
            let tag = data[pos];
            pos += 1;

            match tag {
                1 => {
                    if pos + 8 > data.len() {
                        return Err(Error::Corruption("truncated log_number".into()));
                    }
                    edit.log_number =
                        Some(u64::from_le_bytes(data[pos..pos + 8].try_into().unwrap()));
                    pos += 8;
                }
                2 => {
                    if pos + 8 > data.len() {
                        return Err(Error::Corruption("truncated next_file_number".into()));
                    }
                    edit.next_file_number =
                        Some(u64::from_le_bytes(data[pos..pos + 8].try_into().unwrap()));
                    pos += 8;
                }
                3 => {
                    if pos + 8 > data.len() {
                        return Err(Error::Corruption("truncated last_sequence".into()));
                    }
                    edit.last_sequence =
                        Some(u64::from_le_bytes(data[pos..pos + 8].try_into().unwrap()));
                    pos += 8;
                }
                4 | 6 => {
                    // new_file (tag 4 = legacy, tag 6 = v2 with has_range_deletions)
                    if pos + 4 + 8 + 8 > data.len() {
                        return Err(Error::Corruption("truncated new_file header".into()));
                    }
                    let level = u32::from_le_bytes(data[pos..pos + 4].try_into().unwrap());
                    pos += 4;
                    let number = u64::from_le_bytes(data[pos..pos + 8].try_into().unwrap());
                    pos += 8;
                    let file_size = u64::from_le_bytes(data[pos..pos + 8].try_into().unwrap());
                    pos += 8;

                    if pos + 4 > data.len() {
                        return Err(Error::Corruption("truncated smallest_key_len".into()));
                    }
                    let sk_len =
                        u32::from_le_bytes(data[pos..pos + 4].try_into().unwrap()) as usize;
                    pos += 4;
                    if pos + sk_len > data.len() {
                        return Err(Error::Corruption("truncated smallest_key".into()));
                    }
                    let smallest_key = data[pos..pos + sk_len].to_vec();
                    pos += sk_len;

                    if pos + 4 > data.len() {
                        return Err(Error::Corruption("truncated largest_key_len".into()));
                    }
                    let lk_len =
                        u32::from_le_bytes(data[pos..pos + 4].try_into().unwrap()) as usize;
                    pos += 4;
                    if pos + lk_len > data.len() {
                        return Err(Error::Corruption("truncated largest_key".into()));
                    }
                    let largest_key = data[pos..pos + lk_len].to_vec();
                    pos += lk_len;

                    let has_range_deletions = if tag == 6 {
                        if pos >= data.len() {
                            return Err(Error::Corruption("truncated has_range_deletions".into()));
                        }
                        let v = data[pos] != 0;
                        pos += 1;
                        v
                    } else {
                        false
                    };

                    edit.new_files.push((
                        level,
                        FileMetaData {
                            number,
                            file_size,
                            smallest_key,
                            largest_key,
                            has_range_deletions,
                        },
                    ));
                }
                5 => {
                    // deleted_file
                    if pos + 4 + 8 > data.len() {
                        return Err(Error::Corruption("truncated deleted_file".into()));
                    }
                    let level = u32::from_le_bytes(data[pos..pos + 4].try_into().unwrap());
                    pos += 4;
                    let number = u64::from_le_bytes(data[pos..pos + 8].try_into().unwrap());
                    pos += 8;
                    edit.deleted_files.push((level, number));
                }
                _ => {
                    return Err(Error::Corruption(format!("unknown tag: {}", tag)));
                }
            }
        }

        Ok(edit)
    }
}

impl VersionEdit {
    /// Create a VersionEdit that represents a complete snapshot of a Version.
    /// Used when rewriting the MANIFEST to compact accumulated edits.
    pub fn from_version_snapshot(
        version: &crate::manifest::version::Version,
        log_number: u64,
        next_file_number: u64,
        last_sequence: crate::types::SequenceNumber,
    ) -> Self {
        let mut edit = Self::new();
        edit.set_log_number(log_number);
        edit.set_next_file_number(next_file_number);
        edit.set_last_sequence(last_sequence);

        for (level, files) in version.files.iter().enumerate() {
            for tf in files {
                edit.add_file(level as u32, tf.meta.clone());
            }
        }

        edit
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_encode_decode_empty() {
        let edit = VersionEdit::new();
        let encoded = edit.encode();
        let decoded = VersionEdit::decode(&encoded).unwrap();
        assert_eq!(decoded.log_number, None);
        assert_eq!(decoded.next_file_number, None);
        assert_eq!(decoded.last_sequence, None);
        assert!(decoded.new_files.is_empty());
        assert!(decoded.deleted_files.is_empty());
    }

    #[test]
    fn test_encode_decode_full() {
        let mut edit = VersionEdit::new();
        edit.set_log_number(42);
        edit.set_next_file_number(100);
        edit.set_last_sequence(999);
        edit.add_file(
            0,
            FileMetaData {
                number: 10,
                file_size: 4096,
                smallest_key: b"aaa".to_vec(),
                largest_key: b"zzz".to_vec(),
                has_range_deletions: false,
            },
        );
        edit.add_file(
            1,
            FileMetaData {
                number: 20,
                file_size: 8192,
                smallest_key: b"bbb".to_vec(),
                largest_key: b"yyy".to_vec(),
                has_range_deletions: false,
            },
        );
        edit.delete_file(0, 5);
        edit.delete_file(1, 15);

        let encoded = edit.encode();
        let decoded = VersionEdit::decode(&encoded).unwrap();

        assert_eq!(decoded.log_number, Some(42));
        assert_eq!(decoded.next_file_number, Some(100));
        assert_eq!(decoded.last_sequence, Some(999));
        assert_eq!(decoded.new_files.len(), 2);
        assert_eq!(decoded.new_files[0].0, 0);
        assert_eq!(decoded.new_files[0].1.number, 10);
        assert_eq!(decoded.new_files[1].0, 1);
        assert_eq!(decoded.new_files[1].1.number, 20);
        assert_eq!(decoded.deleted_files.len(), 2);
        assert_eq!(decoded.deleted_files[0], (0, 5));
        assert_eq!(decoded.deleted_files[1], (1, 15));
    }

    #[test]
    fn test_version_edit_all_fields_roundtrip() {
        let mut edit = VersionEdit::new();

        // Set all scalar fields
        edit.set_log_number(u64::MAX);
        edit.set_next_file_number(u64::MAX - 1);
        edit.set_last_sequence(u64::MAX - 2);

        // Add files across multiple levels with varied metadata
        edit.add_file(
            0,
            FileMetaData {
                number: 1,
                file_size: 0,                 // edge case: zero size
                smallest_key: vec![],         // empty key
                largest_key: vec![0xFF; 256], // binary key
                has_range_deletions: true,
            },
        );
        edit.add_file(
            6,
            FileMetaData {
                number: u64::MAX,
                file_size: u64::MAX,
                smallest_key: b"start".to_vec(),
                largest_key: b"stop".to_vec(),
                has_range_deletions: false,
            },
        );

        // Add deletions
        edit.delete_file(0, 999);
        edit.delete_file(3, u64::MAX);

        let encoded = edit.encode();
        let decoded = VersionEdit::decode(&encoded).unwrap();

        // Verify all scalar fields
        assert_eq!(decoded.log_number, Some(u64::MAX));
        assert_eq!(decoded.next_file_number, Some(u64::MAX - 1));
        assert_eq!(decoded.last_sequence, Some(u64::MAX - 2));

        // Verify new_files
        assert_eq!(decoded.new_files.len(), 2);

        let (level0, ref meta0) = decoded.new_files[0];
        assert_eq!(level0, 0);
        assert_eq!(meta0.number, 1);
        assert_eq!(meta0.file_size, 0);
        assert!(meta0.smallest_key.is_empty());
        assert_eq!(meta0.largest_key, vec![0xFF; 256]);
        assert!(meta0.has_range_deletions);

        let (level6, ref meta6) = decoded.new_files[1];
        assert_eq!(level6, 6);
        assert_eq!(meta6.number, u64::MAX);
        assert_eq!(meta6.file_size, u64::MAX);
        assert_eq!(meta6.smallest_key, b"start");
        assert_eq!(meta6.largest_key, b"stop");

        // Verify deleted_files
        assert_eq!(decoded.deleted_files.len(), 2);
        assert_eq!(decoded.deleted_files[0], (0, 999));
        assert_eq!(decoded.deleted_files[1], (3, u64::MAX));
    }

    #[test]
    fn test_version_edit_multiple_files() {
        let mut edit = VersionEdit::new();
        edit.set_log_number(1);
        edit.set_last_sequence(5000);

        // Add 50 files across 7 levels
        for i in 0..50 {
            let level = (i % 7) as u32;
            edit.add_file(
                level,
                FileMetaData {
                    number: i as u64 + 100,
                    file_size: (i as u64 + 1) * 4096,
                    smallest_key: format!("s_{:04}", i).into_bytes(),
                    largest_key: format!("l_{:04}", i).into_bytes(),
                    has_range_deletions: i % 3 == 0,
                },
            );
        }

        // Delete 20 files
        for i in 0..20 {
            let level = (i % 7) as u32;
            edit.delete_file(level, i as u64);
        }

        let encoded = edit.encode();
        let decoded = VersionEdit::decode(&encoded).unwrap();

        assert_eq!(decoded.log_number, Some(1));
        assert_eq!(decoded.last_sequence, Some(5000));
        assert_eq!(decoded.next_file_number, None);

        assert_eq!(decoded.new_files.len(), 50);
        assert_eq!(decoded.deleted_files.len(), 20);

        // Spot-check a few entries
        for i in 0..50 {
            let (level, ref meta) = decoded.new_files[i];
            assert_eq!(level, (i % 7) as u32);
            assert_eq!(meta.number, i as u64 + 100);
            assert_eq!(meta.file_size, (i as u64 + 1) * 4096);
            assert_eq!(meta.smallest_key, format!("s_{:04}", i).into_bytes());
            assert_eq!(meta.largest_key, format!("l_{:04}", i).into_bytes());
        }
        for i in 0..20 {
            assert_eq!(decoded.deleted_files[i], ((i % 7) as u32, i as u64));
        }

        // Encode-decode again to verify stability
        let re_encoded = decoded.encode();
        assert_eq!(encoded, re_encoded);
    }
}
