//! Data block encoding and decoding with prefix compression.
//!
//! Block format:
//! ```text
//! ┌──────────────────────────────────────────────────┐
//! │ Entry 0: shared_len(varint) | unshared_len(varint)│
//! │          | value_len(varint) | unshared_key_bytes │
//! │          | value_bytes                            │
//! │ Entry 1: ...                                      │
//! │ ...                                               │
//! │ Entry N: ...                                      │
//! ├──────────────────────────────────────────────────┤
//! │ Restart[0]: u32 LE                                │
//! │ Restart[1]: u32 LE                                │
//! │ ...                                               │
//! │ Restart[R-1]: u32 LE                             │
//! │ num_restarts: u32 LE                             │
//! └──────────────────────────────────────────────────┘
//! ```

use std::sync::Arc;

use ruc::*;

use crate::error::{Error, Result};

/// Decode a varint from the given buffer. Returns (value, bytes_consumed).
pub fn decode_varint(data: &[u8]) -> Result<(u32, usize)> {
    let mut result: u32 = 0;
    let mut shift = 0;
    for (i, &byte) in data.iter().enumerate() {
        if i >= 5 {
            return Err(eg!(Error::Corruption("varint too long".to_string())));
        }
        result |= ((byte & 0x7F) as u32) << shift;
        if byte & 0x80 == 0 {
            return Ok((result, i + 1));
        }
        shift += 7;
    }
    Err(eg!(Error::Corruption("unterminated varint".to_string())))
}

/// Encode a u32 as a varint, return bytes written.
pub fn encode_varint(buf: &mut [u8], mut value: u32) -> usize {
    let mut i = 0;
    while value >= 0x80 {
        buf[i] = (value as u8) | 0x80;
        value >>= 7;
        i += 1;
    }
    buf[i] = value as u8;
    i + 1
}

/// Encode a u32 as a varint and push to a Vec.
pub fn encode_varint_vec(buf: &mut Vec<u8>, value: u32) {
    let mut tmp = [0u8; 5];
    let n = encode_varint(&mut tmp, value);
    buf.extend_from_slice(&tmp[..n]);
}

/// A read-only view of a data block.
pub struct Block {
    data: Arc<Vec<u8>>,
    restart_offset: usize,
    num_restarts: u32,
}

impl Block {
    /// Parse a data block from shared (Arc) raw bytes — zero-copy from block cache.
    pub fn new(data: Arc<Vec<u8>>) -> Result<Self> {
        if data.len() < 4 {
            return Err(eg!(Error::Corruption("block too short".to_string())));
        }
        let num_restarts = u32::from_le_bytes(data[data.len() - 4..].try_into().unwrap());
        let restarts_size = (num_restarts as usize) * 4 + 4; // restart array + count
        if restarts_size > data.len() {
            return Err(eg!(Error::Corruption("bad restart count".to_string())));
        }
        let restart_offset = data.len() - restarts_size;

        Ok(Self {
            data,
            restart_offset,
            num_restarts,
        })
    }

    /// Parse a data block from owned bytes (convenience for tests and non-cached paths).
    pub fn from_vec(data: Vec<u8>) -> Result<Self> {
        Self::new(Arc::new(data))
    }

    /// Get the restart point offset at index `i`.
    fn restart_point(&self, i: u32) -> u32 {
        let offset = self.restart_offset + (i as usize) * 4;
        u32::from_le_bytes(self.data[offset..offset + 4].try_into().unwrap())
    }

    /// Iterate over all key-value pairs in the block.
    pub fn iter(&self) -> BlockIterator<'_> {
        BlockIterator {
            block: self,
            offset: 0,
            key: Vec::new(),
            value_start: 0,
            value_len: 0,
        }
    }

    /// Binary search for a key using a custom comparator, then linear scan.
    /// Returns Some((key, value)) for the first key where compare(key, target) >= Equal,
    /// or None if all keys are less than target.
    pub fn seek_by<F: Fn(&[u8], &[u8]) -> std::cmp::Ordering>(
        &self,
        target: &[u8],
        compare: F,
    ) -> Option<(Vec<u8>, Vec<u8>)> {
        // Binary search on restart points
        let mut left = 0u32;
        let mut right = self.num_restarts;

        while left < right {
            let mid = left + (right - left) / 2;
            let rp = self.restart_point(mid) as usize;

            match decode_entry_at(&self.data, rp, &[]) {
                Some((key, _, _)) => {
                    if compare(&key, target) == std::cmp::Ordering::Less {
                        left = mid + 1;
                    } else {
                        right = mid;
                    }
                }
                None => {
                    right = mid;
                }
            }
        }

        // Start scanning
        let start = if left > 0 {
            self.restart_point(left - 1) as usize
        } else {
            0
        };

        let mut offset = start;
        let mut current_key = Vec::new();

        while offset < self.restart_offset {
            match decode_entry_at(&self.data, offset, &current_key) {
                Some((key, value, next_off)) => {
                    if compare(&key, target) != std::cmp::Ordering::Less {
                        return Some((key, value));
                    }
                    current_key = key;
                    offset = next_off;
                }
                None => break,
            }
        }

        None
    }

    /// Seek to the last entry where compare(key, target) <= Equal.
    /// Uses binary search on restart points, then forward scan to find the last entry <= target.
    pub fn seek_for_prev_by<F: Fn(&[u8], &[u8]) -> std::cmp::Ordering>(
        &self,
        target: &[u8],
        compare: F,
    ) -> Option<(Vec<u8>, Vec<u8>)> {
        // Binary search on restart points to find the last restart point whose
        // key is <= target.
        let mut left = 0u32;
        let mut right = self.num_restarts;

        while left < right {
            let mid = left + (right - left) / 2;
            let rp = self.restart_point(mid) as usize;

            match decode_entry_at(&self.data, rp, &[]) {
                Some((key, _, _)) => {
                    if compare(&key, target) != std::cmp::Ordering::Greater {
                        left = mid + 1;
                    } else {
                        right = mid;
                    }
                }
                None => {
                    right = mid;
                }
            }
        }

        // `left` is now the first restart point whose key > target.
        // We need to scan from restart point `left - 1` (or 0 if left == 0),
        // but we might also need to check from `left - 2` in case entries
        // between restart points span the boundary.
        // To be safe, start from one restart point before the one that's > target.
        let start_restart = if left > 0 { left - 1 } else { 0 };
        let start = self.restart_point(start_restart) as usize;

        let mut offset = start;
        let mut current_key = Vec::new();
        let mut best: Option<(Vec<u8>, Vec<u8>)> = None;

        while offset < self.restart_offset {
            match decode_entry_at(&self.data, offset, &current_key) {
                Some((key, value, next_off)) => {
                    if compare(&key, target) != std::cmp::Ordering::Greater {
                        best = Some((key.clone(), value));
                    } else {
                        // All subsequent entries will be > target, we're done
                        break;
                    }
                    current_key = key;
                    offset = next_off;
                }
                None => break,
            }
        }

        best
    }

    /// Decode all entries from the given restart point index through the end of
    /// the data region. Returns a Vec of (key, value) pairs.
    /// This is used by TableIterator::prev() to re-decode a block from a restart point.
    pub fn iter_from_restart(&self, restart_index: u32) -> Vec<(Vec<u8>, Vec<u8>)> {
        if restart_index >= self.num_restarts {
            return Vec::new();
        }

        let start = self.restart_point(restart_index) as usize;
        let mut offset = start;
        let mut current_key = Vec::new();
        let mut entries = Vec::new();

        while offset < self.restart_offset {
            match decode_entry_at(&self.data, offset, &current_key) {
                Some((key, value, next_off)) => {
                    entries.push((key.clone(), value));
                    current_key = key;
                    offset = next_off;
                }
                None => break,
            }
        }

        entries
    }

    /// Decode entries from the given restart point index to the next restart point.
    /// Returns only entries within this single segment (not the rest of the block).
    pub fn iter_restart_segment(&self, restart_index: u32) -> Vec<(Vec<u8>, Vec<u8>)> {
        if restart_index >= self.num_restarts {
            return Vec::new();
        }

        let start = self.restart_point(restart_index) as usize;
        let end = if restart_index + 1 < self.num_restarts {
            self.restart_point(restart_index + 1) as usize
        } else {
            self.restart_offset
        };

        let mut offset = start;
        let mut current_key = Vec::new();
        let mut entries = Vec::new();

        while offset < end {
            match decode_entry_at(&self.data, offset, &current_key) {
                Some((key, value, next_off)) => {
                    entries.push((key.clone(), value));
                    current_key = key;
                    offset = next_off;
                }
                None => break,
            }
        }

        entries
    }

    /// Find which restart segment contains a given byte offset in the block data.
    /// Returns the restart index whose restart point is <= offset.
    pub fn restart_index_for_offset(&self, offset: usize) -> u32 {
        let offset = offset as u32;
        let mut left = 0u32;
        let mut right = self.num_restarts;
        while left < right {
            let mid = left + (right - left) / 2;
            if self.restart_point(mid) <= offset {
                left = mid + 1;
            } else {
                right = mid;
            }
        }
        left.saturating_sub(1)
    }

    /// Decode only the first key at the given restart point (zero shared prefix).
    /// O(1) — decodes a single entry, no segment walk needed.
    pub fn first_key_at_restart(&self, restart_index: u32) -> Option<Vec<u8>> {
        if restart_index >= self.num_restarts {
            return None;
        }
        let offset = self.restart_point(restart_index) as usize;
        // At a restart point, shared_len is always 0, so prev_key can be empty.
        decode_entry_at(&self.data, offset, &[]).map(|(key, _, _)| key)
    }

    /// Return the number of restart points in this block.
    pub fn num_restarts(&self) -> u32 {
        self.num_restarts
    }

    /// Return the offset where entry data ends and restart array begins.
    pub fn data_end_offset(&self) -> usize {
        self.restart_offset
    }

    /// Return a reference to the underlying raw data.
    pub fn data(&self) -> &[u8] {
        &self.data
    }

    /// Binary search for a key using restart points, then linear scan.
    /// Returns Some((key, value)) for exact user key match at the latest sequence,
    /// or None if not found.
    pub fn seek(&self, target: &[u8]) -> Option<(Vec<u8>, Vec<u8>)> {
        // Binary search on restart points to find the right region
        let mut left = 0u32;
        let mut right = self.num_restarts;

        while left < right {
            let mid = left + (right - left) / 2;
            let rp = self.restart_point(mid) as usize;

            // Decode the key at restart point (shared_len is always 0 at restart)
            match decode_entry_at(&self.data, rp, &[]) {
                Some((key, _, next_off)) => {
                    let _ = next_off;
                    if key.as_slice() < target {
                        left = mid + 1;
                    } else {
                        right = mid;
                    }
                }
                None => {
                    right = mid;
                }
            }
        }

        // Start scanning from the restart point before `left`
        let start = if left > 0 {
            self.restart_point(left - 1) as usize
        } else {
            0
        };

        let mut offset = start;
        let mut current_key = Vec::new();

        while offset < self.restart_offset {
            match decode_entry_at(&self.data, offset, &current_key) {
                Some((key, value, next_off)) => {
                    if key.as_slice() >= target {
                        return Some((key, value));
                    }
                    current_key = key;
                    offset = next_off;
                }
                None => break,
            }
        }

        None
    }
}

/// Decode one entry at `offset` with the given `prev_key` for prefix decompression.
/// Returns (full_key, value, next_offset).
fn decode_entry_at(
    data: &[u8],
    offset: usize,
    prev_key: &[u8],
) -> Option<(Vec<u8>, Vec<u8>, usize)> {
    let mut pos = offset;

    let (shared_len, n) = decode_varint(&data[pos..]).ok()?;
    pos += n;
    let (unshared_len, n) = decode_varint(&data[pos..]).ok()?;
    pos += n;
    let (value_len, n) = decode_varint(&data[pos..]).ok()?;
    pos += n;

    let shared = shared_len as usize;
    let unshared = unshared_len as usize;
    let vlen = value_len as usize;

    if pos + unshared + vlen > data.len() {
        return None;
    }
    if shared > prev_key.len() {
        return None;
    }

    let mut key = Vec::with_capacity(shared + unshared);
    key.extend_from_slice(&prev_key[..shared]);
    key.extend_from_slice(&data[pos..pos + unshared]);
    pos += unshared;

    let value = data[pos..pos + vlen].to_vec();
    pos += vlen;

    Some((key, value, pos))
}

/// Decode one entry at `offset`, reusing `key_buf` for the key (zero-alloc for key).
/// Returns (value_start, value_len, next_offset) — value is a slice into `data`.
/// The key is reconstructed in-place in `key_buf`.
pub(crate) fn decode_entry_reuse(
    data: &[u8],
    offset: usize,
    key_buf: &mut Vec<u8>,
) -> Option<(usize, usize, usize)> {
    let mut pos = offset;

    let (shared_len, n) = decode_varint(&data[pos..]).ok()?;
    pos += n;
    let (unshared_len, n) = decode_varint(&data[pos..]).ok()?;
    pos += n;
    let (value_len, n) = decode_varint(&data[pos..]).ok()?;
    pos += n;

    let shared = shared_len as usize;
    let unshared = unshared_len as usize;
    let vlen = value_len as usize;

    if pos + unshared + vlen > data.len() {
        return None;
    }
    if shared > key_buf.len() {
        return None;
    }

    // Reconstruct key in-place: truncate to shared prefix, append unshared suffix
    key_buf.truncate(shared);
    key_buf.extend_from_slice(&data[pos..pos + unshared]);
    let value_start = pos + unshared;
    let next_offset = value_start + vlen;

    Some((value_start, vlen, next_offset))
}

/// Iterator over entries in a block.
pub struct BlockIterator<'a> {
    block: &'a Block,
    offset: usize,
    key: Vec<u8>,
    value_start: usize,
    value_len: usize,
}

impl<'a> Iterator for BlockIterator<'a> {
    type Item = (Vec<u8>, Vec<u8>);

    fn next(&mut self) -> Option<Self::Item> {
        if self.offset >= self.block.restart_offset {
            return None;
        }

        match decode_entry_at(&self.block.data, self.offset, &self.key) {
            Some((key, value, next_off)) => {
                self.key = key.clone();
                self.value_start = next_off - value.len();
                self.value_len = value.len();
                self.offset = next_off;
                Some((key, value))
            }
            None => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sst::block_builder::BlockBuilder;

    #[test]
    fn test_varint_encode_decode() {
        for val in [0, 1, 127, 128, 255, 300, 16383, 16384, 0xFFFFFFFF_u32] {
            let mut buf = [0u8; 5];
            let n = encode_varint(&mut buf, val);
            let (decoded, m) = decode_varint(&buf[..n]).unwrap();
            assert_eq!(decoded, val);
            assert_eq!(n, m);
        }
    }

    #[test]
    fn test_block_build_and_read() {
        let mut builder = BlockBuilder::new(16); // restart every 16

        let pairs = vec![
            (b"aaa".to_vec(), b"val_a".to_vec()),
            (b"aab".to_vec(), b"val_ab".to_vec()),
            (b"abc".to_vec(), b"val_abc".to_vec()),
            (b"abd".to_vec(), b"val_abd".to_vec()),
            (b"xyz".to_vec(), b"val_xyz".to_vec()),
        ];

        for (k, v) in &pairs {
            builder.add(k, v);
        }

        let data = builder.finish();
        let block = Block::from_vec(data).unwrap();

        let entries: Vec<_> = block.iter().collect();
        assert_eq!(entries.len(), pairs.len());
        for (i, (k, v)) in entries.iter().enumerate() {
            assert_eq!(k, &pairs[i].0);
            assert_eq!(v, &pairs[i].1);
        }
    }

    #[test]
    fn test_block_seek() {
        let mut builder = BlockBuilder::new(4); // restart every 4 entries

        for i in 0..20 {
            let key = format!("key_{:04}", i);
            let val = format!("val_{}", i);
            builder.add(key.as_bytes(), val.as_bytes());
        }

        let data = builder.finish();
        let block = Block::from_vec(data).unwrap();

        // Exact seek
        let (k, v) = block.seek(b"key_0010").unwrap();
        assert_eq!(k, b"key_0010");
        assert_eq!(v, b"val_10");

        // Seek to first key >= target
        let (k, _v) = block.seek(b"key_0005x").unwrap();
        assert_eq!(k, b"key_0006");

        // Seek past end
        assert!(block.seek(b"zzz").is_none());
    }

    #[test]
    fn test_empty_block() {
        // A block with zero entries: just the restart array [0] and restart count [1]
        let builder = BlockBuilder::new(16);
        let data = builder.finish();
        let block = Block::from_vec(data).unwrap();

        // Iterator should yield no entries
        let entries: Vec<_> = block.iter().collect();
        assert!(entries.is_empty());

        // Seek should return None
        assert!(block.seek(b"anything").is_none());
    }

    #[test]
    fn test_single_entry_block() {
        let mut builder = BlockBuilder::new(16);
        builder.add(b"only_key", b"only_value");
        let data = builder.finish();
        let block = Block::from_vec(data).unwrap();

        let entries: Vec<_> = block.iter().collect();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].0, b"only_key");
        assert_eq!(entries[0].1, b"only_value");

        // Seek for the exact key
        let (k, v) = block.seek(b"only_key").unwrap();
        assert_eq!(k, b"only_key");
        assert_eq!(v, b"only_value");

        // Seek before the key should still find it (first key >= target)
        let (k, _) = block.seek(b"a").unwrap();
        assert_eq!(k, b"only_key");

        // Seek after the key should return None
        assert!(block.seek(b"zzz").is_none());
    }

    #[test]
    fn test_large_value_block() {
        let mut builder = BlockBuilder::new(16);
        let large_value = vec![0xAB_u8; 1024 * 1024 + 1]; // 1MB + 1 byte
        builder.add(b"big", &large_value);
        builder.add(b"small", b"tiny");
        let data = builder.finish();
        let block = Block::from_vec(data).unwrap();

        let entries: Vec<_> = block.iter().collect();
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].0, b"big");
        assert_eq!(entries[0].1, large_value);
        assert_eq!(entries[1].0, b"small");
        assert_eq!(entries[1].1, b"tiny");

        // Seek for the large-value key
        let (k, v) = block.seek(b"big").unwrap();
        assert_eq!(k, b"big");
        assert_eq!(v.len(), 1024 * 1024 + 1);
    }

    #[test]
    fn test_varint_boundary_values() {
        // Test encoding/decoding at varint byte-width boundaries:
        // 1 byte: 0..=127, 2 bytes: 128..=16383, 3 bytes: 16384..
        let boundary_values: &[u32] = &[127, 128, 16383, 16384];
        for &val in boundary_values {
            let mut buf = [0u8; 5];
            let n = encode_varint(&mut buf, val);
            let (decoded, m) = decode_varint(&buf[..n]).unwrap();
            assert_eq!(decoded, val, "varint round-trip failed for {}", val);
            assert_eq!(n, m, "varint byte count mismatch for {}", val);

            // Verify expected byte widths
            match val {
                0..=127 => assert_eq!(n, 1, "value {} should be 1 byte", val),
                128..=16383 => assert_eq!(n, 2, "value {} should be 2 bytes", val),
                16384..=2097151 => assert_eq!(n, 3, "value {} should be 3 bytes", val),
                _ => {}
            }
        }

        // Also build a block whose entries exercise these varint sizes:
        // keys/values with lengths at the boundary points
        let mut builder = BlockBuilder::new(1); // restart every entry to avoid prefix compression
        let key_128 = vec![b'k'; 128];
        let val_16384 = vec![b'v'; 16384];
        builder.add(&key_128, &val_16384);
        let data = builder.finish();
        let block = Block::from_vec(data).unwrap();
        let entries: Vec<_> = block.iter().collect();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].0.len(), 128);
        assert_eq!(entries[0].1.len(), 16384);
    }

    #[test]
    fn test_seek_for_prev_by() {
        let mut builder = BlockBuilder::new(4); // restart every 4 entries

        for i in 0..20 {
            let key = format!("key_{:04}", i * 2); // even keys: 0,2,4,...,38
            let val = format!("val_{}", i);
            builder.add(key.as_bytes(), val.as_bytes());
        }

        let data = builder.finish();
        let block = Block::from_vec(data).unwrap();
        let compare = |a: &[u8], b: &[u8]| a.cmp(b);

        // Exact match: seek_for_prev to key_0010 (exists)
        let (k, v) = block.seek_for_prev_by(b"key_0010", compare).unwrap();
        assert_eq!(k, b"key_0010");
        assert_eq!(v, b"val_5");

        // Between entries: seek_for_prev to key_0011 (between key_0010 and key_0012)
        let (k, _) = block.seek_for_prev_by(b"key_0011", compare).unwrap();
        assert_eq!(k, b"key_0010");

        // After last: seek_for_prev to "zzz" should return last entry
        let (k, _) = block.seek_for_prev_by(b"zzz", compare).unwrap();
        assert_eq!(k, b"key_0038");

        // Before first: seek_for_prev to "aaa" should return None
        assert!(block.seek_for_prev_by(b"aaa", compare).is_none());

        // Exactly first key
        let (k, v) = block.seek_for_prev_by(b"key_0000", compare).unwrap();
        assert_eq!(k, b"key_0000");
        assert_eq!(v, b"val_0");

        // Just before first key
        assert!(block.seek_for_prev_by(b"key_", compare).is_none());
    }

    #[test]
    fn test_iter_from_restart() {
        let mut builder = BlockBuilder::new(4); // restart every 4 entries

        for i in 0..12 {
            let key = format!("key_{:04}", i);
            let val = format!("val_{}", i);
            builder.add(key.as_bytes(), val.as_bytes());
        }

        let data = builder.finish();
        let block = Block::from_vec(data).unwrap();

        // Restart 0: entries 0..4 and beyond
        let entries = block.iter_from_restart(0);
        assert_eq!(entries.len(), 12);
        assert_eq!(entries[0].0, b"key_0000");

        // Restart 1: entries 4..8 and beyond
        let entries = block.iter_from_restart(1);
        assert_eq!(entries.len(), 8); // entries 4 through 11
        assert_eq!(entries[0].0, b"key_0004");

        // Restart 2: entries 8..12
        let entries = block.iter_from_restart(2);
        assert_eq!(entries.len(), 4); // entries 8 through 11
        assert_eq!(entries[0].0, b"key_0008");

        // Out of bounds
        let entries = block.iter_from_restart(100);
        assert!(entries.is_empty());
    }
}
