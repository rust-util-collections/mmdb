//! Data block encoding and decoding with prefix compression.
//!
//! The entries come first, followed by the offsets where decoding can restart.
//!
//! ```text
//! block = entries... | restart_offsets... | restart_count
//! entry = shared_len | suffix_len | value_len | key_suffix | value
//! ```
//!
//! Entry lengths are varints. Restart offsets and the count are little-endian
//! `u32`s. Between restarts, the raw keys `car` then `cat` share two bytes, so
//! the second entry stores just the suffix `t`. A restart stores the full key.

use std::{cmp::Ordering, sync::Arc};

use smallvec::SmallVec;

use crate::error::{Error, Result};
use crate::types::{SequenceNumber, ValueType, decode_internal_key};

/// Decode a varint from the given buffer. Returns (value, bytes_consumed).
pub fn decode_varint(data: &[u8]) -> Result<(u32, usize)> {
    let mut result: u32 = 0;
    let mut shift = 0;
    for (i, &byte) in data.iter().enumerate() {
        if i >= 5 {
            return Err(Error::corruption("varint too long"));
        }
        // The 5th byte of a u32 varint may only carry the top 4 bits; any
        // higher bits set means the value overflows u32 (corrupt length field).
        if i == 4 && byte & 0xF0 != 0 {
            return Err(Error::corruption("varint overflows u32"));
        }
        result |= ((byte & 0x7F) as u32) << shift;
        if byte & 0x80 == 0 {
            return Ok((result, i + 1));
        }
        shift += 7;
    }
    Err(Error::corruption("unterminated varint"))
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
#[derive(Clone)]
pub struct Block {
    data: Arc<Vec<u8>>,
    restart_offset: usize,
    num_restarts: u32,
}

/// Packed index/offset descriptor for a single entry inside a restart segment.
#[derive(Clone, Copy)]
pub(crate) struct RestartSegmentEntry {
    // Expanded keys share an arena that can exceed the encoded block's size.
    pub key_start: usize,
    pub key_end: usize,
    pub val_offset: u32,
    pub val_len: u32,
}

impl RestartSegmentEntry {
    fn new(key_start: usize, key_end: usize, val_offset: usize, val_len: usize) -> Self {
        // Value locations address the encoded block, capped at 64 MiB.
        Self {
            key_start,
            key_end,
            val_offset: val_offset as u32,
            val_len: val_len as u32,
        }
    }
}

impl Block {
    /// Construct a `Block` whose restart table was already validated on cache admission.
    #[inline]
    pub(crate) fn from_validated(
        data: Arc<Vec<u8>>,
        restart_offset: usize,
        num_restarts: u32,
    ) -> Self {
        Self {
            data,
            restart_offset,
            num_restarts,
        }
    }

    /// Parse a data block from shared (Arc) raw bytes — zero-copy from block cache.
    pub fn new(data: Arc<Vec<u8>>) -> Result<Self> {
        if data.len() < 4 {
            return Err(Error::corruption("block too short"));
        }
        let num_restarts = u32::from_le_bytes(data[data.len() - 4..].try_into().unwrap());
        let restarts_size = (num_restarts as usize)
            .checked_mul(4)
            .and_then(|n| n.checked_add(4))
            .ok_or_else(|| Error::corruption("restart count overflow"))?;
        if num_restarts == 0 {
            return Err(Error::corruption("bad restart count"));
        }
        if restarts_size > data.len() {
            return Err(Error::corruption("bad restart count"));
        }
        let restart_offset = data.len() - restarts_size;
        let mut prev_restart = 0usize;
        for i in 0..num_restarts {
            let offset = restart_offset + (i as usize) * 4;
            let restart = u32::from_le_bytes(data[offset..offset + 4].try_into().unwrap()) as usize;
            // A restart must point at an entry. An empty segment (duplicate
            // offset, or a restart at the entry-region end) makes reverse
            // iteration a clean miss while a forward scan still yields keys.
            // An empty block's first restart is the only offset that may
            // equal the entry-region end.
            let empty_block = restart_offset == 0 && i == 0 && restart == 0;
            if (i == 0 && restart != 0)
                || restart > restart_offset
                || (restart == restart_offset && !empty_block)
                || (i > 0 && restart <= prev_restart)
            {
                return Err(Error::corruption("bad restart offset"));
            }
            prev_restart = restart;
        }

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
    #[inline]
    pub(crate) fn restart_point(&self, i: u32) -> u32 {
        let offset = self.restart_offset + (i as usize) * 4;
        u32::from_le_bytes(self.data[offset..offset + 4].try_into().unwrap())
    }

    /// Decode the unshared `(key, value, next_offset)` directly at restart point `i`
    /// with zero key/value copies. At every restart point `shared_len` must be 0.
    #[inline]
    pub(crate) fn decode_restart_entry(&self, i: u32) -> Result<(&[u8], &[u8], usize)> {
        let offset = self.restart_point(i) as usize;
        let seg_end = if i + 1 < self.num_restarts {
            self.restart_point(i + 1) as usize
        } else {
            self.restart_offset
        };
        let data = &self.data[..seg_end];
        if let Some(&[0, b1, b2, ref rest @ ..]) = data.get(offset..)
            && b1 < 0x80
        {
            if b2 < 0x80 {
                let key_start = offset + 3;
                let key_end = key_start + b1 as usize;
                let val_end = key_end + b2 as usize;
                if val_end <= data.len() {
                    return Ok((&data[key_start..key_end], &data[key_end..val_end], val_end));
                }
            } else if let Some(&b3) = rest.first()
                && b3 < 0x80
            {
                let vlen = ((b2 & 0x7f) as usize) | ((b3 as usize) << 7);
                let key_start = offset + 4;
                let key_end = key_start + b1 as usize;
                let val_end = key_end + vlen;
                if val_end <= data.len() {
                    return Ok((&data[key_start..key_end], &data[key_end..val_end], val_end));
                }
            }
        }
        decode_restart_entry_slow(data, offset)
    }

    /// Entry bytes only; excludes the restart array and restart count.
    #[inline]
    fn entry_data(&self) -> &[u8] {
        &self.data[..self.restart_offset]
    }

    /// Iterate over all key-value pairs in the block.
    /// If the block data is corrupted mid-iteration, `next()` returns `None`
    /// (as it would on clean exhaustion) but records the failure — call
    /// `error()` after iteration ends to distinguish the two cases.
    pub fn iter(&self) -> BlockIterator<'_> {
        let seg_end = if self.num_restarts > 1 {
            self.restart_point(1) as usize
        } else {
            self.restart_offset
        };
        BlockIterator {
            block: self,
            offset: 0,
            next_restart_idx: 1,
            seg_end,
            key: Vec::new(),
            value_start: 0,
            value_len: 0,
            error: None,
        }
    }

    /// Binary search for a key using a custom comparator, then linear scan.
    /// Returns `Ok(Some((key, value)))` for the first key where `compare(key, target) >= Equal`,
    /// `Ok(None)` if all keys are less than target, or `Err` if a block entry is
    /// malformed (corrupted data — never a legitimate outcome of a well-formed block).
    #[cfg(test)]
    pub fn seek_by<F: Fn(&[u8], &[u8]) -> Ordering>(
        &self,
        target: &[u8],
        compare: F,
    ) -> Result<Option<(Vec<u8>, Vec<u8>)>> {
        let mut key = Vec::new();
        Ok(self
            .seek_by_reuse(target, compare, &mut key)?
            .map(|value| (key, value.to_vec())))
    }

    /// Find the first key >= target, keeping it in `key` and borrowing its
    /// value from the block. Probes and skipped entries never copy values.
    pub fn seek_by_reuse<F: Fn(&[u8], &[u8]) -> Ordering>(
        &self,
        target: &[u8],
        compare: F,
        key: &mut impl KeyBuffer,
    ) -> Result<Option<&[u8]>> {
        key.replace_suffix(0, &[]);
        if self.restart_offset == 0 {
            return Ok(None);
        }
        // Binary search on restart points
        let mut left = 0u32;
        let mut right = self.num_restarts;

        while left < right {
            let mid = left + (right - left) / 2;
            let (rk, _, _) = self.decode_restart_entry(mid)?;
            if compare(rk, target) == Ordering::Less {
                left = mid + 1;
            } else {
                right = mid;
            }
        }

        // Start scanning bounded to the selected restart segment so an entry
        // with an inflated length cannot straddle into the next segment.
        let (start, seg_end) = if left > 0 {
            let s = self.restart_point(left - 1) as usize;
            let e = if left < self.num_restarts {
                self.restart_point(left) as usize
            } else {
                self.restart_offset
            };
            (s, e)
        } else {
            let e = if self.num_restarts > 1 {
                self.restart_point(1) as usize
            } else {
                self.restart_offset
            };
            (0, e)
        };

        let mut offset = start;
        key.replace_suffix(0, &[]);
        let seg_data = &self.data[..seg_end];

        while offset < seg_end {
            let (value_start, value_len, next_off) = decode_entry_reuse(seg_data, offset, key)?;
            if compare(key, target) != Ordering::Less {
                return Ok(Some(&seg_data[value_start..value_start + value_len]));
            }
            offset = next_off;
        }

        if left < self.num_restarts {
            let (rk, val, _) = self.decode_restart_entry(left)?;
            key.replace_suffix(0, rk);
            return Ok(Some(val));
        }

        Ok(None)
    }

    /// Find the last restart whose first key is at or below the target.
    #[cfg(test)]
    pub fn restart_for_prev(
        &self,
        target: &[u8],
        compare: impl Fn(&[u8], &[u8]) -> Ordering,
    ) -> Result<Option<u32>> {
        if self.restart_offset == 0 {
            return Ok(None);
        }
        let (mut left, mut right) = (0, self.num_restarts);
        while left < right {
            let mid = left + (right - left) / 2;
            let (rk, _, _) = self.decode_restart_entry(mid)?;
            if compare(rk, target) == Ordering::Greater {
                right = mid;
            } else {
                left = mid + 1;
            }
        }
        Ok(left.checked_sub(1))
    }

    /// Decode all entries of `restart_index` into caller-reused arena buffers,
    /// avoiding per-entry `Vec` allocations and value copies during reverse scans.
    pub(crate) fn decode_restart_segment_into(
        &self,
        restart_index: u32,
        keys_arena: &mut Vec<u8>,
        entries: &mut Vec<RestartSegmentEntry>,
    ) -> Result<()> {
        keys_arena.clear();
        entries.clear();
        if restart_index >= self.num_restarts {
            return Ok(());
        }

        let start = self.restart_point(restart_index) as usize;
        let end = if restart_index + 1 < self.num_restarts {
            self.restart_point(restart_index + 1) as usize
        } else {
            self.restart_offset
        };

        let mut offset = start;
        let mut current_key = SmallVec::<[u8; 128]>::new();
        while offset < end {
            let (val_start, val_len, next_off) =
                decode_entry_reuse(&self.entry_data()[..end], offset, &mut current_key)?;
            let key_start = keys_arena.len();
            keys_arena.extend_from_slice(&current_key);
            entries.push(RestartSegmentEntry::new(
                key_start,
                keys_arena.len(),
                val_start,
                val_len,
            ));
            offset = next_off;
        }
        Ok(())
    }

    /// Decode entries from the given restart point index to the next restart point.
    /// Returns only entries within this single segment (not the rest of the block).
    /// Returns `Err` if a block entry is malformed (corrupted data).
    #[cfg(test)]
    pub fn iter_restart_segment(&self, restart_index: u32) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        if restart_index >= self.num_restarts {
            return Ok(Vec::new());
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
            let (key, value, next_off) =
                decode_entry_at(&self.entry_data()[..end], offset, &current_key)?;
            entries.push((key.clone(), value));
            current_key = key;
            offset = next_off;
        }

        Ok(entries)
    }

    /// Return the number of restart points in this block.
    pub fn num_restarts(&self) -> u32 {
        self.num_restarts
    }

    /// Recover sequences without copying values. An invalid key still has a
    /// known entry length. Broken entry framing only hides the rest of its
    /// restart interval. The normal read path reports either error on access.
    pub(crate) fn max_readable_point_sequence(&self) -> SequenceNumber {
        let mut maximum = 0;
        let mut key = SmallVec::<[u8; 128]>::new();
        for restart in 0..self.num_restarts {
            let mut offset = self.restart_point(restart) as usize;
            let end = if restart + 1 < self.num_restarts {
                self.restart_point(restart + 1) as usize
            } else {
                self.restart_offset
            };
            key.clear();
            while offset < end {
                let Ok((_, _, next)) = decode_entry_reuse(&self.data[..end], offset, &mut key)
                else {
                    break;
                };
                if let Ok((_, seq, ValueType::Value | ValueType::Deletion)) =
                    decode_internal_key(&key)
                {
                    maximum = maximum.max(seq);
                }
                offset = next;
            }
        }
        maximum
    }

    /// Return the offset where entry data ends and restart array begins.
    pub fn data_end_offset(&self) -> usize {
        self.restart_offset
    }

    /// Return a reference to the underlying raw data.
    pub fn data(&self) -> &[u8] {
        &self.data
    }

    /// Return a reference to the Arc wrapping the raw block data.
    /// Used by LazyValue to keep the block alive without copying values.
    pub fn data_arc(&self) -> &Arc<Vec<u8>> {
        &self.data
    }

    /// Validate that this block is a non-empty SST point data block whose entries
    /// obey restart-segment boundaries, point `InternalKey` encoding, strict
    /// `compare_internal_key` monotonicity, and index `[first_key, sep]` bounds.
    pub(crate) fn validate_point_block_ordered(&self, first_key: &[u8], sep: &[u8]) -> Result<()> {
        if self.restart_offset == 0 || self.num_restarts == 0 {
            return Err(Error::corruption("empty SST data block"));
        }
        let mut key_buf = SmallVec::<[u8; 128]>::new();
        let mut offset = 0usize;
        let mut next_restart = 1u32;
        let mut seg_end = if self.num_restarts > 1 {
            self.restart_point(1) as usize
        } else {
            self.restart_offset
        };
        let mut first = true;
        while offset < self.restart_offset {
            let mut at_restart = first;
            if offset == seg_end {
                at_restart = true;
                let r = next_restart;
                next_restart = r + 1;
                seg_end = if r + 1 < self.num_restarts {
                    self.restart_point(r + 1) as usize
                } else {
                    self.restart_offset
                };
            }
            let (_, _, next_off) = decode_point_entry_monotonic(
                &self.data[..seg_end],
                offset,
                !first,
                at_restart,
                &mut key_buf,
            )?;
            if first {
                if key_buf.as_slice() != first_key {
                    return Err(Error::corruption(
                        "SST data block disagrees with index bounds",
                    ));
                }
                first = false;
            }
            offset = next_off;
        }
        if crate::types::compare_internal_key(&key_buf, sep).is_gt() {
            return Err(Error::corruption("SST data block exceeds index separator"));
        }
        Ok(())
    }

    /// Binary search for a key using restart points, then linear scan.
    /// Returns Some((key, value)) for exact user key match at the latest sequence,
    /// or None if not found.
    /// Test-only helper: panics on malformed block data (never expected in test
    /// fixtures — see `decode_entry_at`'s doc comment for why any error here is
    /// unambiguously corruption, not clean EOF).
    #[cfg(test)]
    pub fn seek(&self, target: &[u8]) -> Option<(Vec<u8>, Vec<u8>)> {
        if self.restart_offset == 0 {
            return None;
        }
        // Binary search on restart points to find the right region
        let mut left = 0u32;
        let mut right = self.num_restarts;

        while left < right {
            let mid = left + (right - left) / 2;
            let rp = self.restart_point(mid) as usize;

            // Decode the key at restart point (shared_len is always 0 at restart)
            let (key, _, _) =
                decode_entry_at(self.entry_data(), rp, &[]).expect("corrupted test block data");
            if key.as_slice() < target {
                left = mid + 1;
            } else {
                right = mid;
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
            let (key, value, next_off) = decode_entry_at(self.entry_data(), offset, &current_key)
                .expect("corrupted test block data");
            if key.as_slice() >= target {
                return Some((key, value));
            }
            current_key = key;
            offset = next_off;
        }

        None
    }
}

/// Decode one entry at `offset` with the given `prev_key` for prefix decompression.
/// Returns (full_key, value, next_offset).
///
/// Every caller of this function only ever passes an `offset` that its own loop
/// condition (or a restart-point index validated at `Block::new()` time) has
/// already guaranteed is `< restart_offset <= data.len()`. There is therefore no
/// legitimate "ran cleanly off the end of the block" case reachable here — any
/// internal failure (bad varint, truncated payload, or a shared-prefix length
/// that exceeds the previous key) unambiguously means the entry itself is
/// malformed/corrupted, and must be reported as an error rather than silently
/// treated as "no more entries" (which would let callers mistake corruption for
/// clean iterator exhaustion).
fn decode_entry_at(
    data: &[u8],
    offset: usize,
    prev_key: &[u8],
) -> Result<(Vec<u8>, Vec<u8>, usize)> {
    if offset > data.len() {
        return Err(Error::corruption("block entry offset past end of data"));
    }
    let mut pos = offset;

    let (shared_len, n) = decode_varint(&data[pos..])?;
    pos += n;
    if pos > data.len() {
        return Err(Error::corruption("block entry header truncated"));
    }
    let (unshared_len, n) = decode_varint(&data[pos..])?;
    pos += n;
    if pos > data.len() {
        return Err(Error::corruption("block entry header truncated"));
    }
    let (value_len, n) = decode_varint(&data[pos..])?;
    pos += n;

    let shared = shared_len as usize;
    let unshared = unshared_len as usize;
    let vlen = value_len as usize;

    let key_end = pos
        .checked_add(unshared)
        .ok_or_else(|| Error::corruption("block entry key length overflow"))?;
    let value_end = key_end
        .checked_add(vlen)
        .ok_or_else(|| Error::corruption("block entry value length overflow"))?;
    if value_end > data.len() {
        return Err(Error::corruption("block entry extends past end of data"));
    }
    if shared > prev_key.len() {
        return Err(Error::corruption(
            "block entry shared prefix exceeds previous key length",
        ));
    }

    let mut key = Vec::with_capacity(shared + unshared);
    key.extend_from_slice(&prev_key[..shared]);
    key.extend_from_slice(&data[pos..key_end]);

    let value = data[key_end..value_end].to_vec();

    Ok((key, value, value_end))
}

pub(crate) trait KeyBuffer: std::ops::Deref<Target = [u8]> {
    fn replace_suffix(&mut self, shared: usize, suffix: &[u8]);
}
impl KeyBuffer for Vec<u8> {
    fn replace_suffix(&mut self, shared: usize, suffix: &[u8]) {
        self.truncate(shared);
        self.extend_from_slice(suffix);
    }
}
impl KeyBuffer for SmallVec<[u8; 128]> {
    #[inline]
    fn replace_suffix(&mut self, shared: usize, suffix: &[u8]) {
        self.truncate(shared);
        self.extend_from_slice(suffix);
    }
}

#[cold]
fn decode_restart_entry_slow(data: &[u8], offset: usize) -> Result<(&[u8], &[u8], usize)> {
    if offset >= data.len() {
        return Err(Error::corruption("entry offset past block data"));
    }
    let (shared_len, n1) = decode_varint(&data[offset..])?;
    if shared_len != 0 {
        return Err(Error::corruption(
            "shared prefix exceeds previous key length",
        ));
    }
    let pos = offset + n1;
    let (unshared_len, n2) = decode_varint(&data[pos..])?;
    let pos = pos + n2;
    let (value_len, n3) = decode_varint(&data[pos..])?;
    let key_start = pos + n3;
    let key_end = key_start
        .checked_add(unshared_len as usize)
        .ok_or_else(|| Error::corruption("entry key length overflow"))?;
    let val_end = key_end
        .checked_add(value_len as usize)
        .ok_or_else(|| Error::corruption("entry value length overflow"))?;
    if val_end > data.len() {
        return Err(Error::corruption("block entry truncated"));
    }
    Ok((&data[key_start..key_end], &data[key_end..val_end], val_end))
}

#[inline]
fn decode_entry_header(data: &[u8], offset: usize) -> Result<(usize, usize, usize, usize, usize)> {
    if let Some(&[b0, b1, b2, ref rest @ ..]) = data.get(offset..)
        && (b0 | b1) < 0x80
    {
        if b2 < 0x80 {
            let pos = offset + 3;
            let key_end = pos + b1 as usize;
            let value_end = key_end + b2 as usize;
            if value_end <= data.len() {
                return Ok((b0 as usize, b1 as usize, b2 as usize, pos, key_end));
            }
        } else if let Some(&b3) = rest.first()
            && b3 < 0x80
        {
            let vlen = ((b2 & 0x7f) as usize) | ((b3 as usize) << 7);
            let pos = offset + 4;
            let key_end = pos + b1 as usize;
            let value_end = key_end + vlen;
            if value_end <= data.len() {
                return Ok((b0 as usize, b1 as usize, vlen, pos, key_end));
            }
        }
    }
    decode_entry_header_slow(data, offset)
}

#[cold]
fn decode_entry_header_slow(
    data: &[u8],
    offset: usize,
) -> Result<(usize, usize, usize, usize, usize)> {
    if offset > data.len() {
        return Err(Error::corruption("block entry offset past end of data"));
    }
    let mut pos = offset;
    let (shared_len, n) = decode_varint(&data[pos..])?;
    pos += n;
    if pos > data.len() {
        return Err(Error::corruption("block entry header truncated"));
    }
    let (unshared_len, n) = decode_varint(&data[pos..])?;
    pos += n;
    if pos > data.len() {
        return Err(Error::corruption("block entry header truncated"));
    }
    let (value_len, n) = decode_varint(&data[pos..])?;
    pos += n;

    let shared = shared_len as usize;
    let unshared = unshared_len as usize;
    let vlen = value_len as usize;
    let key_end = pos
        .checked_add(unshared)
        .ok_or_else(|| Error::corruption("block entry key length overflow"))?;
    let value_end = key_end
        .checked_add(vlen)
        .ok_or_else(|| Error::corruption("block entry value length overflow"))?;
    if value_end > data.len() {
        return Err(Error::corruption("block entry extends past end of data"));
    }
    Ok((shared, unshared, vlen, pos, key_end))
}

/// Decode one entry at `offset`, reusing `key_buf` for the key (zero-alloc for key).
/// Returns (value_start, value_len, next_offset) — value is a slice into `data`.
/// The key is reconstructed in-place in `key_buf`.
///
/// See `decode_entry_at`'s doc comment: every caller-supplied `offset` is already
/// guaranteed in-bounds, so any internal failure here means corruption, not clean
/// EOF, and must be surfaced as an error.
#[inline]
pub(crate) fn decode_entry_reuse(
    data: &[u8],
    offset: usize,
    key_buf: &mut impl KeyBuffer,
) -> Result<(usize, usize, usize)> {
    let (shared, _unshared, vlen, pos, key_end) = decode_entry_header(data, offset)?;
    if shared > key_buf.len() {
        return Err(Error::corruption(
            "block entry shared prefix exceeds previous key length",
        ));
    }

    // Reconstruct key in-place: truncate to shared prefix, append unshared suffix
    key_buf.replace_suffix(shared, &data[pos..key_end]);
    Ok((key_end, vlen, key_end + vlen))
}

#[cold]
#[inline(never)]
fn invalid_point_ikey_type(inv_vt: u8) -> Error {
    if inv_vt == 2 {
        Error::corruption("range deletion in point data block")
    } else {
        Error::corruption(format!("internal key has invalid value_type: {inv_vt}"))
    }
}

#[inline]
pub(crate) fn validate_point_ikey(ikey: &[u8]) -> Result<()> {
    if ikey.len() < 8 {
        return Err(Error::corruption("internal key too short"));
    }
    let inv_vt = !ikey[ikey.len() - 1];
    if inv_vt <= 1 {
        Ok(())
    } else {
        Err(invalid_point_ikey_type(inv_vt))
    }
}

/// Decode one SST point data-block entry at `offset` within segment slice `data`,
/// enforcing `shared_len == 0` at restarts, valid point `InternalKey`s (`Value`
/// or `Deletion`), and strict `compare_internal_key` monotonicity over `key_buf`.
#[inline]
pub(crate) fn decode_point_entry_monotonic(
    data: &[u8],
    offset: usize,
    has_prev_key: bool,
    at_restart: bool,
    key_buf: &mut impl KeyBuffer,
) -> Result<(usize, usize, usize)> {
    if !at_restart
        && has_prev_key
        && let Some(&[b0, b1, b2]) = offset.checked_add(3).and_then(|end| data.get(offset..end))
        && (b0 | b1 | b2) & 0x80 == 0
    {
        let shared = b0 as usize;
        let unshared = b1 as usize;
        let vlen = b2 as usize;
        let pos = offset + 3;
        let key_end = pos + unshared;
        let value_end = key_end + vlen;
        let prev_len = key_buf.len();
        if unshared > 8
            && shared + 8 < prev_len
            && value_end <= data.len()
            && (!data[key_end - 1]) <= 1
            && key_buf[shared] < data[pos]
        {
            key_buf.replace_suffix(shared, &data[pos..key_end]);
            return Ok((key_end, vlen, value_end));
        }
    }
    decode_point_entry_monotonic_general(data, offset, has_prev_key, at_restart, key_buf)
}

#[inline(never)]
fn decode_point_entry_monotonic_general(
    data: &[u8],
    offset: usize,
    has_prev_key: bool,
    at_restart: bool,
    key_buf: &mut impl KeyBuffer,
) -> Result<(usize, usize, usize)> {
    let (shared, unshared, vlen, pos, key_end) = decode_entry_header(data, offset)?;
    let value_end = key_end + vlen;

    if at_restart && shared != 0 {
        return Err(Error::corruption(
            "block entry shared prefix exceeds previous key length",
        ));
    }

    if !has_prev_key {
        if shared != 0 {
            return Err(Error::corruption(
                "block entry shared prefix exceeds previous key length",
            ));
        }
        if unshared < 8 {
            return Err(Error::corruption("internal key too short"));
        }
        let inv_vt = !data[key_end - 1];
        if inv_vt > 1 {
            return Err(invalid_point_ikey_type(inv_vt));
        }
        key_buf.replace_suffix(0, &data[pos..key_end]);
        return Ok((key_end, vlen, value_end));
    }

    let prev_len = key_buf.len();
    if shared > prev_len {
        return Err(Error::corruption(
            "block entry shared prefix exceeds previous key length",
        ));
    }
    let new_len = shared + unshared;
    if new_len < 8 {
        return Err(Error::corruption("internal key too short"));
    }
    let inv_vt = !if unshared > 0 {
        data[key_end - 1]
    } else {
        key_buf[shared - 1]
    };
    if inv_vt > 1 {
        return Err(invalid_point_ikey_type(inv_vt));
    }

    let up = prev_len - 8;
    let un = new_len - 8;
    let ordered = if shared < up && shared < un && key_buf[shared] != data[pos] {
        key_buf[shared] < data[pos]
    } else if shared <= up.min(un) {
        match key_buf[shared..up].cmp(&data[pos..key_end - 8]) {
            Ordering::Less => true,
            Ordering::Greater => false,
            Ordering::Equal => {
                u64::from_be_bytes(key_buf[up..prev_len].try_into().unwrap())
                    < u64::from_be_bytes(data[key_end - 8..key_end].try_into().unwrap())
            }
        }
    } else if up == un {
        key_buf[shared..prev_len] < data[pos..key_end]
    } else {
        up < un
    };
    if !ordered {
        return Err(Error::corruption("unordered point keys in SST data block"));
    }

    key_buf.replace_suffix(shared, &data[pos..key_end]);
    Ok((key_end, vlen, value_end))
}

/// Iterator over entries in a block.
///
/// `next()` returns `None` both on clean exhaustion and on corruption; call
/// `error()` after iteration ends to distinguish the two (mirrors the
/// `err`/`iter_error()` convention used by `TableIterator`).
pub struct BlockIterator<'a> {
    block: &'a Block,
    offset: usize,
    next_restart_idx: u32,
    seg_end: usize,
    key: Vec<u8>,
    value_start: usize,
    value_len: usize,
    error: Option<Error>,
}

impl<'a> BlockIterator<'a> {
    /// Return the corruption error encountered during iteration, if any.
    /// `None` means iteration reached the clean end of the block.
    pub fn error(&self) -> Option<&Error> {
        self.error.as_ref()
    }
}

impl<'a> Iterator for BlockIterator<'a> {
    type Item = (Vec<u8>, Vec<u8>);

    fn next(&mut self) -> Option<Self::Item> {
        if self.error.is_some() || self.offset >= self.block.restart_offset {
            return None;
        }
        if self.offset == self.seg_end {
            self.key.clear();
            let r = self.next_restart_idx;
            self.next_restart_idx = r + 1;
            self.seg_end = if r + 1 < self.block.num_restarts {
                self.block.restart_point(r + 1) as usize
            } else {
                self.block.restart_offset
            };
        }

        match decode_entry_at(&self.block.data[..self.seg_end], self.offset, &self.key) {
            Ok((key, value, next_off)) => {
                self.key = key.clone();
                self.value_start = next_off - value.len();
                self.value_len = value.len();
                self.offset = next_off;
                Some((key, value))
            }
            Err(e) => {
                self.error = Some(e);
                None
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sst::block_builder::BlockBuilder;

    #[test]
    fn recovery_keeps_readable_sequences_after_a_bad_key_or_restart_interval() {
        use crate::types::InternalKey;

        let mut builder = BlockBuilder::new(2);
        let key = |name: &[u8], seq| InternalKey::new(name, seq, ValueType::Value);
        builder.add(b"a", b"short internal key");
        builder.add(key(b"b", 8).as_bytes(), b"value");
        builder.add(key(b"c", 12).as_bytes(), b"value");
        builder.add(key(b"d", 3).as_bytes(), b"value");
        let data = builder.finish();
        let block = Block::from_vec(data.clone()).unwrap();
        assert_eq!(block.max_readable_point_sequence(), 12);

        // Invalidate the second segment's framing. The valid key following
        // the malformed internal key in the first segment still counts.
        let mut unreadable_second = data.clone();
        unreadable_second[block.restart_point(1) as usize] = 1;
        assert_eq!(
            Block::from_vec(unreadable_second)
                .unwrap()
                .max_readable_point_sequence(),
            8
        );
        let mut unreadable_first = data;
        unreadable_first[0] = 1;
        assert_eq!(
            Block::from_vec(unreadable_first)
                .unwrap()
                .max_readable_point_sequence(),
            12
        );
    }

    #[test]
    #[cfg(target_pointer_width = "64")]
    fn reverse_key_offsets_preserve_the_expanded_arena_position() {
        let start = u32::MAX as usize - 2;
        let end = start + 8;
        let entry = RestartSegmentEntry::new(start, end, 48, 10);
        assert_eq!(entry.key_end - entry.key_start, 8);
        assert!(entry.key_end > u32::MAX as usize);
    }

    #[test]
    fn empty_restart_segment_is_corruption() {
        let mut builder = BlockBuilder::new(1);
        builder.add(b"a", b"1");
        builder.add(b"b", b"2");
        let mut data = builder.finish();
        let num_restarts = u32::from_le_bytes(data[data.len() - 4..].try_into().unwrap());
        assert!(num_restarts >= 2);
        let restart_offset = data.len() - (num_restarts as usize) * 4 - 4;
        assert!(restart_offset > 0);
        let last = restart_offset + (num_restarts as usize - 1) * 4;
        data[last..last + 4].copy_from_slice(&(restart_offset as u32).to_le_bytes());
        match Block::from_vec(data) {
            Err(err) => assert!(
                err.to_string().contains("bad restart offset"),
                "empty last restart segment must not decode, got {err}"
            ),
            Ok(_) => panic!("empty last restart segment must not decode"),
        }
    }

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
    fn test_reverse_restart_selection() {
        let mut builder = BlockBuilder::new(4);
        for i in 0..20 {
            builder.add(&[2 * i], &[i]);
        }
        let block = Block::from_vec(builder.finish()).unwrap();
        for (target, expected) in [
            (&[][..], None),
            (&[0], Some(0)),
            (&[10], Some(1)),
            (&[11], Some(1)),
            (&[255], Some(4)),
        ] {
            assert_eq!(
                block.restart_for_prev(target, <[u8]>::cmp).unwrap(),
                expected
            );
        }
    }

    #[test]
    fn test_iter_restart_segment() {
        let mut builder = BlockBuilder::new(4); // restart every 4 entries

        for i in 0..12 {
            let key = format!("key_{:04}", i);
            let val = format!("val_{}", i);
            builder.add(key.as_bytes(), val.as_bytes());
        }

        let data = builder.finish();
        let block = Block::from_vec(data).unwrap();

        // Restart 0: entries 0..4
        let entries = block.iter_restart_segment(0).unwrap();
        assert_eq!(entries.len(), 4);
        assert_eq!(entries[0].0, b"key_0000");

        // Restart 1: entries 4..8
        let entries = block.iter_restart_segment(1).unwrap();
        assert_eq!(entries.len(), 4);
        assert_eq!(entries[0].0, b"key_0004");

        // Restart 2: entries 8..12
        let entries = block.iter_restart_segment(2).unwrap();
        assert_eq!(entries.len(), 4); // entries 8 through 11
        assert_eq!(entries[0].0, b"key_0008");

        // Out of bounds
        let entries = block.iter_restart_segment(100).unwrap();
        assert!(entries.is_empty());
    }

    /// Regression test for the corruption-vs-EOF conflation fix: a
    /// structurally malformed entry mid-block (its shared-prefix varint
    /// inflated past the previous key's length) must surface as a hard
    /// error from every decode path — never as a silent early end of the
    /// block, which would hide all entries past the corruption point.
    #[test]
    fn test_mid_block_entry_corruption_is_error_not_eof() {
        // restart_interval: 1 → every entry is a restart point, so the
        // restart array gives the exact byte offset of each entry.
        let mut builder = BlockBuilder::new(1);
        const COUNT: usize = 5;
        for i in 0..COUNT {
            let key = format!("key_{:04}", i);
            let val = format!("val_{}", i);
            builder.add(key.as_bytes(), val.as_bytes());
        }
        let mut data = builder.finish();

        // Locate entry #2 via the restart array: [entries][u32 offsets x n][n: u32].
        let n = u32::from_le_bytes(data[data.len() - 4..].try_into().unwrap()) as usize;
        assert_eq!(n, COUNT);
        let restarts_start = data.len() - 4 - 4 * n;
        let entry2_off = u32::from_le_bytes(
            data[restarts_start + 8..restarts_start + 12]
                .try_into()
                .unwrap(),
        ) as usize;

        // Corrupt entry #2's shared-len varint: 0xFF sets the continuation
        // bit, decoding to >= 127 — always larger than the previous key,
        // so a linear scan hits the "shared prefix exceeds previous key"
        // corruption arm exactly at entry #2.
        data[entry2_off] = 0xFF;
        let block = Block::from_vec(data).unwrap();

        // Full iteration: entries 0..2 yielded, then error() must be set
        // (clean EOF would leave it None).
        let mut iter = block.iter();
        let yielded: Vec<_> = (&mut iter).collect();
        assert_eq!(yielded.len(), 2, "expected truncation exactly at entry #2");
        assert_eq!(yielded[1].0, b"key_0001");
        assert!(
            iter.error().is_some(),
            "mid-block corruption must set BlockIterator::error(), not read as clean EOF"
        );

        // seek_by: the linear scan toward a key at/after the corruption
        // point must propagate the decode error, not report "not found".
        let compare = |a: &[u8], b: &[u8]| a.cmp(b);
        assert!(
            block.seek_by(b"key_0003", compare).is_err(),
            "seek_by through a corrupt entry must return Err, not Ok(None)"
        );

        // iter_restart_segment over the corrupt entry must also error.
        assert!(
            block.iter_restart_segment(2).is_err(),
            "iter_restart_segment at the corrupt entry must return Err"
        );
    }

    #[test]
    fn test_entry_cannot_consume_restart_directory() {
        let mut builder = BlockBuilder::new(1);
        builder.add(b"abc", b"xy");
        let mut data = builder.finish();

        // Entry encoding is shared_len, unshared_len, value_len, key, value.
        // Inflate value_len from 2 to 10 so the apparent value ends exactly at
        // the end of the full allocation, consuming the 8-byte restart trailer.
        assert_eq!(&data[..8], &[0, 3, 2, b'a', b'b', b'c', b'x', b'y']);
        data[2] = 10;
        let block = Block::from_vec(data).unwrap();

        let mut iter = block.iter();
        assert!(iter.next().is_none());
        assert!(
            iter.error().is_some(),
            "entry bytes must be bounded before the restart directory"
        );
        assert!(
            block.seek_by(b"abc", |a, b| a.cmp(b)).is_err(),
            "seek must propagate an entry that crosses into restart metadata"
        );
    }

    #[test]
    fn non_initial_restart_prefix_and_straddling_segment_are_corruption() {
        // 1. Non-zero shared_len at restarts[1] must error in BlockIterator and seek_by.
        let mut builder = BlockBuilder::new(1);
        builder.add(b"aaa", b"1");
        builder.add(b"aab", b"2");
        let mut data = builder.finish();
        let restart_offset = data.len() - 12;
        let r1 = u32::from_le_bytes(
            data[restart_offset + 4..restart_offset + 8]
                .try_into()
                .unwrap(),
        ) as usize;
        // Rewrite entry at r1 to claim shared_len = 2, unshared_len = 1 ('b'), val_len = 3 ("2xx")
        // keeping exact byte length (3 varints + 1 key + 3 val = 7 bytes vs 3 varints + 3 key + 1 val = 7 bytes).
        data[r1..restart_offset].copy_from_slice(&[2, 1, 3, b'b', b'2', b'x', b'x']);
        let block = Block::from_vec(data).unwrap();
        let mut iter = block.iter();
        assert_eq!(iter.next().unwrap().0, b"aaa");
        assert!(iter.next().is_none());
        assert!(iter.error().is_some());
        assert!(block.seek_by(b"aab", <[u8]>::cmp).is_err());

        // 2. Entry in segment 0 straddling restarts[1] must error in BlockIterator and seek_by.
        let mut raw = Vec::new();
        // Entry 0 at offset 0: shared=0, unshared=1 ('a'), val_len=9 -> ends at 13 > r1 (5)!
        raw.extend_from_slice(&[0, 1, 9, b'a', 0]);
        // At offset r1 = 5: valid restart entry for "m" -> "v" (shared=0, unshared=1, val=1, 'm', 'v') + 2 pad bytes
        let r1_off = raw.len() as u32;
        raw.extend_from_slice(&[0, 1, 3, b'm', b'v', b'y', b'z']);
        // Restarts = [0, 5], num_restarts = 2
        raw.extend_from_slice(&0u32.to_le_bytes());
        raw.extend_from_slice(&r1_off.to_le_bytes());
        raw.extend_from_slice(&2u32.to_le_bytes());
        let straddle = Block::from_vec(raw).unwrap();
        let mut siter = straddle.iter();
        assert!(siter.next().is_none());
        assert!(siter.error().is_some());
        assert!(straddle.seek_by(b"b", <[u8]>::cmp).is_err());
    }
}
