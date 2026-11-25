//! Core data types: InternalKey, ValueType, SequenceNumber.
//!
//! Encoding follows RocksDB convention:
//!   InternalKey = \[user_key bytes\]\[trailer: !(seq << 8 | type) as big-endian\]  (last 8 bytes)
//!   The trailer is bit-inverted so a higher seq yields smaller bytes.
//!   Sort order: user_key ASC, sequence DESC, value_type DESC

use std::cmp::Ordering;
use std::collections::BTreeMap;
use std::fmt;
use std::sync::Arc;

use crate::error::{Error, Result};

/// Global monotonically increasing sequence number.
pub type SequenceNumber = u64;

/// Maximum valid sequence number (56-bit).
pub const MAX_SEQUENCE_NUMBER: SequenceNumber = (1 << 56) - 1;

/// Maximum allowed user key length for writes (8 MiB).
///
/// Applies to both endpoints of a range deletion. A partitioned SST index
/// leaf stores a data block's first key and separator within the 64 MiB
/// decompressed-block limit.
pub const MAX_USER_KEY_SIZE: usize = 8 * 1024 * 1024;

/// Maximum allowed user key + value payload for a single point-write entry.
/// Range deletions use the smaller [`MAX_RANGE_DELETE_SIZE`] limit.
///
/// Mirrors the table builder's per-entry limit (one entry plus framing must
/// fit in a readable 64 MiB block), minus the 8-byte internal-key trailer.
pub const MAX_WRITE_ENTRY_SIZE: usize = 64 * 1024 * 1024 - 64 - 8;

/// Maximum combined begin + end length for a range deletion.
///
/// Reserves room for metadata already below the 32 MiB SST split threshold,
/// the internal-key trailer, and entry framing in the range-deletion block.
/// Each endpoint also obeys [`MAX_USER_KEY_SIZE`].
pub const MAX_RANGE_DELETE_SIZE: usize = 32 * 1024 * 1024 - 4096 - 64 - 8;

/// Operation type stored in the low 8 bits of the packed trailer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum ValueType {
    Deletion = 0,
    Value = 1,
    RangeDeletion = 2,
}

impl ValueType {
    pub fn from_u8(v: u8) -> Option<Self> {
        match v {
            0 => Some(Self::Deletion),
            1 => Some(Self::Value),
            2 => Some(Self::RangeDeletion),
            _ => None,
        }
    }
}

/// Pack sequence number and value type into a single u64.
/// Layout: `(sequence << 8) | value_type`
#[inline]
pub fn pack_sequence_and_type(seq: SequenceNumber, vt: ValueType) -> u64 {
    assert!(
        seq <= MAX_SEQUENCE_NUMBER,
        "sequence {} exceeds max {}",
        seq,
        MAX_SEQUENCE_NUMBER
    );
    (seq << 8) | (vt as u64)
}

/// Unpack a packed u64 into (sequence, value_type). Inverse of
/// [`pack_sequence_and_type`]; used by tests to validate the encoding.
#[cfg(test)]
pub(crate) fn unpack_sequence_and_type(packed: u64) -> (SequenceNumber, ValueType) {
    let seq = packed >> 8;
    let vt = ValueType::from_u8((packed & 0xFF) as u8).unwrap_or(ValueType::Deletion);
    (seq, vt)
}

/// Internal key used throughout the engine.
///
/// Encoded form: `[user_key][8-byte trailer]`
/// The trailer is `!pack_sequence_and_type(seq, type)` in big-endian.
/// Bit inversion ensures higher seq → smaller bytes → sorts first in lex order.
#[derive(Clone, Debug)]
pub struct InternalKey {
    /// The raw encoded bytes: user_key ++ inverted_packed_trailer (BE).
    encoded: Vec<u8>,
}

impl InternalKey {
    /// Create a new InternalKey from components.
    pub fn new(user_key: &[u8], sequence: SequenceNumber, value_type: ValueType) -> Self {
        let packed = pack_sequence_and_type(sequence, value_type);
        let mut encoded = Vec::with_capacity(user_key.len() + 8);
        encoded.extend_from_slice(user_key);
        encoded.extend_from_slice(&(!packed).to_be_bytes());
        Self { encoded }
    }

    /// Raw encoded bytes.
    #[inline]
    pub fn as_bytes(&self) -> &[u8] {
        &self.encoded
    }

    /// Wrap already-encoded bytes (test helper).
    #[cfg(test)]
    pub(crate) fn from_encoded(encoded: Vec<u8>) -> Self {
        assert!(
            encoded.len() >= 8,
            "InternalKey requires at least 8 bytes, got {}",
            encoded.len()
        );
        Self { encoded }
    }

    /// Extract user key portion (test helper).
    #[cfg(test)]
    pub(crate) fn user_key(&self) -> &[u8] {
        &self.encoded[..self.encoded.len() - 8]
    }

    /// Extract sequence number (test helper).
    #[cfg(test)]
    pub(crate) fn sequence(&self) -> SequenceNumber {
        self.trailer() >> 8
    }

    /// Extract value type (test helper).
    #[cfg(test)]
    pub(crate) fn value_type(&self) -> ValueType {
        ValueType::from_u8((self.trailer() & 0xFF) as u8).unwrap_or(ValueType::Deletion)
    }

    #[cfg(test)]
    fn trailer(&self) -> u64 {
        let offset = self.encoded.len() - 8;
        !u64::from_be_bytes(self.encoded[offset..].try_into().unwrap())
    }

    /// Length of the encoded form.
    #[cfg(test)]
    #[inline]
    pub fn encoded_len(&self) -> usize {
        self.encoded.len()
    }

    /// Consume self, return the encoded bytes.
    pub fn into_bytes(self) -> Vec<u8> {
        self.encoded
    }
}

/// Borrowed version of InternalKey for zero-copy comparisons.
#[derive(Clone, Copy, Debug)]
pub struct InternalKeyRef<'a> {
    encoded: &'a [u8],
}

impl<'a> InternalKeyRef<'a> {
    pub fn new(encoded: &'a [u8]) -> Self {
        assert!(
            encoded.len() >= 8,
            "InternalKeyRef requires at least 8 bytes, got {}",
            encoded.len()
        );
        Self { encoded }
    }

    #[inline]
    pub fn user_key(&self) -> &[u8] {
        &self.encoded[..self.encoded.len() - 8]
    }

    #[inline]
    pub fn sequence(&self) -> SequenceNumber {
        let trailer = self.trailer();
        trailer >> 8
    }

    #[inline]
    pub fn value_type(&self) -> ValueType {
        let trailer = self.trailer();
        ValueType::from_u8((trailer & 0xFF) as u8).unwrap_or(ValueType::Deletion)
    }

    #[inline]
    fn trailer(&self) -> u64 {
        let offset = self.encoded.len() - 8;
        !u64::from_be_bytes(self.encoded[offset..].try_into().unwrap())
    }
}

/// Decode a persisted internal key's length, sequence, and type.
///
/// Every on-disk / compaction / iterator boundary that treats bytes as an
/// internal key must use this (or the infallible accessors only after a
/// successful call). Keys shorter than the 8-byte trailer and trailers with
/// unknown value-type bytes fail as corruption — never skip or coerce.
#[inline]
pub fn decode_internal_key(encoded: &[u8]) -> Result<(&[u8], SequenceNumber, ValueType)> {
    if encoded.len() < 8 {
        return Err(Error::corruption(format!(
            "internal key too short: {} bytes (need at least 8)",
            encoded.len()
        )));
    }
    let ikr = InternalKeyRef::new(encoded);
    let vt = decode_trailer_type(ikr.trailer())?;
    let seq = ikr.sequence();
    let uk_len = ikr.user_key().len();
    Ok((&encoded[..uk_len], seq, vt))
}

#[inline]
fn decode_trailer_type(packed_trailer: u64) -> Result<ValueType> {
    let raw = (packed_trailer & 0xFF) as u8;
    ValueType::from_u8(raw)
        .ok_or_else(|| Error::corruption(format!("invalid value_type byte {} in trailer", raw)))
}

/// Compare two internal keys:
///   1. user_key ascending (bytewise)
///   2. sequence descending
///   3. value_type descending
///
/// With the inverted-BE trailer encoding, the trailer bytes already sort
/// in the correct order (higher seq → smaller bytes → sorts first).
/// We compare user_key portions first, then trailer bytes.
///
/// Defensive: keys shorter than 8 bytes are malformed. They sort before
/// valid keys so they surface as errors rather than causing panics.
#[inline]
pub fn compare_internal_key(a: &[u8], b: &[u8]) -> Ordering {
    match (a.len() >= 8, b.len() >= 8) {
        (true, true) => {
            let a_uk = &a[..a.len() - 8];
            let b_uk = &b[..b.len() - 8];
            a_uk.cmp(b_uk)
                .then_with(|| a[a.len() - 8..].cmp(&b[b.len() - 8..]))
        }
        (false, false) => a.cmp(b),
        (false, true) => Ordering::Less,
        (true, false) => Ordering::Greater,
    }
}

/// Extract the user key from an internal key by stripping the 8-byte trailer.
/// Returns the full slice if the key is shorter than 8 bytes.
#[inline]
pub fn user_key(internal_key: &[u8]) -> &[u8] {
    if internal_key.len() >= 8 {
        &internal_key[..internal_key.len() - 8]
    } else {
        internal_key
    }
}

/// Exclusive upper bound of the keys beginning with `prefix`.
pub(crate) fn prefix_successor(prefix: &[u8]) -> Option<Vec<u8>> {
    let last = prefix.iter().rposition(|&byte| byte < u8::MAX)?;
    let mut upper = prefix[..=last].to_vec();
    upper[last] += 1;
    Some(upper)
}

/// Whether the half-open range tombstone `[begin, end)` can cover any user
/// key inside the half-open window `[lower, upper)` (`None` = unbounded).
///
/// Shared by iterator construction (tombstones outside the iterator's
/// enforced bounds can never affect a yielded key), compact_range file
/// picking, and sub-compaction tombstone scoping, so the boundary semantics
/// cannot drift between compaction and iteration. Note that file-level
/// pruning cannot use a lower bound (a tombstone may extend past its file's
/// largest key) — this predicate is exact only for the tombstone's own
/// extent.
#[inline]
pub fn tombstone_overlaps_bounds(
    begin: &[u8],
    end: &[u8],
    lower: Option<&[u8]>,
    upper: Option<&[u8]>,
) -> bool {
    if begin >= end || lower.zip(upper).is_some_and(|(lo, hi)| lo >= hi) {
        return false;
    }
    lower.is_none_or(|lo| end > lo) && upper.is_none_or(|hi| begin < hi)
}

/// A reusable contiguous batch of mutations, applied atomically by `DB::write`.
/// Size errors are retained until the batch is submitted or cleared.
#[derive(Default)]
pub struct WriteBatch {
    pub(crate) payload: Vec<u8>,
    count: u32,
    invalid: Option<Error>,
    last_entry_offset: usize,
    same_key_run_bytes: usize,
}

#[derive(Clone, Copy)]
pub(crate) struct BatchEntry<'a> {
    pub value_type: ValueType,
    pub key: &'a [u8],
    pub value: &'a [u8],
}

impl BatchEntry<'_> {
    pub(crate) fn metadata_charge(self) -> usize {
        metadata_charge(self.value_type, self.key.len(), self.value.len())
    }
}

fn metadata_charge(kind: ValueType, key_len: usize, value_len: usize) -> usize {
    // A partition separator stores the key, internal trailer and framing.
    // Range roots can use the end key.
    let separator = if kind == ValueType::RangeDeletion {
        key_len.max(value_len)
    } else {
        key_len
    };
    separator + 40
}

// This decoder is for private, validated, in-memory batches. Recovery uses
// a fallible decoder for bytes read from disk.
fn take_batch_field<'a>(input: &mut &'a [u8]) -> &'a [u8] {
    let (len, rest) = input.split_at(4);
    let (field, tail) = rest.split_at(u32::from_le_bytes(len.try_into().unwrap()) as usize);
    *input = tail;
    field
}

fn take_batch_entry<'a>(input: &mut &'a [u8]) -> BatchEntry<'a> {
    let (tag, rest) = input.split_first().unwrap();
    *input = rest;
    let value_type = ValueType::from_u8(*tag).unwrap();
    let key = take_batch_field(input);
    let value = match value_type {
        ValueType::Deletion => &[][..],
        _ => take_batch_field(input),
    };
    BatchEntry {
        value_type,
        key,
        value,
    }
}

impl WriteBatch {
    pub fn new() -> Self {
        Self::default()
    }

    /// Reserve space for the encoded body, measured in bytes.
    pub fn with_capacity(bytes: usize) -> Self {
        Self {
            payload: Vec::with_capacity(bytes),
            ..Self::default()
        }
    }

    pub fn put(&mut self, key: &[u8], value: &[u8]) {
        self.put_prefixed(&[], key, value);
    }

    /// Put `prefix || key` without allocating a concatenated key.
    pub fn put_prefixed(&mut self, prefix: &[u8], key: &[u8], value: &[u8]) {
        self.append(ValueType::Value, prefix, key, value);
    }

    pub fn delete(&mut self, key: &[u8]) {
        self.delete_prefixed(&[], key);
    }

    /// Delete the point key `prefix || key`.
    pub fn delete_prefixed(&mut self, prefix: &[u8], key: &[u8]) {
        self.append(ValueType::Deletion, prefix, key, &[]);
    }

    /// Delete all keys in the half-open range. Empty ranges consume no sequence.
    pub fn delete_range(&mut self, begin: &[u8], end: &[u8]) {
        self.delete_range_prefixed(&[], begin, end);
    }

    /// Delete the half-open range between the two prefixed endpoints.
    pub fn delete_range_prefixed(&mut self, prefix: &[u8], begin: &[u8], end: &[u8]) {
        if begin < end {
            self.append(ValueType::RangeDeletion, prefix, begin, end);
        }
    }

    fn append(&mut self, kind: ValueType, prefix: &[u8], key: &[u8], value: &[u8]) {
        if self.invalid.is_some() {
            return;
        }
        let range = kind == ValueType::RangeDeletion;
        let value_prefix = if range { prefix } else { &[] };
        // A saturated length exceeds every format limit.
        let key_len = prefix.len().saturating_add(key.len());
        let value_len = value_prefix.len().saturating_add(value.len());
        let entry_limit = if range {
            MAX_RANGE_DELETE_SIZE
        } else {
            MAX_WRITE_ENTRY_SIZE
        };
        if key_len > MAX_USER_KEY_SIZE || (range && value_len > MAX_USER_KEY_SIZE) {
            self.invalid = Some(Error::invalid_argument(format!(
                "key size exceeds maximum {MAX_USER_KEY_SIZE}"
            )));
            return;
        }
        if key_len.saturating_add(value_len) > entry_limit || self.count == u32::MAX {
            self.invalid = Some(Error::invalid_argument(
                "write batch exceeds entry size or operation count limit",
            ));
            return;
        }
        // Bound a stream of repeated keys during construction. The commit
        // path also checks interleaved keys and versions in the active table.
        let mut run_bytes = metadata_charge(kind, key_len, value_len);
        if self.count != 0 {
            let previous = take_batch_entry(&mut &self.payload[self.last_entry_offset..]);
            if previous.key.len() == key_len && previous.key.strip_prefix(prefix) == Some(key) {
                run_bytes += self.same_key_run_bytes;
            }
        }
        if run_bytes > crate::sst::table_builder::META_BLOCK_SPLIT_THRESHOLD {
            self.invalid = Some(Error::invalid_argument(
                "same-key metadata exceeds SST partition index budget",
            ));
            return;
        }
        self.same_key_run_bytes = run_bytes;
        self.last_entry_offset = self.payload.len();
        let has_value = kind != ValueType::Deletion;
        self.payload
            .reserve(5 + key_len + if has_value { 4 + value_len } else { 0 });
        self.payload.push(kind as u8);
        self.payload
            .extend_from_slice(&(key_len as u32).to_le_bytes());
        self.payload.extend_from_slice(prefix);
        self.payload.extend_from_slice(key);
        if has_value {
            self.payload
                .extend_from_slice(&(value_len as u32).to_le_bytes());
            self.payload.extend_from_slice(value_prefix);
            self.payload.extend_from_slice(value);
        }
        self.count += 1;
    }

    pub fn is_empty(&self) -> bool {
        self.count == 0
    }

    pub fn len(&self) -> usize {
        self.count as usize
    }

    pub fn clear(&mut self) {
        self.payload.clear();
        self.count = 0;
        self.invalid = None;
        self.same_key_run_bytes = 0;
        self.last_entry_offset = 0;
    }

    pub(crate) fn check_valid(&self) -> Result<()> {
        self.invalid.clone().map_or(Ok(()), Err)
    }

    pub(crate) fn entries(&self) -> impl Iterator<Item = BatchEntry<'_>> {
        let mut body = self.payload.as_slice();
        (0..self.count).map(move |_| take_batch_entry(&mut body))
    }

    pub(crate) fn wal_header(&self, sequence: SequenceNumber) -> [u8; 12] {
        let mut header = [0; 12];
        header[..8].copy_from_slice(&sequence.to_le_bytes());
        header[8..].copy_from_slice(&self.count.to_le_bytes());
        header
    }
}

/// A batch with a sorted point-key index for iteration over uncommitted writes.
pub struct WriteBatchWithIndex {
    batch: WriteBatch,
    index: BTreeMap<Box<[u8]>, (usize, u64)>,
    range_del_entries: Vec<(usize, u64)>,
}

impl WriteBatchWithIndex {
    pub fn new() -> Self {
        Self {
            batch: WriteBatch::new(),
            index: BTreeMap::new(),
            range_del_entries: Vec::new(),
        }
    }

    fn index_point(&mut self, key: &[u8], offset: usize) {
        if self.batch.invalid.is_some() {
            return;
        }
        let location = (offset, self.operation_count() - 1);
        if let Some(old) = self.index.get_mut(key) {
            *old = location;
        } else {
            self.index.insert(key.into(), location);
        }
    }

    pub fn put(&mut self, key: &[u8], value: &[u8]) {
        let offset = self.batch.payload.len();
        self.batch.put(key, value);
        self.index_point(key, offset);
    }

    pub fn delete(&mut self, key: &[u8]) {
        let offset = self.batch.payload.len();
        self.batch.delete(key);
        self.index_point(key, offset);
    }

    pub fn delete_range(&mut self, begin: &[u8], end: &[u8]) {
        let offset = self.batch.payload.len();
        let pos = self.operation_count();
        self.batch.delete_range(begin, end);
        if self.batch.payload.len() > offset {
            self.range_del_entries.push((offset, pos));
        }
    }

    pub fn len(&self) -> usize {
        self.batch.len()
    }

    pub fn is_empty(&self) -> bool {
        self.batch.is_empty()
    }

    /// Consume the index, returning any size error latched while building the batch.
    pub fn into_batch(self) -> Result<WriteBatch> {
        self.check_valid()?;
        Ok(self.batch)
    }

    pub(crate) fn check_valid(&self) -> Result<()> {
        self.batch.check_valid()
    }

    /// Total number of mutations including range deletions.
    pub fn operation_count(&self) -> u64 {
        u64::from(self.batch.count)
    }

    /// Borrow the endpoints and original write position of each range deletion.
    pub fn range_tombstones(&self) -> impl Iterator<Item = (&[u8], &[u8], u64)> {
        self.range_del_entries.iter().map(|&(offset, pos)| {
            let entry = take_batch_entry(&mut &self.batch.payload[offset..]);
            (entry.key, entry.value, pos)
        })
    }

    pub(crate) fn sorted_entries(
        &self,
        base_seq: SequenceNumber,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        self.index
            .values()
            .map(|&(offset, pos)| {
                let entry = take_batch_entry(&mut &self.batch.payload[offset..]);
                let seq = base_seq
                    .checked_add(pos)
                    .filter(|&n| n <= MAX_SEQUENCE_NUMBER)
                    .ok_or_else(|| Error::invalid_argument("sequence number space exhausted"))?;
                let key = InternalKey::new(entry.key, seq, entry.value_type);
                Ok((key.into_bytes(), entry.value.to_vec()))
            })
            .collect()
    }
}

impl Default for WriteBatchWithIndex {
    fn default() -> Self {
        Self::new()
    }
}

// ---------------------------------------------------------------------------
// LazyValue — deferred value materialization
// ---------------------------------------------------------------------------

/// A value that may be either owned or a zero-copy reference into block data.
///
/// Used internally to defer value copying until the caller actually needs
/// ownership.  Skip-heavy paths (dedup, tombstone checks) call `as_slice()`
/// without ever allocating, while the final `Iterator::next()` boundary
/// calls `into_vec()` to produce the owned bytes the public API promises.
#[derive(Clone)]
pub(crate) enum LazyValue {
    /// Immutable arena bytes kept alive by the owning memtable.
    MemTable(crate::memtable::MemTablePin),
    /// Owned bytes (from memtable sources or materialized backward iteration).
    Inline(Vec<u8>),
    /// Zero-copy slice into a cached/pinned SST block.
    /// The `Arc` keeps the underlying block data alive.
    BlockRef {
        data: Arc<Vec<u8>>,
        offset: u32,
        len: u32,
    },
}

impl LazyValue {
    /// View the value bytes without copying.
    #[inline]
    pub fn as_slice(&self) -> &[u8] {
        match self {
            LazyValue::Inline(v) => v,
            LazyValue::MemTable(pin) => pin.as_ref(),
            LazyValue::BlockRef { data, offset, len } => {
                &data[*offset as usize..(*offset as usize) + (*len as usize)]
            }
        }
    }

    /// Consume self and produce owned bytes.
    #[inline]
    pub fn into_vec(self) -> Vec<u8> {
        match self {
            Self::Inline(v) => v,
            other => other.as_slice().to_vec(),
        }
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.as_slice().len()
    }

    /// Create an empty inline value.
    #[inline]
    pub fn empty() -> Self {
        LazyValue::Inline(Vec::new())
    }
}

impl fmt::Debug for LazyValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            LazyValue::MemTable(pin) => write!(f, "MemTable({}B)", pin.as_ref().len()),
            LazyValue::Inline(v) => write!(f, "Inline({}B)", v.len()),
            LazyValue::BlockRef { len, .. } => write!(f, "BlockRef({}B)", len),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_pack_unpack() {
        let seq = 12345u64;
        let vt = ValueType::Value;
        let packed = pack_sequence_and_type(seq, vt);
        let (s2, v2) = unpack_sequence_and_type(packed);
        assert_eq!(s2, seq);
        assert_eq!(v2, vt);
    }

    #[test]
    fn test_internal_key_encoding() {
        let ik = InternalKey::new(b"hello", 100, ValueType::Value);
        assert_eq!(ik.user_key(), b"hello");
        assert_eq!(ik.sequence(), 100);
        assert_eq!(ik.value_type(), ValueType::Value);
        assert_eq!(ik.encoded_len(), 5 + 8);

        let (uk, seq, vt) = decode_internal_key(ik.as_bytes()).unwrap();
        assert_eq!(uk, b"hello");
        assert_eq!(seq, 100);
        assert_eq!(vt, ValueType::Value);
    }

    #[test]
    fn test_decode_internal_key_rejects_short_and_unknown_type() {
        assert!(decode_internal_key(b"short").is_err());
        assert!(decode_internal_key(&[]).is_err());

        // Valid length, unknown type byte in trailer.
        let packed = (42u64 << 8) | 0xFF;
        let mut key = b"key".to_vec();
        key.extend_from_slice(&(!packed).to_be_bytes());
        let err = decode_internal_key(&key).unwrap_err();
        assert!(err.to_string().contains("invalid value_type"));
    }

    #[test]
    fn test_internal_key_ordering() {
        // Same user key, different seq: higher seq should sort first (less)
        let a = InternalKey::new(b"key", 200, ValueType::Value);
        let b = InternalKey::new(b"key", 100, ValueType::Value);
        assert_eq!(
            compare_internal_key(a.as_bytes(), b.as_bytes()),
            Ordering::Less
        );

        // Different user key: bytewise ordering
        let c = InternalKey::new(b"aaa", 1, ValueType::Value);
        let d = InternalKey::new(b"bbb", 1, ValueType::Value);
        assert_eq!(
            compare_internal_key(c.as_bytes(), d.as_bytes()),
            Ordering::Less
        );

        // Same user key, same seq: Value (type=1) has a larger packed trailer,
        // so its inverted trailer is smaller → sorts first (less).
        let e = InternalKey::new(b"key", 100, ValueType::Deletion);
        let f = InternalKey::new(b"key", 100, ValueType::Value);
        assert_eq!(
            compare_internal_key(e.as_bytes(), f.as_bytes()),
            Ordering::Greater
        );
    }

    #[test]
    fn test_compare_internal_key_cases() {
        let cases = vec![
            // (user_key_a, seq_a, user_key_b, seq_b, expected)
            (
                b"a".as_slice(),
                10u64,
                b"b".as_slice(),
                10u64,
                Ordering::Less,
            ),
            (b"b", 10, b"a", 10, Ordering::Greater),
            (b"key", 200, b"key", 100, Ordering::Less), // higher seq first
            (b"key", 100, b"key", 200, Ordering::Greater),
            (b"key", 100, b"key", 100, Ordering::Equal),
            (b"a", 1, b"b", 1000, Ordering::Less),
            // Prefix case: shorter user_key sorts first
            (b"a", 1, b"ab", 1, Ordering::Less),
            (b"ab", 1, b"a", 1, Ordering::Greater),
        ];
        for (uk_a, seq_a, uk_b, seq_b, expected) in cases {
            let a = InternalKey::new(uk_a, seq_a, ValueType::Value);
            let b = InternalKey::new(uk_b, seq_b, ValueType::Value);
            assert_eq!(
                compare_internal_key(a.as_bytes(), b.as_bytes()),
                expected,
                "failed for {:?}@{} vs {:?}@{}",
                uk_a,
                seq_a,
                uk_b,
                seq_b
            );
        }
    }

    #[test]
    fn test_compare_internal_key_short_keys() {
        // Short keys (< 8 bytes) must not panic
        let short = b"abc";
        let valid = InternalKey::new(b"key", 100, ValueType::Value);
        assert_eq!(
            compare_internal_key(short, valid.as_bytes()),
            Ordering::Less
        );
        assert_eq!(
            compare_internal_key(valid.as_bytes(), short),
            Ordering::Greater
        );
        assert_eq!(compare_internal_key(short, short), Ordering::Equal);
        assert_eq!(compare_internal_key(b"a", b"b"), Ordering::Less);
        assert_eq!(compare_internal_key(b"", b""), Ordering::Equal);
    }

    #[test]
    fn test_write_batch() {
        let mut wb = WriteBatch::new();
        assert!(wb.is_empty());
        wb.put(b"k1", b"v1");
        wb.delete(b"k2");
        assert_eq!(wb.len(), 2);
        wb.clear();
        assert!(wb.is_empty());
    }

    #[test]
    fn test_empty_user_key_internal_key() {
        // InternalKey with an empty user key should still round-trip correctly
        let ik = InternalKey::new(b"", 42, ValueType::Value);
        assert_eq!(ik.user_key(), b"");
        assert_eq!(ik.sequence(), 42);
        assert_eq!(ik.value_type(), ValueType::Value);
        assert_eq!(ik.encoded_len(), 8); // just the 8-byte trailer

        // encoded-slice round-trip via InternalKeyRef
        let ikr = InternalKeyRef::new(ik.as_bytes());
        assert_eq!(ikr.user_key(), b"");
        assert_eq!(ikr.sequence(), 42);
        assert_eq!(ikr.value_type(), ValueType::Value);

        // Two empty user keys with different sequences should order by seq DESC
        let a = InternalKey::new(b"", 100, ValueType::Value);
        let b = InternalKey::new(b"", 50, ValueType::Value);
        assert_eq!(
            compare_internal_key(a.as_bytes(), b.as_bytes()),
            Ordering::Less
        );
    }

    #[test]
    fn test_max_sequence_number() {
        let ik = InternalKey::new(b"key", MAX_SEQUENCE_NUMBER, ValueType::Value);
        assert_eq!(ik.user_key(), b"key");
        assert_eq!(ik.sequence(), MAX_SEQUENCE_NUMBER);
        assert_eq!(ik.value_type(), ValueType::Value);

        // MAX_SEQUENCE_NUMBER should sort before any lower sequence
        let ik_low = InternalKey::new(b"key", 0, ValueType::Value);
        assert_eq!(
            compare_internal_key(ik.as_bytes(), ik_low.as_bytes()),
            Ordering::Less,
            "MAX_SEQUENCE_NUMBER should sort first (less) for same user key"
        );

        // Verify pack/unpack round-trip at the boundary
        let packed = pack_sequence_and_type(MAX_SEQUENCE_NUMBER, ValueType::Deletion);
        let (seq, vt) = unpack_sequence_and_type(packed);
        assert_eq!(seq, MAX_SEQUENCE_NUMBER);
        assert_eq!(vt, ValueType::Deletion);
    }

    #[test]
    fn prefixed_batch_matches_v4_record_body_and_reuses_allocation() {
        let mut b = WriteBatch::with_capacity(128);
        let (_, allocated) = crate::test_alloc::count_allocated(|| {
            b.put_prefixed(b"p", b"a", b"v");
            b.delete_prefixed(b"p", b"b");
            b.delete_range_prefixed(b"p", b"c", b"d");
            b.delete_range_prefixed(b"p", b"z", b"a");
        });
        assert_eq!(allocated, 0);
        assert_eq!(b.len(), 3);
        assert_eq!(
            b.payload,
            b"\x01\x02\0\0\0pa\x01\0\0\0v\x00\x02\0\0\0pb\x02\x02\0\0\0pc\x02\0\0\0pd"
        );
        let capacity = b.payload.capacity();
        b.clear();
        assert_eq!(b.payload.capacity(), capacity);
        assert!(b.is_empty());
    }

    #[test]
    fn building_a_giant_same_key_run_stops_growing_before_submission() {
        let key = vec![1; MAX_USER_KEY_SIZE];
        let mut b = WriteBatch::new();
        for split in 0..12 {
            b.delete_prefixed(&key[..split], &key[split..]);
        }
        assert_eq!(b.len(), 3);
        assert_eq!(b.payload.len(), 3 * (5 + key.len()));
        assert!(b.check_valid().is_err());
        b.clear();
        b.delete(&key);
        assert!(b.check_valid().is_ok());
    }

    #[test]
    fn rejected_index_entry_keeps_original_payload_offsets() {
        let mut indexed = WriteBatchWithIndex::new();
        indexed.put(b"k", b"v1");
        indexed.put(b"k", b"v2");
        indexed.delete_range(b"k", b"n");
        indexed.delete_range(b"p", &vec![b'z'; MAX_USER_KEY_SIZE + 1]);
        indexed.put(b"other", b"v");
        assert_eq!(indexed.operation_count(), 3);
        assert_eq!(indexed.index.len(), 1);
        assert_eq!(
            indexed.range_tombstones().collect::<Vec<_>>(),
            vec![(b"k".as_slice(), b"n".as_slice(), 2)]
        );
        assert!(indexed.into_batch().is_err());
    }

    #[test]
    fn test_write_batch_large() {
        let mut wb = WriteBatch::new();
        let count = 10_001;
        for i in 0..count {
            let key = format!("key_{:08}", i);
            let val = format!("val_{:08}", i);
            wb.put(key.as_bytes(), val.as_bytes());
        }
        assert_eq!(wb.len(), count);
        assert!(!wb.is_empty());

        // Verify entries are stored correctly
        assert_eq!(wb.entries().count(), count);
        assert_eq!(wb.entries().next().unwrap().key, b"key_00000000");
        assert_eq!(wb.entries().next().unwrap().value, b"val_00000000");
        assert_eq!(
            wb.entries().last().unwrap().key,
            format!("key_{:08}", count - 1).into_bytes()
        );

        // Clear should reclaim everything
        wb.clear();
        assert!(wb.is_empty());
        assert_eq!(wb.len(), 0);
        assert!(wb.payload.is_empty());
    }

    #[test]
    fn test_delete_range_entry() {
        let mut wb = WriteBatch::new();
        wb.delete_range(b"begin", b"end");
        assert_eq!(wb.len(), 1);

        let entry = &wb.entries().next().unwrap();
        assert_eq!(entry.value_type, ValueType::RangeDeletion);
        assert_eq!(entry.key, b"begin");
        assert_eq!(entry.value, b"end");

        // Mix put, delete, delete_range in one batch
        wb.put(b"k1", b"v1");
        wb.delete(b"k2");
        wb.delete_range(b"a", b"z");
        assert_eq!(wb.len(), 4);

        assert_eq!(wb.entries().nth(1).unwrap().value_type, ValueType::Value);
        assert_eq!(wb.entries().nth(2).unwrap().value_type, ValueType::Deletion);
        assert!(wb.entries().nth(2).unwrap().value.is_empty());
        assert_eq!(
            wb.entries().nth(3).unwrap().value_type,
            ValueType::RangeDeletion
        );
        assert_eq!(wb.entries().nth(3).unwrap().key, b"a");
        assert_eq!(wb.entries().nth(3).unwrap().value, b"z");
    }

    #[test]
    fn empty_or_inverted_bounds_do_not_overlap_tombstones() {
        assert!(!tombstone_overlaps_bounds(
            b"a",
            b"z",
            Some(b"m"),
            Some(b"m")
        ));
        assert!(!tombstone_overlaps_bounds(
            b"a",
            b"z",
            Some(b"r"),
            Some(b"c")
        ));
        assert!(!tombstone_overlaps_bounds(
            b"m",
            b"m",
            Some(b"a"),
            Some(b"z")
        ));
        assert!(tombstone_overlaps_bounds(
            b"a",
            b"z",
            Some(b"m"),
            Some(b"n")
        ));
    }
}

#[cfg(test)]
mod proptests {
    use super::*;
    use proptest::prelude::*;

    fn arb_value_type() -> impl Strategy<Value = ValueType> {
        prop_oneof![Just(ValueType::Value), Just(ValueType::Deletion)]
    }

    proptest! {
        #[test]
        fn lex_order_matches_logical_order(
            uk_a in prop::collection::vec(any::<u8>(), 1..32),
            uk_b in prop::collection::vec(any::<u8>(), 1..32),
            seq_a in 0..MAX_SEQUENCE_NUMBER,
            seq_b in 0..MAX_SEQUENCE_NUMBER,
            vt_a in arb_value_type(),
            vt_b in arb_value_type(),
        ) {
            let ik_a = InternalKey::new(&uk_a, seq_a, vt_a);
            let ik_b = InternalKey::new(&uk_b, seq_b, vt_b);

            // Logical ordering: user_key ASC, then (seq, type) DESC
            let packed_a = pack_sequence_and_type(seq_a, vt_a);
            let packed_b = pack_sequence_and_type(seq_b, vt_b);
            let logical = uk_a.cmp(&uk_b).then_with(|| packed_b.cmp(&packed_a));

            // compare_internal_key should match logical ordering
            let cmp = compare_internal_key(ik_a.as_bytes(), ik_b.as_bytes());
            prop_assert_eq!(cmp, logical,
                "uk_a={:?} seq_a={} vt_a={:?} vs uk_b={:?} seq_b={} vt_b={:?}",
                uk_a, seq_a, vt_a, uk_b, seq_b, vt_b);

            // Also verify round-trip
            prop_assert_eq!(ik_a.user_key(), uk_a.as_slice());
            prop_assert_eq!(ik_a.sequence(), seq_a);
            prop_assert_eq!(ik_a.value_type(), vt_a);
        }
    }
}
