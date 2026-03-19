//! Core data types: InternalKey, ValueType, SequenceNumber.
//!
//! Encoding follows RocksDB convention:
//!   InternalKey = [user_key bytes][packed: seq << 8 | type]  (last 8 bytes)
//!   Sort order: user_key ASC, sequence DESC, value_type DESC

use std::cmp::Ordering;
use std::sync::Arc;

/// Global monotonically increasing sequence number.
pub type SequenceNumber = u64;

/// Maximum valid sequence number (56-bit).
pub const MAX_SEQUENCE_NUMBER: SequenceNumber = (1 << 56) - 1;

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
    (seq << 8) | (vt as u64)
}

/// Unpack a packed u64 into (sequence, value_type).
#[inline]
pub fn unpack_sequence_and_type(packed: u64) -> (SequenceNumber, ValueType) {
    let seq = packed >> 8;
    // Safety: unknown type bytes default to Deletion (invisible) rather than
    // Value (phantom data).  CRC-protected storage makes this path unreachable
    // in practice; it guards against memory corruption or future format drift.
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

    /// Wrap already-encoded bytes (must include the 8-byte trailer).
    pub fn from_encoded(encoded: Vec<u8>) -> Self {
        debug_assert!(encoded.len() >= 8);
        Self { encoded }
    }

    /// Wrap a reference to already-encoded bytes.
    pub fn from_encoded_slice(encoded: &[u8]) -> InternalKeyRef<'_> {
        debug_assert!(encoded.len() >= 8);
        InternalKeyRef { encoded }
    }

    /// Raw encoded bytes.
    #[inline]
    pub fn as_bytes(&self) -> &[u8] {
        &self.encoded
    }

    /// Extract user key portion.
    #[inline]
    pub fn user_key(&self) -> &[u8] {
        &self.encoded[..self.encoded.len() - 8]
    }

    /// Extract sequence number.
    #[inline]
    pub fn sequence(&self) -> SequenceNumber {
        let trailer = self.trailer();
        trailer >> 8
    }

    /// Extract value type.
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

    /// Length of the encoded form.
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
        debug_assert!(encoded.len() >= 8);
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

    #[inline]
    pub fn as_bytes(&self) -> &[u8] {
        self.encoded
    }
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

/// A write batch groups multiple mutations to be applied atomically.
pub struct WriteBatch {
    pub(crate) entries: Vec<WriteBatchEntry>,
}

/// A single entry in a WriteBatch.
pub(crate) struct WriteBatchEntry {
    pub value_type: ValueType,
    pub key: Vec<u8>,
    pub value: Option<Vec<u8>>,
}

impl WriteBatch {
    pub fn new() -> Self {
        Self {
            entries: Vec::new(),
        }
    }

    pub fn put(&mut self, key: &[u8], value: &[u8]) {
        self.entries.push(WriteBatchEntry {
            value_type: ValueType::Value,
            key: key.to_vec(),
            value: Some(value.to_vec()),
        });
    }

    pub fn delete(&mut self, key: &[u8]) {
        self.entries.push(WriteBatchEntry {
            value_type: ValueType::Deletion,
            key: key.to_vec(),
            value: None,
        });
    }

    /// Add a range deletion. Deletes all keys in [begin, end).
    /// Stored as: InternalKey(begin, seq, RangeDeletion) → end
    pub fn delete_range(&mut self, begin: &[u8], end: &[u8]) {
        self.entries.push(WriteBatchEntry {
            value_type: ValueType::RangeDeletion,
            key: begin.to_vec(),
            value: Some(end.to_vec()),
        });
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn clear(&mut self) {
        self.entries.clear();
    }
}

impl Default for WriteBatch {
    fn default() -> Self {
        Self::new()
    }
}

// ---------------------------------------------------------------------------
// WriteBatchWithIndex — indexed batch for iteration over uncommitted writes
// ---------------------------------------------------------------------------

/// A WriteBatch with a secondary sorted index for efficient iteration.
/// Allows iterating over batch contents merged with a DB snapshot.
pub struct WriteBatchWithIndex {
    batch: WriteBatch,
    /// Maps user_key -> (index of the latest entry in batch.entries, write position).
    index: std::collections::BTreeMap<Vec<u8>, (usize, u64)>,
    /// Range tombstones: (begin, end, write_position) from delete_range calls.
    range_del_entries: Vec<(Vec<u8>, Vec<u8>, u64)>,
    /// Monotonic counter: each put/delete/delete_range increments this.
    next_pos: u64,
}

impl WriteBatchWithIndex {
    pub fn new() -> Self {
        Self {
            batch: WriteBatch::new(),
            index: std::collections::BTreeMap::new(),
            range_del_entries: Vec::new(),
            next_pos: 0,
        }
    }

    pub fn put(&mut self, key: &[u8], value: &[u8]) {
        let idx = self.batch.entries.len();
        let pos = self.next_pos;
        self.next_pos += 1;
        self.batch.put(key, value);
        self.index.insert(key.to_vec(), (idx, pos));
    }

    pub fn delete(&mut self, key: &[u8]) {
        let idx = self.batch.entries.len();
        let pos = self.next_pos;
        self.next_pos += 1;
        self.batch.delete(key);
        self.index.insert(key.to_vec(), (idx, pos));
    }

    pub fn delete_range(&mut self, begin: &[u8], end: &[u8]) {
        let pos = self.next_pos;
        self.next_pos += 1;
        self.batch.delete_range(begin, end);
        self.range_del_entries
            .push((begin.to_vec(), end.to_vec(), pos));
    }

    pub fn len(&self) -> usize {
        self.batch.len()
    }

    pub fn is_empty(&self) -> bool {
        self.batch.is_empty()
    }

    pub fn into_batch(self) -> WriteBatch {
        self.batch
    }

    /// Total number of operations (point + range-del).
    /// Used for sequence number range allocation.
    pub fn operation_count(&self) -> u64 {
        self.next_pos
    }

    /// Return the range tombstones with their write positions.
    pub fn range_tombstones(&self) -> &[(Vec<u8>, Vec<u8>, u64)] {
        &self.range_del_entries
    }

    /// Iterate the BTreeMap in order, producing (InternalKey, value) pairs.
    /// Sequence numbers are assigned as `base_seq + write_position`,
    /// preserving temporal ordering regardless of key sort order.
    pub(crate) fn sorted_entries(&self, base_seq: SequenceNumber) -> Vec<(Vec<u8>, Vec<u8>)> {
        let mut result = Vec::with_capacity(self.index.len());
        for (user_key, &(entry_idx, pos)) in &self.index {
            let entry = &self.batch.entries[entry_idx];
            let ikey = InternalKey::new(user_key, base_seq + pos, entry.value_type);
            let value = entry.value.clone().unwrap_or_default();
            result.push((ikey.into_bytes(), value));
        }
        result
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
pub enum LazyValue {
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
            LazyValue::BlockRef { data, offset, len } => {
                &data[*offset as usize..(*offset + *len) as usize]
            }
        }
    }

    /// Consume self and produce owned bytes.
    #[inline]
    pub fn into_vec(self) -> Vec<u8> {
        match self {
            LazyValue::Inline(v) => v,
            LazyValue::BlockRef { data, offset, len } => {
                data[offset as usize..(offset + len) as usize].to_vec()
            }
        }
    }

    #[inline]
    pub fn len(&self) -> usize {
        match self {
            LazyValue::Inline(v) => v.len(),
            LazyValue::BlockRef { len, .. } => *len as usize,
        }
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Create an empty inline value.
    #[inline]
    pub fn empty() -> Self {
        LazyValue::Inline(Vec::new())
    }
}

impl std::fmt::Debug for LazyValue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
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

        // from_encoded_slice round-trip
        let ikr = InternalKey::from_encoded_slice(ik.as_bytes());
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
        assert_eq!(wb.entries.len(), count);
        assert_eq!(wb.entries[0].key, b"key_00000000");
        assert_eq!(wb.entries[0].value, Some(b"val_00000000".to_vec()));
        assert_eq!(
            wb.entries[count - 1].key,
            format!("key_{:08}", count - 1).into_bytes()
        );

        // Clear should reclaim everything
        wb.clear();
        assert!(wb.is_empty());
        assert_eq!(wb.len(), 0);
        assert!(wb.entries.is_empty());
    }

    #[test]
    fn test_delete_range_entry() {
        let mut wb = WriteBatch::new();
        wb.delete_range(b"begin", b"end");
        assert_eq!(wb.len(), 1);

        let entry = &wb.entries[0];
        assert_eq!(entry.value_type, ValueType::RangeDeletion);
        assert_eq!(entry.key, b"begin");
        assert_eq!(entry.value, Some(b"end".to_vec()));

        // Mix put, delete, delete_range in one batch
        wb.put(b"k1", b"v1");
        wb.delete(b"k2");
        wb.delete_range(b"a", b"z");
        assert_eq!(wb.len(), 4);

        assert_eq!(wb.entries[1].value_type, ValueType::Value);
        assert_eq!(wb.entries[2].value_type, ValueType::Deletion);
        assert_eq!(wb.entries[2].value, None);
        assert_eq!(wb.entries[3].value_type, ValueType::RangeDeletion);
        assert_eq!(wb.entries[3].key, b"a");
        assert_eq!(wb.entries[3].value, Some(b"z".to_vec()));
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
