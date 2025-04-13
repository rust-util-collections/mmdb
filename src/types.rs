//! Core data types: InternalKey, ValueType, SequenceNumber.
//!
//! Encoding follows RocksDB convention:
//!   InternalKey = [user_key bytes][packed: seq << 8 | type]  (last 8 bytes)
//!   Sort order: user_key ASC, sequence DESC, value_type DESC

use std::cmp::Ordering;

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
    let vt = ValueType::from_u8((packed & 0xFF) as u8).unwrap_or(ValueType::Value);
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
        ValueType::from_u8((trailer & 0xFF) as u8).unwrap_or(ValueType::Value)
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
        ValueType::from_u8((trailer & 0xFF) as u8).unwrap_or(ValueType::Value)
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
#[inline]
pub fn compare_internal_key(a: &[u8], b: &[u8]) -> Ordering {
    let a_uk = &a[..a.len() - 8];
    let b_uk = &b[..b.len() - 8];
    a_uk.cmp(b_uk)
        .then_with(|| a[a.len() - 8..].cmp(&b[b.len() - 8..]))
}

/// A write batch groups multiple mutations to be applied atomically.
pub struct WriteBatch {
    pub(crate) entries: Vec<WriteBatchEntry>,
    /// Count of entries.
    pub(crate) count: usize,
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
            count: 0,
        }
    }

    pub fn put(&mut self, key: &[u8], value: &[u8]) {
        self.entries.push(WriteBatchEntry {
            value_type: ValueType::Value,
            key: key.to_vec(),
            value: Some(value.to_vec()),
        });
        self.count += 1;
    }

    pub fn delete(&mut self, key: &[u8]) {
        self.entries.push(WriteBatchEntry {
            value_type: ValueType::Deletion,
            key: key.to_vec(),
            value: None,
        });
        self.count += 1;
    }

    /// Add a range deletion. Deletes all keys in [begin, end).
    /// Stored as: InternalKey(begin, seq, RangeDeletion) → end
    pub fn delete_range(&mut self, begin: &[u8], end: &[u8]) {
        self.entries.push(WriteBatchEntry {
            value_type: ValueType::RangeDeletion,
            key: begin.to_vec(),
            value: Some(end.to_vec()),
        });
        self.count += 1;
    }

    pub fn is_empty(&self) -> bool {
        self.count == 0
    }

    pub fn len(&self) -> usize {
        self.count
    }

    pub fn clear(&mut self) {
        self.entries.clear();
        self.count = 0;
    }
}

impl Default for WriteBatch {
    fn default() -> Self {
        Self::new()
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
