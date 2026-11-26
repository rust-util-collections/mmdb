//! Cache-line blocked Bloom filters. Each XXH3-64 probe touches one 64-byte line.
use crate::error::{Error, Result};
use crate::sst::META_BLOCK_HARD_LIMIT;

#[repr(align(64))]
struct Line([u8; 64]);

/// The allocator honors Line's alignment even when compressed bytes were read
/// from an unaligned file offset.
pub(crate) struct AlignedBytes(Vec<Line>);

impl AlignedBytes {
    pub(crate) fn decode(bytes: &[u8]) -> Result<Self> {
        if bytes.is_empty() || !bytes.len().is_multiple_of(64) {
            return Err(Error::corruption("invalid blocked Bloom payload length"));
        }
        Ok(Self(
            bytes
                .as_chunks::<64>()
                .0
                .iter()
                .copied()
                .map(Line)
                .collect(),
        ))
    }

    #[inline]
    pub(crate) fn may_match(&self, key: &[u8]) -> bool {
        self.may_match_hash(bloom_hash(key))
    }

    #[inline]
    pub(crate) fn may_match_hash(&self, hash: u64) -> bool {
        let (line, bits) = probes(hash, self.0.len());
        let data = &self.0[line].0;
        bits.into_iter()
            .all(|bit| data[bit / 8] & (1 << (bit % 8)) != 0)
    }

    #[cfg(test)]
    pub(crate) fn byte_len(&self) -> usize {
        self.0.len() * 64
    }
}

#[inline]
fn mix64(mut x: u64) -> u64 {
    x = (x ^ (x >> 30)).wrapping_mul(0xbf58476d1ce4e5b9);
    x = (x ^ (x >> 27)).wrapping_mul(0x94d049bb133111eb);
    x ^ (x >> 31)
}

#[inline]
fn probes(hash: u64, lines: usize) -> (usize, [usize; 8]) {
    let line = (((hash >> 32) * lines as u64) >> 32) as usize;
    let h1 = mix64(hash);
    let h2 = mix64(hash.wrapping_add(0x9e3779b97f4a7c15));
    (
        line,
        [
            (h1 & 511) as usize,
            ((h1 >> 9) & 511) as usize,
            ((h1 >> 18) & 511) as usize,
            ((h1 >> 27) & 511) as usize,
            (h2 & 511) as usize,
            ((h2 >> 9) & 511) as usize,
            ((h2 >> 18) & 511) as usize,
            ((h2 >> 27) & 511) as usize,
        ],
    )
}

pub struct BloomFilter {
    bits_per_key: u32,
}

impl BloomFilter {
    pub fn new(bits_per_key: u32) -> Self {
        Self { bits_per_key }
    }

    pub fn create_filter(&self, keys: &[&[u8]]) -> Result<Vec<u8>> {
        self.create_filter_from_hashes(&keys.iter().map(|k| bloom_hash(k)).collect::<Vec<_>>())
    }

    pub fn create_filter_from_hashes(&self, hashes: &[u64]) -> Result<Vec<u8>> {
        let size = Self::checked_size("bloom filter", hashes.len(), self.bits_per_key)?;
        let mut bytes = vec![0; size];
        for &hash in hashes {
            let (line, bits) = probes(hash, size / 64);
            for bit in bits {
                bytes[line * 64 + bit / 8] |= 1 << (bit % 8);
            }
        }
        Ok(bytes)
    }

    pub(crate) fn projected_size(count: usize, bits_per_key: u32) -> usize {
        let bits = (count as u64).saturating_mul(bits_per_key as u64);
        usize::try_from(bits.div_ceil(512).max(1).saturating_mul(64)).unwrap_or(usize::MAX)
    }

    pub(crate) fn checked_size(label: &str, count: usize, bits_per_key: u32) -> Result<usize> {
        let size = Self::projected_size(count, bits_per_key);
        if size > META_BLOCK_HARD_LIMIT {
            return Err(Error::invalid_argument(format!(
                "{label} block size {size} exceeds {META_BLOCK_HARD_LIMIT}"
            )));
        }
        Ok(size)
    }

    #[cfg(test)]
    pub fn key_may_match(key: &[u8], bytes: &[u8]) -> bool {
        AlignedBytes::decode(bytes).unwrap().may_match(key)
    }
}

pub(crate) fn bloom_hash(key: &[u8]) -> u64 {
    twox_hash::XxHash3_64::oneshot(key)
}
#[cfg(test)]
mod tests {
    /// The hash is part of the persisted filter format: filters carry no
    /// hash version, so any change makes existing filters return false
    /// negatives. Pin its output.
    #[test]
    fn bloom_hash_is_stable() {
        let pinned: [(&[u8], u64); 5] = [
            (b"", 0x2d06800538d394c2),
            (b"a", 0xe6c632b61e964e1f),
            (b"abcd", 0x6497a96f53a89890),
            (b"hello", 0x9555e8555c62dcfd),
            (b"key_000001", 0xe991a92ea8d5f3bb),
        ];
        for (key, hash) in pinned {
            assert_eq!(bloom_hash(key), hash, "key {key:?}");
        }
    }
    use super::*;

    #[test]
    fn pinned_key_and_prefix_probes() {
        assert_eq!(
            probes(bloom_hash(b"key_000001"), 5),
            (4, [434, 436, 327, 465, 186, 458, 95, 345])
        );
        assert_eq!(
            probes(bloom_hash(b"a"), 5),
            (4, [176, 450, 27, 371, 320, 129, 467, 235])
        );
        let bytes = AlignedBytes::decode(&[0; 128]).unwrap();
        for line in &bytes.0 {
            assert_eq!(line.0.as_ptr() as usize % 64, 0);
        }
    }

    #[test]
    fn independent_probes_reach_poisson_fpr_without_progression_floor() {
        let keys: Vec<Vec<u8>> = (0..10_000)
            .map(|i| format!("present_{i:06}").into_bytes())
            .collect();
        let key_refs: Vec<&[u8]> = keys.iter().map(Vec::as_slice).collect();

        // 12 bpk: independent intra-line probes achieve ~0.41% (vs ~0.57% affine).
        let f12 =
            AlignedBytes::decode(&BloomFilter::new(12).create_filter(&key_refs).unwrap()).unwrap();
        // 24 bpk: independent intra-line probes achieve ~0.009% (vs ~0.067% affine floor).
        let f24 =
            AlignedBytes::decode(&BloomFilter::new(24).create_filter(&key_refs).unwrap()).unwrap();

        let mut fp12 = 0usize;
        let mut fp24 = 0usize;
        const QUERIES: usize = 200_000;
        for i in 0..QUERIES {
            let q = format!("absent_{i:07}");
            if f12.may_match(q.as_bytes()) {
                fp12 += 1;
            }
            if f24.may_match(q.as_bytes()) {
                fp24 += 1;
            }
        }
        let rate12 = fp12 as f64 / QUERIES as f64;
        let rate24 = fp24 as f64 / QUERIES as f64;
        assert!(
            rate12 < 0.0048,
            "12-bpk blocked Bloom FPR {rate12:.5} exceeded independent bound"
        );
        assert!(
            rate24 < 0.00025,
            "24-bpk blocked Bloom FPR {rate24:.5} hit progression floor"
        );
    }

    #[test]
    fn test_bloom_filter_basic() {
        let bf = BloomFilter::new(10);

        let keys: Vec<&[u8]> = vec![b"hello", b"world", b"foo", b"bar"];
        let filter = bf.create_filter(&keys).unwrap();

        // All inserted keys should match
        for &key in &keys {
            assert!(
                BloomFilter::key_may_match(key, &filter),
                "expected {:?} to match",
                key
            );
        }

        // Most non-inserted keys should not match (probabilistic)
        let mut false_positives = 0;
        for i in 0..1000 {
            let key = format!("nonexistent_{}", i);
            if BloomFilter::key_may_match(key.as_bytes(), &filter) {
                false_positives += 1;
            }
        }
        // With 10 bits per key and 4 keys, FP rate should be ~1%
        assert!(
            false_positives < 50,
            "too many false positives: {}",
            false_positives
        );
    }

    #[test]
    fn test_projected_size_matches_encoded_filter() {
        let bf = BloomFilter::new(10);
        let keys: Vec<Vec<u8>> = (0..100).map(|i| format!("key_{i}").into_bytes()).collect();
        let key_refs: Vec<&[u8]> = keys.iter().map(Vec::as_slice).collect();

        assert_eq!(
            BloomFilter::projected_size(key_refs.len(), 10),
            bf.create_filter(&key_refs).unwrap().len()
        );
        assert_eq!(BloomFilter::projected_size(0, 10), 64);
    }

    #[test]
    fn test_create_filter_rejects_oversized_output() {
        use crate::error::ErrorKind;

        let bits_per_key = (META_BLOCK_HARD_LIMIT * 8 + 1) as u32;
        let err = BloomFilter::new(bits_per_key)
            .create_filter(&[b"k"])
            .unwrap_err();

        assert_eq!(err.kind(), ErrorKind::InvalidArgument);
        assert!(err.message().contains("bloom filter block size"));
    }

    #[test]
    fn test_create_filter_from_hashes_matches_create_filter() {
        let bf = BloomFilter::new(10);
        let keys: Vec<Vec<u8>> = (0..500).map(|i| format!("key_{i}").into_bytes()).collect();
        let key_refs: Vec<&[u8]> = keys.iter().map(Vec::as_slice).collect();
        let hashes: Vec<u64> = key_refs.iter().map(|k| bloom_hash(k)).collect();

        assert_eq!(
            bf.create_filter(&key_refs).unwrap(),
            bf.create_filter_from_hashes(&hashes).unwrap(),
            "hash-based construction must be byte-identical to key-based construction"
        );
    }

    #[test]
    fn test_bloom_filter_empty() {
        let bf = BloomFilter::new(10);
        let filter = bf.create_filter(&[]).unwrap();
        // With empty filter, no keys should match
        assert!(!BloomFilter::key_may_match(b"anything", &filter));
    }

    #[test]
    fn test_bloom_filter_many_keys() {
        let bf = BloomFilter::new(12);
        let keys: Vec<Vec<u8>> = (0..10000)
            .map(|i| format!("key_{:06}", i).into_bytes())
            .collect();
        let key_refs: Vec<&[u8]> = keys.iter().map(|k| k.as_slice()).collect();
        let filter = AlignedBytes::decode(&bf.create_filter(&key_refs).unwrap()).unwrap();

        // All keys should match
        for key in &keys {
            assert!(filter.may_match(key));
        }

        // Check FP rate
        let mut fp = 0;
        for i in 10000..210000 {
            let key = format!("key_{:06}", i);
            if filter.may_match(key.as_bytes()) {
                fp += 1;
            }
        }
        let fp_rate = fp as f64 / 200000.0;
        eprintln!("blocked Bloom empirical FPR = {fp_rate:.4}");
        assert!(fp_rate < 0.012, "FP rate too high: {:.4}", fp_rate);
    }

    #[test]
    fn test_bloom_bits_per_key_zero() {
        // bits_per_key=0 should still produce a valid (minimal) filter
        let bf = BloomFilter::new(0);

        let keys: Vec<&[u8]> = vec![b"aaa", b"bbb", b"ccc"];
        let filter = bf.create_filter(&keys).unwrap();

        // Filter should still be valid (at least 2 bytes: min 64 bits -> 8 bytes + 1 for k)
        assert!(filter.len() >= 2);

        // Inserted keys should still match (bloom has no false negatives)
        for &key in &keys {
            assert!(
                BloomFilter::key_may_match(key, &filter),
                "inserted key {:?} must match",
                key
            );
        }
    }

    #[test]
    fn test_bloom_very_high_bits() {
        // bits_per_key=100 should work and have very low FP rate
        let bf = BloomFilter::new(100);
        // k is clamped to max 30

        let keys: Vec<Vec<u8>> = (0..100)
            .map(|i| format!("key_{:04}", i).into_bytes())
            .collect();
        let key_refs: Vec<&[u8]> = keys.iter().map(|k| k.as_slice()).collect();
        let filter = bf.create_filter(&key_refs).unwrap();

        // All inserted keys must match
        for key in &keys {
            assert!(BloomFilter::key_may_match(key, &filter));
        }

        // With 100 bits per key, FP rate should be extremely low
        let mut fp = 0;
        for i in 100..1100 {
            let key = format!("key_{:04}", i);
            if BloomFilter::key_may_match(key.as_bytes(), &filter) {
                fp += 1;
            }
        }
        // Expect near-zero false positives
        assert!(
            fp < 10,
            "with 100 bits/key, expected very few FPs but got {}",
            fp
        );
    }

    #[test]
    fn test_bloom_identical_keys() {
        // All keys are the same -- should not panic or produce invalid filter
        let bf = BloomFilter::new(10);
        let keys: Vec<&[u8]> = vec![b"same"; 1000];
        let filter = bf.create_filter(&keys).unwrap();

        // The identical key should always match
        assert!(BloomFilter::key_may_match(b"same", &filter));

        // A different key should likely not match
        // (not guaranteed, but with only 1 distinct key and 10 bits/key,
        // the filter should be relatively sparse)
        // We just verify no panic and the filter is structurally valid
        let _ = BloomFilter::key_may_match(b"different", &filter);
    }

    #[test]
    fn test_bloom_binary_keys() {
        // Keys containing 0x00 and 0xFF bytes
        let bf = BloomFilter::new(10);
        let k1: Vec<u8> = vec![0x00, 0x00, 0x00];
        let k2: Vec<u8> = vec![0xFF, 0xFF, 0xFF];
        let k3: Vec<u8> = vec![0x00, 0xFF, 0x00, 0xFF];
        let k4: Vec<u8> = vec![]; // empty key
        let keys: Vec<&[u8]> = vec![&k1, &k2, &k3, &k4];
        let filter = bf.create_filter(&keys).unwrap();

        // All inserted keys must match
        assert!(BloomFilter::key_may_match(&k1, &filter));
        assert!(BloomFilter::key_may_match(&k2, &filter));
        assert!(BloomFilter::key_may_match(&k3, &filter));
        assert!(BloomFilter::key_may_match(&k4, &filter));

        // A key not in the set -- just verifying no panic on binary data
        let not_in = vec![0x01, 0x02, 0x03, 0x04, 0x05];
        let _ = BloomFilter::key_may_match(&not_in, &filter);
    }
}
