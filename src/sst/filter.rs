//! Bloom filter for SST files.
//!
//! Uses the "double hashing" technique from Kirsch & Mitzenmacher
//! to generate k hash functions from two base hashes.

/// A bloom filter builder + checker.
pub struct BloomFilter {
    /// Bits per key.
    bits_per_key: u32,
    /// Number of hash functions.
    k: u32,
}

impl BloomFilter {
    /// Create a new bloom filter policy.
    pub fn new(bits_per_key: u32) -> Self {
        // k = ln(2) * (bits_per_key)
        let k = ((bits_per_key as f64) * std::f64::consts::LN_2).round() as u32;
        let k = k.clamp(1, 30);
        Self { bits_per_key, k }
    }

    /// Build a filter for the given set of keys.
    /// Returns the filter data.
    pub fn create_filter(&self, keys: &[&[u8]]) -> Vec<u8> {
        let bits = (keys.len() as u32)
            .saturating_mul(self.bits_per_key)
            .max(64); // minimum 64 bits
        let bytes = bits.div_ceil(8) as usize;
        let bits = (bytes * 8) as u32;

        let mut filter = vec![0u8; bytes + 1]; // +1 for k at the end
        *filter.last_mut().unwrap() = self.k as u8;

        for key in keys {
            let h = bloom_hash(key);
            let delta = h.rotate_left(15); // rotate_left(15) == rotate_right(17)
            let mut h = h;
            for _ in 0..self.k {
                let bit_pos = h % bits;
                filter[(bit_pos / 8) as usize] |= 1 << (bit_pos % 8);
                h = h.wrapping_add(delta);
            }
        }

        filter
    }

    /// Check if a key might be in the filter.
    /// Returns `true` if possibly present, `false` if definitely absent.
    pub fn key_may_match(key: &[u8], filter: &[u8]) -> bool {
        if filter.len() < 2 {
            return true; // degenerate: assume present
        }

        let k = *filter.last().unwrap() as u32;
        if k > 30 {
            return true; // reserved for future use
        }

        let bits = ((filter.len() - 1) * 8) as u32;
        let h = bloom_hash(key);
        let delta = h.rotate_left(15);
        let mut h = h;
        for _ in 0..k {
            let bit_pos = h % bits;
            if filter[(bit_pos / 8) as usize] & (1 << (bit_pos % 8)) == 0 {
                return false;
            }
            h = h.wrapping_add(delta);
        }
        true
    }
}

/// Hash function for bloom filter based on MurmurHash2.
/// Matches LevelDB/RocksDB's BloomHash for compatibility and quality.
fn bloom_hash(key: &[u8]) -> u32 {
    let seed: u32 = 0xbc9f1d34;
    let m: u32 = 0x5bd1e995;
    let r: u32 = 24;

    let len = key.len() as u32;
    // Initialize with seed XOR'd with length (anti-length-extension)
    let mut h: u32 = seed ^ len;

    // Process 4-byte chunks
    let mut i = 0;
    while i + 4 <= key.len() {
        let mut k = u32::from_le_bytes(key[i..i + 4].try_into().unwrap());
        k = k.wrapping_mul(m);
        k ^= k >> r;
        k = k.wrapping_mul(m);

        h = h.wrapping_mul(m);
        h ^= k;
        i += 4;
    }

    // Process remaining bytes
    let remaining = key.len() - i;
    if remaining >= 3 {
        h ^= (key[i + 2] as u32) << 16;
    }
    if remaining >= 2 {
        h ^= (key[i + 1] as u32) << 8;
    }
    if remaining >= 1 {
        h ^= key[i] as u32;
        h = h.wrapping_mul(m);
    }

    // Final avalanche mixing
    h ^= h >> 13;
    h = h.wrapping_mul(m);
    h ^= h >> 15;

    h
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_bloom_filter_basic() {
        let bf = BloomFilter::new(10);

        let keys: Vec<&[u8]> = vec![b"hello", b"world", b"foo", b"bar"];
        let filter = bf.create_filter(&keys);

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
    fn test_bloom_filter_empty() {
        let bf = BloomFilter::new(10);
        let filter = bf.create_filter(&[]);
        // With empty filter, no keys should match
        assert!(!BloomFilter::key_may_match(b"anything", &filter));
    }

    #[test]
    fn test_bloom_filter_many_keys() {
        let bf = BloomFilter::new(10);
        let keys: Vec<Vec<u8>> = (0..10000)
            .map(|i| format!("key_{:06}", i).into_bytes())
            .collect();
        let key_refs: Vec<&[u8]> = keys.iter().map(|k| k.as_slice()).collect();
        let filter = bf.create_filter(&key_refs);

        // All keys should match
        for key in &keys {
            assert!(BloomFilter::key_may_match(key, &filter));
        }

        // Check FP rate
        let mut fp = 0;
        for i in 10000..20000 {
            let key = format!("key_{:06}", i);
            if BloomFilter::key_may_match(key.as_bytes(), &filter) {
                fp += 1;
            }
        }
        let fp_rate = fp as f64 / 10000.0;
        assert!(fp_rate < 0.02, "FP rate too high: {:.4}", fp_rate);
    }

    #[test]
    fn test_bloom_bits_per_key_zero() {
        // bits_per_key=0 should still produce a valid (minimal) filter
        let bf = BloomFilter::new(0);
        assert_eq!(bf.k, 1, "k should be clamped to minimum of 1");

        let keys: Vec<&[u8]> = vec![b"aaa", b"bbb", b"ccc"];
        let filter = bf.create_filter(&keys);

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
        assert!(bf.k <= 30, "k should be clamped to max 30, got {}", bf.k);

        let keys: Vec<Vec<u8>> = (0..100)
            .map(|i| format!("key_{:04}", i).into_bytes())
            .collect();
        let key_refs: Vec<&[u8]> = keys.iter().map(|k| k.as_slice()).collect();
        let filter = bf.create_filter(&key_refs);

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
        let filter = bf.create_filter(&keys);

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
        let filter = bf.create_filter(&keys);

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
