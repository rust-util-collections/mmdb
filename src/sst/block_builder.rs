//! Block builder: constructs data blocks with prefix compression.

use crate::sst::block::encode_varint_vec;

/// Builds a data block with prefix compression and restart points.
pub struct BlockBuilder {
    buffer: Vec<u8>,
    restarts: Vec<u32>,
    last_key: Vec<u8>,
    counter: usize,
    restart_interval: usize,
    finished: bool,
}

impl BlockBuilder {
    pub fn new(restart_interval: usize) -> Self {
        let restarts = vec![0]; // First restart point at offset 0
        Self {
            buffer: Vec::new(),
            restarts,
            last_key: Vec::new(),
            counter: 0,
            restart_interval: restart_interval.max(1),
            finished: false,
        }
    }

    /// Add a key-value pair. Keys must be added in sorted order.
    /// The caller (TableBuilder) is responsible for ordering validation.
    pub fn add(&mut self, key: &[u8], value: &[u8]) {
        assert!(!self.finished, "cannot add after finish");

        let mut shared = 0;
        if self.counter < self.restart_interval {
            // Calculate shared prefix with the last key
            let limit = key.len().min(self.last_key.len());
            while shared < limit && self.last_key[shared] == key[shared] {
                shared += 1;
            }
        } else {
            // Restart point: no prefix sharing
            self.restarts.push(self.buffer.len() as u32);
            self.counter = 0;
        }

        let unshared = key.len() - shared;

        // Encode: shared_len | unshared_len | value_len | unshared_key | value
        encode_varint_vec(&mut self.buffer, shared as u32);
        encode_varint_vec(&mut self.buffer, unshared as u32);
        encode_varint_vec(&mut self.buffer, value.len() as u32);
        self.buffer.extend_from_slice(&key[shared..]);
        self.buffer.extend_from_slice(value);

        self.last_key = key.to_vec();
        self.counter += 1;
    }

    /// Finalize the block and return the encoded data.
    pub fn finish(mut self) -> Vec<u8> {
        self.finished = true;
        // Append restart points
        for rp in &self.restarts {
            self.buffer.extend_from_slice(&rp.to_le_bytes());
        }
        // Append number of restart points
        self.buffer
            .extend_from_slice(&(self.restarts.len() as u32).to_le_bytes());
        self.buffer
    }

    /// Current estimated size of the block.
    pub fn estimated_size(&self) -> usize {
        self.buffer.len() + self.restarts.len() * 4 + 4
    }

    /// Whether the builder is empty (no entries added).
    pub fn is_empty(&self) -> bool {
        self.buffer.is_empty()
    }

    /// Reset the builder for reuse.
    pub fn reset(&mut self) {
        self.buffer.clear();
        self.restarts.clear();
        self.restarts.push(0);
        self.last_key.clear();
        self.counter = 0;
        self.finished = false;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sst::block::Block;

    #[test]
    fn test_prefix_compression() {
        let mut builder = BlockBuilder::new(16);
        builder.add(b"abcdef", b"v1");
        builder.add(b"abcxyz", b"v2");
        let data = builder.finish();

        let block = Block::from_vec(data).unwrap();
        let entries: Vec<_> = block.iter().collect();
        assert_eq!(entries[0], (b"abcdef".to_vec(), b"v1".to_vec()));
        assert_eq!(entries[1], (b"abcxyz".to_vec(), b"v2".to_vec()));
    }

    #[test]
    fn test_restart_points() {
        let mut builder = BlockBuilder::new(2);
        builder.add(b"aaa", b"1");
        builder.add(b"aab", b"2");
        // This should start a new restart point
        builder.add(b"bbb", b"3");
        builder.add(b"bbc", b"4");

        let data = builder.finish();
        let block = Block::from_vec(data).unwrap();

        let entries: Vec<_> = block.iter().collect();
        assert_eq!(entries.len(), 4);
        assert_eq!(entries[2].0, b"bbb".to_vec());
    }
}
