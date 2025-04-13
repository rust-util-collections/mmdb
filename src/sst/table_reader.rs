//! SST table reader: reads key-value pairs from an SST file.

use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use crate::cache::block_cache::BlockCache;
use crate::error::{Error, Result};
use crate::sst::block::Block;
use crate::sst::filter::BloomFilter;
use crate::sst::format::*;
use crate::types::{InternalKeyRef, SequenceNumber, ValueType};

/// Reader for an SST file.
pub struct TableReader {
    path: PathBuf,
    file_size: u64,
    file_number: u64,
    index_block: Block,
    filter_data: Option<Vec<u8>>,
    file: std::sync::Mutex<File>,
    block_cache: Option<Arc<BlockCache>>,
}

impl TableReader {
    /// Open an SST file for reading.
    pub fn open(path: &Path) -> Result<Self> {
        Self::open_full(path, 0, None)
    }

    /// Open an SST file for reading with a known file number (for cache keying).
    pub fn open_with_number(path: &Path, file_number: u64) -> Result<Self> {
        Self::open_full(path, file_number, None)
    }

    /// Open with file number and optional block cache.
    pub fn open_full(
        path: &Path,
        file_number: u64,
        block_cache: Option<Arc<BlockCache>>,
    ) -> Result<Self> {
        let mut file = File::open(path)?;
        let file_size = file.metadata()?.len();

        if file_size < FOOTER_SIZE as u64 {
            return Err(Error::Corruption(format!(
                "SST file too small: {} bytes",
                file_size
            )));
        }

        // Read footer
        file.seek(SeekFrom::End(-(FOOTER_SIZE as i64)))?;
        let mut footer_buf = [0u8; FOOTER_SIZE];
        file.read_exact(&mut footer_buf)?;
        let (metaindex_handle, index_handle) = decode_footer(&footer_buf)?;

        // Read index block
        let index_data = Self::read_block_data(&mut file, &index_handle)?;
        let index_block = Block::new(index_data)?;

        // Read filter from metaindex
        let filter_data = Self::read_filter(&mut file, &metaindex_handle)?;

        Ok(Self {
            path: path.to_path_buf(),
            file_size,
            file_number,
            index_block,
            filter_data,
            file: std::sync::Mutex::new(file),
            block_cache,
        })
    }

    /// Look up a key in the SST (exact byte match). Returns the value if found.
    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        // Check bloom filter first
        if let Some(ref filter) = self.filter_data
            && !BloomFilter::key_may_match(key, filter)
        {
            return Ok(None);
        }

        // Search index block for the data block that might contain the key
        let block_handle = match self.find_data_block(key)? {
            Some(h) => h,
            None => return Ok(None),
        };

        // Read and search the data block
        let block_data = self.read_block_cached(&block_handle)?;
        let block = Block::new(block_data)?;

        match block.seek(key) {
            Some((found_key, value)) if found_key.as_slice() == key => Ok(Some(value)),
            _ => Ok(None),
        }
    }

    /// Look up a user key in an SST that stores internal keys.
    /// Finds the latest entry for `user_key` with sequence <= `sequence`.
    /// Returns:
    /// - `Some(Some(value))` if a Value entry is found
    /// - `Some(None)` if a Deletion tombstone is found
    /// - `None` if the key doesn't exist in this table
    pub fn get_internal(
        &self,
        user_key: &[u8],
        sequence: SequenceNumber,
    ) -> Result<Option<Option<Vec<u8>>>> {
        use crate::types::InternalKey;

        // Check bloom filter with user key
        if let Some(ref filter) = self.filter_data
            && !BloomFilter::key_may_match(user_key, filter)
        {
            return Ok(None);
        }

        // Construct a seek key: (user_key, sequence, Value).
        // With inverted-BE encoding, lex order = logical order, so seeking to this
        // key in the index block finds the right data block via binary search.
        let seek_key = InternalKey::new(user_key, sequence, ValueType::Value);

        use crate::types::compare_internal_key;

        // Use index block seek to find the data block that may contain our key.
        let handle = match self
            .index_block
            .seek_by(seek_key.as_bytes(), compare_internal_key)
        {
            Some((_idx_key, handle_bytes)) => BlockHandle::decode(&handle_bytes),
            None => return Ok(None),
        };

        let block_data = self.read_block_cached(&handle)?;
        let block = Block::new(block_data)?;

        // Seek within the data block. The first entry >= seek_key with matching user_key
        // is our answer (because entries are sorted user_key ASC, seq DESC).
        match block.seek_by(seek_key.as_bytes(), compare_internal_key) {
            Some((encoded_ikey, value)) if encoded_ikey.len() >= 8 => {
                let ik = InternalKeyRef::new(&encoded_ikey);
                if ik.user_key() == user_key {
                    return Ok(Some(match ik.value_type() {
                        ValueType::Value => Some(value),
                        ValueType::Deletion | ValueType::RangeDeletion => None,
                    }));
                }
                Ok(None)
            }
            _ => Ok(None),
        }
    }

    /// Iterate over all key-value pairs in the table.
    pub fn iter(&self) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        let mut result = Vec::new();

        for (_, handle_bytes) in self.index_block.iter() {
            let handle = BlockHandle::decode(&handle_bytes);
            let block_data = self.read_block_cached(&handle)?;
            let block = Block::new(block_data)?;
            for entry in block.iter() {
                result.push(entry);
            }
        }

        Ok(result)
    }

    /// Find the data block handle that might contain the given key.
    fn find_data_block(&self, key: &[u8]) -> Result<Option<BlockHandle>> {
        match self.index_block.seek(key) {
            Some((_idx_key, handle_bytes)) => {
                let handle = BlockHandle::decode(&handle_bytes);
                Ok(Some(handle))
            }
            None => Ok(None),
        }
    }

    fn read_block_data(file: &mut File, handle: &BlockHandle) -> Result<Vec<u8>> {
        file.seek(SeekFrom::Start(handle.offset))?;
        let mut data = vec![0u8; handle.size as usize];
        file.read_exact(&mut data)?;

        // Read and verify trailer
        let mut trailer = [0u8; BLOCK_TRAILER_SIZE];
        file.read_exact(&mut trailer)?;

        let compression_type = CompressionType::from_u8(trailer[0])
            .ok_or_else(|| Error::Corruption("unknown compression type".to_string()))?;

        let stored_crc = u32::from_le_bytes(trailer[1..5].try_into().unwrap());
        let mut hasher = crc32fast::Hasher::new();
        hasher.update(&data);
        hasher.update(&[trailer[0]]);
        let computed_crc = hasher.finalize();

        if stored_crc != computed_crc {
            return Err(Error::Corruption(format!(
                "block CRC mismatch: stored {:#x}, computed {:#x}",
                stored_crc, computed_crc
            )));
        }

        // Decompress if needed
        let data = match compression_type {
            CompressionType::Lz4 => lz4_flex::decompress_size_prepended(&data)
                .map_err(|e| Error::Corruption(format!("LZ4 decompression error: {}", e)))?,
            CompressionType::Zstd => zstd::stream::decode_all(data.as_slice())
                .map_err(|e| Error::Corruption(format!("Zstd decompression error: {}", e)))?,
            CompressionType::None => data,
        };

        Ok(data)
    }

    fn read_filter(file: &mut File, metaindex_handle: &BlockHandle) -> Result<Option<Vec<u8>>> {
        if metaindex_handle.size == 0 {
            return Ok(None);
        }

        let metaindex_data = Self::read_block_data(file, metaindex_handle)?;
        let metaindex = Block::new(metaindex_data)?;

        for (key, value) in metaindex.iter() {
            if key == b"filter.bloom" {
                let filter_handle = BlockHandle::decode(&value);
                let filter_data = Self::read_block_data(file, &filter_handle)?;
                return Ok(Some(filter_data));
            }
        }

        Ok(None)
    }

    pub fn file_size(&self) -> u64 {
        self.file_size
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn file_number(&self) -> u64 {
        self.file_number
    }

    /// Read a block, consulting the cache first if available.
    fn read_block_cached(&self, handle: &BlockHandle) -> Result<Vec<u8>> {
        if let Some(ref cache) = self.block_cache
            && let Some(cached) = cache.get(self.file_number, handle.offset)
        {
            return Ok((*cached).clone());
        }

        let mut file = self.open_file()?;
        let data = Self::read_block_data(&mut file, handle)?;

        if let Some(ref cache) = self.block_cache {
            cache.insert(self.file_number, handle.offset, data.clone());
        }

        Ok(data)
    }

    /// Get a file handle for reading. Uses the held file via mutex.
    fn open_file(&self) -> Result<std::sync::MutexGuard<'_, File>> {
        self.file
            .lock()
            .map_err(|_| Error::Corruption("file mutex poisoned".to_string()))
    }
}

/// Streaming iterator over an SST file — reads blocks on demand.
/// Memory usage: O(1 block) instead of O(entire table).
pub struct TableIterator {
    reader: Arc<TableReader>,
    /// Index block entries: (index_key, block_handle_bytes)
    index_entries: Vec<(Vec<u8>, Vec<u8>)>,
    /// Current index position
    index_pos: usize,
    /// Current block's entries (lazily loaded)
    current_block_entries: Vec<(Vec<u8>, Vec<u8>)>,
    /// Position within current block
    block_pos: usize,
}

impl TableIterator {
    pub fn new(reader: Arc<TableReader>) -> Self {
        let index_entries: Vec<_> = reader.index_block.iter().collect();
        Self {
            reader,
            index_entries,
            index_pos: 0,
            current_block_entries: Vec::new(),
            block_pos: 0,
        }
    }

    /// Seek to the first entry >= target using the index block for O(log N) lookup.
    pub fn seek(&mut self, target: &[u8]) {
        use crate::types::compare_internal_key;

        // Binary search index entries to find the first block that may contain target
        let idx = self.index_entries.partition_point(|(idx_key, _)| {
            compare_internal_key(idx_key, target) == std::cmp::Ordering::Less
        });

        self.index_pos = idx;
        self.current_block_entries.clear();
        self.block_pos = 0;

        // Load the found block and seek within it
        if self.index_pos < self.index_entries.len() {
            let (_, ref handle_bytes) = self.index_entries[self.index_pos];
            let handle = BlockHandle::decode(handle_bytes);
            self.index_pos += 1;

            if let Ok(data) = self.reader.read_block_cached(&handle)
                && let Ok(block) = Block::new(data)
            {
                self.current_block_entries = block.iter().collect();
                // Find first entry >= target within block
                self.block_pos = self.current_block_entries.partition_point(|(k, _)| {
                    compare_internal_key(k, target) == std::cmp::Ordering::Less
                });
            }
        }
    }

    fn load_next_block(&mut self) -> bool {
        while self.index_pos < self.index_entries.len() {
            let (_, ref handle_bytes) = self.index_entries[self.index_pos];
            let handle = BlockHandle::decode(handle_bytes);
            self.index_pos += 1;

            match self.reader.read_block_cached(&handle) {
                Ok(data) => match Block::new(data) {
                    Ok(block) => {
                        self.current_block_entries = block.iter().collect();
                        self.block_pos = 0;
                        if !self.current_block_entries.is_empty() {
                            return true;
                        }
                    }
                    Err(_) => continue,
                },
                Err(_) => continue,
            }
        }
        false
    }
}

impl Iterator for TableIterator {
    type Item = (Vec<u8>, Vec<u8>);

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if self.block_pos < self.current_block_entries.len() {
                let entry = self.current_block_entries[self.block_pos].clone();
                self.block_pos += 1;
                return Some(entry);
            }
            if !self.load_next_block() {
                return None;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sst::table_builder::{TableBuildOptions, TableBuilder};

    fn build_test_table(dir: &Path, count: usize) -> PathBuf {
        let path = dir.join("test.sst");
        let mut builder = TableBuilder::new(&path, TableBuildOptions::default()).unwrap();
        for i in 0..count {
            let key = format!("key_{:06}", i);
            let val = format!("value_{}", i);
            builder.add(key.as_bytes(), val.as_bytes()).unwrap();
        }
        builder.finish().unwrap();
        path
    }

    #[test]
    fn test_table_read_all() {
        let dir = tempfile::tempdir().unwrap();
        let path = build_test_table(dir.path(), 100);

        let reader = TableReader::open(&path).unwrap();
        let entries = reader.iter().unwrap();
        assert_eq!(entries.len(), 100);

        for (i, (k, v)) in entries.iter().enumerate() {
            assert_eq!(k, format!("key_{:06}", i).as_bytes());
            assert_eq!(v, format!("value_{}", i).as_bytes());
        }
    }

    #[test]
    fn test_table_point_lookup() {
        let dir = tempfile::tempdir().unwrap();
        let path = build_test_table(dir.path(), 100);

        let reader = TableReader::open(&path).unwrap();

        let val = reader.get(b"key_000050").unwrap();
        assert_eq!(val, Some(b"value_50".to_vec()));

        let val = reader.get(b"key_000000").unwrap();
        assert_eq!(val, Some(b"value_0".to_vec()));

        let val = reader.get(b"key_000099").unwrap();
        assert_eq!(val, Some(b"value_99".to_vec()));

        let val = reader.get(b"key_999999").unwrap();
        assert_eq!(val, None);

        let val = reader.get(b"aaa").unwrap();
        assert_eq!(val, None);
    }

    #[test]
    fn test_table_large() {
        let dir = tempfile::tempdir().unwrap();
        let path = build_test_table(dir.path(), 10000);

        let reader = TableReader::open(&path).unwrap();

        for i in (0..10000).step_by(100) {
            let key = format!("key_{:06}", i);
            let val = format!("value_{}", i);
            assert_eq!(
                reader.get(key.as_bytes()).unwrap(),
                Some(val.into_bytes()),
                "failed at key {}",
                i
            );
        }
    }

    #[test]
    fn test_bloom_filter_used() {
        let dir = tempfile::tempdir().unwrap();
        let path = build_test_table(dir.path(), 100);

        let reader = TableReader::open(&path).unwrap();
        assert!(reader.filter_data.is_some());

        let val = reader.get(b"nonexistent_key_12345").unwrap();
        assert_eq!(val, None);
    }

    #[test]
    fn test_internal_key_lookup() {
        use crate::types::InternalKey;

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("internal.sst");

        // Build SST with internal keys
        let mut builder = TableBuilder::new(
            &path,
            TableBuildOptions {
                bloom_bits_per_key: 0, // disable bloom for this test
                ..Default::default()
            },
        )
        .unwrap();

        // user_key "aaa" at seq 5 (Value), seq 3 (Deletion)
        // user_key "bbb" at seq 4 (Value)
        // Internal keys sorted by: user_key ASC, but in lex byte order
        // which for internal keys is user_key prefix then trailer bytes.
        let ik1 = InternalKey::new(b"aaa", 3, ValueType::Deletion);
        let ik2 = InternalKey::new(b"aaa", 5, ValueType::Value);
        let ik3 = InternalKey::new(b"bbb", 4, ValueType::Value);

        // Sort by raw bytes (lex order, as the skiplist would)
        let mut entries = vec![
            (ik1.as_bytes().to_vec(), b"".to_vec()),
            (ik2.as_bytes().to_vec(), b"val_aaa".to_vec()),
            (ik3.as_bytes().to_vec(), b"val_bbb".to_vec()),
        ];
        entries.sort_by(|(a, _), (b, _)| a.cmp(b));

        for (k, v) in &entries {
            builder.add(k, v).unwrap();
        }
        builder.finish().unwrap();

        let reader = TableReader::open(&path).unwrap();

        // Look up "aaa" at seq 10 — should find seq 5 Value
        let result = reader.get_internal(b"aaa", 10).unwrap();
        assert!(result.is_some());

        // Look up "bbb" at seq 10
        let result = reader.get_internal(b"bbb", 10).unwrap();
        assert_eq!(result, Some(Some(b"val_bbb".to_vec())));

        // Look up "ccc" — not found
        let result = reader.get_internal(b"ccc", 10).unwrap();
        assert_eq!(result, None);
    }

    #[test]
    fn test_table_iterator_streaming() {
        let dir = tempfile::tempdir().unwrap();
        let path = build_test_table(dir.path(), 500);

        let reader = Arc::new(TableReader::open(&path).unwrap());
        let mut iter = TableIterator::new(reader);

        // Collect all entries via the streaming iterator
        let mut count = 0;
        let mut prev_key: Option<Vec<u8>> = None;
        while let Some((k, v)) = iter.next() {
            let expected_key = format!("key_{:06}", count);
            let expected_val = format!("value_{}", count);
            assert_eq!(
                k,
                expected_key.as_bytes(),
                "key mismatch at index {}",
                count
            );
            assert_eq!(
                v,
                expected_val.as_bytes(),
                "value mismatch at index {}",
                count
            );

            // Verify keys are in sorted order
            if let Some(ref pk) = prev_key {
                assert!(
                    k.as_slice() > pk.as_slice(),
                    "keys not in order at {}",
                    count
                );
            }
            prev_key = Some(k);
            count += 1;
        }
        assert_eq!(count, 500);
    }
}
