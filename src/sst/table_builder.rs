//! SST table builder: constructs a complete SST file from sorted key-value pairs.
//!
//! Usage:
//! ```ignore
//! let mut builder = TableBuilder::new(options, file);
//! builder.add(key1, value1);
//! builder.add(key2, value2);
//! builder.finish()?;
//! ```

use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::Path;

use crate::error::Result;
use crate::sst::block_builder::BlockBuilder;
use crate::sst::filter::BloomFilter;
use crate::sst::format::*;

/// Options for building an SST table.
#[derive(Clone)]
pub struct TableBuildOptions {
    pub block_size: usize,
    pub block_restart_interval: usize,
    pub bloom_bits_per_key: u32,
    /// If true, keys are internal keys (user_key + 8-byte trailer).
    pub internal_keys: bool,
    /// Compression type for data blocks.
    pub compression: CompressionType,
}

impl Default for TableBuildOptions {
    fn default() -> Self {
        Self {
            block_size: 4096,
            block_restart_interval: 16,
            bloom_bits_per_key: 10,
            internal_keys: false,
            compression: CompressionType::None,
        }
    }
}

/// Builds an SST file.
pub struct TableBuilder {
    writer: BufWriter<File>,
    options: TableBuildOptions,

    // Current data block being built
    data_block: BlockBuilder,
    // Index entries: (last_key_of_block, BlockHandle)
    index_entries: Vec<(Vec<u8>, BlockHandle)>,
    // Keys for bloom filter
    filter_keys: Vec<Vec<u8>>,

    // Current file offset
    offset: u64,
    // Number of entries written
    num_entries: u64,
    // Last key added (for ordering check)
    last_key: Vec<u8>,

    // Smallest and largest keys in the table
    smallest_key: Option<Vec<u8>>,
    largest_key: Option<Vec<u8>>,

    finished: bool,
}

impl TableBuilder {
    /// Create a new table builder writing to the given path.
    pub fn new(path: &Path, options: TableBuildOptions) -> Result<Self> {
        let file = File::create(path)?;
        Ok(Self {
            writer: BufWriter::new(file),
            data_block: BlockBuilder::new(options.block_restart_interval),
            options,
            index_entries: Vec::new(),
            filter_keys: Vec::new(),
            offset: 0,
            num_entries: 0,
            last_key: Vec::new(),
            smallest_key: None,
            largest_key: None,
            finished: false,
        })
    }

    /// Add a key-value pair. Must be called in sorted key order.
    pub fn add(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        assert!(!self.finished);
        if self.options.internal_keys {
            assert!(
                self.last_key.is_empty()
                    || crate::types::compare_internal_key(key, &self.last_key)
                        == std::cmp::Ordering::Greater,
                "keys must be added in order"
            );
        } else {
            assert!(
                self.last_key.is_empty() || key > self.last_key.as_slice(),
                "keys must be added in order"
            );
        }

        if self.smallest_key.is_none() {
            self.smallest_key = Some(key.to_vec());
        }
        self.largest_key = Some(key.to_vec());

        // Collect key for bloom filter (use user key if internal_keys mode)
        if self.options.internal_keys && key.len() >= 8 {
            self.filter_keys.push(key[..key.len() - 8].to_vec());
        } else {
            self.filter_keys.push(key.to_vec());
        }

        // Check if we need to flush the current data block
        if self.data_block.estimated_size() >= self.options.block_size
            && !self.data_block.is_empty()
        {
            self.flush_data_block()?;
        }

        self.data_block.add(key, value);
        self.last_key = key.to_vec();
        self.num_entries += 1;

        Ok(())
    }

    /// Finish building the table. Must be called when done adding entries.
    pub fn finish(mut self) -> Result<TableBuildResult> {
        // Flush remaining data block
        if !self.data_block.is_empty() {
            self.flush_data_block()?;
        }

        // Write meta block (bloom filter)
        let filter_handle = self.write_filter_block()?;

        // Write meta index block
        let metaindex_handle = self.write_metaindex_block(&filter_handle)?;

        // Write index block
        let index_handle = self.write_index_block()?;

        // Write footer
        let footer = encode_footer(&metaindex_handle, &index_handle);
        self.writer.write_all(&footer)?;
        self.offset += FOOTER_SIZE as u64;

        self.writer.flush()?;
        self.writer.get_ref().sync_all()?;
        self.finished = true;

        Ok(TableBuildResult {
            file_size: self.offset,
            num_entries: self.num_entries,
            smallest_key: self.smallest_key,
            largest_key: self.largest_key,
        })
    }

    fn flush_data_block(&mut self) -> Result<()> {
        let last_key = self.last_key.clone();

        // Take out the current block builder and replace with a new one
        let builder = std::mem::replace(
            &mut self.data_block,
            BlockBuilder::new(self.options.block_restart_interval),
        );
        let block_data = builder.finish();

        let handle = self.write_raw_block(&block_data)?;
        self.index_entries.push((last_key, handle));

        Ok(())
    }

    fn write_raw_block(&mut self, data: &[u8]) -> Result<BlockHandle> {
        let (block_data, compression_type) = match self.options.compression {
            CompressionType::Lz4 => {
                let compressed = lz4_flex::compress_prepend_size(data);
                // Only use compression if it actually saves space
                if compressed.len() < data.len() {
                    (compressed, CompressionType::Lz4)
                } else {
                    (data.to_vec(), CompressionType::None)
                }
            }
            CompressionType::Zstd => {
                let compressed = zstd::bulk::compress(data, 3).unwrap_or_else(|_| data.to_vec());
                if compressed.len() < data.len() {
                    (compressed, CompressionType::Zstd)
                } else {
                    (data.to_vec(), CompressionType::None)
                }
            }
            CompressionType::None => (data.to_vec(), CompressionType::None),
        };

        let handle = BlockHandle::new(self.offset, block_data.len() as u64);

        self.writer.write_all(&block_data)?;

        // Write block trailer: compression_type(1) + crc32(4)
        let mut hasher = crc32fast::Hasher::new();
        hasher.update(&block_data);
        hasher.update(&[compression_type as u8]);
        let crc = hasher.finalize();

        self.writer.write_all(&[compression_type as u8])?;
        self.writer.write_all(&crc.to_le_bytes())?;

        self.offset += block_data.len() as u64 + BLOCK_TRAILER_SIZE as u64;

        Ok(handle)
    }

    fn write_filter_block(&mut self) -> Result<BlockHandle> {
        if self.options.bloom_bits_per_key == 0 || self.filter_keys.is_empty() {
            // No filter
            return Ok(BlockHandle::default());
        }

        let bf = BloomFilter::new(self.options.bloom_bits_per_key);
        let key_refs: Vec<&[u8]> = self.filter_keys.iter().map(|k| k.as_slice()).collect();
        let filter_data = bf.create_filter(&key_refs);

        self.write_raw_block(&filter_data)
    }

    fn write_metaindex_block(&mut self, filter_handle: &BlockHandle) -> Result<BlockHandle> {
        let mut builder = BlockBuilder::new(1);

        if filter_handle.size > 0 {
            // Add entry: "filter.bloom" => encoded BlockHandle
            let handle_bytes = filter_handle.encode();
            builder.add(b"filter.bloom", &handle_bytes);
        }

        let data = builder.finish();
        self.write_raw_block(&data)
    }

    fn write_index_block(&mut self) -> Result<BlockHandle> {
        let mut builder = BlockBuilder::new(1);

        for (last_key, handle) in &self.index_entries {
            let handle_bytes = handle.encode();
            builder.add(last_key, &handle_bytes);
        }

        let data = builder.finish();
        self.write_raw_block(&data)
    }
}

/// Result of building a table.
#[derive(Debug)]
pub struct TableBuildResult {
    pub file_size: u64,
    pub num_entries: u64,
    pub smallest_key: Option<Vec<u8>>,
    pub largest_key: Option<Vec<u8>>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_build_table() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.sst");

        let mut builder = TableBuilder::new(&path, TableBuildOptions::default()).unwrap();
        for i in 0..100 {
            let key = format!("key_{:06}", i);
            let val = format!("value_{}", i);
            builder.add(key.as_bytes(), val.as_bytes()).unwrap();
        }
        let result = builder.finish().unwrap();

        assert_eq!(result.num_entries, 100);
        assert!(result.file_size > 0);
        assert_eq!(
            result.smallest_key.as_deref(),
            Some(b"key_000000".as_slice())
        );
        assert_eq!(
            result.largest_key.as_deref(),
            Some(b"key_000099".as_slice())
        );
    }

    #[test]
    fn test_empty_table() {
        use crate::sst::table_reader::TableReader;

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("empty.sst");

        let builder = TableBuilder::new(&path, TableBuildOptions::default()).unwrap();
        let result = builder.finish().unwrap();

        assert_eq!(result.num_entries, 0);
        assert!(result.file_size > 0); // footer + index + metaindex still present
        assert!(result.smallest_key.is_none());
        assert!(result.largest_key.is_none());

        // Should be readable and contain no entries
        let reader = TableReader::open(&path).unwrap();
        let entries = reader.iter().unwrap();
        assert!(entries.is_empty());

        // Point lookup on empty table
        assert_eq!(reader.get(b"anything").unwrap(), None);
    }

    #[test]
    fn test_single_entry_table() {
        use crate::sst::table_reader::TableReader;

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("single.sst");

        let mut builder = TableBuilder::new(&path, TableBuildOptions::default()).unwrap();
        builder.add(b"only_key", b"only_value").unwrap();
        let result = builder.finish().unwrap();

        assert_eq!(result.num_entries, 1);
        assert_eq!(result.smallest_key.as_deref(), Some(b"only_key".as_slice()));
        assert_eq!(result.largest_key.as_deref(), Some(b"only_key".as_slice()));

        let reader = TableReader::open(&path).unwrap();
        let entries = reader.iter().unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].0, b"only_key");
        assert_eq!(entries[0].1, b"only_value");

        assert_eq!(
            reader.get(b"only_key").unwrap(),
            Some(b"only_value".to_vec())
        );
        assert_eq!(reader.get(b"other").unwrap(), None);
    }

    #[test]
    fn test_zstd_compression_roundtrip() {
        use crate::sst::table_reader::TableReader;

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("zstd.sst");

        let opts = TableBuildOptions {
            compression: CompressionType::Zstd,
            ..Default::default()
        };
        let mut builder = TableBuilder::new(&path, opts).unwrap();

        // Write enough data that compression is worthwhile
        for i in 0..500 {
            let key = format!("key_{:06}", i);
            // Repeating patterns compress well
            let val = format!(
                "value_{}_padding_data_to_make_it_compressible_{}",
                i,
                "x".repeat(100)
            );
            builder.add(key.as_bytes(), val.as_bytes()).unwrap();
        }
        let result = builder.finish().unwrap();
        assert_eq!(result.num_entries, 500);

        // Read back and verify all entries
        let reader = TableReader::open(&path).unwrap();
        let entries = reader.iter().unwrap();
        assert_eq!(entries.len(), 500);

        for i in 0..500 {
            let key = format!("key_{:06}", i);
            let val = format!(
                "value_{}_padding_data_to_make_it_compressible_{}",
                i,
                "x".repeat(100)
            );
            assert_eq!(entries[i].0, key.as_bytes());
            assert_eq!(entries[i].1, val.as_bytes());
        }

        // Point lookups should also work
        assert_eq!(
            reader.get(b"key_000000").unwrap(),
            Some(
                format!(
                    "value_0_padding_data_to_make_it_compressible_{}",
                    "x".repeat(100)
                )
                .into_bytes()
            )
        );
        assert_eq!(reader.get(b"nonexistent").unwrap(), None);
    }
}
