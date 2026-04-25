//! SST table builder: constructs a complete SST file from sorted key-value pairs.
//!
//! Usage:
//! ```ignore
//! let mut builder = TableBuilder::new(options, file);
//! builder.add(key1, value1);
//! builder.add(key2, value2);
//! builder.finish()?;
//! ```

use std::cmp::Ordering;
use std::collections::HashSet;
use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::Path;

use crate::error::Result;
use crate::sst::block_builder::BlockBuilder;
use crate::sst::filter::BloomFilter;
use crate::sst::format::*;
use crate::types::{InternalKeyRef, ValueType, compare_internal_key};
use ruc::*;

/// Options for building an SST table.
pub struct TableBuildOptions {
    pub block_size: usize,
    pub block_restart_interval: usize,
    pub bloom_bits_per_key: u32,
    /// If true, keys are internal keys (user_key + 8-byte trailer).
    pub internal_keys: bool,
    /// Compression type for data blocks.
    pub compression: CompressionType,
    /// Fixed prefix length for prefix bloom filter. 0 = disabled.
    pub prefix_len: usize,
    /// Block property collectors to attach per-block metadata to the index.
    pub block_property_collectors: Vec<Box<dyn crate::options::BlockPropertyCollector>>,
}

impl Clone for TableBuildOptions {
    fn clone(&self) -> Self {
        Self {
            block_size: self.block_size,
            block_restart_interval: self.block_restart_interval,
            bloom_bits_per_key: self.bloom_bits_per_key,
            internal_keys: self.internal_keys,
            compression: self.compression,
            prefix_len: self.prefix_len,
            // Collectors are per-build; a clone starts with empty collectors
            block_property_collectors: Vec::new(),
        }
    }
}

impl Default for TableBuildOptions {
    fn default() -> Self {
        Self {
            block_size: 4096,
            block_restart_interval: 16,
            bloom_bits_per_key: 10,
            internal_keys: false,
            compression: CompressionType::None,
            prefix_len: 0,
            block_property_collectors: Vec::new(),
        }
    }
}

/// A pending index entry produced when a data block is flushed.
struct PendingIndexEntry {
    last_key: Vec<u8>,
    handle: BlockHandle,
    first_key: Vec<u8>,
    properties: Vec<(String, Vec<u8>)>,
}

/// Builds an SST file.
pub struct TableBuilder {
    writer: BufWriter<File>,
    options: TableBuildOptions,

    // Current data block being built
    data_block: BlockBuilder,
    // Index entries produced by flushed data blocks
    index_entries: Vec<PendingIndexEntry>,
    // First key of the current (not-yet-flushed) data block
    pending_first_key: Option<Vec<u8>>,
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

    // Prefix bloom: collected unique prefixes
    prefix_set: HashSet<Vec<u8>>,

    // Whether any RangeDeletion entry was added
    has_range_deletions: bool,

    /// Buffered range deletion entries (key, value) to write as a separate block.
    range_del_entries: Vec<(Vec<u8>, Vec<u8>)>,

    /// Block property collectors for per-block metadata.
    block_property_collectors: Vec<Box<dyn crate::options::BlockPropertyCollector>>,

    finished: bool,
}

impl TableBuilder {
    /// Create a new table builder writing to the given path.
    pub fn new(path: &Path, mut options: TableBuildOptions) -> Result<Self> {
        let file = File::create(path).c(d!())?;
        let collectors = std::mem::take(&mut options.block_property_collectors);
        Ok(Self {
            writer: BufWriter::new(file),
            data_block: BlockBuilder::new(options.block_restart_interval),
            options,
            index_entries: Vec::new(),
            pending_first_key: None,
            filter_keys: Vec::new(),
            offset: 0,
            num_entries: 0,
            last_key: Vec::new(),
            smallest_key: None,
            largest_key: None,
            prefix_set: HashSet::new(),
            has_range_deletions: false,
            range_del_entries: Vec::new(),
            block_property_collectors: collectors,
            finished: false,
        })
    }

    /// Add a key-value pair. Must be called in sorted key order.
    pub fn add(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        assert!(!self.finished);
        if self.options.internal_keys {
            assert!(
                self.last_key.is_empty()
                    || compare_internal_key(key, &self.last_key) == Ordering::Greater,
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

        // Buffer range deletions into a separate block
        if self.options.internal_keys && key.len() >= 8 {
            let ikr = InternalKeyRef::new(key);
            if ikr.value_type() == ValueType::RangeDeletion {
                self.has_range_deletions = true;
                self.range_del_entries.push((key.to_vec(), value.to_vec()));
                self.last_key = key.to_vec();
                self.num_entries += 1;
                return Ok(());
            }
        }

        // Collect key for bloom filter (use user key if internal_keys mode)
        let user_key_for_bloom = if self.options.internal_keys && key.len() >= 8 {
            &key[..key.len() - 8]
        } else {
            key
        };
        self.filter_keys.push(user_key_for_bloom.to_vec());

        // Collect prefix for prefix bloom filter
        if self.options.prefix_len > 0 && user_key_for_bloom.len() >= self.options.prefix_len {
            self.prefix_set
                .insert(user_key_for_bloom[..self.options.prefix_len].to_vec());
        }

        // Check if we need to flush the current data block
        if self.data_block.estimated_size() >= self.options.block_size
            && !self.data_block.is_empty()
        {
            self.flush_data_block().c(d!())?;
        }

        // Record first key of a new data block
        if self.data_block.is_empty() {
            self.pending_first_key = Some(key.to_vec());
        }

        self.data_block.add(key, value);
        self.last_key = key.to_vec();
        self.num_entries += 1;

        for collector in &mut self.block_property_collectors {
            collector.add(key, value);
        }

        Ok(())
    }

    /// Finish building the table. Must be called when done adding entries.
    pub fn finish(mut self) -> Result<TableBuildResult> {
        // Flush remaining data block
        if !self.data_block.is_empty() {
            self.flush_data_block().c(d!())?;
        }

        // Write meta block (bloom filter)
        let filter_handle = self.write_filter_block().c(d!())?;

        // Write prefix filter block
        let prefix_filter_handle = self.write_prefix_filter_block().c(d!())?;

        // Write range-del block if any
        let range_del_handle = self.write_range_del_block().c(d!())?;

        // Write meta index block
        let metaindex_handle = self
            .write_metaindex_block(&filter_handle, &prefix_filter_handle, &range_del_handle)
            .c(d!())?;

        // Write index block
        let index_handle = self.write_index_block().c(d!())?;

        // Write footer
        let footer = encode_footer(&metaindex_handle, &index_handle);
        self.writer.write_all(&footer).c(d!())?;
        self.offset += FOOTER_SIZE as u64;

        self.writer.flush().c(d!())?;
        self.writer.get_ref().sync_all().c(d!())?;
        self.finished = true;

        Ok(TableBuildResult {
            file_size: self.offset,
            num_entries: self.num_entries,
            smallest_key: self.smallest_key,
            largest_key: self.largest_key,
            has_range_deletions: self.has_range_deletions,
        })
    }

    fn flush_data_block(&mut self) -> Result<()> {
        let last_key = self.last_key.clone();
        let first_key = self.pending_first_key.take().unwrap_or_default();

        // Take out the current block builder and replace with a new one
        let builder = std::mem::replace(
            &mut self.data_block,
            BlockBuilder::new(self.options.block_restart_interval),
        );
        let block_data = builder.finish();

        // Collect block properties from all collectors, then reset for next block
        let props: Vec<(String, Vec<u8>)> = self
            .block_property_collectors
            .iter_mut()
            .map(|c| (c.name().to_string(), c.finish_block()))
            .collect();

        let handle = self.write_raw_block(&block_data).c(d!())?;
        self.index_entries.push(PendingIndexEntry {
            last_key,
            handle,
            first_key,
            properties: props,
        });

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

        self.writer.write_all(&block_data).c(d!())?;

        // Write block trailer: compression_type(1) + crc32(4)
        let mut hasher = crc32fast::Hasher::new();
        hasher.update(&block_data);
        hasher.update(&[compression_type as u8]);
        let crc = hasher.finalize();

        self.writer.write_all(&[compression_type as u8]).c(d!())?;
        self.writer.write_all(&crc.to_le_bytes()).c(d!())?;

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

        self.write_raw_block(&filter_data).c(d!())
    }

    fn write_prefix_filter_block(&mut self) -> Result<BlockHandle> {
        if self.options.prefix_len == 0
            || self.options.bloom_bits_per_key == 0
            || self.prefix_set.is_empty()
        {
            return Ok(BlockHandle::default());
        }

        let mut prefixes: Vec<&[u8]> = self.prefix_set.iter().map(|p| p.as_slice()).collect();
        prefixes.sort();

        let bf = BloomFilter::new(self.options.bloom_bits_per_key);
        let filter_data = bf.create_filter(&prefixes);

        self.write_raw_block(&filter_data).c(d!())
    }

    fn write_range_del_block(&mut self) -> Result<BlockHandle> {
        if self.range_del_entries.is_empty() {
            return Ok(BlockHandle::default());
        }

        let mut builder = BlockBuilder::new(self.options.block_restart_interval);
        for (key, value) in &self.range_del_entries {
            builder.add(key, value);
        }
        let data = builder.finish();
        self.write_raw_block(&data).c(d!())
    }

    fn write_metaindex_block(
        &mut self,
        filter_handle: &BlockHandle,
        prefix_filter_handle: &BlockHandle,
        range_del_handle: &BlockHandle,
    ) -> Result<BlockHandle> {
        let mut builder = BlockBuilder::new(1);

        if filter_handle.size > 0 {
            let handle_bytes = filter_handle.encode();
            builder.add(b"filter.bloom", &handle_bytes);
        }

        if prefix_filter_handle.size > 0 {
            let handle_bytes = prefix_filter_handle.encode();
            builder.add(b"filter.prefix", &handle_bytes);
            builder.add(
                PREFIX_FILTER_LEN_NAME.as_bytes(),
                &(self.options.prefix_len as u64).to_le_bytes(),
            );
        }

        if range_del_handle.size > 0 {
            let handle_bytes = range_del_handle.encode();
            builder.add(RANGE_DEL_BLOCK_NAME.as_bytes(), &handle_bytes);
        }

        let data = builder.finish();
        self.write_raw_block(&data).c(d!())
    }

    fn write_index_block(&mut self) -> Result<BlockHandle> {
        let mut builder = BlockBuilder::new(1);

        for entry in &self.index_entries {
            let value = if entry.properties.is_empty() {
                encode_index_value(&entry.handle, &entry.first_key)
            } else {
                let prop_refs: Vec<(&str, &[u8])> = entry
                    .properties
                    .iter()
                    .map(|(n, d)| (n.as_str(), d.as_slice()))
                    .collect();
                encode_index_value_with_props(&entry.handle, &entry.first_key, &prop_refs)
                    .c(d!())?
            };
            builder.add(&entry.last_key, &value);
        }

        let data = builder.finish();
        self.write_raw_block(&data).c(d!())
    }
}

/// Result of building a table.
#[derive(Debug)]
pub struct TableBuildResult {
    pub file_size: u64,
    pub num_entries: u64,
    pub smallest_key: Option<Vec<u8>>,
    pub largest_key: Option<Vec<u8>>,
    /// Whether any range deletion entry was written to this table.
    pub has_range_deletions: bool,
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

        for (i, entry) in entries.iter().enumerate().take(500) {
            let key = format!("key_{:06}", i);
            let val = format!(
                "value_{}_padding_data_to_make_it_compressible_{}",
                i,
                "x".repeat(100)
            );
            assert_eq!(entry.0, key.as_bytes());
            assert_eq!(entry.1, val.as_bytes());
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

    #[test]
    fn test_range_del_block_separate_storage() {
        use crate::sst::table_reader::TableReader;
        use crate::types::{InternalKey, ValueType};

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("range_del.sst");

        let opts = TableBuildOptions {
            internal_keys: true,
            ..Default::default()
        };
        let mut builder = TableBuilder::new(&path, opts).unwrap();

        // Add a mix of point entries and range deletions in sorted order.
        // Internal key ordering: user_key ASC, sequence DESC.
        builder
            .add(
                InternalKey::new(b"aaa", 10, ValueType::Value).as_bytes(),
                b"val_a",
            )
            .unwrap();
        builder
            .add(
                InternalKey::new(b"bbb", 9, ValueType::RangeDeletion).as_bytes(),
                b"ddd",
            )
            .unwrap();
        builder
            .add(
                InternalKey::new(b"ccc", 8, ValueType::Value).as_bytes(),
                b"val_c",
            )
            .unwrap();
        builder
            .add(
                InternalKey::new(b"eee", 7, ValueType::RangeDeletion).as_bytes(),
                b"ggg",
            )
            .unwrap();
        builder
            .add(
                InternalKey::new(b"fff", 6, ValueType::Value).as_bytes(),
                b"val_f",
            )
            .unwrap();

        let result = builder.finish().unwrap();
        assert_eq!(result.num_entries, 5);
        assert!(result.has_range_deletions);

        // Read back and verify
        let reader = TableReader::open(&path).unwrap();

        // Data block iterator should NOT return range deletion entries
        let entries = reader.iter().unwrap();
        let keys: Vec<&[u8]> = entries.iter().map(|(k, _)| k.as_slice()).collect();
        for k in &keys {
            if k.len() >= 8 {
                let ikr = InternalKeyRef::new(k);
                assert_ne!(
                    ikr.value_type(),
                    ValueType::RangeDeletion,
                    "data blocks should not contain range deletions"
                );
            }
        }
        assert_eq!(entries.len(), 3, "only point entries in data blocks");

        // get_range_tombstones() should return the correct tombstones
        let tombstones = reader.get_range_tombstones().unwrap();
        assert_eq!(tombstones.len(), 2);
        assert_eq!(tombstones[0].0, b"bbb");
        assert_eq!(tombstones[0].1, b"ddd");
        assert_eq!(tombstones[0].2, 9);
        assert_eq!(tombstones[1].0, b"eee");
        assert_eq!(tombstones[1].1, b"ggg");
        assert_eq!(tombstones[1].2, 7);
    }
}
