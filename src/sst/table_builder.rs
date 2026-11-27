//! SST table builder: constructs a complete SST file from sorted key-value pairs.

use std::{
    collections::{BTreeMap, HashSet},
    fs::File,
    io::{BufWriter, Write},
    path::Path,
    sync::Arc,
};

use crate::error::{Error, Result, ResultExt};
use crate::sst::{
    META_BLOCK_HARD_LIMIT,
    block_builder::BlockBuilder,
    filter::{BloomFilter, bloom_hash},
    format::*,
    range::RangeFragment,
    table_reader::MAX_DECOMPRESSED_BLOCK_SIZE,
};
use crate::types::{
    InternalKey, MAX_RANGE_DELETE_SIZE, MAX_SEQUENCE_NUMBER, MAX_USER_KEY_SIZE,
    MAX_WRITE_ENTRY_SIZE, SequenceNumber, ValueType, compare_internal_key, decode_internal_key,
    user_key,
};

/// Soft threshold at which callers that can split their output across
/// multiple SST files (flush, compaction) should cut the current file, so
/// the hard metadata-block limit is never approached in normal operation.
pub(crate) const META_BLOCK_SPLIT_THRESHOLD: usize = 32 * 1024 * 1024;

/// One range-deletion interval cannot split its sequence array across leaves.
/// Reserve both maximum-sized endpoints and 128 bytes for field lengths,
/// block framing and the restart array. The newest state consumes one stripe.
pub(crate) const MAX_RANGE_STRIPES: usize =
    (META_BLOCK_HARD_LIMIT - 2 * MAX_USER_KEY_SIZE - 128) / size_of::<u64>();

/// Conservative per-entry overhead estimate for meta block accounting:
/// covers the 16-byte block handle, varint length headers, and the restart
/// array slot.
const META_ENTRY_OVERHEAD: usize = 64;

// Compile-time ties between the write-path limits in `types` and the SST
// format limits enforced here, reserving framing for individual entries.
// Aggregate metadata limits are checked as the table is built.
const _: () = {
    assert!(MAX_WRITE_ENTRY_SIZE + 8 <= MAX_DECOMPRESSED_BLOCK_SIZE - 64);
    assert!(
        MAX_RANGE_DELETE_SIZE + 8 + META_ENTRY_OVERHEAD + META_BLOCK_SPLIT_THRESHOLD
            <= META_BLOCK_HARD_LIMIT
    );
    assert!(2 * (MAX_USER_KEY_SIZE + 8) + META_ENTRY_OVERHEAD <= META_BLOCK_SPLIT_THRESHOLD);
};

/// Options for building an SST table.
pub struct TableBuildOptions {
    pub block_size: usize,
    pub block_restart_interval: usize,
    pub bloom_bits_per_key: u32,
    /// Optional separate budget for `filter.prefix`; `None` uses `bloom_bits_per_key`.
    pub prefix_bloom_bits_per_key: Option<u32>,
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
            prefix_bloom_bits_per_key: self.prefix_bloom_bits_per_key,
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
            bloom_bits_per_key: 12,
            prefix_bloom_bits_per_key: None,
            compression: CompressionType::None,
            prefix_len: 0,
            block_property_collectors: Vec::new(),
        }
    }
}

type RangeExtents = Vec<(Vec<u8>, Vec<u8>)>;

const PARTITION_BYTES: usize = 64 * 1024;
fn range_partition_value(handle: BlockHandle, begin: &[u8]) -> Vec<u8> {
    let mut value = Vec::with_capacity(16 + begin.len());
    value.extend_from_slice(&handle.encode());
    value.extend_from_slice(begin);
    value
}

/// Pick a strict interior user-key prefix when it shortens the separator.
/// The earliest valid trailer makes the result a seekable internal key.
fn shorten_separator(last: &mut Vec<u8>, next: &[u8]) {
    let lo = user_key(last);
    let hi = user_key(next);
    let Some(i) = lo.iter().zip(hi).position(|(a, b)| a != b) else {
        return;
    };
    if i + 1 >= lo.len() || lo[i] >= hi[i] || (lo[i] + 1 == hi[i] && i + 1 == hi.len()) {
        return;
    }
    let mut prefix = lo[..=i].to_vec();
    prefix[i] += 1;
    *last = InternalKey::new(&prefix, MAX_SEQUENCE_NUMBER, ValueType::RangeDeletion).into_bytes();
}

/// A pending index entry produced when a data block is flushed.
struct PendingIndexEntry {
    last_key: Vec<u8>,
    handle: BlockHandle,
    first_key: Vec<u8>,
    properties: Vec<(String, Vec<u8>)>,
}

/// Serialize one logical resident directory in bounded, CRC-protected pages.
/// A page's trailing handle points strictly backwards. Opening a table walks
/// these pages once; point lookup uses only the resulting in-memory directory.
struct DirectoryWriter {
    page: BlockBuilder,
    previous: BlockHandle,
    target: usize,
}

impl DirectoryWriter {
    fn new() -> Self {
        Self {
            page: BlockBuilder::new(usize::MAX),
            previous: BlockHandle::default(),
            target: META_BLOCK_SPLIT_THRESHOLD,
        }
    }

    fn append(
        &mut self,
        key: &[u8],
        value: &[u8],
        write: &mut impl FnMut(&[u8]) -> Result<BlockHandle>,
    ) -> Result<()> {
        if !self.page.is_empty() && self.page.estimated_size() >= self.target {
            self.flush(write)?;
        }
        self.page.add(key, value);
        Ok(())
    }

    fn flush(&mut self, write: &mut impl FnMut(&[u8]) -> Result<BlockHandle>) -> Result<()> {
        let mut data = std::mem::replace(&mut self.page, BlockBuilder::new(usize::MAX)).finish();
        data.extend_from_slice(&self.previous.encode());
        self.previous = write(&data)?;
        Ok(())
    }

    fn finish(
        mut self,
        write: &mut impl FnMut(&[u8]) -> Result<BlockHandle>,
    ) -> Result<BlockHandle> {
        self.flush(write)?;
        Ok(self.previous)
    }
}

/// Only the pending separator, the open index leaf and one root page are
/// retained while compaction streams arbitrarily many versions of a key.
struct IndexWriter {
    pending: Option<PendingIndexEntry>,
    leaf: BlockBuilder,
    last_separator: Vec<u8>,
    directory: DirectoryWriter,
}

impl IndexWriter {
    fn new() -> Self {
        Self {
            pending: None,
            leaf: BlockBuilder::new(1),
            last_separator: Vec::new(),
            directory: DirectoryWriter::new(),
        }
    }

    fn emit_pending(
        &mut self,
        next: Option<&[u8]>,
        write: &mut impl FnMut(&[u8]) -> Result<BlockHandle>,
    ) -> Result<()> {
        let Some(mut entry) = self.pending.take() else {
            return Ok(());
        };
        if let Some(next) = next {
            shorten_separator(&mut entry.last_key, next);
        }
        let props: Vec<_> = entry
            .properties
            .iter()
            .map(|(n, d)| (n.as_str(), d.as_slice()))
            .collect();
        let value = encode_index_value_with_props(&entry.handle, &entry.first_key, &props)?;
        if !self.leaf.is_empty()
            && self
                .leaf
                .estimated_size()
                .saturating_add(entry.last_key.len() + value.len() + 32)
                > PARTITION_BYTES
        {
            self.flush_leaf(write)?;
        }
        self.leaf.add(&entry.last_key, &value);
        self.last_separator = entry.last_key;
        Ok(())
    }

    fn flush_leaf(&mut self, write: &mut impl FnMut(&[u8]) -> Result<BlockHandle>) -> Result<()> {
        let data = std::mem::replace(&mut self.leaf, BlockBuilder::new(1)).finish();
        let h = write(&data)?;
        self.directory
            .append(&self.last_separator, &h.encode(), write)
    }

    fn finish(
        mut self,
        write: &mut impl FnMut(&[u8]) -> Result<BlockHandle>,
    ) -> Result<BlockHandle> {
        self.emit_pending(None, write)?;
        if !self.leaf.is_empty() {
            self.flush_leaf(write)?;
        }
        self.directory.finish(write)
    }
}

/// Builds an SST file.
pub struct TableBuilder {
    writer: BufWriter<File>,
    options: TableBuildOptions,

    // Current data block being built
    data_block: BlockBuilder,
    // Index entries produced by flushed data blocks
    index: Option<IndexWriter>,
    // First key of the current (not-yet-flushed) data block
    pending_first_key: Option<Vec<u8>>,
    // One bloom hash per distinct user key. Point lookup probes the user
    // key once, so extra versions must not grow the filter.
    filter_key_hashes: Vec<u64>,
    /// User key of the last hash in `filter_key_hashes`. `None` until the
    /// first point key is hashed. Empty is a real user key, so this is not
    /// a bare `Vec`.
    bloom_user_key: Option<Vec<u8>>,

    // Current file offset
    offset: u64,
    // Last key added (for ordering check)
    last_key: Vec<u8>,

    // Smallest key in the table; last_key supplies the largest at finish.
    smallest_key: Option<Vec<u8>>,
    max_sequence: SequenceNumber,

    // Prefix bloom: collected unique prefixes
    prefix_set: HashSet<Vec<u8>>,

    // Whether any RangeDeletion entry was added
    has_range_deletions: bool,

    /// Buffered range deletion entries (key, value) to write as a separate block.
    range_del_entries: Vec<RangeFragment>,
    range_versions: usize,
    snapshot_stripes: Option<Vec<u64>>,

    /// Cumulative index bytes used to choose output-file cut points.
    index_block_projected: usize,
    /// Encoded endpoint estimate for buffered range deletions.
    range_del_projected: usize,
    carried_projection: usize,

    /// Block property collectors for per-block metadata.
    block_property_collectors: Vec<Box<dyn crate::options::BlockPropertyCollector>>,

    /// Reusable destination buffer for LZ4 and Zstd block compression.
    compress_buf: Vec<u8>,
    /// Reusable Zstd context initialized lazily on the first Zstd block.
    zstd_compressor: Option<zstd::bulk::Compressor<'static>>,

    finished: bool,
}

/// Buffer 256 KiB per SST output stream (~32x fewer `write` syscalls than the
/// 8 KiB stdlib default without inflating subcompaction memory).
const SST_WRITE_BUF_CAPACITY: usize = 256 * 1024;

impl TableBuilder {
    /// Create a new table builder writing to the given path.
    pub fn new(path: &Path, mut options: TableBuildOptions) -> Result<Self> {
        let file = File::create(path).ctx()?;
        let collectors = std::mem::take(&mut options.block_property_collectors);
        Ok(Self {
            writer: BufWriter::with_capacity(SST_WRITE_BUF_CAPACITY, file),
            data_block: BlockBuilder::new(options.block_restart_interval),
            options,
            index: Some(IndexWriter::new()),
            pending_first_key: None,
            filter_key_hashes: Vec::new(),
            bloom_user_key: None,
            offset: 0,
            last_key: Vec::new(),
            smallest_key: None,
            max_sequence: 0,
            prefix_set: HashSet::new(),
            has_range_deletions: false,

            range_del_entries: Vec::new(),
            range_versions: 0,
            snapshot_stripes: None,
            index_block_projected: 0,
            range_del_projected: 0,
            carried_projection: 0,
            block_property_collectors: collectors,
            compress_buf: Vec::new(),
            zstd_compressor: None,
            finished: false,
        })
    }

    /// Projected encoded size of the index block, including the entry the
    /// currently open data block will contribute when flushed.
    fn projected_index_size(&self) -> usize {
        let mut size = self.index_block_projected;
        if !self.data_block.is_empty() {
            size += self.pending_first_key.as_ref().map_or(0, |k| k.len())
                + self.last_key.len()
                + META_ENTRY_OVERHEAD;
        }
        size
    }

    #[inline]
    fn prefix_bloom_bits(&self) -> u32 {
        self.options
            .prefix_bloom_bits_per_key
            .unwrap_or(self.options.bloom_bits_per_key)
    }

    fn projected_filter_size(&self) -> usize {
        if self.options.bloom_bits_per_key == 0 || self.filter_key_hashes.is_empty() {
            return 0;
        }
        BloomFilter::projected_size(
            self.filter_key_hashes.len(),
            self.options.bloom_bits_per_key,
        )
    }

    fn projected_prefix_filter_size(&self) -> usize {
        let prefix_bits = self.prefix_bloom_bits();
        if self.options.prefix_len == 0 || prefix_bits == 0 || self.prefix_set.is_empty() {
            return 0;
        }
        BloomFilter::projected_size(self.prefix_set.len(), prefix_bits)
    }

    fn next_filter_exceeds_hard_limit(&self) -> bool {
        if self.options.bloom_bits_per_key > 0
            && !self.filter_key_hashes.is_empty()
            && BloomFilter::projected_size(
                self.filter_key_hashes.len().saturating_add(1),
                self.options.bloom_bits_per_key,
            ) > META_BLOCK_HARD_LIMIT
        {
            return true;
        }
        let prefix_bits = self.prefix_bloom_bits();
        self.options.prefix_len > 0
            && prefix_bits > 0
            && !self.prefix_set.is_empty()
            && BloomFilter::projected_size(self.prefix_set.len().saturating_add(1), prefix_bits)
                > META_BLOCK_HARD_LIMIT
    }

    /// Largest projected single-block metadata structure. Callers that can
    /// split output across multiple SSTs should cut the current file once this
    /// reaches `META_BLOCK_SPLIT_THRESHOLD`.
    pub(crate) fn projected_meta_size(&self) -> usize {
        let size = self
            .projected_index_size()
            .max(
                self.range_projection()
                    .saturating_sub(self.carried_projection),
            )
            .max(self.projected_filter_size())
            .max(self.projected_prefix_filter_size());
        if self.next_filter_exceeds_hard_limit() {
            size.max(META_BLOCK_SPLIT_THRESHOLD)
        } else {
            size
        }
    }

    fn range_projection(&self) -> usize {
        self.range_del_projected.saturating_mul(2).saturating_add(
            self.range_del_entries
                .len()
                .saturating_mul(16)
                .saturating_mul(
                    self.snapshot_stripes
                        .as_ref()
                        .map_or(self.range_versions, |s| s.len().saturating_add(1))
                        .min(self.range_versions),
                ),
        )
    }

    #[cfg(test)]
    pub(crate) fn add_point(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        self.add(InternalKey::new(key, 1, ValueType::Value).as_bytes(), value)
    }

    /// Add a key-value pair. Must be called in sorted key order.
    pub fn add(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        assert!(!self.finished);
        let (user_key_for_bloom, seq, vt) = decode_internal_key(key).ctx()?;
        assert!(
            self.last_key.is_empty() || compare_internal_key(key, &self.last_key).is_gt(),
            "keys must be added in order"
        );

        if self.smallest_key.is_none()
            || (self.last_key.is_empty()
                && self
                    .smallest_key
                    .as_deref()
                    .is_some_and(|s| compare_internal_key(key, s).is_lt()))
        {
            self.smallest_key = Some(key.to_vec());
        }

        // A single entry (plus block framing) must fit within the reader's maximum
        // decompressed block size; otherwise the SST written here would be rejected
        // as unreadable on read-back, silently losing the data. Reject oversized
        // entries up front. The margin covers the block's restart array and the
        // per-entry varint headers.
        const BLOCK_FRAMING_MARGIN: usize = 64;
        let entry_size = key.len().saturating_add(value.len());
        if entry_size > MAX_DECOMPRESSED_BLOCK_SIZE - BLOCK_FRAMING_MARGIN {
            return Err(Error::invalid_argument(format!(
                "entry size {} exceeds maximum block size {}",
                entry_size,
                MAX_DECOMPRESSED_BLOCK_SIZE - BLOCK_FRAMING_MARGIN
            )));
        }

        if vt == ValueType::RangeDeletion {
            if user_key(key) >= value {
                return Err(Error::invalid_argument("empty range tombstone"));
            }
            self.add_range_fragment(RangeFragment::new(
                user_key_for_bloom,
                value,
                Arc::from([seq]),
            ));
            self.last_key.clear();
            self.last_key.extend_from_slice(key);
            return Ok(());
        }

        // Versions of one user key are adjacent. Lookup hashes that user key
        // once, so only the first version contributes a filter entry.
        if self.options.bloom_bits_per_key > 0
            && self.bloom_user_key.as_deref() != Some(user_key_for_bloom)
        {
            BloomFilter::checked_size(
                "bloom filter",
                self.filter_key_hashes.len().saturating_add(1),
                self.options.bloom_bits_per_key,
            )?;
            let hash = bloom_hash(user_key_for_bloom);
            self.filter_key_hashes.push(hash);
            let bloom_key = self.bloom_user_key.get_or_insert_with(Vec::new);
            bloom_key.clear();
            bloom_key.extend_from_slice(user_key_for_bloom);
        }

        // Collect prefix for prefix bloom filter when prefix Bloom is enabled.
        let prefix_bits = self.prefix_bloom_bits();
        if prefix_bits > 0
            && self.options.prefix_len > 0
            && user_key_for_bloom.len() >= self.options.prefix_len
        {
            let prefix = &user_key_for_bloom[..self.options.prefix_len];
            if !self.prefix_set.contains(prefix) {
                BloomFilter::checked_size(
                    "prefix bloom filter",
                    self.prefix_set.len().saturating_add(1),
                    prefix_bits,
                )?;
                self.prefix_set.insert(prefix.to_vec());
            }
        }

        // Flush the current data block if it is full, or if adding this entry would
        // push the finished block past the reader's maximum decompressed block size.
        let projected = self.data_block.estimated_size().saturating_add(entry_size);
        if !self.data_block.is_empty()
            && (self.data_block.estimated_size() >= self.options.block_size
                || projected > MAX_DECOMPRESSED_BLOCK_SIZE - BLOCK_FRAMING_MARGIN)
        {
            self.flush_data_block().ctx()?;
        }

        // Record first key of a new data block
        if self.data_block.is_empty() {
            self.pending_first_key = Some(key.to_vec());
        }

        self.data_block.add(key, value);
        self.max_sequence = self.max_sequence.max(seq);
        self.last_key.clear();
        self.last_key.extend_from_slice(key);

        for collector in &mut self.block_property_collectors {
            collector.add(user_key_for_bloom);
        }

        Ok(())
    }

    /// Finish building the table. Must be called when done adding entries.
    pub fn finish(mut self) -> Result<TableBuildResult> {
        // Flush remaining data block
        if !self.data_block.is_empty() {
            self.flush_data_block().ctx()?;
        }

        // Write meta block (bloom filter)
        let filter_handle = self.write_filter_block().ctx()?;

        // Write prefix filter block
        let prefix_filter_handle = self.write_prefix_filter_block().ctx()?;

        // Compaction buffers packed ranges separately from point rows. Fold in
        // their extreme keys once, irrespective of how many stripes they carry.
        let by_first = |a: &&RangeFragment, b: &&RangeFragment| {
            a.begin
                .cmp(&b.begin)
                .then_with(|| b.sequences().last().cmp(&a.sequences().last()))
        };
        let by_last = |a: &&RangeFragment, b: &&RangeFragment| {
            a.begin
                .cmp(&b.begin)
                .then_with(|| b.sequences()[0].cmp(&a.sequences()[0]))
        };
        if let Some(first) = self.range_del_entries.iter().min_by(by_first) {
            let key = InternalKey::new(
                &first.begin,
                *first.sequences().last().unwrap(),
                ValueType::RangeDeletion,
            );
            if self
                .smallest_key
                .as_deref()
                .is_none_or(|k| compare_internal_key(key.as_bytes(), k).is_lt())
            {
                self.smallest_key = Some(key.into_bytes());
            }
        }
        if let Some(last) = self.range_del_entries.iter().max_by(by_last) {
            let key = InternalKey::new(&last.begin, last.sequences()[0], ValueType::RangeDeletion);
            if self.last_key.is_empty()
                || compare_internal_key(key.as_bytes(), &self.last_key).is_gt()
            {
                self.last_key = key.into_bytes();
            }
        }
        let (range_del_handle, range_tombstones) = self.write_range_del_block().ctx()?;

        // Write meta index block
        let metaindex_handle = self
            .write_metaindex_block(&filter_handle, &prefix_filter_handle, &range_del_handle)
            .ctx()?;

        // Write index block
        let index_handle = self.write_index_block().ctx()?;

        // Write footer
        let footer = encode_footer(&metaindex_handle, &index_handle);
        self.writer.write_all(&footer).ctx()?;
        self.offset += FOOTER_SIZE as u64;

        self.writer.flush().ctx()?;
        self.writer.get_ref().sync_all().ctx()?;
        self.finished = true;

        let largest_key = self.smallest_key.as_ref().map(|_| self.last_key);
        Ok(TableBuildResult {
            file_size: self.offset,
            smallest_key: self.smallest_key,
            largest_key,
            has_range_deletions: self.has_range_deletions,
            range_tombstones,
        })
    }

    fn flush_data_block(&mut self) -> Result<()> {
        let last_key = self.data_block.last_key().to_vec();
        let first_key = self.pending_first_key.take().unwrap_or_default();

        // Collect block properties from all collectors, then reset for next block
        let props: Vec<(String, Vec<u8>)> = self
            .block_property_collectors
            .iter_mut()
            .map(|c| (c.name().to_string(), c.finish_block()))
            .collect();

        // Properties are only known now, including for the final data block.
        // Validate them before publishing a block to the pending index.
        let props_size = props.iter().fold(0usize, |total, (name, data)| {
            total
                .saturating_add(name.len())
                .saturating_add(data.len())
                .saturating_add(8)
        });
        let projected_index = self
            .index_block_projected
            .saturating_add(last_key.len())
            .saturating_add(first_key.len())
            .saturating_add(props_size)
            .saturating_add(META_ENTRY_OVERHEAD);
        if projected_index - self.index_block_projected > META_BLOCK_HARD_LIMIT {
            return Err(Error::invalid_argument(format!(
                "index block size {} including block properties exceeds maximum {}",
                projected_index, META_BLOCK_HARD_LIMIT
            )));
        }

        // Finalize `data_block` in place and write without reallocating its Vecs.
        let mut data_block = std::mem::replace(&mut self.data_block, BlockBuilder::new(1));
        let handle = {
            let block_data = data_block.finish_in_place();
            self.write_raw_block(block_data)
        };
        data_block.reset();
        self.data_block = data_block;
        let handle = handle.ctx()?;

        self.index_block_projected = projected_index;
        let mut index = self.index.take().expect("active index");
        index.emit_pending(Some(first_key.as_slice()), &mut |data| {
            self.write_raw_block(data)
        })?;
        index.pending = Some(PendingIndexEntry {
            last_key,
            handle,
            first_key,
            properties: props,
        });
        self.index = Some(index);

        Ok(())
    }

    fn write_raw_block(&mut self, data: &[u8]) -> Result<BlockHandle> {
        if data.len() > MAX_DECOMPRESSED_BLOCK_SIZE {
            return Err(Error::invalid_argument(format!(
                "encoded block size {} exceeds maximum readable block size {}",
                data.len(),
                MAX_DECOMPRESSED_BLOCK_SIZE
            )));
        }
        let compression_type = match self.options.compression {
            CompressionType::Lz4 => {
                let max_out = 4 + lz4_flex::block::get_maximum_output_size(data.len());
                self.compress_buf.resize(max_out, 0);
                self.compress_buf[..4].copy_from_slice(&(data.len() as u32).to_le_bytes());
                match lz4_flex::block::compress_into(data, &mut self.compress_buf[4..]) {
                    Ok(written) if 4 + written < data.len() => {
                        self.compress_buf.truncate(4 + written);
                        CompressionType::Lz4
                    }
                    _ => CompressionType::None,
                }
            }
            CompressionType::Zstd => {
                if self.zstd_compressor.is_none() {
                    self.zstd_compressor = zstd::bulk::Compressor::new(3).ok();
                }
                self.compress_buf.clear();
                self.compress_buf
                    .reserve(zstd::zstd_safe::compress_bound(data.len()));
                if let Some(ref mut comp) = self.zstd_compressor
                    && comp
                        .compress_to_buffer(data, &mut self.compress_buf)
                        .is_ok()
                    && self.compress_buf.len() < data.len()
                {
                    CompressionType::Zstd
                } else {
                    CompressionType::None
                }
            }
            CompressionType::None => CompressionType::None,
        };
        let block_data: &[u8] = if compression_type == CompressionType::None {
            data
        } else {
            &self.compress_buf
        };

        let handle = BlockHandle::new(self.offset, block_data.len() as u64);

        self.writer.write_all(block_data).ctx()?;

        // Write block trailer: compression_type(1) + crc32(4)
        let mut hasher = crc32fast::Hasher::new();
        hasher.update(block_data);
        hasher.update(&[compression_type as u8]);
        let crc = hasher.finalize();

        self.writer.write_all(&[compression_type as u8]).ctx()?;
        self.writer.write_all(&crc.to_le_bytes()).ctx()?;

        self.offset += block_data.len() as u64 + BLOCK_TRAILER_SIZE as u64;

        Ok(handle)
    }

    fn write_filter_block(&mut self) -> Result<BlockHandle> {
        if self.options.bloom_bits_per_key == 0 || self.filter_key_hashes.is_empty() {
            // No filter
            return Ok(BlockHandle::default());
        }

        let bf = BloomFilter::new(self.options.bloom_bits_per_key);
        let filter_data = bf
            .create_filter_from_hashes(&self.filter_key_hashes)
            .ctx()?;

        self.write_raw_block(&filter_data).ctx()
    }

    fn write_prefix_filter_block(&mut self) -> Result<BlockHandle> {
        let prefix_bits = self.prefix_bloom_bits();
        if self.options.prefix_len == 0 || prefix_bits == 0 || self.prefix_set.is_empty() {
            return Ok(BlockHandle::default());
        }

        BloomFilter::checked_size("prefix bloom filter", self.prefix_set.len(), prefix_bits)?;
        let mut prefixes: Vec<&[u8]> = self.prefix_set.iter().map(|p| p.as_slice()).collect();
        prefixes.sort();

        let bf = BloomFilter::new(prefix_bits);
        let filter_data = bf.create_filter(&prefixes).ctx()?;

        self.write_raw_block(&filter_data).ctx()
    }

    /// Clip all crossing tombstones at an output split. Right fragments are
    /// carried into the next output, even when no point survives at the split.
    pub(crate) fn clip_at(&mut self, split: &[u8]) -> Vec<RangeFragment> {
        let mut carry = Vec::new();
        let mut boundary = None;
        for left in &mut self.range_del_entries {
            if left.end.as_ref() > split {
                let split = boundary.get_or_insert_with(|| Arc::<[u8]>::from(split));
                let mut right = left.clone();
                right.begin = split.clone();
                left.end = split.clone();
                carry.push(right);
            }
        }
        carry
    }

    pub(crate) fn start_output(&mut self, snapshots: &[u64], carry: Vec<RangeFragment>) {
        self.snapshot_stripes = Some(snapshots.to_vec());
        for range in carry {
            self.add_range_fragment(range);
        }
        self.carried_projection = self.range_projection();
    }

    pub(crate) fn add_range_fragment(&mut self, range: RangeFragment) {
        let seqs = range.sequences();
        // Every nonempty fragment retains its newest stripe, including after
        // snapshot pruning and when a file split carries it into another SST.
        self.max_sequence = self.max_sequence.max(*seqs.last().unwrap());
        self.has_range_deletions = true;
        self.range_versions = self.range_versions.saturating_add(seqs.len());
        self.range_del_projected = self
            .range_del_projected
            .saturating_add(range.begin.len() + range.end.len() + META_ENTRY_OVERHEAD);
        self.range_del_entries.push(range);
    }

    fn write_range_del_block(&mut self) -> Result<(BlockHandle, RangeExtents)> {
        let mut extents = Vec::new();
        if self.range_del_entries.is_empty() {
            return Ok((BlockHandle::default(), extents));
        }
        let ranges = std::mem::take(&mut self.range_del_entries);
        let mut events = Vec::with_capacity(ranges.len() * 2);
        for range in &ranges {
            events.push((range.begin.as_ref(), range.sequences(), true));
            events.push((range.end.as_ref(), range.sequences(), false));
        }
        events.sort_unstable_by(|a, b| a.0.cmp(b.0));
        let mut live = BTreeMap::<u64, usize>::new();
        let mut root = DirectoryWriter::new();
        let mut leaf = BlockBuilder::new(16);
        let mut first_begin = None;
        let mut prev = events[0].0;
        let mut leaf_end = prev;
        let mut payload = Vec::new();
        let mut idx = 0;
        while idx < events.len() {
            let boundary = events[idx].0;
            if prev < boundary && !live.is_empty() {
                payload.clear();
                payload.extend_from_slice(&(prev.len() as u32).to_le_bytes());
                payload.extend_from_slice(prev);
                if let Some(snapshots) = &self.snapshot_stripes {
                    let mut last = None;
                    for upper in snapshots
                        .iter()
                        .copied()
                        .chain(std::iter::once(MAX_SEQUENCE_NUMBER))
                    {
                        if let Some((&seq, _)) = live.range(..=upper).next_back()
                            && last != Some(seq)
                        {
                            payload.extend_from_slice(&seq.to_le_bytes());
                            last = Some(seq);
                        }
                    }
                } else {
                    for seq in live.keys() {
                        payload.extend_from_slice(&seq.to_le_bytes());
                    }
                }
                if !leaf.is_empty()
                    && leaf
                        .estimated_size()
                        .saturating_add(payload.len() + boundary.len() + 32)
                        > PARTITION_BYTES
                {
                    let handle = self.write_raw_block(
                        &std::mem::replace(&mut leaf, BlockBuilder::new(16)).finish(),
                    )?;
                    let begin: &[u8] = first_begin.take().unwrap();
                    extents.push((begin.to_vec(), leaf_end.to_vec()));
                    root.append(leaf_end, &range_partition_value(handle, begin), &mut |d| {
                        self.write_raw_block(d)
                    })?;
                }
                first_begin.get_or_insert(prev);
                leaf.add(boundary, &payload);
                leaf_end = boundary;
            }
            while idx < events.len() && events[idx].0 == boundary {
                let (_, stripes, add) = events[idx];
                for seq in stripes {
                    if add {
                        *live.entry(*seq).or_default() += 1;
                    } else {
                        let count = live.get_mut(seq).expect("end event follows its start");
                        *count -= 1;
                        if *count == 0 {
                            live.remove(seq);
                        }
                    }
                }
                idx += 1;
            }
            prev = boundary;
        }
        if let Some(begin) = first_begin {
            extents.push((begin.to_vec(), leaf_end.to_vec()));
            let handle = self.write_raw_block(&leaf.finish())?;
            root.append(leaf_end, &range_partition_value(handle, begin), &mut |d| {
                self.write_raw_block(d)
            })?;
        }
        Ok((root.finish(&mut |d| self.write_raw_block(d))?, extents))
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

        if filter_handle.size > 0 || prefix_filter_handle.size > 0 {
            builder.add(FILTER_FORMAT_NAME.as_bytes(), &[FILTER_FORMAT_BLOCKED_64B]);
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
        builder.add(
            MAX_SEQUENCE_NAME.as_bytes(),
            &self.max_sequence.to_le_bytes(),
        );

        let data = builder.finish();
        self.write_raw_block(&data).ctx()
    }

    fn write_index_block(&mut self) -> Result<BlockHandle> {
        self.index
            .take()
            .expect("active index")
            .finish(&mut |d| self.write_raw_block(d))
    }
}

/// Result of building a table.
#[derive(Debug)]
pub struct TableBuildResult {
    pub file_size: u64,
    pub smallest_key: Option<Vec<u8>>,
    pub largest_key: Option<Vec<u8>>,
    /// Whether any range deletion entry was written to this table.
    pub has_range_deletions: bool,
    /// Range tombstone user-key extents `[begin, end)` written to this table.
    /// Non-empty exactly when `has_range_deletions` is true.
    pub range_tombstones: Vec<(Vec<u8>, Vec<u8>)>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::ErrorKind;
    use crate::options::BlockPropertyCollector;
    use crate::sst::table_reader::TableReader;

    struct LargePropertyCollector(String);

    impl BlockPropertyCollector for LargePropertyCollector {
        fn add(&mut self, _key: &[u8]) {}

        fn finish_block(&mut self) -> Vec<u8> {
            vec![7; u16::MAX as usize]
        }

        fn name(&self) -> &str {
            &self.0
        }
    }

    #[test]
    fn chained_directory_pages_preserve_index_order() {
        use crate::types::InternalKey;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("chain.sst");
        let mut b = TableBuilder::new(
            &path,
            TableBuildOptions {
                block_size: 1,
                ..Default::default()
            },
        )
        .unwrap();
        // Each separator of a 64 KiB user key owns one leaf. A tiny root
        // page target exercises page rollover with a small fixture.
        b.index.as_mut().unwrap().directory.target = 1;
        let mut key = vec![7; 64 * 1024];
        for i in 1..=6_u8 {
            key[0] = i;
            b.add(
                InternalKey::new(&key, i.into(), ValueType::Value).as_bytes(),
                &[i],
            )
            .unwrap();
        }
        b.finish().unwrap();
        let r = TableReader::open(&path).unwrap();
        for i in 1..=6_u8 {
            key[0] = i;
            assert_eq!(r.get_internal(&key, i.into()).unwrap(), Some(Some(vec![i])));
        }
        // The footer is unchecksummed. Even a valid earlier directory page
        // cannot replace the head and silently truncate the key space.
        let mut bytes = std::fs::read(&path).unwrap();
        let footer_at = bytes.len() - FOOTER_SIZE;
        let (meta, head) = decode_footer(bytes[footer_at..].try_into().unwrap()).unwrap();
        let end = (head.offset + head.size) as usize;
        let previous = BlockHandle::decode(&bytes[end - 16..end]).unwrap();
        assert_ne!(previous, BlockHandle::default());
        bytes[footer_at..].copy_from_slice(&encode_footer(&meta, &previous));
        std::fs::write(&path, bytes).unwrap();
        let error = TableReader::open(&path)
            .err()
            .expect("truncated directory chain");
        assert_eq!(error.kind(), ErrorKind::Corruption);
        assert!(error.to_string().contains("index directory head"));
    }

    #[test]
    fn recovery_rejects_invalid_internal_keys_and_unordered_index_roots() {
        use crate::{DB, DbOptions};
        let dir = tempfile::tempdir().unwrap();
        let db = DB::open(DbOptions::default(), dir.path()).unwrap();
        db.put(b"key", b"value").unwrap();
        db.close().unwrap();
        let path = std::fs::read_dir(dir.path())
            .unwrap()
            .map(|p| p.unwrap().path())
            .find(|p| p.extension().is_some_and(|e| e == "sst"))
            .unwrap();
        let original = std::fs::read(&path).unwrap();
        let (meta, head) =
            decode_footer(original[original.len() - FOOTER_SIZE..].try_into().unwrap()).unwrap();
        let valid = InternalKey::new(b"v", 1, ValueType::Value).into_bytes();
        let invalid_type = vec![0; 8];
        let earlier = InternalKey::new(b"a", 1, ValueType::Value).into_bytes();
        for (keys, reason) in [
            (vec![vec![0; 7]], "internal key too short"),
            (vec![invalid_type], "invalid value_type"),
            (
                vec![valid.clone(), valid.clone()],
                "unordered SST index partitions",
            ),
            (vec![valid, earlier], "unordered SST index partitions"),
        ] {
            let mut image = original[..head.offset as usize].to_vec();
            let mut root = BlockBuilder::new(1);
            for k in keys {
                root.add(&k, &BlockHandle::new(0, 1).encode());
            }
            let mut payload = root.finish();
            payload.extend_from_slice(&BlockHandle::default().encode());
            let bad = BlockHandle::new(head.offset, payload.len() as u64);
            payload.push(CompressionType::None as u8);
            let crc = crc32fast::hash(&payload);
            image.extend_from_slice(&payload);
            image.extend_from_slice(&crc.to_le_bytes());
            image.extend_from_slice(&encode_footer(&meta, &bad));
            std::fs::write(&path, image).unwrap();
            for result in [
                DB::open_read_only(dir.path()),
                DB::open(DbOptions::default(), dir.path()),
            ] {
                let error = result.err().expect("invalid root accepted during recovery");
                assert_eq!(error.kind(), ErrorKind::Corruption);
                assert!(error.to_string().contains(reason), "{error}");
            }
        }
    }

    #[test]
    fn compact_same_maximum_key_across_generations_with_live_snapshots() {
        use crate::{DB, DbOptions};
        let directory = tempfile::tempdir().unwrap();
        let opts = DbOptions::default()
            .with_num_levels(2)
            .with_l0_compaction_trigger(12)
            .with_l0_slowdown_trigger(16)
            .with_l0_stop_trigger(20)
            .with_pin_l0_first_block_and_warm_index(false)
            .with_block_cache_capacity(0);
        let db = DB::open(opts, directory.path()).unwrap();
        let key = vec![b'm'; MAX_USER_KEY_SIZE];
        let mut snapshots = Vec::new();
        for i in 0..5 {
            db.put(&key, &[i]).unwrap();
            snapshots.push(db.snapshot());
            db.flush().unwrap();
        }
        assert_eq!(db.get_property("num-files-at-level0").as_deref(), Some("5"));
        db.compact().unwrap();
        assert_eq!(db.get_property("num-files-at-level1").as_deref(), Some("1"));
        for (i, snapshot) in snapshots.iter().enumerate() {
            assert_eq!(snapshot.get(&key).unwrap(), Some(vec![i as u8]));
        }
    }

    #[test]
    fn flush_clips_a_crossing_range_with_many_live_snapshot_stripes() {
        use crate::{DB, DbOptions};
        let directory = tempfile::tempdir().unwrap();
        let db = DB::open(DbOptions::default(), directory.path()).unwrap();
        let mut snapshots = Vec::new();
        for n in 0..1_500_u32 {
            db.put(b"m", &n.to_be_bytes()).unwrap();
            snapshots.push(db.snapshot());
            db.delete_range(b"a", b"z").unwrap();
            snapshots.push(db.snapshot());
        }
        db.flush().unwrap();
        let mut bounds = Vec::new();
        for e in std::fs::read_dir(directory.path()).unwrap() {
            let p = e.unwrap().path();
            if p.extension().is_some_and(|ext| ext == "sst") {
                let reader = TableReader::open(&p).unwrap();
                bounds.extend(
                    reader
                        .range_partition_extents()
                        .map(|(a, b)| (a.to_vec(), b.to_vec())),
                );
            }
        }
        bounds.sort();
        assert_eq!(
            bounds,
            vec![
                (b"a".to_vec(), b"m".to_vec()),
                (b"m".to_vec(), b"z".to_vec())
            ]
        );
        for (n, pair) in snapshots.as_chunks::<2>().0.iter().enumerate() {
            assert_eq!(
                pair[0].get(b"m").unwrap().unwrap(),
                (n as u32).to_be_bytes()
            );
            assert!(pair[1].get(b"m").unwrap().is_none());
        }
        db.compact().unwrap();
        for (n, pair) in snapshots.as_chunks::<2>().0.iter().enumerate().step_by(17) {
            assert_eq!(
                pair[0].get(b"m").unwrap().unwrap(),
                (n as u32).to_be_bytes()
            );
            assert!(pair[1].get(b"m").unwrap().is_none());
        }
    }

    #[test]
    fn uncompressed_block_write_does_not_copy_input() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("raw.sst");
        let mut builder = TableBuilder::new(&path, TableBuildOptions::default()).unwrap();
        let data = vec![42; 16 * 1024];
        let (handle, allocated) =
            crate::test_alloc::count_allocated(|| builder.write_raw_block(&data).unwrap());
        assert_eq!(allocated, 0, "raw write allocated {allocated} bytes");
        builder.writer.flush().unwrap();
        let bytes = std::fs::read(path).unwrap();
        assert_eq!(handle.size, data.len() as u64);
        assert_eq!(&bytes[..data.len()], data);
        assert_eq!(bytes[data.len()], CompressionType::None as u8);
        let crc = crc32fast::hash(&bytes[..data.len() + 1]);
        assert_eq!(&bytes[data.len() + 1..], crc.to_le_bytes());
    }

    #[test]
    fn builder_reuses_key_storage() {
        for bloom_bits_per_key in [0, 10] {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("keys.sst");
            let mut builder = TableBuilder::new(
                &path,
                TableBuildOptions {
                    block_size: 4 * 1024 * 1024,
                    bloom_bits_per_key,
                    ..Default::default()
                },
            )
            .unwrap();
            let mut key = InternalKey::new(&[b'a'; 1024], 1, ValueType::Value).into_bytes();
            let (_, allocated) = crate::test_alloc::count_allocated(|| {
                for i in 0..1000u64 {
                    key[1016..1024].copy_from_slice(&i.to_be_bytes());
                    builder.add(&key, b"v").unwrap();
                }
            });
            eprintln!(
                "1000 1KiB builder keys (bloom_bits_per_key={bloom_bits_per_key}) allocated {allocated} bytes"
            );
            assert!(
                allocated < 512 * 1024,
                "bloom_bits_per_key={bloom_bits_per_key} allocated {allocated} bytes"
            );
            let result = builder.finish().unwrap();
            assert_eq!(result.largest_key.as_deref(), Some(key.as_slice()));
            assert_eq!(
                TableReader::open(&path)
                    .unwrap()
                    .get(user_key(&key))
                    .unwrap(),
                Some(b"v".to_vec())
            );
        }
    }

    #[test]
    fn empty_user_key_has_present_table_bounds() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("empty-key.sst");
        let mut builder = TableBuilder::new(&path, TableBuildOptions::default()).unwrap();
        builder.add_point(b"", b"value").unwrap();
        let result = builder.finish().unwrap();
        assert_eq!(result.smallest_key.as_deref().map(user_key), Some(&[][..]));
        assert_eq!(result.largest_key.as_deref().map(user_key), Some(&[][..]));
        assert_eq!(
            TableReader::open(&path).unwrap().get(b"").unwrap(),
            Some(b"value".to_vec())
        );
    }

    #[test]
    fn final_collector_properties_respect_index_limit() {
        for count in [512, 1024] {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("properties.sst");
            let options = TableBuildOptions {
                compression: CompressionType::Lz4,
                block_property_collectors: (0..count)
                    .map(|i| {
                        Box::new(LargePropertyCollector(format!("p{i:04}")))
                            as Box<dyn BlockPropertyCollector>
                    })
                    .collect(),
                ..Default::default()
            };
            let mut builder = TableBuilder::new(&path, options).unwrap();
            builder.add_point(b"key", b"value").unwrap();
            // Every property fits its u16 fields, but 1024 together exceed
            // the reader's decompressed index limit, even when compressed.
            if count == 1024 {
                assert_eq!(
                    builder.finish().unwrap_err().kind(),
                    ErrorKind::InvalidArgument
                );
            } else {
                builder.finish().unwrap();
                let reader = TableReader::open(&path).unwrap();
                assert_eq!(reader.get(b"key").unwrap(), Some(b"value".to_vec()));
                assert_eq!(reader.iter().unwrap().len(), 1);
            }
        }
    }

    #[test]
    fn test_build_table() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.sst");

        let mut builder = TableBuilder::new(&path, TableBuildOptions::default()).unwrap();
        for i in 0..100 {
            let key = format!("key_{:06}", i);
            let val = format!("value_{}", i);
            builder.add_point(key.as_bytes(), val.as_bytes()).unwrap();
        }
        let result = builder.finish().unwrap();

        assert_eq!(TableReader::open(&path).unwrap().iter().unwrap().len(), 100);
        assert!(result.file_size > 0);
        assert_eq!(
            result.smallest_key.as_deref().map(user_key),
            Some(b"key_000000".as_slice())
        );
        assert_eq!(
            result.largest_key.as_deref().map(user_key),
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
        builder.add_point(b"only_key", b"only_value").unwrap();
        let result = builder.finish().unwrap();

        assert_eq!(TableReader::open(&path).unwrap().iter().unwrap().len(), 1);
        assert_eq!(
            result.smallest_key.as_deref().map(user_key),
            Some(b"only_key".as_slice())
        );
        assert_eq!(
            result.largest_key.as_deref().map(user_key),
            Some(b"only_key".as_slice())
        );

        let reader = TableReader::open(&path).unwrap();
        let entries = reader.iter().unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(user_key(&entries[0].0), b"only_key");
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
            builder.add_point(key.as_bytes(), val.as_bytes()).unwrap();
        }
        builder.finish().unwrap();
        assert_eq!(TableReader::open(&path).unwrap().iter().unwrap().len(), 500);

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
            assert_eq!(user_key(&entry.0), key.as_bytes());
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
        use crate::types::{InternalKey, InternalKeyRef, ValueType};

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("range_del.sst");

        let opts = TableBuildOptions::default();
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
        // Point-entry iteration excludes the 2 range-deletion entries (5 added total).
        assert_eq!(TableReader::open(&path).unwrap().iter().unwrap().len(), 3);
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

    #[test]
    fn test_filter_projection_drives_meta_split_threshold() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("filter_projection.sst");
        let bits_per_key = ((META_BLOCK_SPLIT_THRESHOLD - 1) * 8) as u32;
        let mut builder = TableBuilder::new(
            &path,
            TableBuildOptions {
                bloom_bits_per_key: bits_per_key,
                ..Default::default()
            },
        )
        .unwrap();

        builder.add_point(b"k", b"v").unwrap();

        assert_eq!(builder.projected_filter_size(), META_BLOCK_SPLIT_THRESHOLD);
        assert!(builder.projected_meta_size() >= META_BLOCK_SPLIT_THRESHOLD);
    }

    #[test]
    fn test_internal_keys_reject_malformed_entries() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("bad_ikey.sst");
        let mut builder = TableBuilder::new(&path, TableBuildOptions::default()).unwrap();

        let err = builder.add(b"short", b"v").unwrap_err();
        assert!(err.to_string().contains("too short"));

        let packed = (1u64 << 8) | 0xAB;
        let mut bad = b"k".to_vec();
        bad.extend_from_slice(&(!packed).to_be_bytes());
        let err = builder.add(&bad, b"v").unwrap_err();
        assert!(err.to_string().contains("invalid value_type"));
    }

    /// One user key is hashed once. A bits-per-key setting that open accepts
    /// for a single key must still accept every version of that key. Two
    /// distinct keys at this size still exceed the filter block limit.
    #[test]
    fn repeated_user_key_versions_share_one_bloom_entry() {
        use crate::sst::filter::BloomFilter;
        use crate::types::{InternalKey, MAX_SEQUENCE_NUMBER, ValueType};

        // One key fits. Two distinct keys are one byte over the block limit.
        // `(HARD - 1) * 4` lands exactly on the limit for two keys, which is
        // still accepted (`projected > limit` is the rejection).
        let bits_per_key = ((META_BLOCK_HARD_LIMIT - 1) * 4 + 8) as u32;
        assert!(BloomFilter::checked_size("bloom filter", 1, bits_per_key).is_ok());
        assert!(BloomFilter::checked_size("bloom filter", 2, bits_per_key).is_err());

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("same_user_key.sst");
        let mut builder = TableBuilder::new(
            &path,
            TableBuildOptions {
                bloom_bits_per_key: bits_per_key,
                ..Default::default()
            },
        )
        .unwrap();
        builder
            .add(
                InternalKey::new(b"k", MAX_SEQUENCE_NUMBER, ValueType::Value).as_bytes(),
                b"new",
            )
            .unwrap();
        builder
            .add(
                InternalKey::new(b"k", 1, ValueType::Value).as_bytes(),
                b"old",
            )
            .unwrap();
        assert_eq!(builder.filter_key_hashes.len(), 1);
        builder.finish().unwrap();

        let reader = TableReader::open(&path).unwrap();
        let found = reader
            .get_internal(b"k", MAX_SEQUENCE_NUMBER)
            .unwrap()
            .unwrap();
        assert_eq!(found, Some(b"new".to_vec()));

        let path = dir.path().join("two_user_keys.sst");
        let mut builder = TableBuilder::new(
            &path,
            TableBuildOptions {
                bloom_bits_per_key: bits_per_key,
                ..Default::default()
            },
        )
        .unwrap();
        builder
            .add(
                InternalKey::new(b"a", 1, ValueType::Value).as_bytes(),
                b"va",
            )
            .unwrap();
        let err = builder
            .add(
                InternalKey::new(b"b", 1, ValueType::Value).as_bytes(),
                b"vb",
            )
            .unwrap_err();
        assert_eq!(err.kind(), ErrorKind::InvalidArgument);
    }

    #[test]
    fn test_bloom_filter_hard_limit_is_checked_before_allocation() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("oversized_filter.sst");
        let bits_per_key = (META_BLOCK_HARD_LIMIT * 8 + 1) as u32;
        let mut builder = TableBuilder::new(
            &path,
            TableBuildOptions {
                bloom_bits_per_key: bits_per_key,
                ..Default::default()
            },
        )
        .unwrap();

        let err = builder.add_point(b"k", b"v").unwrap_err();
        assert_eq!(err.kind(), ErrorKind::InvalidArgument);
        assert!(err.message().contains("bloom filter block size"));
        assert!(builder.filter_key_hashes.is_empty());
    }

    #[test]
    fn test_prefix_filter_hard_limit_is_checked_before_allocation() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("oversized_prefix_filter.sst");
        let bits_per_key = (META_BLOCK_HARD_LIMIT * 8 + 1) as u32;
        let mut builder = TableBuilder::new(
            &path,
            TableBuildOptions {
                bloom_bits_per_key: bits_per_key,
                prefix_len: 1,
                ..Default::default()
            },
        )
        .unwrap();
        builder.prefix_set.insert(b"k".to_vec());

        let err = builder.write_prefix_filter_block().unwrap_err();
        assert_eq!(err.kind(), ErrorKind::InvalidArgument);
        assert!(err.message().contains("prefix bloom filter block size"));
    }

    #[test]
    fn test_prefix_set_empty_when_bloom_disabled() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("no_bloom_prefix.sst");
        let mut builder = TableBuilder::new(
            &path,
            TableBuildOptions {
                bloom_bits_per_key: 0,
                prefix_len: 2,
                ..Default::default()
            },
        )
        .unwrap();
        builder.add_point(b"aakey", b"v1").unwrap();
        builder.add_point(b"bbkey", b"v2").unwrap();
        assert!(
            builder.prefix_set.is_empty(),
            "prefix_set must stay empty when bloom is disabled"
        );
        builder.finish().unwrap();
    }

    #[test]
    fn zstd_builder_compresses_data_blocks_and_writes_zstd_trailer() {
        use crate::sst::table_reader::TableReader;

        let dir = tempfile::tempdir().unwrap();
        let build = |name: &str, compression: CompressionType| -> u64 {
            let path = dir.path().join(name);
            let mut b = TableBuilder::new(
                &path,
                TableBuildOptions {
                    compression,
                    ..Default::default()
                },
            )
            .unwrap();
            let val = [0x5au8; 4096];
            for i in 0..256u32 {
                let ik = InternalKey::new(&i.to_be_bytes(), 1, ValueType::Value);
                b.add(ik.as_bytes(), &val).unwrap();
            }
            b.finish().unwrap().file_size
        };

        let none_size = build("none.sst", CompressionType::None);
        let zstd_size = build("zstd.sst", CompressionType::Zstd);
        assert!(
            zstd_size * 10 < none_size,
            "Zstd SST ({zstd_size} B) did not compress relative to uncompressed ({none_size} B)"
        );

        let zstd_path = dir.path().join("zstd.sst");
        let reader = TableReader::open(&zstd_path).unwrap();
        let entries = reader.cached_index_entries().unwrap();
        assert!(!entries.is_empty());
        let raw = std::fs::read(&zstd_path).unwrap();
        let first_h = entries[0].handle;
        let trailer_type = raw[(first_h.offset + first_h.size) as usize];
        assert_eq!(trailer_type, CompressionType::Zstd as u8);
        for i in [0u32, 127, 255] {
            let got = reader
                .point_value::<Vec<u8>>(&i.to_be_bytes(), 10, false)
                .unwrap();
            assert_eq!(got, Some((Some(vec![0x5au8; 4096]), 1)));
        }
    }
}
