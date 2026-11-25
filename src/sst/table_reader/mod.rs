//! SST table reader: reads key-value pairs from an SST file.
mod iterator;
pub use iterator::TableIterator;

use std::{cmp::Ordering, fs::File, io, path::Path, sync::Arc};

#[cfg(not(unix))]
use parking_lot::Mutex;

use crate::cache::block_cache::BlockCache;
use crate::error::{Error, Result, ResultExt};
use crate::sst::block::Block;
use crate::sst::filter::AlignedBytes;
use crate::sst::format::{
    BLOCK_TRAILER_SIZE, BlockHandle, CompressionType, FILTER_FORMAT_BLOCKED_64B,
    FILTER_FORMAT_NAME, FOOTER_SIZE, PREFIX_FILTER_LEN_NAME, RANGE_DEL_BLOCK_NAME, decode_footer,
    decode_index_value_with_props,
};
use crate::sst::range::RangeFragment;
use crate::stats::DbStats;
use crate::types::{SequenceNumber, ValueType, compare_internal_key, decode_internal_key};

/// A range tombstone: (begin_key, end_key, sequence_number).
type RangeTombstoneEntry = (Vec<u8>, Vec<u8>, SequenceNumber);

/// Maximum allowed decompressed block size. Used by readers to cap
/// block buffer allocations from damaged length headers and by compaction to
/// reserve enough output file numbers when compacting compressed inputs.
pub(crate) const MAX_DECOMPRESSED_BLOCK_SIZE: usize = 64 * 1024 * 1024; // 64 MB

/// Parsed index entry: separator key + block handle + optional first key + block properties.
pub struct IndexEntry {
    /// Upper bound for this data block, strictly below the following block.
    pub separator_key: Vec<u8>,
    /// Handle pointing to the data block.
    pub handle: BlockHandle,
    /// First key of the data block for deferred block reads.
    pub first_key: Vec<u8>,
    /// Block properties collected during SST build.
    pub properties: Vec<(Vec<u8>, Vec<u8>)>,
}

/// Shared index entries parsed from the SST index block.
type IndexEntries = Arc<Vec<IndexEntry>>;

/// A first data block read, checksummed, and decompressed by
/// [`TableReader::prepare_first_block_pin`], ready to be published into the
/// shared cache by [`TableReader::publish_prepared_pin`] without further I/O.
pub struct PreparedBlockPin {
    file_number: u64,
    index_leaf: Option<(u64, Vec<u8>)>,
    offset: u64,
    data: Vec<u8>,
}

/// Bloom filter data and range-del handle read from SST metaindex.
struct MetaIndexData {
    bloom: Option<Vec<u8>>,
    prefix: Option<Vec<u8>>,
    prefix_len: Option<usize>,
    range_del_handle: Option<BlockHandle>,
}

// Test-only seam: fires once, immediately before `prepare_first_block_pin`
// reads its data block, so a test can deterministically prove that step
// runs unlocked (e.g. by blocking here while a concurrent write proceeds).
// `pub(crate)` + thread-local so `db.rs`'s own tests can reach it without
// any cross-test leakage; `#[cfg(test)]`-only so it never builds into
// `test-utils` or release.
#[cfg(test)]
thread_local! {
    pub(crate) static PREPARE_FIRST_BLOCK_PIN_HOOK: std::cell::RefCell<Option<Box<dyn FnOnce()>>> =
        const { std::cell::RefCell::new(None) };
    /// Fires once on this thread right after a block's raw bytes are read.
    static AFTER_BLOCK_READ: std::cell::RefCell<Option<Box<dyn FnOnce()>>> =
        const { std::cell::RefCell::new(None) };
    /// Readahead hints issued on this thread.
    pub(crate) static WILLNEED_HINTS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

/// An open, immutable SST file. On unix, block reads are positional
/// (`pread`), so concurrent cache misses on one file proceed in parallel;
/// elsewhere a mutex guards the shared file cursor.
struct SstFile {
    #[cfg(unix)]
    file: File,
    #[cfg(not(unix))]
    file: Mutex<File>,
    /// Captured at open; SST files never change size.
    size: u64,
}

impl SstFile {
    fn new(file: File, size: u64) -> Self {
        Self {
            #[cfg(unix)]
            file,
            #[cfg(not(unix))]
            file: Mutex::new(file),
            size,
        }
    }

    fn read_exact_at(&self, buf: &mut [u8], offset: u64) -> io::Result<()> {
        #[cfg(unix)]
        {
            use std::os::unix::fs::FileExt;
            self.file.read_exact_at(buf, offset)
        }
        #[cfg(not(unix))]
        {
            use std::io::{Read, Seek, SeekFrom};
            let mut file = self.file.lock();
            file.seek(SeekFrom::Start(offset))?;
            file.read_exact(buf)
        }
    }
}

/// Consecutive index partitions can represent historical versions of the
/// same large key. Keep the shared user bytes only once in the resident root.
#[derive(PartialEq, Eq, PartialOrd, Ord)]
struct PartitionKey {
    prefix: Arc<[u8]>,
    trailer: [u8; 8],
}

impl PartitionKey {
    fn decode(bytes: &[u8], previous: &mut Option<Arc<[u8]>>) -> Result<Self> {
        let (prefix, _, _) = decode_internal_key(bytes)?;
        if previous.as_deref() != Some(prefix) {
            *previous = Some(Arc::from(prefix));
        }
        Ok(Self {
            prefix: previous.as_ref().unwrap().clone(),
            trailer: bytes[prefix.len()..].try_into().unwrap(),
        })
    }

    fn compare_internal(&self, key: &[u8]) -> Ordering {
        let Some(n) = key.len().checked_sub(8) else {
            return Ordering::Greater;
        };
        self.prefix
            .as_ref()
            .cmp(&key[..n])
            .then_with(|| self.trailer[..].cmp(&key[n..]))
    }
}

struct RangePartition {
    begin: Vec<u8>,
    end: Vec<u8>,
    handle: BlockHandle,
}

fn decode_interval(value: &[u8]) -> Result<(&[u8], &[u8])> {
    let length = value
        .get(..4)
        .ok_or_else(|| Error::corruption("truncated range interval"))?;
    let length = u32::from_le_bytes(length.try_into().unwrap()) as usize;
    let (begin, seqs) = value[4..]
        .split_at_checked(length)
        .ok_or_else(|| Error::corruption("truncated interval begin"))?;
    if seqs.is_empty() || !seqs.len().is_multiple_of(8) {
        return Err(Error::corruption("invalid interval sequences"));
    }
    Ok((begin, seqs))
}

/// Reader for an SST file.
pub struct TableReader {
    file_number: u64,
    partitions: Vec<(PartitionKey, BlockHandle)>,
    filter_data: Option<AlignedBytes>,
    prefix_filter_data: Option<AlignedBytes>,
    prefix_filter_len: Option<usize>,
    file: SstFile,
    block_cache: Option<Arc<BlockCache>>,
    stats: Option<Arc<DbStats>>,
    range_partitions: Vec<RangePartition>,
}

impl TableReader {
    fn validate_prefix_filter_metadata(
        prefix_filter: Option<&[u8]>,
        prefix_len: Option<usize>,
    ) -> Result<()> {
        if prefix_filter.is_some() != prefix_len.is_some() || prefix_len == Some(0) {
            return Err(Error::corruption(
                "inconsistent prefix filter metadata".to_string(),
            ));
        }
        Ok(())
    }

    /// Open an SST file for reading.
    pub fn open(path: &Path) -> Result<Self> {
        Self::open_with_all(path, 0, None, None)
    }

    /// Open with file number, optional block cache, and optional stats.
    pub fn open_with_all(
        path: &Path,
        file_number: u64,
        block_cache: Option<Arc<BlockCache>>,
        stats: Option<Arc<DbStats>>,
    ) -> Result<Self> {
        let file = File::open(path).ctx()?;
        let file_size = file.metadata().ctx()?.len();

        if file_size < FOOTER_SIZE as u64 {
            return Err(Error::corruption(format!(
                "SST file too small: {} bytes",
                file_size
            )));
        }
        let file = SstFile::new(file, file_size);

        // Read footer
        let mut footer_buf = [0u8; FOOTER_SIZE];
        file.read_exact_at(&mut footer_buf, file_size - FOOTER_SIZE as u64)
            .ctx()?;
        let (metaindex_handle, index_handle) = decode_footer(&footer_buf).ctx()?;

        if index_handle
            .offset
            .checked_add(index_handle.size)
            .and_then(|n| n.checked_add(BLOCK_TRAILER_SIZE as u64))
            != Some(file_size - FOOTER_SIZE as u64)
        {
            return Err(Error::corruption(
                "index directory head does not precede footer",
            ));
        }
        let mut last_user = None;
        let partitions = Self::read_directory(&file, index_handle, |key, value| {
            if value.len() != 16 {
                return Err(Error::corruption("invalid index partition handle"));
            }
            Ok((
                PartitionKey::decode(&key, &mut last_user)?,
                BlockHandle::decode(&value)?,
            ))
        })?;
        if partitions.windows(2).any(|p| p[0].0 >= p[1].0) {
            return Err(Error::corruption("unordered SST index partitions"));
        }
        let meta = Self::read_metaindex(&file, &metaindex_handle).ctx()?;
        let range_partitions = if let Some(handle) = meta.range_del_handle {
            let parts = Self::read_directory(&file, handle, |end, value| {
                let handle = BlockHandle::decode(&value)?;
                let begin = &value[16..];
                if begin >= end.as_slice() {
                    return Err(Error::corruption("empty range partition"));
                }
                Ok(RangePartition {
                    begin: begin.to_vec(),
                    end,
                    handle,
                })
            })?;
            if parts.is_empty() || parts.windows(2).any(|p| p[0].end > p[1].begin) {
                return Err(Error::corruption("invalid range directory"));
            }
            parts
        } else {
            Vec::new()
        };

        let reader = Self {
            file_number,
            partitions,
            filter_data: meta
                .bloom
                .as_deref()
                .map(AlignedBytes::decode)
                .transpose()?,
            prefix_filter_data: meta
                .prefix
                .as_deref()
                .map(AlignedBytes::decode)
                .transpose()?,
            prefix_filter_len: meta.prefix_len,

            file,
            block_cache,
            stats,

            range_partitions,
        };

        Ok(reader)
    }

    fn read_directory<T>(
        file: &SstFile,
        mut page: BlockHandle,
        mut entry: impl FnMut(Vec<u8>, Vec<u8>) -> Result<T>,
    ) -> Result<Vec<T>> {
        let mut entries = Vec::new();
        loop {
            let mut data = Self::read_block_data(file, &page)?;
            let tail = data
                .len()
                .checked_sub(16)
                .ok_or_else(|| Error::corruption("truncated directory page"))?;
            let previous = BlockHandle::decode(&data[tail..])?;
            data.truncate(tail);
            let block = Block::from_vec(data)?;
            let start = entries.len();
            let mut iter = block.iter();
            for (k, v) in &mut iter {
                entries.push(entry(k, v)?);
            }
            if let Some(e) = iter.error() {
                return Err(e.clone());
            }
            entries[start..].reverse();
            if previous == BlockHandle::default() {
                break;
            }
            if previous.size == 0
                || previous
                    .offset
                    .checked_add(previous.size)
                    .and_then(|n| n.checked_add(BLOCK_TRAILER_SIZE as u64))
                    .is_none_or(|end| end > page.offset)
            {
                return Err(Error::corruption("directory predecessor is out of bounds"));
            }
            page = previous;
        }
        entries.reverse();
        Ok(entries)
    }

    fn index_leaf(&self, part: usize, fill_cache: bool) -> Result<IndexEntries> {
        let Some((_, handle)) = self.partitions.get(part) else {
            return Ok(Arc::new(Vec::new()));
        };
        let block = Block::new(self.read_block_cached_opt(handle, fill_cache)?)?;
        Ok(Arc::new(Self::parse_index_entries(&block)?))
    }

    #[cfg(test)]
    pub(crate) fn cached_index_entries(&self) -> Result<IndexEntries> {
        let mut all = Vec::new();
        for i in 0..self.partitions.len() {
            all.extend(Arc::into_inner(self.index_leaf(i, false)?).unwrap());
        }
        Ok(Arc::new(all))
    }

    /// Parse index entries from the index block, propagating decode errors.
    fn parse_index_entries(index_block: &Block) -> Result<Vec<IndexEntry>> {
        let mut iter = index_block.iter();
        let mut entries = Vec::new();
        for (k, v) in &mut iter {
            let d = decode_index_value_with_props(&v).ctx()?;
            decode_internal_key(&k)?;
            decode_internal_key(d.first_key)?;
            if compare_internal_key(d.first_key, &k).is_gt()
                || entries.last().is_some_and(|prev: &IndexEntry| {
                    !compare_internal_key(&prev.separator_key, d.first_key).is_lt()
                })
            {
                return Err(Error::corruption("unordered SST index entries"));
            }
            entries.push(IndexEntry {
                separator_key: k,
                handle: d.handle,
                first_key: d.first_key.to_vec(),
                properties: d
                    .properties
                    .into_iter()
                    .map(|(n, p)| (n.to_vec(), p.to_vec()))
                    .collect(),
            });
        }
        // A corrupt index entry makes the iterator stop early with the error
        // recorded internally; without this check the truncated entry list
        // would silently hide every data block past the corruption point.
        if let Some(e) = iter.error() {
            return Err(e.clone()).ctx();
        }
        Ok(entries)
    }

    /// Look up a key in the SST (exact byte match). Returns the value if found.
    #[cfg(test)]
    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        Ok(self
            .get_internal(key, crate::types::MAX_SEQUENCE_NUMBER)?
            .flatten())
    }

    /// Look up a user key in an SST that stores internal keys.
    /// Finds the latest entry for `user_key` with sequence <= `sequence`.
    /// Returns:
    /// - `Some(Some(value))` if a Value entry is found
    /// - `Some(None)` if a Deletion tombstone is found
    /// - `None` if the key doesn't exist in this table
    #[cfg(test)]
    pub fn get_internal(
        &self,
        user_key: &[u8],
        sequence: SequenceNumber,
    ) -> Result<Option<Option<Vec<u8>>>> {
        Ok(self
            .get_internal_with_seq(user_key, sequence, true)?
            .map(|(value, _)| value))
    }

    /// Like `get_internal` but also returns the sequence number of the found entry.
    /// Returns `Some((result, entry_seq))` if found, `None` if key not in this table.
    /// When `fill_cache` is false, a cache miss does not populate the block cache.
    #[cfg(test)]
    pub fn get_internal_with_seq(
        &self,
        user_key: &[u8],
        sequence: SequenceNumber,
        fill_cache: bool,
    ) -> Result<Option<(Option<Vec<u8>>, SequenceNumber)>> {
        Ok(self
            .point_value::<crate::PinnedSlice>(user_key, sequence, fill_cache)?
            .map(|(v, seq)| (v.map(crate::PinnedSlice::into_vec), seq)))
    }

    pub fn point_value<V: crate::pinned::PointValue>(
        &self,
        user_key: &[u8],
        sequence: SequenceNumber,
        fill_cache: bool,
    ) -> Result<Option<(Option<V>, SequenceNumber)>> {
        use crate::types::pack_sequence_and_type;
        use smallvec::SmallVec;

        // Check bloom filter with user key
        if let Some(ref filter) = self.filter_data
            && !filter.may_match(user_key)
        {
            return Ok(None);
        }

        let mut seek_key = SmallVec::<[u8; 128]>::from_slice(user_key);
        seek_key.extend_from_slice(
            &(!pack_sequence_and_type(sequence, ValueType::RangeDeletion)).to_be_bytes(),
        );

        let handle = match self.find_data_block(&seek_key, fill_cache).ctx()? {
            Some(handle) => handle,
            None => return Ok(None),
        };

        let block_data = self.read_block_cached_opt(&handle, fill_cache).ctx()?;
        let block = Block::new(block_data).ctx()?;

        let mut encoded_ikey = SmallVec::<[u8; 128]>::new();
        match block
            .seek_by_reuse(&seek_key, compare_internal_key, &mut encoded_ikey)
            .ctx()?
        {
            Some(value) => {
                let (uk, entry_seq, vt) = decode_internal_key(&encoded_ikey).ctx()?;
                if uk == user_key {
                    return Ok(Some((
                        match vt {
                            ValueType::Value => Some(V::project(value.len(), || {
                                crate::PinnedSlice::from_block(
                                    block.data_arc().clone(),
                                    (value.as_ptr() as usize - block.data().as_ptr() as usize)
                                        as u32,
                                    value.len() as u32,
                                )
                            })),
                            ValueType::Deletion | ValueType::RangeDeletion => None,
                        },
                        entry_seq,
                    )));
                }
                Ok(None)
            }
            None => Ok(None),
        }
    }

    /// Find the highest-seq range tombstone covering `user_key` with seq <= `read_seq`.
    /// Returns 0 if none found. Only meaningful for SSTs that contain range deletions.
    ///
    /// Binary-search the resident directory by exclusive maximum end key.
    /// Disjoint elementary intervals ensure at most one leaf can cover a key.
    #[cfg(test)]
    pub fn max_covering_tombstone_seq(
        &self,
        user_key: &[u8],
        read_seq: SequenceNumber,
    ) -> Result<SequenceNumber> {
        self.covering_sequence(user_key, read_seq, true)
    }

    pub fn covering_sequence(
        &self,
        user_key: &[u8],
        read_seq: SequenceNumber,
        fill_cache: bool,
    ) -> Result<SequenceNumber> {
        let i = self
            .range_partitions
            .partition_point(|p| p.end.as_slice() <= user_key);
        let Some(part) = self
            .range_partitions
            .get(i)
            .filter(|p| p.begin.as_slice() <= user_key)
        else {
            return Ok(0);
        };
        let block = Block::new(self.read_block_cached_opt(&part.handle, fill_cache)?)?;
        // A tombstone's end is exclusive, so seek the first strictly greater end.
        let cmp = |end: &[u8], key: &[u8]| {
            if end <= key {
                Ordering::Less
            } else {
                Ordering::Greater
            }
        };
        let Some(v) =
            block.seek_by_reuse(user_key, cmp, &mut smallvec::SmallVec::<[u8; 128]>::new())?
        else {
            return Err(Error::corruption("range leaf ends before its separator"));
        };
        let (begin, seqs) = decode_interval(v)?;
        if begin > user_key {
            return Ok(0);
        }
        let seqs = seqs.as_chunks::<8>().0;
        let n = seqs.partition_point(|s| u64::from_le_bytes(*s) <= read_seq);
        Ok(n.checked_sub(1).map_or(0, |j| u64::from_le_bytes(seqs[j])))
    }

    pub(crate) fn has_data_partitions(&self) -> bool {
        !self.partitions.is_empty()
    }

    pub(crate) fn has_range_deletions(&self) -> bool {
        !self.range_partitions.is_empty()
    }

    pub(crate) fn range_partition_extents(&self) -> impl Iterator<Item = (&[u8], &[u8])> {
        self.range_partitions
            .iter()
            .map(|p| (p.begin.as_slice(), p.end.as_slice()))
    }

    /// Read each packed interval once for all subcompactions. Endpoints and
    /// stripe arrays stay shared when an interval crosses an output boundary.
    pub(crate) fn collect_range_fragments(&self, output: &mut Vec<RangeFragment>) -> Result<()> {
        self.visit_ranges(None, None, false, |begin, end, sequences| {
            let stripes = sequences.iter().copied().map(u64::from_le_bytes).collect();
            output.push(RangeFragment::new(begin, end, stripes));
        })
    }

    #[cfg(test)]
    pub(crate) fn get_range_tombstones(&self) -> Result<Vec<RangeTombstoneEntry>> {
        let mut triples = Vec::new();
        self.visit_ranges(None, None, false, |begin, end, sequences| {
            triples.extend(
                sequences
                    .iter()
                    .map(|s| (begin.to_vec(), end.to_vec(), u64::from_le_bytes(*s))),
            );
        })?;
        Ok(triples)
    }

    /// A scan at a fixed sequence needs at most one version of each interval.
    pub(crate) fn read_tombstones(
        &self,
        lower: Option<&[u8]>,
        upper: Option<&[u8]>,
        snapshot: u64,
        fill_cache: bool,
    ) -> Result<Vec<RangeTombstoneEntry>> {
        let mut triples = Vec::new();
        self.visit_ranges(lower, upper, fill_cache, |begin, end, seqs| {
            let n = seqs.partition_point(|s| u64::from_le_bytes(*s) <= snapshot);
            if let Some(i) = n.checked_sub(1) {
                triples.push((begin.to_vec(), end.to_vec(), u64::from_le_bytes(seqs[i])));
            }
        })?;
        Ok(triples)
    }

    fn visit_ranges(
        &self,
        lower: Option<&[u8]>,
        upper: Option<&[u8]>,
        fill_cache: bool,
        mut visit: impl FnMut(&[u8], &[u8], &[[u8; 8]]),
    ) -> Result<()> {
        let first = self
            .range_partitions
            .partition_point(|p| lower.is_some_and(|lo| p.end.as_slice() <= lo));
        for part in &self.range_partitions[first..] {
            if upper.is_some_and(|hi| part.begin.as_slice() >= hi) {
                break;
            }
            let block = Block::new(self.read_block_cached_opt(&part.handle, fill_cache)?)?;
            let mut iter = block.iter();
            let mut prev_end = part.begin.as_slice().to_vec();
            for (end, v) in &mut iter {
                let (begin, seqs) = decode_interval(&v)?;
                if begin < prev_end.as_slice() || begin >= end.as_slice() {
                    return Err(Error::corruption("overlapping range intervals"));
                }
                let stripes = seqs.as_chunks::<8>().0;
                if u64::from_le_bytes(*stripes.last().unwrap()) > crate::types::MAX_SEQUENCE_NUMBER
                    || stripes
                        .windows(2)
                        .any(|pair| u64::from_le_bytes(pair[0]) >= u64::from_le_bytes(pair[1]))
                {
                    return Err(Error::corruption(
                        "invalid range sequence ordering or maximum",
                    ));
                }
                if crate::types::tombstone_overlaps_bounds(begin, &end, lower, upper) {
                    visit(begin, &end, stripes);
                }
                prev_end = end;
            }
            if let Some(e) = iter.error() {
                return Err(e.clone());
            }
            if prev_end != part.end {
                return Err(Error::corruption("incorrect range partition end"));
            }
        }
        Ok(())
    }

    /// Iterate over all key-value pairs in the table.
    #[cfg(test)]
    pub fn iter(&self) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        let mut result = Vec::new();

        for entry in self.cached_index_entries()?.iter() {
            let handle = entry.handle;
            let block_data = self.read_block_cached(&handle).ctx()?;
            let block = Block::new(block_data).ctx()?;
            let mut iter = block.iter();
            for entry in &mut iter {
                result.push(entry);
            }
            if let Some(e) = iter.error() {
                return Err(e.clone()).ctx();
            }
        }

        Ok(result)
    }

    /// Find the data block handle that might contain the given key.
    fn find_data_block(&self, key: &[u8], fill_cache: bool) -> Result<Option<BlockHandle>> {
        let part = self
            .partitions
            .partition_point(|(k, _)| k.compare_internal(key).is_lt());
        let Some((_, handle)) = self.partitions.get(part) else {
            return Ok(None);
        };
        let block = Block::new(self.read_block_cached_opt(handle, fill_cache)?)?;
        let mut found = smallvec::SmallVec::<[u8; 128]>::new();
        block
            .seek_by_reuse(key, compare_internal_key, &mut found)?
            .map(|v| decode_index_value_with_props(v).map(|entry| entry.handle))
            .transpose()
    }

    fn read_block_data(file: &SstFile, handle: &BlockHandle) -> Result<Vec<u8>> {
        const MAX_COMPRESSED_BLOCK_SIZE: u64 = 64 * 1024 * 1024;
        let file_size = file.size;
        let end = handle
            .offset
            .checked_add(handle.size)
            .and_then(|n| n.checked_add(BLOCK_TRAILER_SIZE as u64))
            .ok_or_else(|| Error::corruption("block handle range overflow"))
            .ctx()?;
        if end > file_size {
            return Err(Error::corruption(format!(
                "block handle out of bounds: offset={}, size={}, file_size={}",
                handle.offset, handle.size, file_size
            )));
        }
        if handle.size > MAX_COMPRESSED_BLOCK_SIZE {
            return Err(Error::corruption(format!(
                "compressed block size {} exceeds limit {}",
                handle.size, MAX_COMPRESSED_BLOCK_SIZE
            )));
        }
        // Read the block and its trailer in one positional read.
        let size = handle.size as usize;
        let mut data = vec![0u8; size + BLOCK_TRAILER_SIZE];
        file.read_exact_at(&mut data, handle.offset).ctx()?;
        let trailer: [u8; BLOCK_TRAILER_SIZE] = data[size..].try_into().unwrap();
        data.truncate(size);
        #[cfg(test)]
        if let Some(hook) = AFTER_BLOCK_READ.with(|hook| hook.borrow_mut().take()) {
            hook();
        }

        let compression_type = CompressionType::from_u8(trailer[0])
            .ok_or_else(|| Error::corruption("unknown compression type"))
            .ctx()?;

        let stored_crc = u32::from_le_bytes(trailer[1..5].try_into().unwrap());
        let mut hasher = crc32fast::Hasher::new();
        hasher.update(&data);
        hasher.update(&[trailer[0]]);
        let computed_crc = hasher.finalize();

        if stored_crc != computed_crc {
            return Err(Error::corruption(format!(
                "block CRC mismatch: stored {:#x}, computed {:#x}",
                stored_crc, computed_crc
            )));
        }

        // Decompress if needed, enforcing MAX_DECOMPRESSED_BLOCK_SIZE.
        let data = match compression_type {
            CompressionType::Lz4 => {
                if data.len() < 4 {
                    return Err(Error::corruption(
                        "LZ4 block too small for size header".to_string(),
                    ));
                }
                let uncompressed_size =
                    u32::from_le_bytes([data[0], data[1], data[2], data[3]]) as usize;
                if uncompressed_size > MAX_DECOMPRESSED_BLOCK_SIZE {
                    return Err(Error::corruption(format!(
                        "LZ4 decompressed size {} exceeds limit {}",
                        uncompressed_size, MAX_DECOMPRESSED_BLOCK_SIZE
                    )));
                }
                lz4_flex::decompress_size_prepended(&data)
                    .map_err(|e| Error::corruption(format!("LZ4 decompression error: {}", e)))
                    .ctx()?
            }
            CompressionType::Zstd => {
                // zstd::bulk::compress() embeds the frame's uncompressed content
                // size by default (confirmed by the zstd crate's own test suite).
                // Read it via the stable zstd_safe API to right-size the
                // allocation, instead of always allocating the full
                // MAX_DECOMPRESSED_BLOCK_SIZE regardless of actual block size
                // (the alternative, `Decompressor::upper_bound()`, requires the
                // unstable "experimental" cargo feature, which is not enabled).
                // The claimed size is still bounded against
                // MAX_DECOMPRESSED_BLOCK_SIZE before use, exactly like the LZ4
                // path above, since a damaged frame header could otherwise
                // report an oversized length.
                let capacity = match zstd::zstd_safe::get_frame_content_size(&data) {
                    Ok(Some(size)) => {
                        if size > MAX_DECOMPRESSED_BLOCK_SIZE as u64 {
                            return Err(Error::corruption(format!(
                                "Zstd decompressed size {} exceeds limit {}",
                                size, MAX_DECOMPRESSED_BLOCK_SIZE
                            )));
                        }
                        size as usize
                    }
                    // Frame doesn't embed a content size — fall back to the
                    // conservative bound (matches prior behavior for this case).
                    Ok(None) => MAX_DECOMPRESSED_BLOCK_SIZE,
                    Err(_) => {
                        return Err(Error::corruption("malformed Zstd frame header"));
                    }
                };
                zstd::bulk::decompress(data.as_slice(), capacity)
                    .map_err(|e| Error::corruption(format!("Zstd decompression error: {}", e)))
                    .ctx()?
            }
            CompressionType::None => data,
        };

        Ok(data)
    }

    fn read_metaindex(file: &SstFile, metaindex_handle: &BlockHandle) -> Result<MetaIndexData> {
        // Every writer emits a metaindex block (an empty block still carries
        // its restart array), and the footer has no checksum: a zero size can
        // only be corruption, and reading it as "no metadata" would drop the
        // file's range tombstones.
        if metaindex_handle.size == 0 {
            return Err(Error::corruption("zero-size SST metaindex block"));
        }

        let metaindex_data = Self::read_block_data(file, metaindex_handle).ctx()?;
        let metaindex = Block::from_vec(metaindex_data).ctx()?;

        let mut bloom = None;
        let mut prefix = None;
        let mut prefix_len = None;
        let mut filter_format = None;
        let mut range_del_handle = None;

        let mut iter = metaindex.iter();
        for (key, value) in &mut iter {
            if key == b"filter.bloom" {
                let handle = BlockHandle::decode(&value).ctx()?;
                bloom = Some(Self::read_block_data(file, &handle).ctx()?);
            } else if key == b"filter.prefix" {
                let handle = BlockHandle::decode(&value).ctx()?;
                prefix = Some(Self::read_block_data(file, &handle).ctx()?);
            } else if key == FILTER_FORMAT_NAME.as_bytes() {
                if value.len() != 1 {
                    return Err(Error::corruption(
                        "invalid filter.format length".to_string(),
                    ));
                }
                filter_format = Some(value[0]);
            } else if key == PREFIX_FILTER_LEN_NAME.as_bytes() {
                if value.len() != 8 {
                    return Err(Error::corruption(
                        "bad prefix filter length metadata".to_string(),
                    ));
                }
                let len = u64::from_le_bytes(value.as_slice().try_into().unwrap());
                prefix_len = Some(usize::try_from(len).map_err(|_| {
                    Error::corruption("prefix filter length overflows usize".to_string())
                })?);
            } else if key == RANGE_DEL_BLOCK_NAME.as_bytes() {
                range_del_handle = Some(BlockHandle::decode(&value).ctx()?);
            }
        }
        if let Some(e) = iter.error() {
            return Err(e.clone()).ctx();
        }
        Self::validate_prefix_filter_metadata(prefix.as_deref(), prefix_len).ctx()?;
        Self::validate_filter_format(bloom.is_some() || prefix.is_some(), filter_format)?;

        Ok(MetaIndexData {
            bloom,
            prefix,
            prefix_len,

            range_del_handle,
        })
    }

    fn validate_filter_format(has_filter: bool, format: Option<u8>) -> Result<()> {
        if format != has_filter.then_some(FILTER_FORMAT_BLOCKED_64B) {
            return Err(Error::corruption(
                "missing or unsupported SST filter.format",
            ));
        }
        Ok(())
    }

    /// Check if a prefix may exist in this SST file using the prefix bloom filter.
    /// Returns `true` if the prefix might be present (or if no prefix bloom exists).
    pub fn prefix_may_match(&self, prefix: &[u8]) -> bool {
        match (self.prefix_filter_data.as_ref(), self.prefix_filter_len) {
            (Some(filter), Some(prefix_len)) if prefix_len > 0 && prefix.len() >= prefix_len => {
                filter.may_match(&prefix[..prefix_len])
            }
            _ => true, // No filter, or a query shorter than its fixed prefix.
        }
    }

    /// Read a block, consulting the cache first if available.
    /// Returns Arc<Vec<u8>> — zero-copy on cache hit.
    #[cfg(test)]
    fn read_block_cached(&self, handle: &BlockHandle) -> Result<Arc<Vec<u8>>> {
        self.read_block_cached_opt(handle, true)
    }

    /// Read and decompress a block on a cache miss. When
    /// `fill_cache` is false a cache miss does **not** insert the block into
    /// the block cache. Used by scans (user iterators with
    /// `ReadOptions::fill_cache == false`, and compaction reads) to avoid
    /// evicting hot point-read blocks.
    fn read_block_cached_opt(
        &self,
        handle: &BlockHandle,
        fill_cache: bool,
    ) -> Result<Arc<Vec<u8>>> {
        if let Some(ref cache) = self.block_cache
            && let Some(cached) = cache.get(self.file_number, handle.offset)
        {
            if let Some(ref s) = self.stats {
                s.record_cache_hit();
            }
            return Ok(cached);
        }

        if let Some(ref s) = self.stats
            && self.block_cache.is_some()
        {
            s.record_cache_miss();
        }

        let data = Self::read_block_data(&self.file, handle).ctx()?;

        if fill_cache && let Some(ref cache) = self.block_cache {
            return Ok(cache.insert(self.file_number, handle.offset, data));
        }

        Ok(Arc::new(data))
    }

    /// Read, checksum-verify, and decompress this file's first data block
    /// (if any, and if not already cached) without touching the shared
    /// cache. Pure I/O/CPU work with no cache-visible side effect, so it is
    /// safe to call before taking any lock that must stay short.
    ///
    /// Returns `None` when there is nothing to prepare: no block cache
    /// configured, no data blocks, the block is already cached, or the
    /// index/read/decode failed (logged and treated as best-effort, same
    /// as the previous single-phase behavior).
    pub fn prepare_first_block_pin(&self) -> Option<PreparedBlockPin> {
        let cache = self.block_cache.as_ref()?;
        if !cache.is_enabled() {
            return None;
        }
        let leaf = self.partitions.first()?.1;
        let cached = cache.get(self.file_number, leaf.offset);
        let warm = cached.is_none();
        let bytes = match cached {
            Some(bytes) => bytes,
            None => Arc::new(Self::read_block_data(&self.file, &leaf).ok()?),
        };
        let handle = {
            let block = Block::new(bytes.clone()).ok()?;
            let (_, value) = block.iter().next()?;
            decode_index_value_with_props(&value).ok()?.handle
        };
        if cache.get(self.file_number, handle.offset).is_some() {
            return None;
        }
        let index_leaf = if warm {
            Some((leaf.offset, Arc::into_inner(bytes)?))
        } else {
            None
        };
        #[cfg(test)]
        if let Some(hook) = PREPARE_FIRST_BLOCK_PIN_HOOK.with(|h| h.borrow_mut().take()) {
            hook();
        }
        let data = Self::read_block_data(&self.file, &handle).ok()?;
        Some(PreparedBlockPin {
            file_number: self.file_number,
            offset: handle.offset,
            index_leaf,
            data,
        })
    }

    /// Publish a block prepared by
    /// [`prepare_first_block_pin`](Self::prepare_first_block_pin) into the
    /// shared cache as pinned (non-evictable). Only an in-memory map
    /// insert — no I/O — so it is safe to call while holding a lock that
    /// must stay short (e.g. the flush-install critical section).
    pub fn publish_prepared_pin(&self, prepared: PreparedBlockPin) {
        if prepared.file_number != self.file_number {
            // Defensive: a prepared pin must be published through a reader
            // for the same physical file it was read from.
            return;
        }
        if let Some(ref cache) = self.block_cache {
            if let Some((offset, bytes)) = prepared.index_leaf {
                cache.insert(prepared.file_number, offset, bytes);
            }
            cache.insert_pinned(prepared.file_number, prepared.offset, prepared.data);
        }
    }

    /// Hint the OS to prefetch the given file range into page cache.
    /// Uses `posix_fadvise` on Linux; no-op on other platforms.
    fn advise_willneed(&self, offset: u64, len: u64) {
        #[cfg(test)]
        WILLNEED_HINTS.with(|hints| hints.set(hints.get() + 1));
        #[cfg(target_os = "linux")]
        {
            use std::os::unix::io::AsRawFd;
            let (Ok(offset), Ok(len)) = (i64::try_from(offset), i64::try_from(len)) else {
                return;
            };
            // SAFETY: `posix_fadvise` is an advisory hint with no safety
            // invariants beyond a valid fd. The fd belongs to the `File` this
            // reader owns, which stays open for the duration of `&self`.
            // Checked conversions keep offset and length non-negative in the
            // platform `off_t` representation.
            unsafe {
                libc::posix_fadvise(
                    self.file.file.as_raw_fd(),
                    offset,
                    len,
                    libc::POSIX_FADV_WILLNEED,
                );
            }
        }
        #[cfg(not(target_os = "linux"))]
        {
            let _ = (offset, len);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{FILTER_FORMAT_BLOCKED_64B, TableReader};

    #[test]
    fn partitioned_index_seeks_and_scans_across_every_leaf() {
        use crate::iterator::merge::SeekableIterator;
        use crate::sst::{
            table_builder::{TableBuildOptions, TableBuilder},
            table_reader::TableIterator,
        };
        use crate::types::{InternalKey, MAX_SEQUENCE_NUMBER, ValueType, decode_internal_key};
        use std::sync::Arc;

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("leaves.sst");
        let mut builder = TableBuilder::new(
            &path,
            TableBuildOptions {
                block_size: 1,
                ..Default::default()
            },
        )
        .unwrap();
        let count = 4_000;
        let keys: Vec<_> = (0..count)
            .map(|i| {
                let mut user = format!("{:08}", i * 4).into_bytes();
                user.resize(72, b'x');
                InternalKey::new(&user, 1, ValueType::Value).into_bytes()
            })
            .collect();
        for key in &keys {
            builder.add(key, b"value").unwrap();
        }
        let trailing = InternalKey::new(b"zzz", 5, ValueType::RangeDeletion);
        builder.add(trailing.as_bytes(), b"zzzz").unwrap();
        builder.finish().unwrap();
        let reader = Arc::new(TableReader::open(&path).unwrap());
        assert!(reader.partitions.len() > 2);
        let mut total_leaf_bytes = 0;
        for (i, (separator, handle)) in reader.partitions.iter().enumerate() {
            total_leaf_bytes += handle.size as usize;
            let leaf = reader.index_leaf(i, false).unwrap();
            let encoded = &leaf.last().unwrap().separator_key;
            assert_eq!(
                separator.compare_internal(encoded),
                std::cmp::Ordering::Equal
            );
            let (user, seq, ty) = decode_internal_key(encoded).unwrap();
            if i + 1 < reader.partitions.len() {
                assert!(user.len() < 72);
                assert_eq!((seq, ty), (MAX_SEQUENCE_NUMBER, ValueType::RangeDeletion));
            } else {
                assert_eq!(encoded, keys.last().unwrap());
            }
            let mut iter = TableIterator::new(reader.clone());
            let start = &leaf[0].first_key;
            iter.seek(start);
            assert_eq!(&iter.next().unwrap().0, start);
            iter.seek_for_prev(start);
            assert_eq!(&iter.current().unwrap().0, start);
        }
        let directory_bytes: usize = reader
            .partitions
            .iter()
            .map(|(s, _)| {
                s.prefix.len()
                    + 2 * std::mem::size_of::<usize>()
                    + std::mem::size_of::<(super::PartitionKey, super::BlockHandle)>()
            })
            .sum();
        eprintln!(
            "{} data blocks, {} index leaves, {} directory bytes vs {} encoded leaf bytes",
            keys.len(),
            reader.partitions.len(),
            directory_bytes,
            total_leaf_bytes
        );
        assert!(directory_bytes * 2 < total_leaf_bytes);
        let mut iter = TableIterator::new(reader);
        assert_eq!(iter.by_ref().map(|(k, _)| k).collect::<Vec<_>>(), keys);
        iter.seek_to_last();
        assert_eq!(iter.current().unwrap().0, *keys.last().unwrap());
        for key in keys.iter().rev().skip(1) {
            assert_eq!(&iter.prev().unwrap().0, key);
        }
        assert!(iter.prev().is_none());
        assert!(iter.iter_error().is_none());
    }

    #[test]
    fn crossing_ranges_probe_one_leaf_and_keep_only_snapshot_stripes() {
        use crate::cache::block_cache::BlockCache;
        use crate::sst::table_builder::{TableBuildOptions, TableBuilder};
        use crate::stats::DbStats;
        use crate::types::{InternalKey, ValueType};
        use std::sync::{Arc, atomic::Ordering};

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("ranges.sst");
        let mut builder = TableBuilder::new(&path, TableBuildOptions::default()).unwrap();
        let stripes: Vec<_> = (1..=256).map(|s| s * 3).collect();
        builder.start_output(&stripes, vec![]);
        for seq in (1..=800).rev() {
            let key = InternalKey::new(b"a", seq, ValueType::RangeDeletion);
            builder.add(key.as_bytes(), b"z").unwrap();
        }
        for i in 0..150 {
            let start = format!("k{i:03}");
            let key = InternalKey::new(start.as_bytes(), 900, ValueType::RangeDeletion);
            builder
                .add(key.as_bytes(), format!("{start}~").as_bytes())
                .unwrap();
        }
        builder.finish().unwrap();
        let stats = Arc::new(DbStats::new());
        let reader = TableReader::open_with_all(
            &path,
            1,
            Some(Arc::new(BlockCache::new(8 << 20))),
            Some(stats.clone()),
        )
        .unwrap();
        assert!(reader.range_partitions.len() > 2);
        for (key, snapshot, expected) in [
            (b"y".as_slice(), 1000, 800),
            (b"k080", 1000, 900),
            (b"k080~", 1000, 800),
            (b"k080", 255, 255),
            (b"k080", 0, 0),
            (b"z", 1000, 0),
        ] {
            let before = stats.block_cache_misses.load(Ordering::Relaxed);
            assert_eq!(
                reader.max_covering_tombstone_seq(key, snapshot).unwrap(),
                expected
            );
            assert!(stats.block_cache_misses.load(Ordering::Relaxed) - before <= 1);
        }
        let triples = reader.get_range_tombstones().unwrap();
        let at_k080: Vec<_> = triples
            .iter()
            .filter(|(b, _, _)| b == b"k080")
            .map(|(_, _, seq)| *seq)
            .collect();
        assert_eq!(at_k080.len(), stripes.len() + 1);
        assert!(at_k080.contains(&900));
        assert!(!at_k080.contains(&800));
    }

    #[test]
    fn warm_point_get_allocates_only_the_matching_value() {
        use std::sync::Arc;

        use crate::cache::block_cache::BlockCache;
        use crate::sst::table_builder::{TableBuildOptions, TableBuilder};
        use crate::types::{InternalKey, ValueType};

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("point.sst");
        let mut builder = TableBuilder::new(
            &path,
            TableBuildOptions {
                block_size: 2 * 1024 * 1024,

                bloom_bits_per_key: 0,
                ..Default::default()
            },
        )
        .unwrap();
        let value = vec![7; 16 * 1024];
        for i in 0..64 {
            let key = InternalKey::new(format!("key{i:04}").as_bytes(), 1, ValueType::Value);
            builder.add(key.as_bytes(), &value).unwrap();
        }
        builder.finish().unwrap();
        let cache = Arc::new(BlockCache::new(4 * 1024 * 1024));
        let reader = TableReader::open_with_all(&path, 1, Some(cache), None).unwrap();
        reader.get_internal_with_seq(b"key0047", 1, true).unwrap();

        let (result, allocated) = crate::test_alloc::count_allocated(|| {
            reader.get_internal_with_seq(b"key0047", 1, true).unwrap()
        });
        assert_eq!(result, Some((Some(value.clone()), 1)));
        eprintln!("warm point hit allocated {allocated} bytes");
        assert!(
            allocated < value.len() + 1024,
            "allocated {allocated} bytes"
        );

        let (result, allocated) = crate::test_alloc::count_allocated(|| {
            reader
                .get_internal_with_seq(b"key0047-missing", 1, true)
                .unwrap()
        });
        assert_eq!(result, None);
        assert!(allocated < 1024, "miss allocated {allocated} bytes");
    }

    #[test]
    fn point_get_preserves_snapshot_versions_across_blocks() {
        use crate::sst::format::CompressionType;
        use crate::sst::table_builder::{TableBuildOptions, TableBuilder};
        use crate::types::{InternalKey, ValueType};

        for compression in [
            CompressionType::None,
            CompressionType::Lz4,
            CompressionType::Zstd,
        ] {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("versions.sst");
            let mut builder = TableBuilder::new(
                &path,
                TableBuildOptions {
                    block_size: 256,
                    block_restart_interval: 2,
                    compression,
                    ..Default::default()
                },
            )
            .unwrap();
            for user_key in [b"a".as_slice(), b"a\0", b"aa"] {
                for seq in (1..=100).rev() {
                    let vt = if seq == 50 {
                        ValueType::Deletion
                    } else {
                        ValueType::Value
                    };
                    let key = InternalKey::new(user_key, seq, vt);
                    let value = [seq as u8; 80];
                    builder
                        .add(key.as_bytes(), if seq == 50 { &[] } else { &value })
                        .unwrap();
                }
            }
            builder.finish().unwrap();
            let reader = TableReader::open(&path).unwrap();
            for user_key in [b"a".as_slice(), b"a\0", b"aa"] {
                for seq in 0..=101 {
                    let expected = match seq {
                        0 => None,
                        50 => Some((None, 50)),
                        _ => Some((Some(vec![seq.min(100) as u8; 80]), seq.min(100))),
                    };
                    assert_eq!(
                        reader.get_internal_with_seq(user_key, seq, false).unwrap(),
                        expected
                    );
                }
            }
            for missing in [b"".as_slice(), b"a\0x", b"ab", b"z"] {
                assert_eq!(
                    reader.get_internal_with_seq(missing, 100, false).unwrap(),
                    None
                );
            }
        }
    }

    /// Regression: every cache-miss read of a file held one `Mutex<File>`
    /// through the read, CRC, and decompression, so independent misses on
    /// the same SST ran one at a time.
    #[test]
    fn cache_miss_reads_of_one_file_run_concurrently() {
        use std::sync::mpsc;
        use std::time::Duration;

        use crate::sst::table_builder::{TableBuildOptions, TableBuilder};
        use crate::types::{InternalKey, ValueType};

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("000001.sst");
        let mut builder = TableBuilder::new(
            &path,
            TableBuildOptions {
                block_size: 1,
                ..Default::default()
            },
        )
        .unwrap();
        for i in 0..4u64 {
            let key = InternalKey::new(format!("k{i}").as_bytes(), 10 - i, ValueType::Value);
            builder.add(key.as_bytes(), b"v").unwrap();
        }
        builder.finish().unwrap();
        let reader = TableReader::open(&path).unwrap();
        let index = reader.cached_index_entries().unwrap();
        let (first, second) = (index[0].handle, index[1].handle);

        let (parked_tx, parked_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel::<()>();
        std::thread::scope(|scope| {
            let reader = &reader;
            scope.spawn(move || {
                super::AFTER_BLOCK_READ.with(|hook| {
                    *hook.borrow_mut() = Some(Box::new(move || {
                        parked_tx.send(()).unwrap();
                        release_rx.recv().unwrap();
                    }));
                });
                reader.read_block_cached_opt(&first, false).unwrap();
            });
            parked_rx.recv_timeout(Duration::from_secs(10)).unwrap();

            let (done_tx, done_rx) = mpsc::channel();
            scope.spawn(move || {
                reader.read_block_cached_opt(&second, false).unwrap();
                done_tx.send(()).unwrap();
            });
            let finished = done_rx.recv_timeout(Duration::from_secs(5)).is_ok();
            release_tx.send(()).unwrap();
            assert!(
                finished,
                "a miss on another block waited for the parked read"
            );
        });
    }

    #[test]
    fn test_zero_prefix_filter_length_is_rejected() {
        assert!(
            TableReader::validate_prefix_filter_metadata(Some(&[1, 2]), Some(0)).is_err(),
            "a present filter cannot use the disabled zero-length prefix"
        );
        assert!(TableReader::validate_prefix_filter_metadata(Some(&[1, 2]), Some(3)).is_ok());
        assert!(
            TableReader::validate_prefix_filter_metadata(Some(&[1, 2]), None).is_err(),
            "prefix bloom requires its prefix length"
        );
    }

    #[test]
    fn require_blocked_filter_marker() {
        assert!(TableReader::validate_filter_format(true, None).is_err());
        assert!(TableReader::validate_filter_format(true, Some(1)).is_err());
        assert!(TableReader::validate_filter_format(true, Some(FILTER_FORMAT_BLOCKED_64B)).is_ok());
        assert!(TableReader::validate_filter_format(false, None).is_ok());
    }
}
