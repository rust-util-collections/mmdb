# SST Subsystem Review Patterns

## Files
- `src/sst/table_builder.rs` (~22KB) — SST file writer
- `src/sst/table_reader.rs` (~66KB) — SST file reader, complex seek logic
- `src/sst/block.rs` (~25KB) — data block codec, prefix compression
- `src/sst/block_builder.rs` — block construction with restart points
- `src/sst/filter.rs` (~9KB) — bloom filter (double hashing)
- `src/sst/format.rs` (~9KB) — footer, BlockHandle, compression

## Architecture
- Data blocks: prefix-compressed entries with restart points (every N entries)
- Index block: last_key → (data block handle, first_key) per block
- Filter blocks: whole-key bloom + prefix bloom
- Footer: 48 bytes (metaindex handle, index handle, magic)
- Compression: None / LZ4 / Zstd, configurable per level

## Critical Invariants

### INV-S1: Prefix Compression Correctness
The shared prefix length stored in each entry must not exceed the actual common prefix with the previous key.
**Check**: Verify `shared = common_prefix_len(prev_key, current_key)` is computed correctly, especially at restart points where shared must be 0.

### INV-S2: Restart Point Accuracy
Restart points must record the exact offset of the first entry with shared_prefix_len = 0.
**Check**: Verify restart array is populated at the correct interval and offsets point to the right entries.

### INV-S3: Block Handle Consistency
The BlockHandle (offset, size) stored in the index/metaindex must match the actual position and size of the data/filter block on disk.
**Check**: Verify table_builder tracks cumulative offset correctly, accounting for compression header overhead.

### INV-S4: Bloom Filter Correctness
A key present in the SST must ALWAYS return true from the bloom filter. False negatives are data-loss bugs.
**Check**: Verify the same hash function is used in both `add_key()` (builder) and `may_contain()` (reader). Verify the number of hash probes is consistent.

### INV-S5: Compression Round-Trip
`decompress(compress(data))` must produce the original data byte-for-byte.
**Check**: Verify compression type byte is written before the compressed payload, and the reader uses the same type to select the decompressor.

### INV-S6: Footer Magic Validation
The reader must validate the magic number in the footer before trusting any other field.
**Check**: Verify `TableReader::open()` checks magic before reading index/metaindex handles.

## Common Bug Patterns

### Prefix Compression Off-by-One (technical-patterns.md 2.4)
`common_prefix_len()` returns N, but the entry stores N-1 or N+1 as the shared length.
**Impact**: All subsequent entries in the block decode incorrect keys.
**Check**: Unit test prefix compression with keys that share exactly 0, 1, and full-length prefixes.

### Block Boundary Seek Miss
Binary search on the index block finds the wrong data block because the index key (last key in block) comparison uses the wrong ordering.
**Check**: Verify index seek uses `>=` (find first block whose last key >= target).

### Bloom Filter Hash Seed Mismatch
Builder uses one seed for the bloom hash, reader uses another.
**Check**: Verify both paths use the same double-hashing formula: `h1 + i * h2` for probes.

### Decompression Buffer Overflow
Decompressed block size exceeds the allocated buffer because the original size is read from a corrupted/untrusted field.
**Check**: Verify decompressed size is bounded by a maximum block size constant.

## Review Checklist
- [ ] Prefix compression: shared_len = 0 at restart points
- [ ] Restart point offsets are accurate
- [ ] BlockHandle (offset, size) matches on-disk layout after compression
- [ ] Bloom filter: same hash in builder and reader, no false negatives possible
- [ ] Compression type written and read consistently
- [ ] Footer magic validated before using index/metaindex handles
- [ ] Binary search on index block uses correct comparison
- [ ] Decompressed block size bounded to prevent allocation bomb
- [ ] File descriptor closed on all error paths in TableReader::open()
