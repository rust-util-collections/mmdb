# SST Review Patterns

**Files:** `table_builder`, `table_reader/{mod,iterator}`, `block`, `block_builder`,
`filter`, `format`, `mod`.

**Arch:** Prefix-compressed data blocks + restarts; index `last_key → (handle, first_key)`
lazily parsed into shared `OnceLock<Arc<Vec<IndexEntry>>>` for both iterators and
point reads; point lookups use `Block::seek_by_reuse` (reusing one key buffer and
copying only a matching value); `TableBuilder` reuses key buffers and borrows
uncompressed block slices; whole-key + prefix blooms with a metaindex hash-version
marker; dedicated range-del block (legacy files keep tombstones inline in data
blocks); 48B footer (metaindex, index, magic); None/LZ4/Zstd.

## Invariants

**S1 Prefix** — shared_len = true common prefix with prev; **0 at restarts**.
**S2 Restarts** — offsets of shared=0 entries; correct interval.
**S3 Handles** — index/meta (offset,size) match on-disk after compression header.
**S4 Bloom** — no false negative: one hash per distinct user key, same hash version + probe count builder/reader.
Filters without a recognized `filter.hash` marker are legacy and must be skipped
(falling back to normal index/block lookup), not probed with the current hash.
**S5 Compress round-trip** — encoded block data followed by type byte and CRC32;
CRC covers data plus type, and the reader uses the matching decompress path.
**S6 Magic** — validate before trusting footer handles.

## Bug patterns

**Off-by-one shared_len (tech 2.4)** — rest of block wrong. Test 0/1/full prefixes.
**Index seek** — first block with last_key ≥ target.
**Hash seed mismatch** — same double-hash `h1 + i*h2` and the same version marker; unmarked legacy filters must be skipped.
**Decompress size** — cap against `MAX_DECOMPRESSED_BLOCK_SIZE` before allocating from a stored frame length.

## Checklist

- [ ] shared=0 at restarts; offsets accurate
- [ ] Handles match post-compress layout
- [ ] Bloom no FN; one hash per user key; unmarked legacy filters skipped
- [ ] Point reads reuse key buffer and copy only the matched value
- [ ] Range-del block handle read; legacy inline tombstones handled
- [ ] Compress type consistent
- [ ] Magic before index/meta use
- [ ] Index binary-search compare correct
- [ ] Decompress size bounded
- [ ] `open_with_all` keeps File RAII; raw-fd restore if ever extracted
