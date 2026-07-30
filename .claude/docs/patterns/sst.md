# SST Review Patterns

**Files:** `table_builder`, `table_reader/{mod,iterator}`, `block`, `block_builder`,
`filter`, `format`, `mod`.

**Arch:** Prefix-compressed data blocks + restarts; index last_key → (handle, first_key);
whole-key + prefix blooms; 48B footer (metaindex, index, magic); None/LZ4/Zstd.

## Invariants

**S1 Prefix** — shared_len = true common prefix with prev; **0 at restarts**.
**S2 Restarts** — offsets of shared=0 entries; correct interval.
**S3 Handles** — index/meta (offset,size) match on-disk after compression header.
**S4 Bloom** — no false negative: same hash + probe count builder/reader.
**S5 Compress round-trip** — type byte before payload; matching decompress path.
**S6 Magic** — validate before trusting footer handles.

## Bug patterns

**Off-by-one shared_len (tech 2.4)** — rest of block wrong. Test 0/1/full prefixes.
**Index seek** — first block with last_key ≥ target.
**Hash seed mismatch** — same double-hash `h1 + i*h2`.
**Decompress size** — cap by max block size (untrusted length).

## Checklist

- [ ] shared=0 at restarts; offsets accurate
- [ ] Handles match post-compress layout
- [ ] Bloom no FN; same hash paths
- [ ] Compress type consistent
- [ ] Magic before index/meta use
- [ ] Index binary-search compare correct
- [ ] Decompress size bounded
- [ ] `open_with_all` keeps File RAII; raw-fd restore if ever extracted
