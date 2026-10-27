# SST Review Patterns

**Files:** `table_builder`, `table_reader/{mod,iterator}`, `block`, `block_builder`,
`filter`, `format`, `mod`.

**Arch:** Prefix-compressed data blocks + restarts; index last_key → (handle, first_key);
whole-key + prefix blooms with a metaindex hash-version marker; dedicated range-del
block (legacy files keep tombstones inline in data blocks); 48B footer (metaindex,
index, magic); None/LZ4/Zstd.

## Invariants

**S1 Prefix** — shared_len = true common prefix with prev; **0 at restarts**.
**S2 Restarts** — offsets of shared=0 entries; correct interval.
**S3 Handles** — index/meta (offset,size) match on-disk after compression header.
**S4 Bloom** — no false negative: same hash version + probe count builder/reader.
Filters without a recognized `filter.hash` marker are legacy and must be bypassed,
not probed with the current hash.
**S5 Compress round-trip** — type byte before encoded block data; matching decompress path.
**S6 Magic** — validate before trusting footer handles.

## Bug patterns

**Off-by-one shared_len (tech 2.4)** — rest of block wrong. Test 0/1/full prefixes.
**Index seek** — first block with last_key ≥ target.
**Hash seed mismatch** — same double-hash `h1 + i*h2` and the same version marker; unmarked legacy filters must be bypassed.
**Decompress size** — cap by max block size before accepting the stored length.

## Checklist

- [ ] shared=0 at restarts; offsets accurate
- [ ] Handles match post-compress layout
- [ ] Bloom no FN; same hash version; unmarked filters bypassed
- [ ] Range-del block handle read; legacy inline tombstones handled
- [ ] Compress type consistent
- [ ] Magic before index/meta use
- [ ] Index binary-search compare correct
- [ ] Decompress size bounded
- [ ] `open_with_all` keeps File RAII; raw-fd restore if ever extracted
