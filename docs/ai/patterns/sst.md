# SST Review Patterns

The SST implementation lives in `table_builder`, `table_reader/{mod,iterator}`,
`block`, `block_builder`, `filter`, and `format`. Files contain prefix-compressed
data blocks, blocked Bloom filters, range-deletion and point-index leaves,
their partition directories, a metaindex, and a 48-byte footer.

## Invariants

**S1 Prefix** — An entry shares a prefix with its immediate predecessor. At
every restart, the shared length is zero.

**S2 Restarts** — Restart offsets increase strictly from zero and stay inside
the entry region. Parsing bounds both the restart count and directory size.

**S3 Handles** — Offsets and lengths address compressed payloads followed by
a compression byte and CRC32. A directory page's predecessor must end before
the current page. Readers load the directory once and fetch leaves via the
block cache, respecting `fill_cache`.

**S4 Bloom** — The builder hashes each distinct user key once. The key and
prefix filters both require `filter.format = 2`. Eight XXH3-64 probes with
seed zero stay within one aligned 64-byte line. Verify known probes, present
keys and the measured false-positive rate on absent keys.

**S5 Compression** — CRC covers stored bytes and the compression byte. The
reader caps the decoded size before allocating for LZ4 or Zstd.

**S6 Footer** — Decode `TABLE_MAGIC_V2` before dereferencing any handle.

## Review cases

- Check a seek at a data-block separator, between blocks, and at both ends of a leaf. The separator bounds the previous block and sorts before the next block's first key. Equal-user-key boundaries retain the full internal key.
- Check forward and reverse movement across an index leaf and the last point block of an SST ending with a range tombstone.
- Exercise a tombstone crossing a file split at the latest sequence and at each active snapshot. The adjacent fragments share an exclusive split boundary.
- Confirm a point lookup seeks the first range partition whose maximum end exceeds the user key and reads at most one leaf. The MANIFEST range-deletion flag agrees with its directory.
- Check that cached point lookups preserve pinned value storage. A value-free existence check reads only its visibility metadata.
- Validate that malformed entry headers, directory pages, filter markers and block checksums produce typed errors. Retain file ownership through RAII.
