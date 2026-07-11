# Cache Subsystem Review Patterns

## Files

- `src/cache/block_cache.rs` — shared block-cache pool and member views
- `src/cache/table_cache.rs` — cached `TableReader` handles
- `src/cache/mod.rs` — module boundary
- `tests/shared_cache.rs` — cross-DB isolation coverage

## Architecture

- `BlockCachePool` owns a segmented, capacity-bounded moka cache and sharded
  reverse index.
- Each `BlockCache` is one DB member view. Pool keys are
  `(member_id, file_number, block_offset)`.
- Pinned first blocks of L0 files are member-local and outside the LRU store.
- `TableCache` coalesces concurrent opens and keeps `Arc<TableReader>` handles;
  live Versions/iterators may outlive cache eviction.

## Critical invariants

### INV-CA1: Member isolation

Two DBs may use identical SST file numbers and offsets but must never observe
each other's bytes.

**Check**: Every pool insert/get/invalidate key includes the member ID. Member
IDs are monotonic and never reused after `detach()`.

### INV-CA2: Pinned-block lifecycle

Every pinned L0 block must be removed when its SST leaves the live Version or
when its member detaches.

**Check**: All file-removal paths call `unpin_file()` or
`invalidate_file()` only after the MANIFEST install succeeds. Replacement pins
must adjust `pinned_count`/`pinned_bytes` without double-counting.

### INV-CA3: Pinned fast-path ordering

`get()` may skip the pinned mutex only when `pinned_count == 0`.

**Check**: A new pin increments the counter before map insertion while holding
the pinned mutex; removal decrements only after deleting entries under that
mutex. `Relaxed` is valid because the counter is a hint and the mutex-protected
map is authoritative.

### INV-CA4: Reverse-index boundedness

The reverse index must be pruned on moka eviction and bulk invalidation. All
index operations for one `(member, file)` must choose the same shard.

**Check**: `add`, `remove`, and `take_file` share `shard_for()`;
`take_member()` visits every shard.

### INV-CA5: Detach isolation

`detach()` is idempotent, sweeps only its member, clears pinned accounting, and
turns the view into permanent cache bypass. Another member remains usable.

**Check**: Pinned insertion re-checks `detached` under the pinned mutex. An
in-flight unpinned insert may survive the cutoff only as unreachable,
capacity-bounded LRU data; it must never become visible to another member.

### INV-CA6: Table-reader lifetime

Table-cache eviction must not invalidate readers held by a Version or iterator.
Prewarm is best effort; authoritative open/error handling remains in
`VersionSet::log_and_apply`.

**Check**: Handles are `Arc<TableReader>`, load coalescing returns the typed
error chain, and file deletion evicts the matching table-cache entry.

## Accepted race windows

The moka insert and reverse-index add are intentionally not atomic. A racing
invalidation may miss an offset and delay reclamation until LRU eviction. This
is not wrong-data corruption because file/member IDs are never reused. Report
only if a change breaks those premises or makes retention unbounded.

## Review checklist

- [ ] Pool keys include member, file number, and offset on every path
- [ ] Member IDs are never reused
- [ ] L0 pin/unpin lifecycle covers every successful file removal
- [ ] Pin counters remain exact under replacement, unpin, and detach
- [ ] Reverse-index shard selection and eviction pruning are consistent
- [ ] Detach is idempotent and cannot affect sibling members
- [ ] Capacity 0 bypasses pinned and unpinned caching
- [ ] TableReader Arcs outlive cache eviction safely
