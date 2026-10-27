# Cache Review Patterns

**Files:** `block_cache.rs`, `table_cache.rs`, `mod.rs`; tests: `shared_cache.rs`.

**Arch:** `BlockCachePool` = shared moka + sharded reverse index. Each `BlockCache`
is a DB member: keys `(member_id, file_number, offset)`. L0 first blocks pinned
member-local (off LRU). `TableCache` coalesces opens; Versions/iters may outlive eviction.

## Invariants

**CA1 Member isolation** — same file#/offset across DBs never share bytes.
Keys always carry member_id; IDs monotonic, never reused after `detach()`.

**CA2 Pin lifecycle** — every L0 pin ends when SST leaves Version or member
detaches. `unpin_file`/`invalidate_file` only after MANIFEST install succeeds.
Replacement pins must not double-count `pinned_*`.

**CA3 Pin fast path** — `get()` skips pin mutex only if `pinned_count == 0`.
Inc count before map insert under mutex; dec after delete under mutex. `Relaxed`
OK — map is truth.

**CA4 Reverse index** — prune on moka evict + bulk invalidate. Same `shard_for`
for add/remove/take_file; `take_member` walks all shards.

**CA5 Detach** — idempotent; own member only; clear pins; permanent bypass.
Re-check `detached` under pin mutex on insert. In-flight unpinned insert may
remain as unreachable LRU-bounded data only — never another member's bytes.

**CA6 TableReader life** — eviction must not invalidate Version/iter Arcs.
Prewarm best-effort; authoritative open errors in `log_and_apply`. Delete →
evict matching table-cache entry.

## Accepted races

Insert vs reverse-index not atomic: invalidation may lag reclaim to LRU.
OK while IDs never reuse and retention stays capacity-bounded.

## Checklist

- [ ] Keys include member + file + offset
- [ ] Member IDs never reused
- [ ] L0 pin/unpin on every successful file removal
- [ ] Pin counters exact under replace/unpin/detach
- [ ] Shard selection + eviction pruning consistent
- [ ] Detach idempotent; siblings unaffected
- [ ] Capacity 0 bypasses all caching
- [ ] TableReader Arc outlives cache eviction
