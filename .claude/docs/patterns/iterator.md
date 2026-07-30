# Iterator Review Patterns

**Files:** `db_iter`, `merge`, `source`, `level_iter`, `bidi_iter`, `range_del`, `mod`.

**Arch:** DBIterator = dedupe + snapshot + tombstone + prefix. MergingIterator =
heap merge (+ single-source fast path). IterSource = peeked adapter. LevelIterator =
lazy L1+. Bidi = direction + heap rebuild. FragmentedRangeTombstoneList =
immutable non-overlapping intervals, O(log T) (compact still uses sweep tracker).

## Invariants

**I1 Forward mono** — `next` advances user key; one latest visible version per key.
**I2 Completeness** — every snapshot-visible key in range once. Sources: active +
imm mems + L0 + L1+.
**I3 Range del** — tomb `[start,end)` seq S hides key with seq S'<S. Every yield
checks fragmented list; correct seq compare.
**I4 Direction switch** — no skip/dup; reposition children + rebuild; current key
rules if unconsumed.
**I5 Prefix** — stop at exact prefix: `key[..n]==prefix`, not loose end bound alone.
**I6 Snapshot** — latest visible version with `seq <= snapshot_seq` (not first only).

## Bug patterns

**Heap after seek** — rebuild after child moves.
**Double yield on reverse** — account for consumed current.
**Seek skips tombstone check** — every post-seek yield still queries list.
**Level file boundary skip** — first key of next file must yield.

## Optimizations to preserve

- `next_into` buffer reuse — no alias live MemTable/SST bytes
- init_heap multi-source `posix_fadvise(WILLNEED)`
- SetBounds → Level/Table iters; upper bound file skip
- SkipPoint without skipping past matches
- Cross-level tombstone: timestamp from L only covers L and deeper
  (`level > source_level` ignored). Guards bottommost seq-zero vs stale deeper
  tomb. Same/shallower levels still cover (mem DeleteRange must hit deeper + same mem Put)
- Sequential fadvise hints; deferred block read via index first_key
- L0 first-block pin for init_heap peek; unpin when leave L0 (incl. trivial move);
  index/bloom live on TableReader, not block_cache
- Atomic L0 counter for write throttle

## Checklist

- [ ] Heap OK after next/prev/seek
- [ ] Same-user-key higher-seq skipped correctly
- [ ] All sources present
- [ ] Fragmented list on every yield
- [ ] Direction switch rebuilds
- [ ] Prefix byte-exact
- [ ] Seek+tombstone checked
- [ ] Arcs pin SV/Mem/SST for iter life
- [ ] next_into alias-safe
- [ ] Bounds/SkipPoint/fadvise correct
- [ ] L0 pins unpinned on file drop
