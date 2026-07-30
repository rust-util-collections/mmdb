# MemTable Review Patterns

**Files:** `mod.rs`, `skiplist.rs`, `skiplist_impl.rs`.

**Arch:** Lock-free skiplist, single-writer multi-reader; OrdInternalKey
(user ASC, seq DESC); approx size; separate range-del collection; arena nodes;
`MemTableCursorIter` = level-0 chain via raw ptr for O(1) scan.

## Invariants

**M1 Single writer** — live puts under DB write serialize. Recovery/tests may put
on local unpublished tables only — never concurrent put.

**M2 Concurrent readers** — consistent while writing: link store Release, load Acquire.

**M3 Ordering** — `(user_key ASC, seq DESC)` so latest first per user key.

**M4 Size** — every stored shape updates atomic (key/value bytes, node overhead,
duplicated range-del entry). Severe undercount → late flush/OOM. `approximate_size` = read only.

**M5 Range dels** — dedicated collection + independent iter for trackers.

## Bug patterns

**Bad memory order (tech 6.1)** — Relaxed publish of next before payload visible.
**Arena padding** — track alignment in size.
**Dup full key** — should be impossible (unique seq); define behavior if not.

## Checklist

- [ ] One writer at a time (or unpublished local ownership)
- [ ] Release store / Acquire load on links
- [ ] Comparator user ASC + seq DESC
- [ ] Size all ValueTypes + node + range-del dup
- [ ] Range-del separate iter
- [ ] No reader/writer data races
- [ ] Arena alignment in size
- [ ] Cursor raw ptrs valid (arena, no free-per-node); Send via Arc holding arena
