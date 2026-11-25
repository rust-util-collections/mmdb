# MemTable Review Patterns

**Files:** `mod.rs`, `skiplist.rs`, `skiplist_impl.rs`, `arena_tests.rs`, `src/pinned.rs`; Miri test: `tests/arena_miri.rs`.

**Arch:** Slab-arena `ConcurrentSkipList` (`#[repr(C, align(8))] NodeHeader` + inline atomic tower + key/trailer + value), ordered `(user_key ASC, (seq, ValueType) DESC)`, paired with `RwLock<Vec<MemRangeTombstone>>` and 64-bucket `metadata_buckets` (`AtomicUsize`). `MemTablePin` backs zero-copy `PinnedSlice` reads; `MemTableCursorIter` walks level-0/tower links with `Arc<MemTable>` anchoring slab lifetimes across `Send`.

## Invariants

**M1 Serialized insertion & slab preservation** — `ConcurrentSkipList::insert` holds `arena.lock()` across bump allocation, `search`, raw-pointer node/tower/KV initialization, bottom-up `Ordering::Release` tower stores (`0..height`), `height.fetch_max(Release)`, and `tail.store(Release)`. Nodes `> ARENA_MAX_BLOCK / 4` (`256 KiB`) allocate dedicated slabs without altering the active shared slab's `active` pointer or `offset` cursor.

**M2 Lock-free reader & pin soundness** — readers load `height`, `head`, `tail`, and every tower link with `Ordering::Acquire`. `search` returns the exact `(current, successor)` node that passed its level-0 comparison (never re-loading `current.next[0]`, which could jump to a newly spliced higher-sequence version). `lower_bound(user, &tag)` compares composite `(user, tag)` without key allocation. `Slab` owns raw `NonNull<u8>` allocations freed only in `Slab::drop` after the last `Arc<MemTable>` (including `MemTablePin` and `MemTableCursorIter`) drops.

**M3 Point & range-tombstone semantics** — `get_ref_with_seq` seeks `(user, seq, ValueType::RangeDeletion)` (`2`, highest `ValueType`); because `MemTable::put` drops empty/inverted ranges (`key >= value`), any stored `RangeDeletion` keyed at `user` strictly covers `user` (`[user, end)` with `user < end`) and resolves as deleted at its sequence without skipping to an older `Value`.

**M4 `approximate_size()` & same-key metadata admission** — `MemTable::put` charges full newly reserved slab bytes from `Arena::allocate` (upfront-charging any abandoned active-slab tail on growth plus exact dedicated-slab layouts), plus `(new_cap - old_cap) * size_of::<MemRangeTombstone>() + begin.len() + end.len()` for valid `RangeDeletion`s. Separate admission counters charge `separator + 40` (`max(key, val) + 40` for `RangeDeletion`, `key + 40` for points) identically in `WriteBatch::append`, `MemTable::put` (`metadata_buckets`), `SkipListMemTable::same_key_metadata`, and `DB::admit_same_key_runs` (`META_BLOCK_SPLIT_THRESHOLD`).

**M5 Bidirectional cursor** — `MemTableCursorIter` tracks `exhausted_backward` to distinguish before-the-start `null` from past-the-end `null` across `prev()`, `seek_for_prev()`, and forward resumption (`next` / `next_into` / `next_lazy`).

## Checklist

- [ ] Full node header/tower/KV written via raw pointers before bottom-up `Release` link stores; readers use `Acquire`
- [ ] Dedicated `> 256 KiB` slabs preserve active slab pointer and offset; `offset <= capacity` never underflows
- [ ] `search` returns the compared node rather than re-reading `next[0]`
- [ ] `approximate_size()` charges full reserved slab bytes + `MemRangeTombstone` capacity/endpoints across all `ValueType`s
- [ ] `same_key_metadata` formula matches across `WriteBatch`, `metadata_buckets`, and `admit_same_key_runs`
- [ ] Empty/inverted `RangeDeletion` (`begin >= end`) ignored before `MemRangeTombstone` storage
- [ ] `MemTablePin` and `MemTableCursorIter` retain `Arc<MemTable>`; `cargo +nightly miri test --test arena_miri` stays clean
