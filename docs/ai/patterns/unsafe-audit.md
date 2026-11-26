# Rust Soundness Invariants for Internal `unsafe` Blocks

MMDB is an in-process embedded Rust LSM key-value library. Its `unsafe` blocks
are small, self-contained internal primitives; none are exposed as `pub unsafe`
in `src/lib.rs`.

## Live inventory

Derive the current list via search (`rg '\bunsafe\b' src`) rather than relying
on static line counts:

- `src/memtable/skiplist_impl.rs` — `#[repr(C, align(8))] NodeHeader` + inline
  atomic tower + KV payload in raw `NonNull<u8>` arena `Slab`s, checked `Layout`
  allocation/deallocation, and `Send`/`Sync` impls
- `src/memtable/mod.rs` — `MemTablePin` (`PinnedSlice` backing) retaining
  `Arc<MemTable>` across shared zero-copy slice reads (`Send`/`Sync`)
- `src/memtable/skiplist.rs` — level-0/tower cursor reads and `Send` impl on
  `MemTableCursorIter`
- `src/db.rs` — `WriteQueueState` `Send` impl, raw field projections on parked
  stack `WriteRequest`s in group commit, cooperative `libc::flock` on `DirLock`,
  and advisory Linux `libc::sync_file_range` WAL writeback hints
- `src/sst/table_reader/mod.rs` — advisory `libc::posix_fadvise` hint on an open
  SST `File` descriptor
- `src/test_alloc.rs` (`#[cfg(test)]` only) — `GlobalAlloc` wrapper forwarding
  to `std::alloc::System` while updating allocation-free thread-local `Cell`
  counters

Safe byte-slice parsing in `src/sst/block.rs`, `src/sst/filter.rs` (`#[repr(align(64))]`),
`src/sst/format.rs`, and `src/types.rs` uses checked indexing and `from_le_bytes`
rather than `unsafe`.

## Verification checklist (when adding, changing, or reviewing `unsafe`)

### 1. `// SAFETY:` documentation

- Every `unsafe` block and `unsafe impl` carries a specific `// SAFETY:` comment
  naming its exact preconditions.
- Every stated precondition holds locally from surrounding types, locks, or
  atomic orderings.

### 2. Rust & Stacked/Tree Borrows soundness invariants

| Property | Invariant to verify |
|----------|---------------------|
| Non-null & initialized | Pointer comes from a non-zero checked `Layout` (`alloc` null check → `handle_alloc_error`) and all fields/bytes are written before `Release` publication |
| Lifetime | Backing `Arc<MemTable>`, `Arc<Vec<u8>>`, parked stack frame, or `File` descriptor strictly outlives every raw dereference |
| Alignment | `#[repr(C, align(8))] NodeHeader` + `assert!(align_of::<Link>() <= align_of::<NodeHeader>())` + `.pad_to_align()` keep every node and tower link aligned |
| Provenance (Stacked/Tree Borrows) | Arena `Slab`s retain raw `NonNull<u8>` (never `Box`/`Vec` `Unique` retag on move); `node_kv` derives key/value slices from `tower(node)` rather than shrinking provenance through `&NodeHeader`; concurrent bump writes touch strictly disjoint `[offset..offset+size]` ranges; group-commit leader projects individual raw fields (`&(*ptr).batch`, `(*ptr).result = ...`) without forming `&*ptr` or `&mut *ptr` over a shared `WriteRequest` |
| Synchronization | Cross-thread visibility goes through bottom-up `Release`/`Acquire` atomics or a `parking_lot` lock |

### 3. Subsystem invariants

- **Arena skiplist & `MemTablePin` (`skiplist_impl.rs`, `skiplist.rs`, `mod.rs`, `pinned.rs`)**:
  - `arena.lock()` serializes bump allocation and tower splice. Dedicated slabs (`> 256 KiB`) push to `slabs` without mutating the active shared slab's pointer or `offset` (`offset <= capacity`). Slabs stay at fixed addresses until `Slab::drop` runs on the final `Arc<MemTable>` drop.
  - Node header, tower links, key, trailer, and value bytes are initialized via raw pointer writes before level `0..height` links are published bottom-up with `Ordering::Release`; readers load `height`, `head`, `tail`, and links with `Ordering::Acquire`.
  - `MemTablePin` and `MemTableCursorIter` own `Arc<MemTable>`; empty values (`len == 0`) use `LazyValue::Inline(Vec::new())` without pinning slabs/blocks.
  - `cargo +nightly miri test --test arena_miri` exercises concurrent insertion, traversal, and `MemTablePin` reads under Miri.
- **Group commit, file lock, & WAL writeback (`db.rs`)**:
  - Each `*mut WriteRequest` points to a writer thread's live stack frame inside `enqueue_write` until the leader publishes `STATE_DONE` via `(*rp).state.swap(STATE_DONE, Ordering::AcqRel)` (after writing `result` and taking `waiter`). Neither follower nor leader forms `&WriteRequest` or `&mut WriteRequest` while queued; `write_queue` serializes `wake_queue_head` (which clones `waiter` and sets `STATE_WAKE` before `drain_group`) against `drain_group` (which sets `STATE_IN_GROUP`), and no thread dereferences `req_ptr` once `STATE_DONE` is swapped.
  - `flock` and `sync_file_range` borrow live descriptors owned by `DirLock` / `WalWriter` (`i64::try_from` checked offsets).
- **SST readahead (`sst/table_reader/mod.rs`)**:
  - `posix_fadvise` passes `self.file.as_raw_fd()` with checked `i64::try_from` offsets while `TableReader` owns the open `SstFile`; cached data blocks are safe `Arc<Vec<u8>>` buffers.
- **Test counting allocator (`test_alloc.rs`, `#[cfg(test)]`)**:
  - Forwards exact `(ptr, layout, new_size)` arguments to `System`; `try_with` touches only `const`-initialized `Cell` thread-locals so teardown never panics or recurses into allocation.

## Review priority

| File | Priority | Focus |
|------|----------|-------|
| `src/memtable/skiplist_impl.rs` | CRITICAL | Checked `Layout`, `Slab` provenance, dedicated-slab cursor safety, `Release`/`Acquire` tower splice |
| `src/memtable/mod.rs` | HIGH | `MemTablePin` slice provenance and `Arc<MemTable>` retention (`Send`/`Sync`) |
| `src/memtable/skiplist.rs` | HIGH | `Arc<MemTable>` lifetime anchoring raw cursor pointers + `Send` |
| `src/db.rs` | HIGH | Field-projected stack `WriteRequest` barrier, `Send`, `flock` & `sync_file_range` FD lifetimes |
| `src/test_alloc.rs` | MEDIUM | Exact `Layout` forwarding and non-allocating TLS access in tests |
| `src/sst/table_reader/mod.rs` | LOW | Open `File` lifetime and `i64` bounds around advisory `posix_fadvise` |
