# Rust Soundness Invariants for Internal `unsafe` Blocks

MMDB is an in-process embedded Rust LSM key-value library. Its `unsafe` blocks
are small, self-contained internal primitives; none are exposed as `pub unsafe`
in `src/lib.rs`.

## Live inventory

Derive the current list via search (`rg '\bunsafe\b' src`) rather than relying
on static line counts:

- `src/memtable/skiplist_impl.rs` — arena-allocated node storage and atomic
  tower links, aligned raw slab allocation/deallocation and `Slab` ownership
- `src/memtable/mod.rs` — `MemTablePin` retains the arena owning its immutable slice
- `src/memtable/skiplist.rs` — level-0 cursor node reads and `Send` impl on
  `MemTableCursorIter`
- `src/db.rs` — the group-commit queue’s `Send` implementation, cooperative `libc::flock` calls on
  open `LOCK` descriptors, advisory Linux WAL writeback, and stack-allocated `WriteRequest` pointers inside
  the group-commit queue
- `src/sst/table_reader/mod.rs` — advisory `libc::posix_fadvise` hint on an open
  SST `File` descriptor
- `src/test_alloc.rs` (`#[cfg(test)]` only) — `GlobalAlloc` wrapper forwarding
  to `std::alloc::System` while updating allocation-free thread-local `Cell`
  counters

Safe byte-slice parsing in `src/sst/block.rs`, `src/sst/format.rs`, and
`src/types.rs` uses checked indexing and `from_le_bytes` rather than `unsafe`.

## Verification checklist (when adding, changing, or reviewing `unsafe`)

### 1. `// SAFETY:` documentation

- Every `unsafe` block and `unsafe impl` carries a specific `// SAFETY:` comment
  naming its exact preconditions.
- Every stated precondition holds locally from surrounding types, locks, or
  atomic orderings.

### 2. Rust soundness invariants

| Property | Invariant to verify |
|----------|---------------------|
| Non-null & initialized | Pointer comes from a completed allocation and initialized fields before publication |
| Lifetime | Backing arena, `Arc`, stack frame, or `File` descriptor outlives every read |
| Alignment | Allocation layout matches `std::alloc::Layout` of the target type |
| Synchronization | Cross-thread visibility goes through `Release`/`Acquire` atomics or a `parking_lot` lock |
| Aliasing | No `&mut T` coexists with shared `&T` references to the same allocation |

### 3. Subsystem invariants

- **Arena skiplist (`skiplist_impl.rs`, `skiplist.rs`)**:
  - The insertion guard serializes bump allocation and splice. Slabs keep a
    fixed address until the last owning table or pin drops.
  - Node fields are fully initialized before level links publish the pointer
    with `Ordering::Release`; readers load links with `Ordering::Acquire` and
    keep `height <= MAX_HEIGHT`.
  - `MemTableCursorIter` retains `Arc<MemTable>`, keeping the skiplist and its
    arena alive for the cursor's entire lifetime across thread moves (`Send`).
  - `cargo +nightly miri test --test arena_miri` exercises concurrent insertion,
    traversal and pinned reads entirely in memory.
- **Group commit & file lock (`db.rs`)**:
  - Each `*mut WriteRequest` points to a writer thread's local stack frame that
    remains parked on `write_cv` under `write_queue` until `done == true`; the
    leader never touches the pointer after setting `done` and releasing the
    queue lock, and no pointer is stored outside the queue.
  - `flock` borrows a live `File` descriptor owned by `DirLock`, and drops
    release the lock before closing the handle.
- **SST readahead (`sst/table_reader/mod.rs`)**:
  - `posix_fadvise` passes `self.file.as_raw_fd()` while `TableReader` owns the
    open `File`; data blocks themselves are safe `Arc<Vec<u8>>` buffers.
- **Test counting allocator (`test_alloc.rs`, `#[cfg(test)]`)**:
  - Forwards exact `(ptr, layout, new_size)` arguments to `System`; `try_with`
    touches only `const`-initialized `Cell` thread-locals so teardown never
    panics or recurses into allocation.

## Review priority

| File | Priority | Focus |
|------|----------|-------|
| `src/memtable/skiplist_impl.rs` | CRITICAL | Arena layout, single-writer precondition, `Release`/`Acquire` publication |
| `src/memtable/skiplist.rs` | HIGH | `Arc<MemTable>` lifetime anchoring raw cursor pointers + `Send` |
| `src/db.rs` | HIGH | Stack `WriteRequest` wait barrier, `Send`/`Sync`, `flock` lifetime |
| `src/test_alloc.rs` | MEDIUM | Exact `Layout` forwarding and non-allocating TLS access in tests |
| `src/sst/table_reader/mod.rs` | LOW | Open `File` lifetime around advisory `posix_fadvise` |
