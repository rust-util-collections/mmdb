# Unsafe Code Audit Patterns

## Overview
Unsafe code is currently concentrated in:

- `src/memtable/skiplist_impl.rs` — raw skiplist nodes and manual layout;
- `src/memtable/skiplist.rs` — cursor dereferences and `Send` contract;
- `src/db.rs` — DB `Send`/`Sync`, file locking, and group-commit request pointers;
- `src/sst/table_reader/mod.rs` — advisory `posix_fadvise`.

Derive the live inventory with code search at review time. Do not copy exact
counts into documentation: additions/removals are themselves the audit signal.
Safe byte parsing in `block.rs`, `format.rs`, and `types.rs` needs ordinary
correctness review, not an unsafe audit unless new unsafe code is introduced.

## Audit Protocol

For ANY change that adds, modifies, or removes an `unsafe` block:

### Step 1: SAFETY Comment
Every `unsafe` block MUST have a `// SAFETY:` comment. Check:
- [ ] Comment exists and is non-trivial (not just "this is safe")
- [ ] Comment lists specific prerequisites (e.g., "pointer is non-null and aligned")
- [ ] Prerequisites are verifiable from the surrounding code

### Step 2: Undefined Behavior Checklist
Check for ALL categories of undefined behavior in Rust:

| UB Category | Check |
|-------------|-------|
| **Null pointer dereference** | Is the pointer guaranteed non-null? |
| **Dangling pointer** | Does the pointed-to memory outlive the pointer use? |
| **Alignment violation** | Is the pointer properly aligned for the target type? |
| **Data race** | Is the access synchronized (atomic or mutex-protected)? |
| **Invalid value** | Is the value a valid instance of its type (e.g., bool is 0 or 1)? |
| **Aliasing violation** | Is there a `&mut` and `&` to the same data simultaneously? |
| **Uninitialized memory** | Is the memory read without being initialized first? |

### Step 3: Skiplist-Specific Checks
For changes in `skiplist_impl.rs` or `skiplist.rs`:

- **Node allocation**: Verify allocated memory is properly sized for the node + key + value
- **Pointer stores**: Verify `Release` ordering on pointer stores to node links
- **Pointer loads**: Verify `Acquire` ordering on pointer loads from node links
- **Level array**: Verify level count doesn't exceed MAX_HEIGHT
- **Deallocation**: Verify nodes are not freed while any reader may hold a pointer
  - Current pattern: nodes live as long as the skiplist (arena-based, no individual deallocation)
  - If individual deallocation is added: MUST use epoch-based reclamation or hazard pointers
- **Public API**: `pub unsafe fn node_kv` and `pub unsafe fn node_next0` (defined in `skiplist_impl.rs`) are callable from anywhere in the crate (`mod memtable` is private, so external crates cannot reach them). Any change to their signatures or preconditions must update all call sites in `skiplist.rs` and `skiplist_impl.rs`.
- **Cursor Send safety**: `unsafe impl Send for MemTableCursorIter` (skiplist.rs) relies on arena-backed nodes never being individually freed and the iterator holding an `Arc<MemTable>` that keeps the arena alive.

### Step 4: SST-Level Checks
For changes in `table_reader/mod.rs` (the only SST file with unsafe code):

- The unsafe block is a `libc::posix_fadvise` call. Its memory-safety
  prerequisite is a live `File` owning the fd. Invalid/overflowed offset or
  length is still a robustness bug (or debug panic) but not memory unsafety;
  classify it separately and remember the syscall is advisory.
- Block data access in the SST layer is entirely safe code (`Arc<Vec<u8>>`-backed, bounds-checked) — do not assume cache-backed pointer casts exist.

> **Note**: `block.rs`, `format.rs`, and `types.rs` contain **zero** unsafe blocks. All integer parsing in these files uses safe `from_le_bytes()` / `from_be_bytes()`. Block iteration is safe code — no unsafe audit is required.

### Step 5: DB-Level Unsafe Checks
For changes in `db.rs`:

- **Group-commit raw pointers**: `WriteRequest` pointers (`*mut WriteRequest`) are stack addresses of blocked writer threads, pushed into the queue under the `write_queue` lock. Verify every deref happens while the owning writer is still blocked (it must not return until the leader sets `done` under the same lock), and that no pointer is retained after `done` is signaled.
- **`unsafe impl Send/Sync for DB`**: Verify the SAFETY comments' claims still hold — all shared mutable state behind locks/atomics, raw request pointers never stored in `DB` itself.
- **`libc::flock`**: Advisory file lock on a held `File`; verify the fd is valid and the lock file outlives the DB handle.

### Step 6: Transmute/Cast Checks
For any `transmute`, `as *const`, or `as *mut` (note: the codebase currently contains **zero** `transmute` calls — any new one deserves extra scrutiny):

- **Size**: Source and target types must have the same size
- **Validity**: Every bit pattern of the source must be valid for the target type
- **Alignment**: Target alignment must not exceed source alignment
- **Lifetime**: Transmuted references must respect original lifetime bounds

## Risk Classification

| Location | Risk | Reason |
|----------|------|--------|
| skiplist_impl.rs | CRITICAL | Raw pointers, concurrent access, manual memory layout |
| skiplist.rs | HIGH | Raw skiplist pointer dereferences and cursor `Send` contract |
| db.rs | HIGH | Group-commit pointer protocol, DB `Send`/`Sync`, file locking |
| table_reader/mod.rs | LOW | Advisory `posix_fadvise` syscall |

## Red Flags
Report immediately if you see:
- `unsafe` block without SAFETY comment
- `transmute` between types of different sizes
- Raw pointer dereference without a proven non-null/alignment/lifetime invariant
- `slice::from_raw_parts` with unchecked length
- `Relaxed` pointer publication without another proven synchronization edge
- `Box::from_raw` on a pointer that might have been freed
- Public unsafe API without an explicit, sufficient `# Safety` contract
