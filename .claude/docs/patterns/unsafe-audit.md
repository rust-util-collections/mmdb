# Unsafe Code Audit Patterns

## Overview
MMDB contains ~76 unsafe blocks, concentrated in:
- `src/memtable/skiplist_impl.rs` — lock-free skiplist nodes with raw pointers
- `src/sst/block.rs` — block data parsing from raw bytes
- `src/types.rs` — internal key encoding/decoding
- `src/sst/format.rs` — footer/handle parsing

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
For changes in `skiplist_impl.rs`:

- **Node allocation**: Verify allocated memory is properly sized for the node + key + value
- **Pointer stores**: Verify `Release` ordering on pointer stores to node links
- **Pointer loads**: Verify `Acquire` ordering on pointer loads from node links
- **Level array**: Verify level count doesn't exceed MAX_HEIGHT
- **Deallocation**: Verify nodes are not freed while any reader may hold a pointer
  - Current pattern: nodes live as long as the skiplist (arena-based, no individual deallocation)
  - If individual deallocation is added: MUST use epoch-based reclamation or hazard pointers

### Step 4: Block Parsing Checks
For changes in `block.rs`, `format.rs`:

- **Slice bounds**: Verify all `slice::from_raw_parts` calls have correct length
- **Alignment**: Use `read_unaligned` for integers parsed from byte slices
- **Lifetime**: The resulting `&[u8]` must not outlive the source buffer
- **Bounds**: Validate offset + size <= buffer.len() BEFORE creating the slice

### Step 5: Transmute/Cast Checks
For any `transmute`, `as *const`, or `as *mut`:

- **Size**: Source and target types must have the same size
- **Validity**: Every bit pattern of the source must be valid for the target type
- **Alignment**: Target alignment must not exceed source alignment
- **Lifetime**: Transmuted references must respect original lifetime bounds

## Risk Classification

| Location | Risk | Reason |
|----------|------|--------|
| skiplist_impl.rs | CRITICAL | Raw pointers, concurrent access, manual memory layout |
| block.rs | HIGH | Raw byte parsing, slice construction from offsets |
| types.rs | MEDIUM | Integer encode/decode, but bounded by key format |
| format.rs | MEDIUM | Footer parsing, bounded by fixed-size format |

## Red Flags
Report immediately if you see:
- `unsafe` block without SAFETY comment
- `transmute` between types of different sizes
- Raw pointer dereference without null check
- `slice::from_raw_parts` with unchecked length
- `Relaxed` ordering on pointer stores in concurrent code
- `Box::from_raw` on a pointer that might have been freed
- Any `unsafe` in a public API (external callers can trigger UB)
