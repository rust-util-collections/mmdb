# Unsafe Code Audit

## Live concentration

- `skiplist_impl.rs` — nodes, layout
- `skiplist.rs` — cursor deref, `Send`
- `db.rs` — DB Send/Sync, flock, group-commit request pointers
- `table_reader/mod.rs` — `posix_fadvise`

Inventory via search; do not document counts (drift is the signal).
Safe parsing in `block`/`format`/`types` is ordinary review unless new unsafe appears.

## Protocol (add/change/remove unsafe)

### 1. SAFETY comment
- Present and specific (prereqs named)
- Prereqs checkable from nearby code

### 2. UB checklist

| | Check |
|-|-------|
| Null | pointer non-null |
| Dangle | referent outlives use |
| Align | type alignment |
| Race | atomic or mutex |
| Validity | type bit patterns legal |
| Alias | no simultaneous &mut + & |
| Uninit | no read before init |

### 3. Skiplist
- Alloc size = node + key + value
- Link store Release / load Acquire
- height ≤ MAX_HEIGHT
- No free while readers may hold (arena today; epoch/HP if individual free)
- `node_kv` / `node_next0` crate-private but widely call-site paths — keep contracts
- Cursor `Send`: arena never frees nodes; Arc keeps arena

### 4. SST
- Only fadvise unsafe: live owning File·fd. Bad offset/len = robustness, not mem unsafety (advisory).
- Blocks are `Arc<Vec<u8>>` safe code — no cast audit unless new unsafe.

### 5. DB
- `*mut WriteRequest` = stack of blocked writers under write_queue lock; no deref after `done`; no store into DB fields
- `Send`/`Sync`: shared mut only via locks/atomics
- `flock`: valid fd; lock file outlives DB

### 6. Transmute / cast
- No transmute in tree today — any new one is high scrutiny
- Same size/validity/align/lifetime if added

## Risk

| Location | Risk | Why |
|----------|------|-----|
| skiplist_impl | CRITICAL | concurrent raw layout |
| skiplist | HIGH | deref + Send |
| db | HIGH | pointer protocol, Send/Sync, lock |
| table_reader | LOW | advisory fadvise |

## Red flags
No SAFETY · transmute size mismatch · unchecked `from_raw_parts` · Relaxed pointer
publish without another edge · `Box::from_raw` on possibly freed · public unsafe without `# Safety`.
