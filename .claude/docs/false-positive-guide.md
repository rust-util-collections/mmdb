# MMDB False Positive Guide

Before reporting any finding, check it against this guide. If a finding matches a false positive pattern below, either suppress it or explicitly note why it does NOT apply.

---

## FP-1: Rust Ownership System Already Prevents It

**Pattern**: Reporting a use-after-free, double-free, or dangling reference in safe Rust code.
**Rule**: If the code is safe Rust (no `unsafe` block), the borrow checker prevents these at compile time. Only report memory safety issues inside `unsafe` blocks or when raw pointers are involved.
**Exception**: Logical use-after-free (e.g., using an index into a Vec after the Vec has been modified) is still valid even in safe code.

## FP-2: Lock Held Across Entire Operation

**Pattern**: Reporting a TOCTOU race when the relevant Mutex is held for the entire check-then-act sequence.
**Rule**: If you see a potential race condition, FIRST verify that the lock protecting the shared state is held continuously from the check to the act. If it is, the race is impossible.
**How to verify**: Trace the `MutexGuard` or `RwLockGuard` lifetime — it lives until the end of the scope or explicit `drop()`.

## FP-3: ArcSwap Load is Intentionally Point-in-Time

**Pattern**: Reporting that `super_version.load()` might return stale data after a concurrent flush/compaction.
**Rule**: This is by design for the lock-free read path. The loaded SuperVersion is a consistent snapshot at the time of load. Staleness is acceptable because:
- Snapshot-based reads use a pinned sequence number that guarantees correctness
- Non-snapshot reads return any version that was current at some point during the call
**When to report**: Only if the loaded SuperVersion is used to make a decision that requires seeing the LATEST state (e.g., picking compaction inputs).

## FP-4: Unwrap/Expect on Known-Valid State

**Pattern**: Reporting `unwrap()` or `expect()` as potential panics.
**Rule**: Many `unwrap()` calls are on states that are guaranteed by prior logic. Before reporting:
1. Check if the Option/Result is populated by a prior operation in the same scope
2. Check if the calling condition guarantees the value (e.g., `if vec.is_empty() { return; } vec[0].unwrap()`)
3. Check if it's in test code (panics are acceptable in tests)
**When to report**: Only if you can construct a concrete scenario where the unwrap WILL fail in production.

## FP-5: Clippy Would Catch It

**Pattern**: Reporting a lint that `cargo clippy -D warnings` already enforces.
**Rule**: The CI runs clippy with deny-all-warnings. Do not duplicate clippy's work. Focus on semantic correctness that clippy cannot detect.
**Examples of what NOT to report**: unused variables, unnecessary clones, redundant closures, missing `Default` impl.

## FP-6: "Consider" Without Concrete Downside

**Pattern**: Suggesting a refactor, adding error handling, or "defensive" code without identifying a specific failure scenario.
**Rule**: Do not report findings that are purely advisory. Every finding must have a concrete scenario where the current code produces a wrong result, crashes, or leaks resources.
**Bad**: "Consider adding bounds checking here"
**Good**: "When `prefix_len > key.len()`, this will panic at `key[..prefix_len]` — this can happen when a corrupted SST has an invalid prefix length"

## FP-7: Test-Only Code Held to Production Standards

**Pattern**: Reporting error handling, performance, or resource management issues in test code.
**Rule**: Test code has different standards. Acceptable in tests:
- `unwrap()` and `expect()` liberally
- Temporary directory cleanup via `Drop` (no need for explicit checks)
- Blocking operations without timeouts
- Hardcoded constants
**When to report**: Only if the test itself is incorrect (testing the wrong thing) or if test-utils code (`#[cfg(feature = "test-utils")]`) has safety issues that could leak into production.

## FP-8: Intentional Unsafe with Safety Comment

**Pattern**: Reporting an `unsafe` block that has a `// SAFETY:` comment explaining why it's sound.
**Rule**: Read the safety comment first. If the justification is sound and the prerequisites are maintained by the surrounding code, do not report it. Only report if:
1. The safety comment's prerequisites are NOT actually guaranteed
2. A subsequent change has invalidated the safety comment's assumptions
3. The safety comment is missing or vague ("// SAFETY: this is fine")

## FP-9: Performance Issue Without Hot Path Evidence

**Pattern**: Reporting an allocation, clone, or lock acquisition as a performance issue without evidence it's on a hot path.
**Rule**: MMDB has clear hot paths and cold paths:
- **Hot**: get(), iterator next()/prev(), block decode, bloom filter probe
- **Warm**: flush, compaction inner loop
- **Cold**: open(), close(), option parsing, error formatting, WAL recovery
Only report performance issues on hot/warm paths. Cold path performance is not a concern.

## FP-10: Incomplete Concurrency Analysis

**Pattern**: Reporting a race condition based on reading only the code under review, without checking the full locking protocol.
**Rule**: Before reporting any concurrency bug:
1. Identify ALL locks involved (grep for `Mutex`, `RwLock`, `ArcSwap`)
2. Trace the lock acquisition order for the reported scenario
3. Check if a higher-level lock serializes the operation
4. Verify that the `parking_lot` lock semantics match your assumption (parking_lot Mutexes are NOT reentrant)

## FP-11: Compaction Tombstone Retained by Design

**Pattern**: Reporting that compaction is not dropping tombstones "efficiently".
**Rule**: Tombstone retention in non-bottommost compactions is correct and intentional. Tombstones must be written to the output file if:
- The compaction is not at the bottommost level
- Any live snapshot has a sequence number <= tombstone's sequence
- The tombstone covers keys in levels below the output level
Only report tombstone issues if tombstones are dropped when they SHOULD be retained, or retained at the bottommost level when no snapshots reference them.

## FP-12: Off-by-One in Half-Open Intervals — Verify First

**Pattern**: Reporting a range boundary error in `[start, end)` intervals.
**Rule**: MMDB consistently uses half-open intervals `[start, end)`. Before reporting:
1. Verify the actual semantics at the point of use (not just the variable names)
2. Check the comparison operator: `<` is correct for end, `<=` is wrong
3. Check the iterator: `seek(start)` + `while key < end` is correct
**When to report**: Only when you can demonstrate a concrete key that is incorrectly included or excluded.
