# Storage-Engine Maintenance TODO

This file records open maintenance tasks and the evidence needed to resolve them.
Confirmed defects are registered in `docs/audit.md`.

## Open tasks

None.

## Resolved investigation: lazy iterator direction changes

**Disposition: Rejected.** See `docs/audit.md`, `iterator: backward-to-forward
switching skips remaining keys after a saved overshoot`.

`BidiIterator::lazy` preserves the input `DBIterator`'s remaining forward
sequence, including its current seek position. `next()` consumes the front of
that sequence; `next_back()` consumes its back.

With keys `a,b,c,d,e`:

- After `seek_to_last(); prev()`, the current key is `d`. Direct forward
  collection returns `d,e`. Wrapping that position and consuming `e,d` through
  `next_back()` exhausts the remaining sequence, so `next()` correctly returns
  `None` with no iterator error. The earlier expectation of `c,b,a` was incorrect.
- For a fresh iterator, consuming `e,d` through `next_back()` leaves `a,b,c`
  in forward order. To consume the whole range as `e,d,c,b,a`, keep calling
  `next_back()` on a fresh lazy iterator.

The production iterator implementation needs no change for this claim.
Resetting the input position or returning a raw `prev_overshoot` entry would
change the remaining-sequence contract or bypass normal version resolution.
Previous notes about applied production patches and a missing test-helper
import do not describe the current checkout and have been removed.

### Regression coverage

- `tests/integration.rs::test_bidi_reverse_primed_preserves_remaining_entries`
  checks the direct forward sequence and all four two-read direction
  combinations against a `VecDeque` of that sequence, before flush and after
  flush/compaction. Both ends must report exhaustion with no iterator error.
- `tests/integration.rs::test_bidi_backward_then_forward_reads_remaining_in_order`
  checks `next_back() -> e`, `next_back() -> d`, then `next() -> a,b,c`, followed
  by exhaustion in both directions with no iterator error.
- Existing `test_bidi_iterator_interleaved` and
  `lazy_bidi_preserves_the_remaining_frontier` cover repeated direction changes
  and seeked, inspected, or partially consumed input.

### Validation

The CI validation set passed with `RUSTFLAGS=-D warnings`:

```sh
cargo +stable check --all-targets
cargo +stable fmt --all -- --check
cargo +stable clippy --all-targets -- -D warnings
cargo +stable test --lib
cargo +stable test --doc
cargo +stable test --test crash_recovery
cargo +stable test --test e2e_scenarios
cargo +stable test --test integration
cargo +stable test --test proptest_db
cargo +stable test --test bidi_debug
cargo +stable test --test lazy_delete
cargo +stable test --test shared_cache
cargo +stable test --test read_only
```

All 13 commands passed: 459 tests passed, including 67 integration tests and
327 library tests. The read-only helper is intentionally ignored by the normal
test runner and invoked by the passing `strace` acceptance test.

These checks validate the current checkout; they do not establish a full
storage-engine or concurrency audit.
