//! Run with `cargo +nightly miri test --test arena_miri`. No store I/O.

#[cfg(miri)]
#[test]
fn concurrent_arena_pins() {
    mmdb::concurrent_arena_pins();
}
