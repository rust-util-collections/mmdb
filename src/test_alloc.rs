//! Test-only allocation counter for hot-path allocation regressions.
//!
//! Counting is per thread and enabled only inside [`count_allocated`], so
//! concurrently running tests do not disturb each other's measurements.

use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;

struct CountingAllocator;

thread_local! {
    static COUNTING: Cell<bool> = const { Cell::new(false) };
    static ALLOCATED: Cell<usize> = const { Cell::new(0) };
    /// Bytes allocated minus bytes freed since counting started; frees of
    /// older allocations can make it negative.
    static LIVE: Cell<isize> = const { Cell::new(0) };
    static PEAK_LIVE: Cell<isize> = const { Cell::new(0) };
}

/// Record `allocated` new bytes and `freed` released bytes.
fn record(allocated: usize, freed: usize) {
    // `try_with`: thread-local storage may already be torn down while a
    // thread exits, and an allocator must not panic.
    let _ = COUNTING.try_with(|counting| {
        if counting.get() {
            let _ = ALLOCATED.try_with(|total| total.set(total.get() + allocated));
            let _ = LIVE.try_with(|live| {
                live.set(live.get() + allocated as isize - freed as isize);
                let _ = PEAK_LIVE.try_with(|peak| peak.set(peak.get().max(live.get())));
            });
        }
    });
}

// SAFETY: every method forwards its arguments unchanged to `System`, which
// upholds the `GlobalAlloc` contract; counting only touches `Cell`s in
// const-initialized thread-locals, which never allocate.
unsafe impl GlobalAlloc for CountingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        record(layout.size(), 0);
        // SAFETY: forwarded from the caller, who upholds `alloc`'s contract.
        unsafe { System.alloc(layout) }
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        record(layout.size(), 0);
        // SAFETY: forwarded from the caller, who upholds `alloc_zeroed`'s contract.
        unsafe { System.alloc_zeroed(layout) }
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        record(new_size, layout.size());
        // SAFETY: forwarded from the caller, who upholds `realloc`'s contract.
        unsafe { System.realloc(ptr, layout, new_size) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        record(0, layout.size());
        // SAFETY: forwarded from the caller, who upholds `dealloc`'s contract.
        unsafe { System.dealloc(ptr, layout) }
    }
}

#[global_allocator]
static ALLOCATOR: CountingAllocator = CountingAllocator;

/// Allocation totals of one [`measure`] call on the current thread.
pub(crate) struct Allocations {
    /// Every byte allocated, including reallocations.
    pub total: usize,
    /// Highest bytes-held-at-once above the starting point.
    pub peak_live: usize,
}

/// Run `f` and return its result with the allocations it made on this thread.
pub(crate) fn measure<R>(f: impl FnOnce() -> R) -> (R, Allocations) {
    ALLOCATED.with(|total| total.set(0));
    LIVE.with(|live| live.set(0));
    PEAK_LIVE.with(|peak| peak.set(0));
    COUNTING.with(|counting| counting.set(true));
    let result = f();
    COUNTING.with(|counting| counting.set(false));
    let allocations = Allocations {
        total: ALLOCATED.with(Cell::get),
        peak_live: PEAK_LIVE.with(Cell::get).max(0) as usize,
    };
    (result, allocations)
}

/// Run `f` and return its result with the bytes it allocated on this thread.
pub(crate) fn count_allocated<R>(f: impl FnOnce() -> R) -> (R, usize) {
    let (result, allocations) = measure(f);
    (result, allocations.total)
}
