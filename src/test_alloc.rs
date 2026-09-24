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
}

fn record(bytes: usize) {
    // `try_with`: thread-local storage may already be torn down while a
    // thread exits, and an allocator must not panic.
    let _ = COUNTING.try_with(|counting| {
        if counting.get() {
            let _ = ALLOCATED.try_with(|total| total.set(total.get() + bytes));
        }
    });
}

// SAFETY: every method forwards its arguments unchanged to `System`, which
// upholds the `GlobalAlloc` contract; counting only touches `Cell`s in
// const-initialized thread-locals, which never allocate.
unsafe impl GlobalAlloc for CountingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        record(layout.size());
        // SAFETY: forwarded from the caller, who upholds `alloc`'s contract.
        unsafe { System.alloc(layout) }
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        record(layout.size());
        // SAFETY: forwarded from the caller, who upholds `alloc_zeroed`'s contract.
        unsafe { System.alloc_zeroed(layout) }
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        record(new_size);
        // SAFETY: forwarded from the caller, who upholds `realloc`'s contract.
        unsafe { System.realloc(ptr, layout, new_size) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        // SAFETY: forwarded from the caller, who upholds `dealloc`'s contract.
        unsafe { System.dealloc(ptr, layout) }
    }
}

#[global_allocator]
static ALLOCATOR: CountingAllocator = CountingAllocator;

/// Run `f` and return its result with the bytes it allocated on this thread.
pub(crate) fn count_allocated<R>(f: impl FnOnce() -> R) -> (R, usize) {
    ALLOCATED.with(|total| total.set(0));
    COUNTING.with(|counting| counting.set(true));
    let result = f();
    COUNTING.with(|counting| counting.set(false));
    (result, ALLOCATED.with(Cell::get))
}
