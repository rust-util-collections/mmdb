//! A write-serialized, read-concurrent skip list with arena allocation.
//!
//! The DB's group commit model guarantees a single leader writes to the memtable
//! at any time (under write_queue lock), while reads happen concurrently via
//! shared references.  This means:
//! - **Single-writer**: `&self` insert, serialized externally by the DB write_queue lock
//! - **Concurrent readers**: `&self` iter/get/range, lock-free via atomic pointers
//!
//! Nodes are arena-allocated in contiguous blocks for cache-friendly level-0
//! traversal. Each node carries an inline `[AtomicPtr; MAX_HEIGHT]` array,
//! eliminating a separate heap allocation for next-pointers.
//!
//! Max height 12, probability p = 0.25.

use std::cell::UnsafeCell;
use std::ops::RangeBounds;
use std::ptr;
use std::sync::atomic::{AtomicPtr, AtomicUsize, Ordering};

/// Maximum height of the skip list.
const MAX_HEIGHT: usize = 12;

/// Initial arena block size.
const ARENA_INITIAL_BLOCK: usize = 4096;

/// Maximum arena block size (blocks double up to this cap).
const ARENA_MAX_BLOCK: usize = 1 << 20; // 1MB

// ---------------------------------------------------------------------------
// Arena
// ---------------------------------------------------------------------------

/// Bump-pointer arena that allocates from doubling blocks (4KB → 1MB cap).
///
/// Only mutated during `insert()` (single-writer); readers follow published
/// pointers. No per-allocation free — memory is released when the Arena drops.
struct Arena {
    blocks: UnsafeCell<Vec<Vec<u8>>>,
    current_offset: UnsafeCell<usize>,
    bytes_allocated: AtomicUsize,
}

impl Arena {
    fn new() -> Self {
        Self {
            blocks: UnsafeCell::new(Vec::new()),
            current_offset: UnsafeCell::new(0),
            bytes_allocated: AtomicUsize::new(0),
        }
    }

    /// Allocate `size` bytes with given alignment from the arena.
    ///
    /// # Safety
    /// Must be called under external write serialization (single-writer).
    unsafe fn alloc(&self, size: usize, align: usize) -> *mut u8 {
        unsafe {
            let blocks = &mut *self.blocks.get();
            let offset = &mut *self.current_offset.get();

            // Try current block.
            if let Some(block) = blocks.last() {
                let base = block.as_ptr() as usize;
                let aligned = (base + *offset + align - 1) & !(align - 1);
                let new_offset = aligned - base + size;
                if new_offset <= block.capacity() {
                    *offset = new_offset;
                    self.bytes_allocated.fetch_add(size, Ordering::Relaxed);
                    return aligned as *mut u8;
                }
            }

            // New block with doubling growth.
            let prev_cap = blocks.last().map_or(0, Vec::capacity);
            let block_size = if prev_cap == 0 {
                ARENA_INITIAL_BLOCK
            } else {
                (prev_cap * 2).min(ARENA_MAX_BLOCK)
            }
            .max(size + align);

            let block = Vec::<u8>::with_capacity(block_size);
            let base = block.as_ptr() as usize;
            let aligned = (base + align - 1) & !(align - 1);
            *offset = aligned - base + size;
            self.bytes_allocated.fetch_add(size, Ordering::Relaxed);
            blocks.push(block);
            aligned as *mut u8
        }
    }

    /// Allocate space for one `Node<K, V>`.
    ///
    /// # Safety
    /// Same single-writer requirement as `alloc`.
    unsafe fn alloc_node<K, V>(&self) -> *mut Node<K, V> {
        unsafe {
            self.alloc(
                std::mem::size_of::<Node<K, V>>(),
                std::mem::align_of::<Node<K, V>>(),
            ) as *mut Node<K, V>
        }
    }
}

// ---------------------------------------------------------------------------
// Node
// ---------------------------------------------------------------------------

/// A node in the skip list.
///
/// `#[repr(C)]` ensures key+value are laid out first (cache-hot during
/// level-0 traversal), followed by the fixed-size pointer tower.
#[repr(C)]
struct Node<K, V> {
    key: K,
    value: V,
    height: u8,
    /// next[i] is the pointer to the next node at level i.
    /// Levels `height..MAX_HEIGHT` are unused (always null).
    next: [AtomicPtr<Node<K, V>>; MAX_HEIGHT],
    /// Backward pointer at level 0 only. Enables O(1) `prev()` instead
    /// of O(log N) `seek_lt_raw()`.
    prev0: AtomicPtr<Node<K, V>>,
}

// ---------------------------------------------------------------------------
// ConcurrentSkipList
// ---------------------------------------------------------------------------

/// A concurrent skip list with single-writer / multi-reader semantics.
///
/// Insert is `&self` (externally serialized by the caller).
/// Get / iter / range are `&self` (lock-free).
pub struct ConcurrentSkipList<K: Ord + Clone, V: Clone> {
    /// Head pointers for each level.
    head: [AtomicPtr<Node<K, V>>; MAX_HEIGHT],
    /// Pointer to the last node at level 0. Enables O(1) `tail_ptr()`.
    tail: AtomicPtr<Node<K, V>>,
    /// Number of entries.
    len: AtomicUsize,
    /// Current maximum height in the list.
    max_height: AtomicUsize,
    /// All allocated node pointers — needed to run destructors (K, V may own
    /// heap data). Only mutated under writer serialization.
    all_nodes: UnsafeCell<Vec<*mut Node<K, V>>>,
    /// Arena backing store for all nodes.
    arena: Arena,
}

// SAFETY: Node pointers are stable (arena-allocated, never moved).
// Single-writer guarantee ensures no concurrent mutations. Readers only follow
// AtomicPtr chains that are fully initialized before publication (Release/Acquire).
unsafe impl<K: Ord + Clone + Send, V: Clone + Send> Send for ConcurrentSkipList<K, V> {}
unsafe impl<K: Ord + Clone + Send + Sync, V: Clone + Send + Sync> Sync
    for ConcurrentSkipList<K, V>
{
}

impl<K: Ord + Clone, V: Clone> ConcurrentSkipList<K, V> {
    pub fn new() -> Self {
        Self {
            head: std::array::from_fn(|_| AtomicPtr::new(ptr::null_mut())),
            tail: AtomicPtr::new(ptr::null_mut()),
            len: AtomicUsize::new(0),
            max_height: AtomicUsize::new(1),
            all_nodes: UnsafeCell::new(Vec::new()),
            arena: Arena::new(),
        }
    }

    /// Insert a key-value pair. Must be called under external serialization.
    pub fn insert(&self, key: K, value: V) {
        let height = random_height();
        let cur_max = self.max_height.load(Ordering::Relaxed);
        if height > cur_max {
            self.max_height.store(height, Ordering::Relaxed);
        }

        // Build predecessor array: prev[i] = pointer to the node that should
        // precede the new node at level i (null means head).
        let mut prev: [*mut Node<K, V>; MAX_HEIGHT] = [ptr::null_mut(); MAX_HEIGHT];

        let max_h = height.max(cur_max);
        for level in (0..max_h).rev() {
            // Start walking from the predecessor at the level above (or head).
            let mut current: *mut Node<K, V> = if level + 1 < max_h && !prev[level + 1].is_null() {
                prev[level + 1]
            } else {
                ptr::null_mut()
            };

            if current.is_null() {
                // Walk from head at this level.
                let mut next = self.head[level].load(Ordering::Acquire);
                while !next.is_null() {
                    // SAFETY: next is a valid node published via Release.
                    let node = unsafe { &*next };
                    if node.key >= key {
                        break;
                    }
                    current = next;
                    next = node.next[level].load(Ordering::Acquire);
                }
                prev[level] = current;
            } else {
                // Continue from predecessor of upper level.
                // SAFETY: current is a valid node (was set above or from upper level).
                let mut next = unsafe { &*current }.next[level].load(Ordering::Acquire);
                while !next.is_null() {
                    let node = unsafe { &*next };
                    if node.key >= key {
                        break;
                    }
                    current = next;
                    next = node.next[level].load(Ordering::Acquire);
                }
                prev[level] = current;
            }
        }

        // Arena-allocate and initialize the new node.
        let new_node: *mut Node<K, V> = unsafe { self.arena.alloc_node() };
        unsafe {
            ptr::write(
                new_node,
                Node {
                    key,
                    value,
                    height: height as u8,
                    next: std::array::from_fn(|_| AtomicPtr::new(ptr::null_mut())),
                    prev0: AtomicPtr::new(ptr::null_mut()),
                },
            );
        }

        // Track for destructor.
        // SAFETY: single-writer — no concurrent mutation of all_nodes.
        unsafe {
            (*self.all_nodes.get()).push(new_node);
        }

        // Link the new node into each level (and maintain prev0 at level 0).
        let new_ref = unsafe { &*new_node };
        #[allow(clippy::needless_range_loop)]
        for level in 0..height {
            if prev[level].is_null() {
                // New node becomes the first at this level.
                let old_head = self.head[level].load(Ordering::Relaxed);
                new_ref.next[level].store(old_head, Ordering::Relaxed);
                // Level-0 backward pointer maintenance
                if level == 0 {
                    new_ref.prev0.store(ptr::null_mut(), Ordering::Relaxed);
                    if !old_head.is_null() {
                        unsafe { &*old_head }
                            .prev0
                            .store(new_node, Ordering::Release);
                    }
                }
                self.head[level].store(new_node, Ordering::Release);
            } else {
                // SAFETY: prev[level] is a valid node.
                let p = unsafe { &*prev[level] };
                let old_next = p.next[level].load(Ordering::Relaxed);
                new_ref.next[level].store(old_next, Ordering::Relaxed);
                // Level-0 backward pointer maintenance
                if level == 0 {
                    new_ref.prev0.store(prev[level], Ordering::Relaxed);
                    if !old_next.is_null() {
                        unsafe { &*old_next }
                            .prev0
                            .store(new_node, Ordering::Release);
                    }
                }
                p.next[level].store(new_node, Ordering::Release);
            }
        }

        // Update tail pointer: if new node has no level-0 successor, it is the
        // new tail.
        if new_ref.next[0].load(Ordering::Relaxed).is_null() {
            self.tail.store(new_node, Ordering::Release);
        }

        self.len.fetch_add(1, Ordering::Relaxed);
    }

    /// Look up a key. Lock-free.
    pub fn get<Q>(&self, key: &Q) -> Option<V>
    where
        K: std::borrow::Borrow<Q>,
        Q: Ord + ?Sized,
    {
        let max_h = self.max_height.load(Ordering::Acquire);
        let mut current: *const Node<K, V> = ptr::null();

        for level in (0..max_h).rev() {
            let mut next = if current.is_null() {
                self.head[level].load(Ordering::Acquire)
            } else {
                // SAFETY: current is a valid published node.
                unsafe { &*current }.next[level].load(Ordering::Acquire)
            };

            while !next.is_null() {
                let n = unsafe { &*next };
                match n.key.borrow().cmp(key) {
                    std::cmp::Ordering::Less => {
                        current = next;
                        next = n.next[level].load(Ordering::Acquire);
                    }
                    std::cmp::Ordering::Equal => return Some(n.value.clone()),
                    std::cmp::Ordering::Greater => break,
                }
            }
        }
        None
    }

    /// Find the first entry with key >= `target` using O(log N) skiplist
    /// traversal. Returns `(key, value)` or None if no such entry exists.
    pub fn lower_bound(&self, target: &K) -> Option<(K, V)> {
        let max_h = self.max_height.load(Ordering::Acquire);
        let mut current: *const Node<K, V> = ptr::null();
        let mut candidate: *const Node<K, V> = ptr::null();

        for level in (0..max_h).rev() {
            let mut next = if current.is_null() {
                self.head[level].load(Ordering::Acquire)
            } else {
                unsafe { &*current }.next[level].load(Ordering::Acquire)
            };

            while !next.is_null() {
                let n = unsafe { &*next };
                if n.key < *target {
                    current = next;
                    next = n.next[level].load(Ordering::Acquire);
                } else {
                    // n.key >= target — this is a candidate
                    candidate = next;
                    break;
                }
            }
        }

        // After the level traversal, we need to check level 0 from `current`
        // to find the exact first node >= target.
        let start = if current.is_null() {
            self.head[0].load(Ordering::Acquire)
        } else {
            unsafe { &*current }.next[0].load(Ordering::Acquire)
        };

        let mut ptr = start;
        while !ptr.is_null() {
            let n = unsafe { &*ptr };
            if n.key >= *target {
                return Some((n.key.clone(), n.value.clone()));
            }
            ptr = n.next[0].load(Ordering::Acquire);
        }

        // Fall back to candidate if the level-0 walk didn't find anything
        if !candidate.is_null() {
            let n = unsafe { &*candidate };
            return Some((n.key.clone(), n.value.clone()));
        }

        None
    }

    /// Iterate forward from the first entry with key >= `target`.
    /// Uses O(log N) seek then O(1) per-entry level-0 traversal.
    /// Collects entries starting from the seek point (not the entire list).
    pub fn range_from(&self, target: &K) -> Vec<(K, V)> {
        let max_h = self.max_height.load(Ordering::Acquire);
        let mut current: *const Node<K, V> = ptr::null();

        // Seek using skip list levels
        for level in (0..max_h).rev() {
            let mut next = if current.is_null() {
                self.head[level].load(Ordering::Acquire)
            } else {
                unsafe { &*current }.next[level].load(Ordering::Acquire)
            };

            while !next.is_null() {
                let n = unsafe { &*next };
                if n.key < *target {
                    current = next;
                    next = n.next[level].load(Ordering::Acquire);
                } else {
                    break;
                }
            }
        }

        // Walk level 0 from the found position
        let start = if current.is_null() {
            self.head[0].load(Ordering::Acquire)
        } else {
            unsafe { &*current }.next[0].load(Ordering::Acquire)
        };

        let mut result = Vec::new();
        let mut ptr = start;
        while !ptr.is_null() {
            let n = unsafe { &*ptr };
            if n.key >= *target {
                result.push((n.key.clone(), n.value.clone()));
            }
            ptr = n.next[0].load(Ordering::Acquire);
        }
        result
    }

    /// Return a snapshot iterator over all entries, sorted by key.
    /// Entries are collected at creation time (safe for concurrent modification).
    pub fn iter(&self) -> SkipListIter<K, V> {
        let entries = self.collect_all();
        let len = entries.len();
        SkipListIter {
            entries,
            front: 0,
            back_exclusive: len,
        }
    }

    /// Return a snapshot iterator over the given range, sorted by key.
    pub fn range<R: RangeBounds<K>>(&self, bounds: R) -> SkipListIter<K, V> {
        let all = self.collect_all();
        let entries: Vec<(K, V)> = all
            .into_iter()
            .filter(|(k, _)| bounds.contains(k))
            .collect();
        let len = entries.len();
        SkipListIter {
            entries,
            front: 0,
            back_exclusive: len,
        }
    }

    /// Access key and value of a node by opaque pointer. Zero-copy.
    ///
    /// # Safety
    /// `ptr` must be a valid non-null node pointer from this skiplist.
    pub unsafe fn node_kv(&self, ptr: *const ()) -> (&K, &V) {
        unsafe {
            let node = &*(ptr as *const Node<K, V>);
            (&node.key, &node.value)
        }
    }

    /// Get the next level-0 pointer from a node. Returns null if end.
    ///
    /// # Safety
    /// `ptr` must be a valid non-null node pointer from this skiplist.
    pub unsafe fn node_next0(&self, ptr: *const ()) -> *const () {
        unsafe {
            let node = &*(ptr as *const Node<K, V>);
            node.next[0].load(Ordering::Acquire) as *const ()
        }
    }

    /// Follow the level-0 backward pointer. O(1).
    ///
    /// # Safety
    /// `ptr` must be a valid node pointer obtained from this skiplist.
    pub unsafe fn node_prev0(&self, ptr: *const ()) -> *const () {
        unsafe {
            let node = &*(ptr as *const Node<K, V>);
            node.prev0.load(Ordering::Acquire) as *const ()
        }
    }

    /// Return a raw pointer to the first level-0 node (for cursor iteration).
    pub fn head_ptr(&self) -> *const () {
        self.head[0].load(Ordering::Acquire) as *const ()
    }

    /// Seek to first entry >= target, returning raw node pointer. O(log N).
    pub fn seek_ge_raw(&self, target: &K) -> *const () {
        let max_h = self.max_height.load(Ordering::Acquire);
        let mut current: *const Node<K, V> = ptr::null();

        for level in (0..max_h).rev() {
            let mut next = if current.is_null() {
                self.head[level].load(Ordering::Acquire)
            } else {
                unsafe { &*current }.next[level].load(Ordering::Acquire)
            };

            while !next.is_null() {
                let n = unsafe { &*next };
                if n.key < *target {
                    current = next;
                    next = n.next[level].load(Ordering::Acquire);
                } else {
                    break;
                }
            }
        }

        let start = if current.is_null() {
            self.head[0].load(Ordering::Acquire)
        } else {
            unsafe { &*current }.next[0].load(Ordering::Acquire)
        };

        // Walk level-0 to find exact first >= target
        let mut ptr = start;
        while !ptr.is_null() {
            let n = unsafe { &*ptr };
            if n.key >= *target {
                return ptr as *const ();
            }
            ptr = n.next[0].load(Ordering::Acquire);
        }
        ptr::null()
    }

    /// Seek to last entry < target, returning raw node pointer. O(log N).
    ///
    /// Returns null if no entry < target exists (i.e. target <= first key).
    pub fn seek_lt_raw(&self, target: &K) -> *const () {
        let max_h = self.max_height.load(Ordering::Acquire);
        if max_h == 0 {
            return ptr::null();
        }

        // `current` tracks the last node at each level with key < target.
        // After descending all levels, `current` is the answer.
        let mut current: *const Node<K, V> = ptr::null();

        for level in (0..max_h).rev() {
            let mut next = if current.is_null() {
                self.head[level].load(Ordering::Acquire)
            } else {
                unsafe { &*current }.next[level].load(Ordering::Acquire)
            };

            while !next.is_null() {
                let n = unsafe { &*next };
                if n.key < *target {
                    current = next;
                    next = n.next[level].load(Ordering::Acquire);
                } else {
                    break;
                }
            }
        }

        if current.is_null() {
            ptr::null()
        } else {
            current as *const ()
        }
    }

    /// Seek to last entry <= target, returning raw node pointer. O(log N).
    ///
    /// Returns null if no entry <= target exists (i.e. target < first key).
    pub fn seek_le_raw(&self, target: &K) -> *const () {
        // First try exact seek_ge
        let ge_ptr = self.seek_ge_raw(target);
        if !ge_ptr.is_null() {
            let (k, _) = unsafe { self.node_kv(ge_ptr) };
            if k == target {
                return ge_ptr;
            }
        }
        // No exact match; fall back to largest key < target
        self.seek_lt_raw(target)
    }

    /// Return a raw pointer to the last level-0 node. O(1) via cached tail.
    ///
    /// Returns null if the skip list is empty.
    pub fn tail_ptr(&self) -> *const () {
        self.tail.load(Ordering::Acquire) as *const ()
    }

    /// Number of entries.
    pub fn len(&self) -> usize {
        self.len.load(Ordering::Relaxed)
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    // -- Internal helpers --

    /// Collect all entries at level 0 (the bottom level has all entries).
    fn collect_all(&self) -> Vec<(K, V)> {
        let mut result = Vec::new();
        let mut ptr = self.head[0].load(Ordering::Acquire);
        while !ptr.is_null() {
            // SAFETY: ptr is a valid published node.
            let node = unsafe { &*ptr };
            result.push((node.key.clone(), node.value.clone()));
            ptr = node.next[0].load(Ordering::Acquire);
        }
        result
    }
}

impl<K: Ord + Clone, V: Clone> Default for ConcurrentSkipList<K, V> {
    fn default() -> Self {
        Self::new()
    }
}

impl<K: Ord + Clone, V: Clone> Drop for ConcurrentSkipList<K, V> {
    fn drop(&mut self) {
        // Run destructors for K and V (they may own heap data, e.g. Vec<u8>).
        // Arena frees the node memory itself when it drops.
        let nodes = self.all_nodes.get_mut();
        for &node_ptr in nodes.iter() {
            unsafe {
                ptr::drop_in_place(node_ptr);
            }
        }
    }
}

/// Random height using geometric distribution with p = 0.25.
fn random_height() -> usize {
    let mut h = 1;
    while h < MAX_HEIGHT && cheap_random_bool() {
        h += 1;
    }
    h
}

/// Returns true ~25% of the time using a fast thread-local RNG.
fn cheap_random_bool() -> bool {
    thread_local! {
        static STATE: std::cell::Cell<u64> = std::cell::Cell::new(
            {
                let x = 0u8;
                let addr = &x as *const u8 as u64;
                addr ^ 0x517cc1b727220a95
            }
        );
    }
    STATE.with(|s| {
        let mut x = s.get();
        // xorshift64
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        s.set(x);
        (x & 3) == 0 // 25% probability
    })
}

/// Snapshot iterator over skip list entries. Supports `DoubleEndedIterator`.
pub struct SkipListIter<K, V> {
    entries: Vec<(K, V)>,
    front: usize,
    /// Exclusive upper bound.
    back_exclusive: usize,
}

impl<K: Clone, V: Clone> Iterator for SkipListIter<K, V> {
    type Item = (K, V);

    fn next(&mut self) -> Option<Self::Item> {
        if self.front < self.back_exclusive {
            let item = self.entries[self.front].clone();
            self.front += 1;
            Some(item)
        } else {
            None
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let remaining = self.back_exclusive.saturating_sub(self.front);
        (remaining, Some(remaining))
    }
}

impl<K: Clone, V: Clone> DoubleEndedIterator for SkipListIter<K, V> {
    fn next_back(&mut self) -> Option<Self::Item> {
        if self.back_exclusive > self.front {
            self.back_exclusive -= 1;
            Some(self.entries[self.back_exclusive].clone())
        } else {
            None
        }
    }
}

impl<K: Clone, V: Clone> ExactSizeIterator for SkipListIter<K, V> {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_insert_and_get() {
        let sl = ConcurrentSkipList::new();
        sl.insert(10, "ten");
        sl.insert(5, "five");
        sl.insert(20, "twenty");

        assert_eq!(sl.get(&10), Some("ten"));
        assert_eq!(sl.get(&5), Some("five"));
        assert_eq!(sl.get(&20), Some("twenty"));
        assert_eq!(sl.get(&1), None);
        assert_eq!(sl.len(), 3);
    }

    #[test]
    fn test_forward_iter() {
        let sl = ConcurrentSkipList::new();
        for i in (0..10).rev() {
            sl.insert(i, i * 10);
        }
        let items: Vec<_> = sl.iter().collect();
        assert_eq!(items.len(), 10);
        for (i, (k, v)) in items.iter().enumerate() {
            assert_eq!(*k, i as i32);
            assert_eq!(*v, (i as i32) * 10);
        }
    }

    #[test]
    fn test_reverse_iter() {
        let sl = ConcurrentSkipList::new();
        for i in 0..5 {
            sl.insert(i, i);
        }
        let items: Vec<_> = sl.iter().rev().collect();
        assert_eq!(items, vec![(4, 4), (3, 3), (2, 2), (1, 1), (0, 0)]);
    }

    #[test]
    fn test_bidirectional_iter() {
        let sl = ConcurrentSkipList::new();
        for i in 0..6 {
            sl.insert(i, i);
        }
        let mut it = sl.iter();
        assert_eq!(it.next(), Some((0, 0)));
        assert_eq!(it.next_back(), Some((5, 5)));
        assert_eq!(it.next(), Some((1, 1)));
        assert_eq!(it.next_back(), Some((4, 4)));
        assert_eq!(it.next(), Some((2, 2)));
        assert_eq!(it.next_back(), Some((3, 3)));
        assert_eq!(it.next(), None);
        assert_eq!(it.next_back(), None);
    }

    #[test]
    fn test_range() {
        let sl = ConcurrentSkipList::new();
        for i in 0..10 {
            sl.insert(i, i);
        }
        let items: Vec<_> = sl.range(3..7).collect();
        assert_eq!(items, vec![(3, 3), (4, 4), (5, 5), (6, 6)]);

        let items: Vec<_> = sl.range(3..=7).collect();
        assert_eq!(items, vec![(3, 3), (4, 4), (5, 5), (6, 6), (7, 7)]);

        let items: Vec<_> = sl.range(..3).collect();
        assert_eq!(items, vec![(0, 0), (1, 1), (2, 2)]);
    }

    #[test]
    fn test_empty() {
        let sl: ConcurrentSkipList<i32, i32> = ConcurrentSkipList::new();
        assert_eq!(sl.len(), 0);
        assert!(sl.is_empty());
        assert_eq!(sl.get(&0), None);
        assert_eq!(sl.iter().next(), None);
        assert_eq!(sl.iter().next_back(), None);
    }

    #[test]
    fn test_single_entry() {
        let sl = ConcurrentSkipList::new();
        sl.insert(42, "answer");
        assert_eq!(sl.len(), 1);
        assert_eq!(sl.get(&42), Some("answer"));

        let mut it = sl.iter();
        assert_eq!(it.next(), Some((42, "answer")));
        assert_eq!(it.next(), None);

        let mut it = sl.iter();
        assert_eq!(it.next_back(), Some((42, "answer")));
        assert_eq!(it.next_back(), None);
    }

    #[test]
    fn test_large_dataset() {
        let sl = ConcurrentSkipList::new();
        for i in (0..10_000).rev() {
            sl.insert(i, i);
        }
        assert_eq!(sl.len(), 10_000);

        let items: Vec<_> = sl.iter().collect();
        assert_eq!(items.len(), 10_000);
        for (i, item) in items.iter().enumerate() {
            assert_eq!(*item, (i as i32, i as i32));
        }

        let rev: Vec<_> = sl.iter().rev().collect();
        assert_eq!(rev.len(), 10_000);
        assert_eq!(rev[0], (9999, 9999));
        assert_eq!(rev[9999], (0, 0));
    }

    #[test]
    fn test_concurrent_reads() {
        use std::sync::Arc;
        use std::thread;

        let sl = Arc::new(ConcurrentSkipList::new());
        // Single writer inserts all entries first.
        for i in 0..1000 {
            sl.insert(i, i * 2);
        }

        // Multiple concurrent readers.
        let mut handles = Vec::new();
        for _ in 0..8 {
            let sl = Arc::clone(&sl);
            handles.push(thread::spawn(move || {
                for i in 0..1000 {
                    assert_eq!(sl.get(&i), Some(i * 2));
                }
                let items: Vec<_> = sl.iter().collect();
                assert_eq!(items.len(), 1000);
            }));
        }
        for h in handles {
            h.join().unwrap();
        }
    }

    #[test]
    fn test_vec_u8_keys() {
        let sl = ConcurrentSkipList::new();
        sl.insert(b"banana".to_vec(), b"yellow".to_vec());
        sl.insert(b"apple".to_vec(), b"red".to_vec());
        sl.insert(b"cherry".to_vec(), b"dark_red".to_vec());

        let items: Vec<_> = sl.iter().collect();
        assert_eq!(items.len(), 3);
        assert_eq!(items[0].0, b"apple");
        assert_eq!(items[1].0, b"banana");
        assert_eq!(items[2].0, b"cherry");

        assert_eq!(sl.get(b"banana".as_slice()), Some(b"yellow".to_vec()));
    }

    #[test]
    fn test_seek_lt_raw() {
        let sl = ConcurrentSkipList::new();
        for i in [10, 20, 30, 40, 50] {
            sl.insert(i, i * 10);
        }

        // seek_lt(30) should return 20
        let ptr = sl.seek_lt_raw(&30);
        assert!(!ptr.is_null());
        let (k, v) = unsafe { sl.node_kv(ptr) };
        assert_eq!(*k, 20);
        assert_eq!(*v, 200);

        // seek_lt(10) should return null (nothing less than first entry)
        let ptr = sl.seek_lt_raw(&10);
        assert!(ptr.is_null());

        // seek_lt(11) should return 10
        let ptr = sl.seek_lt_raw(&11);
        assert!(!ptr.is_null());
        let (k, _) = unsafe { sl.node_kv(ptr) };
        assert_eq!(*k, 10);

        // seek_lt(51) should return 50
        let ptr = sl.seek_lt_raw(&51);
        assert!(!ptr.is_null());
        let (k, _) = unsafe { sl.node_kv(ptr) };
        assert_eq!(*k, 50);

        // seek_lt(0) should return null
        let ptr = sl.seek_lt_raw(&0);
        assert!(ptr.is_null());
    }

    #[test]
    fn test_seek_le_raw() {
        let sl = ConcurrentSkipList::new();
        for i in [10, 20, 30, 40, 50] {
            sl.insert(i, i);
        }

        // seek_le(30) should return 30 (exact match)
        let ptr = sl.seek_le_raw(&30);
        assert!(!ptr.is_null());
        let (k, _) = unsafe { sl.node_kv(ptr) };
        assert_eq!(*k, 30);

        // seek_le(25) should return 20 (no exact, falls back to lt)
        let ptr = sl.seek_le_raw(&25);
        assert!(!ptr.is_null());
        let (k, _) = unsafe { sl.node_kv(ptr) };
        assert_eq!(*k, 20);

        // seek_le(9) should return null (nothing <= 9)
        let ptr = sl.seek_le_raw(&9);
        assert!(ptr.is_null());
    }

    #[test]
    fn test_tail_ptr() {
        let sl = ConcurrentSkipList::new();

        // Empty skiplist
        assert!(sl.tail_ptr().is_null());

        sl.insert(10, 100);
        sl.insert(5, 50);
        sl.insert(20, 200);

        let ptr = sl.tail_ptr();
        assert!(!ptr.is_null());
        let (k, v) = unsafe { sl.node_kv(ptr) };
        assert_eq!(*k, 20);
        assert_eq!(*v, 200);
    }

    #[test]
    fn test_seek_lt_raw_large() {
        let sl = ConcurrentSkipList::new();
        for i in (0..1000).rev() {
            sl.insert(i * 2, i); // even numbers 0,2,4,...,1998
        }

        // seek_lt(500) should return 498
        let ptr = sl.seek_lt_raw(&500);
        assert!(!ptr.is_null());
        let (k, _) = unsafe { sl.node_kv(ptr) };
        assert_eq!(*k, 498);

        // seek_lt(1) should return 0
        let ptr = sl.seek_lt_raw(&1);
        assert!(!ptr.is_null());
        let (k, _) = unsafe { sl.node_kv(ptr) };
        assert_eq!(*k, 0);

        // seek_lt(1999) should return 1998
        let ptr = sl.seek_lt_raw(&1999);
        assert!(!ptr.is_null());
        let (k, _) = unsafe { sl.node_kv(ptr) };
        assert_eq!(*k, 1998);
    }
}
