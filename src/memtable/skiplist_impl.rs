//! Append-only, variable-height skip list with inline key/value bytes.
//! Readers follow acquire-loaded links. One insertion guard protects both
//! the bump allocator and predecessor updates; readers never take the guard.

use std::{
    alloc::{Layout, alloc, dealloc, handle_alloc_error},
    cmp::Ordering as Cmp,
    ptr::{self, NonNull},
    slice,
    sync::atomic::{AtomicPtr, AtomicUsize, Ordering},
};

use crate::types::compare_internal_key;
use parking_lot::Mutex;

const MAX_HEIGHT: usize = 12;
const ARENA_INITIAL_BLOCK: usize = 4096;
const ARENA_MAX_BLOCK: usize = 1 << 20;

#[cfg(test)]
thread_local! {
    // Schedule a concurrent splice just after the final comparison in search.
    static AFTER_SEARCH: std::cell::RefCell<Option<Box<dyn FnOnce()>>> = const { std::cell::RefCell::new(None) };
}

struct Slab {
    ptr: NonNull<u8>,
    layout: Layout,
}

impl Slab {
    fn new(layout: Layout) -> Self {
        // SAFETY: every caller supplies a nonempty Layout.
        let ptr =
            NonNull::new(unsafe { alloc(layout) }).unwrap_or_else(|| handle_alloc_error(layout));
        Self { ptr, layout }
    }
}

impl Drop for Slab {
    fn drop(&mut self) {
        // SAFETY: this slab uniquely owns the allocation and its exact Layout.
        unsafe { dealloc(self.ptr.as_ptr(), self.layout) };
    }
}

// SAFETY: ownership of a slab can move between threads. Its allocation
// remains at a fixed address until the owning list is exclusively dropped.
unsafe impl Send for Slab {}

#[derive(Default)]
struct Arena {
    slabs: Vec<Slab>,
    active: Option<usize>,
    offset: usize,
    random: u64,
}

impl Arena {
    fn random_height(&mut self) -> usize {
        // SplitMix64 produces independent low bits. Count pairs of zeroes
        // for a geometric tower with p=1/4.
        self.random = self.random.wrapping_add(0x9e3779b97f4a7c15);
        let mut bits = self.random;
        bits = (bits ^ (bits >> 30)).wrapping_mul(0xbf58476d1ce4e5b9);
        bits = (bits ^ (bits >> 27)).wrapping_mul(0x94d049bb133111eb);
        (1 + ((bits ^ (bits >> 31)).trailing_zeros() as usize / 2)).min(MAX_HEIGHT)
    }

    /// Returns a new node's address and the additional slab capacity reserved.
    fn allocate(&mut self, node: Layout) -> (*mut NodeHeader, usize) {
        if node.size() > ARENA_MAX_BLOCK / 4 {
            let slab = Slab::new(node);
            let ptr = slab.ptr.as_ptr();
            self.slabs.push(slab);
            return (ptr.cast(), node.size());
        }
        let old_capacity = self.active.map_or(0, |idx| self.slabs[idx].layout.size());
        let mut reserved = 0;
        if old_capacity - self.offset < node.size() {
            reserved = (old_capacity * 2)
                .clamp(ARENA_INITIAL_BLOCK, ARENA_MAX_BLOCK)
                .max(node.size());
            let slab = Slab::new(Layout::from_size_align(reserved, node.align()).unwrap());
            self.active = Some(self.slabs.len());
            self.slabs.push(slab);
            self.offset = 0;
        }
        let base = self.slabs[self.active.unwrap()].ptr;
        // SAFETY: every node layout is padded to the same alignment and the
        // bounds check above reserves its complete extent in this slab.
        let ptr = unsafe { base.as_ptr().add(self.offset) };
        self.offset += node.size();
        (ptr.cast(), reserved)
    }
}

#[repr(C, align(8))]
struct NodeHeader {
    key_len: u32,
    val_len: u32,
    height: u8,
    _pad: [u8; 7],
}

type Link = AtomicPtr<NodeHeader>;

fn node_layout(height: usize, key_len: usize, value_len: usize) -> Layout {
    let (layout, _) = Layout::new::<NodeHeader>()
        .extend(Layout::array::<Link>(height).unwrap())
        .unwrap();
    let (layout, _) = layout
        .extend(Layout::array::<u8>(key_len).unwrap())
        .unwrap();
    let (layout, _) = layout
        .extend(Layout::array::<u8>(value_len).unwrap())
        .unwrap();
    layout.pad_to_align()
}

// The trailing Link array starts immediately after NodeHeader. Both fields
// are naturally aligned on supported targets.
const _: () = assert!(align_of::<Link>() <= align_of::<NodeHeader>());

unsafe fn tower(node: *mut NodeHeader) -> *mut Link {
    // SAFETY: caller supplies a live raw allocation with a trailing tower.
    unsafe { node.add(1).cast() }
}

pub struct ConcurrentSkipList {
    head: [Link; MAX_HEIGHT],
    tail: Link,
    height: AtomicUsize,
    arena: Mutex<Arena>,
}

impl ConcurrentSkipList {
    pub fn new() -> Self {
        Self {
            head: std::array::from_fn(|_| Link::new(ptr::null_mut())),
            tail: Link::new(ptr::null_mut()),
            height: AtomicUsize::new(1),
            arena: Mutex::new(Arena::default()),
        }
    }

    fn link(&self, node: *mut NodeHeader, level: usize) -> &Link {
        if node.is_null() {
            &self.head[level]
        } else {
            // SAFETY: internal traversal reaches node only at this or a higher
            // level. Its initialized tower has at least level + 1 slots.
            unsafe { &*tower(node).add(level) }
        }
    }

    /// `ptr` must be a non-null published node of this list.
    pub unsafe fn node_kv(&self, ptr: *const ()) -> (&[u8], &[u8]) {
        let node = ptr.cast_mut().cast::<NodeHeader>();
        // SAFETY: the release splice published the header, the entire tower
        // and the immutable payload. The list owns storage for both slices.
        unsafe {
            let header = &*node;
            let key = tower(node).add(header.height as usize).cast::<u8>();
            let value = key.add(header.key_len as usize);
            (
                slice::from_raw_parts(key, header.key_len as usize),
                slice::from_raw_parts(value, header.val_len as usize),
            )
        }
    }

    // Walk forward while the predicate holds, descending one level at a time.
    // The predecessor callback is inlined out of lookups.
    fn search(
        &self,
        mut before: impl FnMut(&[u8]) -> bool,
        mut predecessor: impl FnMut(usize, *mut NodeHeader),
    ) -> (*mut NodeHeader, *mut NodeHeader) {
        let mut current = ptr::null_mut();
        let mut successor = ptr::null_mut();
        for level in (0..self.height.load(Ordering::Acquire)).rev() {
            successor = loop {
                let next = self.link(current, level).load(Ordering::Acquire);
                // SAFETY: a non-null acquire-loaded link points to a live node.
                if next.is_null() || !before(unsafe { self.node_kv(next.cast()) }.0) {
                    break next;
                }
                current = next;
            };
            predecessor(level, current);
        }
        #[cfg(test)]
        if let Some(hook) = AFTER_SEARCH.with(|cell| cell.borrow_mut().take()) {
            hook();
        }
        // Re-reading current.next[0] could pick up a newly inserted node
        // preceding the target, including a value newer than the read sequence.
        (current, successor)
    }

    pub fn insert(&self, key: &[u8], trailer: &[u8; 8], value: &[u8]) -> usize {
        let mut arena = self.arena.lock();
        let height = arena.random_height();
        let key_len = key.len().checked_add(8).unwrap();
        let header = NodeHeader {
            key_len: u32::try_from(key_len).unwrap(),
            val_len: u32::try_from(value.len()).unwrap(),
            height: height as u8,
            _pad: [0; 7],
        };
        let (node, reserved) = arena.allocate(node_layout(height, key_len, value.len()));
        let mut prev = [ptr::null_mut(); MAX_HEIGHT];
        self.search(
            |bytes| cmp_target(bytes, key, trailer).is_lt(),
            |level, p| prev[level] = p,
        );

        // SAFETY: this aligned node is fresh, the layout contains header,
        // tower, key, trailer, and value. Nothing can reach it before splice.
        unsafe {
            node.write(header);
            let links = tower(node);
            for (i, &p) in prev[..height].iter().enumerate() {
                links
                    .add(i)
                    .write(Link::new(self.link(p, i).load(Ordering::Relaxed)));
            }
            let data = links.add(height).cast::<u8>();
            ptr::copy_nonoverlapping(key.as_ptr(), data, key.len());
            ptr::copy_nonoverlapping(trailer.as_ptr(), data.add(key.len()), 8);
            ptr::copy_nonoverlapping(value.as_ptr(), data.add(key_len), value.len());
        }
        let at_end = self.link(prev[0], 0).load(Ordering::Relaxed).is_null();
        for (i, &p) in prev[..height].iter().enumerate() {
            self.link(p, i).store(node, Ordering::Release);
        }
        self.height.fetch_max(height, Ordering::Release);
        if at_end {
            self.tail.store(node, Ordering::Release);
        }
        reserved
    }

    pub fn versions_of<'a>(&'a self, user: &'a [u8]) -> impl Iterator<Item = (&'a [u8], &'a [u8])> {
        let (_, mut node) = self.search(|bytes| bytes[..bytes.len() - 8] < *user, |_, _| {});
        std::iter::from_fn(move || {
            if node.is_null() {
                return None;
            }
            // SAFETY: node came from the acquired predecessor/successor link.
            let (key, value) = unsafe { self.node_kv(node.cast()) };
            if &key[..key.len() - 8] != user {
                return None;
            }
            node = self.link(node, 0).load(Ordering::Acquire);
            Some((key, value))
        })
    }

    pub fn lower_bound(&self, key: &[u8], trailer: &[u8; 8]) -> Option<(&[u8], &[u8])> {
        let (_, node) = self.search(|bytes| cmp_target(bytes, key, trailer).is_lt(), |_, _| {});
        // SAFETY: the node belongs to this list and was acquired from level zero.
        (!node.is_null()).then(|| unsafe { self.node_kv(node.cast()) })
    }

    pub fn seek_lt_raw(&self, target: &[u8]) -> *const () {
        self.search(|k| compare_internal_key(k, target).is_lt(), |_, _| {})
            .0
            .cast()
    }

    pub fn seek_le_raw(&self, target: &[u8]) -> *const () {
        self.search(|k| compare_internal_key(k, target).is_le(), |_, _| {})
            .0
            .cast()
    }

    pub fn seek_ge_raw(&self, target: &[u8]) -> *const () {
        self.search(|k| compare_internal_key(k, target).is_lt(), |_, _| {})
            .1
            .cast()
    }

    pub fn head_ptr(&self) -> *const () {
        self.head[0].load(Ordering::Acquire).cast()
    }

    pub fn tail_ptr(&self) -> *const () {
        self.tail.load(Ordering::Acquire).cast()
    }

    /// `ptr` must be a non-null node obtained from this list.
    pub unsafe fn node_next0(&self, ptr: *const ()) -> *const () {
        self.link(ptr.cast_mut().cast(), 0)
            .load(Ordering::Acquire)
            .cast()
    }

    pub fn iter_ref(&self) -> impl Iterator<Item = (&[u8], &[u8])> {
        let mut node = self.head_ptr();
        std::iter::from_fn(move || {
            if node.is_null() {
                return None;
            }
            // SAFETY: node was obtained from the head or an acquire-loaded link.
            unsafe {
                let entry = self.node_kv(node);
                node = self.node_next0(node);
                Some(entry)
            }
        })
    }
}

fn cmp_target(encoded: &[u8], key: &[u8], trailer: &[u8; 8]) -> Cmp {
    let (user, tag) = encoded.split_at(encoded.len() - 8);
    user.cmp(key).then_with(|| tag.cmp(trailer))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{InternalKey, ValueType, pack_sequence_and_type};
    use std::sync::Arc;

    #[test]
    fn forward_search_returns_the_node_that_passed_its_last_comparison() {
        let tag = |seq| (!pack_sequence_and_type(seq, ValueType::Value)).to_be_bytes();
        let prepare = || {
            let list = Arc::new(ConcurrentSkipList::new());
            list.insert(b"k", &tag(1), b"old");
            let writer = list.clone();
            AFTER_SEARCH.with(|slot| {
                *slot.borrow_mut() = Some(Box::new(move || {
                    writer.insert(b"k", &tag(2), b"future");
                }))
            });
            list
        };

        let list = prepare();
        assert_eq!(list.lower_bound(b"k", &tag(1)).unwrap().1, b"old");

        let list = prepare();
        let seek = InternalKey::new(b"k", 1, ValueType::Value);
        let ptr = list.seek_ge_raw(seek.as_bytes());
        assert!(!ptr.is_null());
        // SAFETY: ptr is a non-null cursor obtained from this list.
        assert_eq!(unsafe { list.node_kv(ptr) }.1, b"old");

        // A newly spliced predecessor with a different key must not turn a
        // same-key admission scan into an empty one.
        let list_for_splice = list.clone();
        AFTER_SEARCH.with(|slot| {
            *slot.borrow_mut() = Some(Box::new(move || {
                list_for_splice.insert(b"j", &tag(4), b"other");
            }))
        });
        assert_eq!(list.versions_of(b"k").count(), 2);
    }
}
