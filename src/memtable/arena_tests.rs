//! Memory-only checks shared by the native suite and the Miri test target.

use super::MemTable;
use crate::{PinnedSlice, types::ValueType};
use std::{sync::Arc, thread};

#[cfg_attr(test, test)]
pub fn concurrent_arena_pins() {
    let mem = Arc::new(MemTable::new());
    mem.put(b"stable", b"retained", 1, ValueType::Value);
    let pin = mem
        .point_value::<PinnedSlice>(b"stable", 1)
        .unwrap()
        .0
        .unwrap();
    thread::scope(|scope| {
        scope.spawn(|| {
            for i in 2..130u64 {
                let k = i.to_be_bytes();
                mem.put(&k, &k, i, ValueType::Value);
                mem.put(b"stable", &k, i, ValueType::Value);
            }
        });
        for _ in 0..2 {
            scope.spawn(|| {
                for _ in 0..24 {
                    let (value, _) = mem.point_value::<PinnedSlice>(b"stable", 1).unwrap();
                    assert_eq!(value.unwrap().as_ref(), b"retained");
                    assert_eq!(pin.as_ref(), b"retained");
                    for (key, value) in mem.iter_ref() {
                        assert!(key.len() >= 8);
                        assert!(!value.is_empty());
                    }
                }
            });
        }
    });
    drop(mem);
    assert_eq!(pin.into_vec(), b"retained");
}
