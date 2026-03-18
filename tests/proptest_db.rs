//! Property-based tests for MMDB using proptest.
//!
//! Tests that the database behaves as a correct ordered key-value store
//! under random sequences of operations.

use std::collections::BTreeMap;

use proptest::collection::vec;
use proptest::prelude::*;

use mmdb::{DB, DbOptions, WriteBatch};

/// An operation that can be applied to both the DB and a reference model.
#[derive(Debug, Clone)]
enum Op {
    Put(Vec<u8>, Vec<u8>),
    Delete(Vec<u8>),
    Get(Vec<u8>),
}

fn arb_key() -> impl Strategy<Value = Vec<u8>> {
    vec(any::<u8>(), 1..32)
}

fn arb_value() -> impl Strategy<Value = Vec<u8>> {
    vec(any::<u8>(), 0..128)
}

fn arb_op() -> impl Strategy<Value = Op> {
    prop_oneof![
        4 => (arb_key(), arb_value()).prop_map(|(k, v)| Op::Put(k, v)),
        1 => arb_key().prop_map(Op::Delete),
        3 => arb_key().prop_map(Op::Get),
    ]
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(15))]

    #[test]
    fn test_random_operations(ops in vec(arb_op(), 1..100)) {
        let dir = tempfile::tempdir().unwrap();
        let db = DB::open(DbOptions {
            create_if_missing: true,
            write_buffer_size: 2048, // small to trigger flushes
            ..Default::default()
        }, dir.path()).unwrap();

        let mut model: BTreeMap<Vec<u8>, Vec<u8>> = BTreeMap::new();

        for op in &ops {
            match op {
                Op::Put(k, v) => {
                    db.put(k, v).unwrap();
                    model.insert(k.clone(), v.clone());
                }
                Op::Delete(k) => {
                    db.delete(k).unwrap();
                    model.remove(k);
                }
                Op::Get(k) => {
                    let db_result = db.get(k).unwrap();
                    let model_result = model.get(k).cloned();
                    prop_assert_eq!(
                        db_result, model_result,
                        "mismatch on get({:?})", k
                    );
                }
            }
        }

        // Final consistency check: verify all model entries exist in DB
        for (k, v) in &model {
            let db_val = db.get(k).unwrap();
            prop_assert_eq!(
                db_val.as_deref(), Some(v.as_slice()),
                "final check failed for key {:?}", k
            );
        }
    }

    #[test]
    fn test_write_batch_atomicity(batches in vec(
        vec((arb_key(), arb_value()), 1..10),
        1..10
    )) {
        let dir = tempfile::tempdir().unwrap();
        let db = DB::open(DbOptions {
            create_if_missing: true,
            write_buffer_size: 2048,
            ..Default::default()
        }, dir.path()).unwrap();

        let mut model: BTreeMap<Vec<u8>, Vec<u8>> = BTreeMap::new();

        for batch_entries in &batches {
            let mut batch = WriteBatch::new();
            for (k, v) in batch_entries {
                batch.put(k, v);
                model.insert(k.clone(), v.clone());
            }
            db.write(batch).unwrap();
        }

        for (k, v) in &model {
            let db_val = db.get(k).unwrap();
            prop_assert_eq!(
                db_val.as_deref(), Some(v.as_slice()),
                "batch test failed for key {:?}", k
            );
        }
    }

    #[test]
    fn test_iterator_consistency(ops in vec(
        (arb_key(), arb_value()), 1..50
    )) {
        let dir = tempfile::tempdir().unwrap();
        let db = DB::open(DbOptions {
            create_if_missing: true,
            write_buffer_size: 1024,
            ..Default::default()
        }, dir.path()).unwrap();

        let mut model: BTreeMap<Vec<u8>, Vec<u8>> = BTreeMap::new();

        for (k, v) in &ops {
            db.put(k, v).unwrap();
            model.insert(k.clone(), v.clone());
        }

        // Compare iterator output with model
        let db_entries: Vec<(Vec<u8>, Vec<u8>)> = db.iter().unwrap().collect();
        let model_entries: Vec<(Vec<u8>, Vec<u8>)> = model.into_iter().collect();

        prop_assert_eq!(
            db_entries.len(), model_entries.len(),
            "iterator count mismatch"
        );

        for (db_entry, model_entry) in db_entries.iter().zip(model_entries.iter()) {
            prop_assert_eq!(
                db_entry, model_entry,
                "iterator entry mismatch"
            );
        }
    }
}
