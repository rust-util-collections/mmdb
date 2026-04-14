use mmdb::{BidiIterator, DB, DbOptions};

fn main() {
    let dir = tempfile::tempdir().unwrap();
    let opts = DbOptions {
        create_if_missing: true,
        write_buffer_size: 1024,
        l0_compaction_trigger: 3,
        ..Default::default()
    };
    let db = DB::open(opts, dir.path()).unwrap();

    for i in 0..15 {
        let key = format!("rev_{:04}", i);
        let val = format!("v_{}", i);
        db.put(key.as_bytes(), val.as_bytes()).unwrap();
    }
    db.flush().unwrap();

    // Test 1: forward with DB::iter
    let fwd_iter = db.iter().unwrap();
    let mut count = 0;
    for (k, _v) in fwd_iter {
        count += 1;
        if count <= 3 {
            eprintln!("  fwd key: {:?}", String::from_utf8_lossy(&k));
        }
    }
    eprintln!("Forward count: {}", count);

    // Test 2: reverse with seek_to_last
    let mut rev_iter = db.iter().unwrap();
    rev_iter.seek_to_last();
    eprintln!("After seek_to_last, valid={}", rev_iter.valid());
    if rev_iter.valid()
        && let Some(k) = rev_iter.key()
    {
        eprintln!("  Last key: {:?}", String::from_utf8_lossy(k));
    }

    // Test 3: BidiIterator reverse
    let rev_entries: Vec<_> = BidiIterator::lazy(db.iter().unwrap()).rev().collect();
    eprintln!("BidiIterator rev count: {}", rev_entries.len());
}
