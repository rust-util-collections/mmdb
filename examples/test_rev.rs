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

    for round in 0..4 {
        for i in 0..15 {
            let key = format!("rev_{:04}", round * 15 + i);
            let val = format!("v_{}", round * 15 + i);
            db.put(key.as_bytes(), val.as_bytes()).unwrap();
        }
        db.flush().unwrap();
    }

    // Check forward before compact
    let forward_pre: Vec<_> = BidiIterator::lazy(db.iter().unwrap()).collect();
    eprintln!("Forward BEFORE compact: {} entries", forward_pre.len());
    let reverse_pre: Vec<_> = BidiIterator::lazy(db.iter().unwrap()).rev().collect();
    eprintln!("Reverse BEFORE compact: {} entries", reverse_pre.len());

    db.compact().unwrap();

    let forward: Vec<_> = BidiIterator::lazy(db.iter().unwrap()).collect();
    eprintln!("Forward AFTER compact: {} entries", forward.len());
    let reverse: Vec<_> = BidiIterator::lazy(db.iter().unwrap()).rev().collect();
    eprintln!("Reverse AFTER compact: {} entries", reverse.len());

    if forward.len() != 60 || reverse.len() != 60 {
        eprintln!("FAIL: expected 60/60");
        std::process::exit(1);
    }
    eprintln!("PASS");
}
