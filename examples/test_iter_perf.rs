use mmdb::{DB, DbOptions, WriteOptions};
use std::time::Instant;

fn main() {
    let dir = tempfile::tempdir().unwrap();
    let mut opts = DbOptions::default();
    opts.write_buffer_size = 1024 * 1024 * 1024;
    let db = DB::open(opts, dir.path()).unwrap();

    let wo = WriteOptions::default();

    // Time single-thread writes
    println!("Benchmarking single-thread writes...");
    let start = Instant::now();
    let n = 10_000;
    for i in 0u64..n {
        db.put_with_options(&wo, &i.to_be_bytes(), &[0u8; 128])
            .unwrap();
    }
    let elapsed = start.elapsed();
    println!(
        "{} writes in {:?} ({:.1}µs/write)",
        n,
        elapsed,
        elapsed.as_micros() as f64 / n as f64
    );
}
