//! Mixed read/write latency on a flushed working set while small write buffers rotate.
//! Set MMDB_PROFILE_IMMUTABLES and run each configuration in a fresh process
//! to compare process-wide peak RSS. Linux exposes VmHWM in /proc/self/status.

use std::{sync::Barrier, thread, time::Instant};

use mmdb::{DB, DbOptions};

const KEYS: u64 = 32_768;
const OPERATIONS_PER_THREAD: u64 = 10_000;

fn report(name: &str, mut durations: Vec<u128>) {
    durations.sort_unstable();
    let us = |per_mille: usize| {
        durations[(durations.len() * per_mille).div_ceil(1000) - 1] as f64 / 1000.0
    };
    println!(
        "{name} n={} p99_us={:.2} p999_us={:.2}",
        durations.len(),
        us(990),
        us(999)
    );
}

fn main() {
    let immutables = std::env::var("MMDB_PROFILE_IMMUTABLES")
        .map_or(2, |s| s.parse().expect("positive queue size"));
    let dir = tempfile::tempdir().unwrap();
    let options = DbOptions::default()
        .with_write_buffer_size(256 * 1024)
        .with_max_immutable_memtables(immutables);
    let db = DB::open(options, dir.path()).unwrap();
    for i in 0..KEYS {
        db.put(&i.to_be_bytes(), &[7; 128]).unwrap();
    }
    db.flush().unwrap();
    let barrier = Barrier::new(4);
    let mut reads = Vec::new();
    let mut writes = Vec::new();
    let flushes_before = db.get_property("stats.flushes_completed").unwrap();
    thread::scope(|scope| {
        let mut workers = Vec::new();
        for id in 0..4 {
            let db = &db;
            let barrier = &barrier;
            workers.push(scope.spawn(move || {
                let mut samples = Vec::with_capacity(OPERATIONS_PER_THREAD as usize);
                barrier.wait();
                for i in 0..OPERATIONS_PER_THREAD {
                    let key = ((i * 3571 + id * 251) % KEYS).to_be_bytes();
                    let start = Instant::now();
                    if id < 2 {
                        assert_eq!(db.get(&key).unwrap().unwrap().len(), 128);
                    } else {
                        db.put(&key, &[id as u8; 128]).unwrap();
                    }
                    samples.push(start.elapsed().as_nanos());
                }
                samples
            }));
        }
        for (id, worker) in workers.into_iter().enumerate() {
            let samples = worker.join().unwrap();
            if id < 2 {
                reads.extend(samples);
            } else {
                writes.extend(samples);
            }
        }
    });
    db.flush().unwrap();
    let flushes_after = db.get_property("stats.flushes_completed").unwrap();
    println!(
        "max_immutable_memtables={immutables} flushes_before={flushes_before} flushes_after={flushes_after}"
    );
    report("read", reads);
    report("write", writes);
    if let Ok(status) = std::fs::read_to_string("/proc/self/status")
        && let Some(hwm) = status.lines().find(|l| l.starts_with("VmHWM:"))
    {
        println!("{hwm}");
    }
}
