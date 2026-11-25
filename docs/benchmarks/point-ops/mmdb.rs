use mmdb::{CompressionType, DB, DbOptions, DurabilityMode, WriteOptions};
use std::{
    env,
    path::Path,
    sync::Barrier,
    thread,
    time::{Duration, Instant},
};

fn read_all(db: &DB, rows: u64) {
    for i in 0..rows {
        let key = ((i * 8_191) % rows).to_be_bytes();
        let value = db.get_pinned(&key).unwrap().unwrap();
        assert_eq!(value.len(), 128);
        assert_eq!(value[0], 0x42);
    }
}

fn write_all(db: &DB, rows: u64, writers: u64, sync: bool) -> Duration {
    let ready = Barrier::new(writers as usize + 1);
    let go = Barrier::new(writers as usize + 1);
    thread::scope(|scope| {
        let (ready, go) = (&ready, &go);
        let mut jobs = Vec::new();
        for worker in 0..writers {
            jobs.push(scope.spawn(move || {
                let value = [0x42; 128];
                let sync_options = WriteOptions::default().with_durability(DurabilityMode::WalSync);
                ready.wait();
                go.wait();
                for i in 0..rows / writers {
                    let key = (((i * writers + worker) * 65_537) % rows).to_be_bytes();
                    if sync {
                        db.put_with_options(&sync_options, &key, &value).unwrap();
                    } else {
                        db.put(&key, &value).unwrap();
                    }
                }
                Instant::now()
            }));
        }
        ready.wait();
        let start = Instant::now();
        go.wait();
        let last_write = jobs
            .into_iter()
            .map(|job| job.join().unwrap())
            .max()
            .unwrap();
        last_write - start
    })
}

fn main() {
    let args: Vec<String> = env::args().collect();
    assert!(args.len() == 3 || (args.len() == 4 && args[3] == "sync"));
    let sync = args.len() == 4;
    let rows = if sync { 8_192 } else { 131_072 };
    let path = Path::new(&args[1]);
    assert!(
        !path.join("CURRENT").exists(),
        "benchmark requires a fresh database"
    );
    let writers: u64 = args[2].parse().unwrap();
    assert!(matches!(writers, 1 | 8));

    let options = DbOptions::default()
        .with_error_if_exists(true)
        .with_write_buffer_size(64 * 1024 * 1024)
        .with_block_cache_capacity(64 * 1024 * 1024)
        .with_block_size(4096)
        .with_bloom_bits_per_key(12)
        .with_compression(CompressionType::None);
    let levels = options.num_levels;
    let db = DB::open(options, path).unwrap();

    let write_elapsed = write_all(&db, rows, writers, sync);
    db.flush().unwrap();
    let files: Vec<_> = (0..levels)
        .map(|level| {
            db.get_property(&format!("num-files-at-level{level}"))
                .unwrap()
        })
        .collect();
    println!("sst_counts={}", files.join(","));

    read_all(&db, rows);
    if sync {
        println!(
            "writers={writers} write_seconds={:.6}",
            write_elapsed.as_secs_f64()
        );
    } else {
        let start = Instant::now();
        read_all(&db, rows);
        let read_elapsed = start.elapsed();
        println!(
            "writers={writers} write_seconds={:.6} read_seconds={:.6}",
            write_elapsed.as_secs_f64(),
            read_elapsed.as_secs_f64()
        );
    }
    db.close().unwrap();
}
