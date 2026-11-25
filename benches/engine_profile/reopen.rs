use std::{env, fs, path::Path, process, sync::Arc, time::Instant};

use mmdb::{BlockCachePool, DB};

use super::{Config, MIB, key, rss_kib, value};

pub(super) fn prepare(config: &Config, path: &Path) {
    let pool = Arc::new(BlockCachePool::new(config.pool_mib * MIB as u64));
    let db = DB::open(config.options(&pool), path).unwrap();
    let mut bytes = [0; 128];
    for i in 0..config.operations {
        value(config.seed ^ i as u64, config.entropy, &mut bytes);
        db.put(&key(0, i), &bytes).unwrap();
    }
    db.sync_wal().unwrap();
    // Acknowledged WAL data, but no close/drop flush. The parent alone reopens it.
    process::exit(0);
}

pub(super) fn run(config: &Config, path: &Path) {
    assert!(
        process::Command::new(env::current_exe().unwrap())
            .arg("prepare-reopen")
            .arg(path)
            .status()
            .unwrap()
            .success()
    );
    let files = fs::read_dir(path).unwrap().map(Result::unwrap);
    let wal_bytes: u64 = files
        .filter(|f| f.path().extension().is_some_and(|x| x == "wal"))
        .map(|f| f.metadata().unwrap().len())
        .sum();
    assert!(wal_bytes > 0, "fixture must retain WAL data");
    let pool = Arc::new(BlockCachePool::new(config.pool_mib * MIB as u64));
    let start = Instant::now();
    let db = DB::open(config.options(&pool), path).unwrap();
    let reopen = start.elapsed();
    let open_peak_kib = rss_kib("VmHWM:").unwrap_or_default();
    let open_rss_kib = rss_kib("VmRSS:").unwrap_or_default();
    let mut expected = [0; 128];
    for i in 0..config.operations {
        value(config.seed ^ i as u64, config.entropy, &mut expected);
        assert_eq!(&*db.get_pinned(&key(0, i)).unwrap().unwrap(), &expected);
    }
    println!(
        "reopen wal_bytes={wal_bytes} rows={} open_us={} open_peak_kib={open_peak_kib} open_rss_kib={open_rss_kib} after_verify_peak_kib={}",
        config.operations,
        reopen.as_micros(),
        rss_kib("VmHWM:").unwrap_or_default()
    );
    db.close().unwrap();
}
