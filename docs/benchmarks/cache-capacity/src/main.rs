//! Linux point-read profile with separate engine-cache and SST page-cache accounting.
use mmdb::{CompressionType, DB, DbOptions};
use std::{
    env, fs,
    path::Path,
    process::Command,
    time::{Duration, Instant},
};

const VALUE_SIZE: usize = 8192;
const BLOCK_SIZE: usize = 64 * 1024;
const MIX_STEP: u64 = 0x9e3779b97f4a7c15;

fn mix(mut word: u64) -> u64 {
    word = (word ^ (word >> 30)).wrapping_mul(0xbf58476d1ce4e5b9);
    word = (word ^ (word >> 27)).wrapping_mul(0x94d049bb133111eb);
    word ^ (word >> 31)
}

fn value(key: u64) -> [u8; VALUE_SIZE] {
    let mut bytes = [0; VALUE_SIZE];
    let mut state = mix(key);
    for chunk in bytes.as_chunks_mut::<8>().0 {
        state = state.wrapping_add(MIX_STEP);
        chunk.copy_from_slice(&mix(state).to_le_bytes());
    }
    bytes
}

fn field(text: &str, name: &str) -> u64 {
    let value = text
        .lines()
        .find_map(|line| line.strip_prefix(name))
        .unwrap();
    value.split_whitespace().next().unwrap().parse().unwrap()
}

fn check_reserve(reserve_mib: u64) {
    let meminfo = fs::read_to_string("/proc/meminfo").unwrap();
    assert!(
        field(&meminfo, "MemAvailable:") >= reserve_mib * 1024,
        "host memory reserve exhausted"
    );
}

fn prepare(dir: &Path, rows: u64) {
    assert!(rows.is_power_of_two() && !dir.exists() && !dir.with_extension("rows").exists());
    let options = DbOptions::default()
        .with_compression(CompressionType::None)
        .with_block_size(BLOCK_SIZE)
        .with_target_file_size_base(256 << 20)
        .with_block_cache_capacity(0);
    let db = DB::open(options, dir).unwrap();
    assert_eq!(
        db.ingest_sorted((0..rows).map(|key| (key.to_be_bytes(), value(key))))
            .unwrap(),
        rows
    );
    db.close().unwrap();
    fs::write(dir.with_extension("rows"), rows.to_string()).unwrap();
    println!("prepare rows={rows} value_bytes={VALUE_SIZE} block_bytes={BLOCK_SIZE}");
}

fn gauges(db: &DB, dir: &Path, phase: &str) {
    let files = fs::read_dir(dir)
        .unwrap()
        .map(|f| f.unwrap().path())
        .filter(|p| p.extension().is_some_and(|ext| ext == "sst"))
        .collect::<Vec<_>>();
    assert!(!files.is_empty());
    // Fincore samples file residency without reading the payload into this process.
    let stat = Command::new("fincore")
        .args(["--bytes", "--noheadings", "--output", "RES,SIZE"])
        .args(&files)
        .output()
        .unwrap();
    assert!(
        stat.status.success(),
        "{}",
        String::from_utf8_lossy(&stat.stderr)
    );
    let mut resident = 0_u64;
    let mut file_bytes = 0_u64;
    for line in String::from_utf8(stat.stdout).unwrap().lines() {
        let mut fields = line.split_whitespace().map(|n| n.parse::<u64>().unwrap());
        resident += fields.next().unwrap();
        file_bytes += fields.next().unwrap();
    }
    let pool = db.block_cache_pool().usage();
    let db_memory = db.memory_usage();
    let meminfo = fs::read_to_string("/proc/meminfo").unwrap();
    let status = fs::read_to_string("/proc/self/status").unwrap();
    println!(
        "{phase} sst_files={} sst_bytes={file_bytes} sst_resident_bytes={resident} segmented_bytes={} overflow_bytes={} active_bytes={} immutable_bytes={} l0_pin_bytes={} rss_kib={} peak_kib={} anon_kib={} file_rss_kib={} available_kib={} host_cached_kib={}",
        files.len(),
        pool.segmented_weighted_bytes,
        pool.overflow_weighted_bytes,
        db_memory.active_memtable_bytes,
        db_memory.immutable_memtable_bytes,
        db_memory.l0_pin_bytes,
        field(&status, "VmRSS:"),
        field(&status, "VmHWM:"),
        field(&status, "RssAnon:"),
        field(&status, "RssFile:"),
        field(&meminfo, "MemAvailable:"),
        field(&meminfo, "Cached:")
    );
}

fn cache_stat(db: &DB, name: &str) -> u64 {
    db.get_property(name).unwrap().parse().unwrap()
}
fn disk_bytes() -> u64 {
    field(&fs::read_to_string("/proc/self/io").unwrap(), "read_bytes:")
}

fn run(dir: &Path, capacity_mib: u64, samples: usize, seed: u64, reserve_mib: u64) {
    check_reserve(reserve_mib + capacity_mib);
    let rows = fs::read_to_string(dir.with_extension("rows"))
        .unwrap()
        .trim()
        .parse::<u64>()
        .unwrap();
    assert!(rows.is_power_of_two() && samples > 0);
    let db = DB::open_read_only_with_options(
        DbOptions::default().with_block_cache_capacity(capacity_mib << 20),
        dir,
    )
    .unwrap();
    let snapshot = db.snapshot();
    gauges(&db, dir, "open");
    let start = Instant::now();
    // Eight 8-KiB values fill a nominal 64-KiB block; stride four covers the partial boundaries too.
    for key in (0..rows).step_by(4) {
        if key % (1 << 17) == 0 {
            check_reserve(reserve_mib);
        }
        let actual = snapshot.get_pinned(&key.to_be_bytes()).unwrap().unwrap();
        assert_eq!(actual.as_ref(), &value(key));
    }
    let warm = start.elapsed();
    gauges(&db, dir, "warm");
    let hits = cache_stat(&db, "stats.block_cache_hits");
    let misses = cache_stat(&db, "stats.block_cache_misses");
    let reads = disk_bytes();
    let mut times = Vec::with_capacity(samples);
    let mut rng = seed;
    let mut service = Duration::ZERO;
    for _ in 0..samples {
        rng = rng.wrapping_add(MIX_STEP);
        let key = mix(rng) & (rows - 1);
        let encoded = key.to_be_bytes();
        let start = Instant::now();
        let actual = snapshot.get_pinned(&encoded).unwrap().unwrap();
        let elapsed = start.elapsed();
        service += elapsed;
        times.push(elapsed.as_nanos() as u64);
        assert_eq!(actual.as_ref(), &value(key));
    }
    let reads = disk_bytes() - reads;
    let hits = cache_stat(&db, "stats.block_cache_hits") - hits;
    let misses = cache_stat(&db, "stats.block_cache_misses") - misses;
    times.sort_unstable();
    gauges(&db, dir, "end");
    println!(
        "measured rows={rows} samples={samples} seed={seed} capacity_mib={capacity_mib} hit_probes={hits} miss_probes={misses} read_bytes={reads} warm_ms={} service_ns={} p50_ns={} p99_ns={}",
        warm.as_millis(),
        service.as_nanos(),
        times[samples.div_ceil(2) - 1],
        times[(samples * 99).div_ceil(100) - 1]
    );
}

fn main() {
    let args: Vec<_> = env::args().skip(1).collect();
    match args.as_slice() {
        [mode, dir, rows] if mode == "prepare" => prepare(Path::new(dir), rows.parse().unwrap()),
        [mode, dir, mib, samples, seed, reserve] if mode == "trial" => run(
            Path::new(dir),
            mib.parse().unwrap(),
            samples.parse().unwrap(),
            seed.parse().unwrap(),
            reserve.parse().unwrap(),
        ),
        _ => panic!(
            "usage: mmdb-cache-capacity prepare DIR ROWS | trial DIR CAPACITY_MIB SAMPLES SEED RESERVE_MIB"
        ),
    }
}
