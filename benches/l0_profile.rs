//! Reproduce L0 stalls and selection across cold map prefixes.
//! Run each `cargo bench --features bench-metrics --bench l0_profile -- MODE`
//! invocation in a fresh process. Modes: gap, native, sentinel.
use std::{
    collections::BTreeMap,
    env, fs,
    path::{Path, PathBuf},
    sync::{
        Arc, Barrier,
        atomic::{AtomicU64, Ordering},
    },
    thread,
    time::{Duration, Instant},
};

use mmdb::{
    BlockPropertyCollector, DB, DBIterator, DbOptions, DurabilityMode, WriteOptions,
    take_thread_write_profile,
};

const PREFIXES: [u8; 8] = [0x20, 0x40, 0x48, 0x50, 0x58, 0x60, 0x68, 0x90];
const ROWS: usize = 1024;
const VALUE_SIZE: usize = 256;
const KEY_SIZE: usize = 16;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Mode {
    Gap,
    Native,
    Sentinel,
}

impl Mode {
    // A single hot prefix sorts above both the cold data and the old one-byte
    // fence key. A pair of hot prefixes straddles the cold data even without it.
    fn hot_row(self, op: usize) -> (usize, usize) {
        let (prefix_index, n) = if self == Mode::Gap {
            (if op.is_multiple_of(2) { 0 } else { 7 }, op / 2)
        } else {
            (7, op)
        };
        (prefix_index, n.wrapping_mul(73) % ROWS)
    }

    fn is_cold(self, prefix: u8) -> bool {
        prefix != PREFIXES[7] && (self != Mode::Gap || prefix != PREFIXES[0])
    }
}

fn setting(name: &str, default: u64) -> u64 {
    match env::var(format!("MMDB_L0_{name}")) {
        Ok(s) => s.parse().unwrap_or_else(|_| panic!("invalid {name}: {s}")),
        Err(env::VarError::NotPresent) => default,
        Err(e) => panic!("{name}: {e}"),
    }
}

fn key(prefix: u8, suffix: usize) -> [u8; KEY_SIZE] {
    let mut key = [prefix; KEY_SIZE];
    key[8..].copy_from_slice(&(suffix as u64).to_be_bytes());
    key
}

fn value(key: &[u8; KEY_SIZE], epoch: usize, seed: u64) -> [u8; VALUE_SIZE] {
    let mut bytes = [0; VALUE_SIZE];
    bytes[..KEY_SIZE].copy_from_slice(key);
    let mut state = u64::from_be_bytes(key[8..].try_into().unwrap())
        ^ ((key[0] as u64) << 48)
        ^ seed
        ^ (epoch as u64).rotate_left(32);
    for chunk in bytes[KEY_SIZE..].as_chunks_mut::<8>().0 {
        state = state.wrapping_add(0x9e37_79b9_7f4a_7c15);
        let v = (state ^ (state >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        let v = (v ^ (v >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        chunk.copy_from_slice(&(v ^ (v >> 31)).to_le_bytes());
    }
    bytes
}

struct ColdRows {
    mode: Mode,
    in_block: u64,
    total: Arc<AtomicU64>,
}

impl BlockPropertyCollector for ColdRows {
    fn add(&mut self, key: &[u8]) {
        if key.len() == KEY_SIZE && self.mode.is_cold(key[0]) {
            self.in_block += 1;
        }
    }

    fn finish_block(&mut self) -> Vec<u8> {
        if self.in_block != 0 {
            self.total.fetch_add(self.in_block, Ordering::Relaxed);
            self.in_block = 0;
        }
        Vec::new()
    }

    fn name(&self) -> &str {
        "profile.cold_rows"
    }
}

fn sst_files(dir: &Path) -> BTreeMap<PathBuf, u64> {
    dir.read_dir()
        .unwrap()
        .map(Result::unwrap)
        .filter_map(|e| {
            let path = e.path();
            path.extension()
                .is_some_and(|ext| ext == "sst")
                .then(|| (path, e.metadata().unwrap().len()))
        })
        .collect()
}

fn preload(db: &DB, dir: &Path, mode: Mode, seed: u64) -> BTreeMap<PathBuf, u64> {
    let mut cold = BTreeMap::new();
    let mut previous = BTreeMap::new();
    for prefix in PREFIXES {
        let n = db
            .ingest_sorted((0..ROWS).map(|i| {
                let k = key(prefix, i);
                (k, value(&k, 0, seed))
            }))
            .unwrap();
        assert_eq!(n, ROWS as u64);
        let current = sst_files(dir);
        if mode.is_cold(prefix) {
            cold.extend(
                current
                    .iter()
                    .filter(|(p, _)| !previous.contains_key(*p))
                    .map(|(p, len)| (p.clone(), *len)),
            );
        }
        previous = current;
    }
    assert_eq!(property(db, "num-files-at-level0"), 0);
    assert_eq!(property(db, "num-files-at-level1"), previous.len() as u64);
    assert!(cold.len() > 1);
    cold
}

fn rss_kib(field: &str) -> Option<u64> {
    fs::read_to_string("/proc/self/status")
        .ok()?
        .lines()
        .find_map(|line| {
            line.strip_prefix(field)?
                .split_whitespace()
                .next()?
                .parse()
                .ok()
        })
}

fn property(db: &DB, name: &str) -> u64 {
    db.get_property(name).unwrap().parse().unwrap()
}

fn arrival(first: Instant, i: usize, rate: u64) -> Instant {
    if rate == 0 {
        return Instant::now();
    }
    let when = first + Duration::from_nanos(i as u64 * 1_000_000_000 / rate);
    if let Some(wait) = when.checked_duration_since(Instant::now()) {
        thread::sleep(wait);
    }
    when
}

#[derive(Default)]
struct Times {
    arrival_ns: Vec<u64>,
    service_ns: Vec<u64>,
}

impl Times {
    fn new(n: usize) -> Self {
        Self {
            arrival_ns: Vec::with_capacity(n),
            service_ns: Vec::with_capacity(n),
        }
    }
    fn record(&mut self, arrival: Instant, service: Instant) {
        let ack = Instant::now();
        self.arrival_ns.push((ack - arrival).as_nanos() as u64);
        self.service_ns.push((ack - service).as_nanos() as u64);
    }
    fn report(&mut self, name: &str) {
        self.arrival_ns.sort_unstable();
        self.service_ns.sort_unstable();
        let ix = (self.arrival_ns.len() * 99).div_ceil(100) - 1;
        println!(
            "{name}_ops={} {name}_arrival_p99_ns={} {name}_service_p99_ns={}",
            self.arrival_ns.len(),
            self.arrival_ns[ix],
            self.service_ns[ix]
        );
    }
}

fn verify(mut iter: DBIterator, mode: Mode, updates: usize, seed: u64) {
    let mut epochs = vec![0; PREFIXES.len() * ROWS];
    for i in 0..updates {
        let (index, suffix) = mode.hot_row(i);
        epochs[index * ROWS + suffix] = i + 1;
    }
    for (index, prefix) in PREFIXES.into_iter().enumerate() {
        for suffix in 0..ROWS {
            let k = key(prefix, suffix);
            let expected = value(&k, epochs[index * ROWS + suffix], seed);
            let (actual_key, actual_value) = iter.step_next().unwrap().unwrap();
            assert_eq!(actual_key, k);
            assert_eq!(actual_value, expected);
        }
    }
    assert!(iter.step_next().unwrap().is_none());
}

fn main() {
    let mode = match env::args().nth(1).as_deref() {
        Some("gap") => Mode::Gap,
        Some("native") => Mode::Native,
        Some("sentinel") => Mode::Sentinel,
        _ => panic!("expected gap, native or sentinel"),
    };
    let n = usize::try_from(setting("OPS", 16_384)).unwrap();
    let rate = setting("RATE", 0);
    let seed = setting("SEED", 7);
    let subs = usize::try_from(setting("SUBS", 1)).unwrap();
    let trace_cold = setting("COLD_ROWS", 1) != 0;
    assert!(n > 0 && n as u64 <= u64::MAX / 1_000_000_000 && subs > 0);
    let emitted = Arc::new(AtomicU64::new(0));
    let tmp = tempfile::tempdir().unwrap();
    let mut opts = DbOptions::default()
        .with_num_levels(2)
        .with_prefix_len(8)
        .with_target_file_size_base(128 << 10)
        .with_write_buffer_size(128 << 10)
        .with_max_immutable_memtables(4)
        .with_l0_compaction_trigger(2)
        .with_l0_slowdown_trigger(3)
        .with_l0_stop_trigger(5)
        .with_max_subcompactions(subs)
        .with_max_background_compactions(1);
    if trace_cold {
        let total = Arc::clone(&emitted);
        opts = opts.with_block_property_collectors(vec![Arc::new(move || {
            Box::new(ColdRows {
                mode,
                in_block: 0,
                total: Arc::clone(&total),
            })
        })]);
    }
    let db = DB::open(opts.clone(), tmp.path()).unwrap();
    let cold_files = preload(&db, tmp.path(), mode, seed);
    let snapshot = db.snapshot();
    // Warm all input blocks in both arms; verify the identical starting data.
    verify(snapshot.iter().unwrap(), mode, 0, seed);
    emitted.store(0, Ordering::Relaxed);
    let l1 = property(&db, "num-files-at-level1");
    let preloaded_rss = rss_kib("VmRSS:");
    let fence = WriteOptions::default().with_durability(DurabilityMode::WalSync);
    let ready = Barrier::new(2);
    let first = std::sync::OnceLock::new();
    let (mut writes, mut reads, counters, write_time) = thread::scope(|s| {
        let reader = s.spawn(|| {
            let mut times = Times::new(n);
            ready.wait();
            let start = *first.get_or_init(Instant::now);
            for i in 0..n {
                let k = key(PREFIXES[i % PREFIXES.len()], i.wrapping_mul(97) % ROWS);
                let scheduled = arrival(start, i, rate);
                let service = Instant::now();
                let bytes = db.get_pinned(&k).unwrap().unwrap();
                times.record(scheduled, service);
                assert_eq!(&bytes[..KEY_SIZE], &k);
            }
            times
        });
        let mut times = Times::new(n);
        take_thread_write_profile();
        ready.wait();
        let start = *first.get_or_init(Instant::now);
        for i in 0..n {
            let (index, suffix) = mode.hot_row(i);
            let k = key(PREFIXES[index], suffix);
            let v = value(&k, i + 1, seed);
            let scheduled = arrival(start, i, rate);
            let service = Instant::now();
            db.put(&k, &v).unwrap();
            times.record(scheduled, service);
            if (i + 1) % 256 == 0 {
                if mode == Mode::Sentinel {
                    db.delete_with_options(&fence, &[1]).unwrap();
                } else {
                    db.sync_wal().unwrap();
                }
            }
        }
        let write_time = start.elapsed();
        let counters = take_thread_write_profile();
        (times, reader.join().unwrap(), counters, write_time)
    });
    let after = db.l0_compaction_profile();
    if let (Some(preload), Some(peak)) = (preloaded_rss, rss_kib("VmHWM:")) {
        println!("preload_rss_kib={preload} peak_rss_kib={peak}");
    }
    let l0 = property(&db, "num-files-at-level0");
    let imm = db.memory_usage().immutable_memtable_count;
    let logical = (n * (KEY_SIZE + VALUE_SIZE)) as u64;
    writes.report("write");
    reads.report("read");
    println!(
        "mode={mode:?} seed={seed} rate={rate} subs={subs} trace_cold={trace_cold} user_bytes={logical} writer_ns={} l0_slowdowns={} l0_stops={} l0_drains={} l0_stall_ns={} immutable_wait_ns={}",
        write_time.as_nanos(),
        counters.l0_slowdowns,
        counters.l0_stops,
        counters.l0_drains,
        counters.l0_slowdown_ns + counters.l0_stop_ns,
        counters.immutable_wait_ns
    );
    println!(
        "l1_start={l1} l0_after_workers={l0} immutable_after_workers={imm} rewrite_jobs_after_workers={} input_bytes_after_workers={} output_bytes_after_workers={}",
        after.rewrite_jobs, after.input_file_bytes, after.output_file_bytes
    );
    db.flush().unwrap();
    verify(snapshot.iter().unwrap(), mode, 0, seed);
    verify(db.iter().unwrap(), mode, n, seed);
    drop(snapshot);
    db.close().unwrap();
    let settled = db.l0_compaction_profile();
    let cold_removed = cold_files
        .iter()
        .filter(|(p, _)| !p.exists())
        .map(|(_, n)| n)
        .sum::<u64>();
    let cold_logical = if trace_cold {
        (emitted.load(Ordering::Relaxed) * (KEY_SIZE + VALUE_SIZE) as u64).to_string()
    } else {
        "disabled".to_string()
    };
    println!(
        "flushes={} l0_end={} rewrite_jobs={} multi_target_jobs={} parallel_jobs={} ranges={} peak_ranges={} peak_l0={} trivial_moves={} input_file_bytes={} output_file_bytes={} initial_cold_files={} initial_cold_consumed_bytes={cold_removed} logical_cold_emitted_bytes={cold_logical}",
        property(&db, "stats.flushes_completed"),
        property(&db, "num-files-at-level0"),
        settled.rewrite_jobs,
        settled.multiple_target_jobs,
        settled.parallel_jobs,
        settled.ranges_started,
        settled.peak_active_ranges,
        settled.peak_installed_l0_files,
        settled.trivial_moves,
        settled.input_file_bytes,
        settled.output_file_bytes,
        cold_files.len()
    );
    let reopened = DB::open(opts, tmp.path()).unwrap();
    verify(reopened.iter().unwrap(), mode, n, seed);
}
