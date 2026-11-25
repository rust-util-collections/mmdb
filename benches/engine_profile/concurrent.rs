use std::{
    collections::BTreeMap,
    path::Path,
    sync::{Arc, Barrier, OnceLock, mpsc},
    thread,
    time::{Duration, Instant},
};

use mmdb::{BlockCachePool, DB, DurabilityMode, WriteBatch, WriteOptions};

use super::{Config, KEYS, MIB, key, mix, property, rss_kib, setting, value};

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
enum Operation {
    SmallPut,
    Batch,
    Read,
    FreshSnapshot,
    HeldSnapshot,
}

struct Stream {
    operation: Operation,
    db: usize,
    prefix: u64,
    count: usize,
    entries: usize,
    period: Duration,
}

struct Sample {
    service_ns: u64,
    latency_ns: u64,
    finish_ns: u64,
}

#[derive(Default)]
struct Samples {
    offered: usize,
    rejected: usize,
    bytes: u64,
    outstanding_half: usize,
    outstanding_end: usize,
    rows: Vec<Sample>,
    #[cfg(feature = "bench-metrics")]
    profile: mmdb::ThreadWriteProfile,
}

fn streams(config: &Config, memory: bool, placement: &str) -> Vec<Stream> {
    let count = config.operations;
    let period = Duration::from_nanos(1_000_000_000_u64.checked_div(config.rate).unwrap_or(0));
    assert!(config.rate <= 1_000_000_000);
    let control = setting("CONTROL", String::from("mixed"));
    assert!(matches!(control.as_str(), "mixed" | "writer" | "reader"));
    let mut streams = Vec::new();
    let mut add = |operation, db, prefix, entries, ratio: u32| {
        streams.push(Stream {
            operation,
            db,
            prefix,
            entries,
            count: count.div_ceil(ratio as usize),
            period: period * ratio,
        });
    };
    if control != "reader" {
        add(Operation::SmallPut, 0, 0, 1, 1);
    }
    if control != "writer" {
        add(Operation::Read, 0, 0, 1, 1);
    }
    if control == "mixed" && memory {
        for i in 0..16 {
            let db = if placement == "all" { i } else { 0 };
            add(
                Operation::Batch,
                db,
                1 + i as u64,
                setting("BATCH_ENTRIES", 64),
                1,
            );
        }
    } else if control == "mixed" {
        add(Operation::FreshSnapshot, 0, 0, 1, 1);
        add(Operation::HeldSnapshot, 0, 0, 1, 1);
        add(
            Operation::Batch,
            usize::from(placement == "split"),
            1,
            setting("BATCH_ENTRIES", 4096),
            setting("BATCH_RATIO", 128),
        );
    }
    streams
}

fn preload(db: &DB, config: &Config) {
    let opts = WriteOptions::default().with_durability(DurabilityMode::DisableWal);
    let mut bytes = [0; 128];
    for start in (0..KEYS).step_by(1024) {
        let mut batch = WriteBatch::new();
        for i in start..start + 1024 {
            value(config.seed ^ i as u64, config.entropy, &mut bytes);
            batch.put(&key(0, i), &bytes);
        }
        db.write_with_options(&opts, batch).unwrap();
    }
    db.flush().unwrap();
}

fn nanos(d: Duration) -> u64 {
    u64::try_from(d.as_nanos()).expect("timing exceeds u64 nanoseconds")
}

fn run_stream(
    db: &DB,
    config: &Config,
    s: Stream,
    barrier: &Barrier,
    clock: &OnceLock<Instant>,
) -> Samples {
    assert!(s.entries > 0 && s.count > 0 && s.count <= u32::MAX as usize);
    let opts = config.write_options();
    let held = (s.operation == Operation::HeldSnapshot).then(|| db.snapshot());
    // Use distinct precomputed values for scalar puts too. A repeated single
    // random value would still compress well across successive WAL records.
    let slots = s.entries.max(512);
    let mut corpus = vec![0; slots * 256];
    value(config.seed ^ s.prefix, config.entropy, &mut corpus);
    let mut samples = Samples {
        offered: s.count,
        rows: Vec::with_capacity(s.count),
        ..Default::default()
    };
    barrier.wait();
    barrier.wait();
    let start = *clock.get().unwrap();
    for i in 0..s.count {
        let scheduled = start + s.period * i as u32;
        if !s.period.is_zero()
            && let Some(delay) = scheduled.checked_duration_since(Instant::now())
        {
            thread::sleep(delay);
        }
        let dispatch = Instant::now();
        let begin;
        let suffix = mix(i as u64 ^ config.seed) as usize % KEYS;
        let k = key(s.prefix, suffix);
        let result = match s.operation {
            Operation::SmallPut => {
                let offset = suffix % slots * 256;
                begin = Instant::now();
                db.put_with_options(&opts, &k, &corpus[offset..offset + 128])
            }
            Operation::Batch => {
                let mut batch = WriteBatch::new();
                for j in 0..s.entries {
                    let k = key(s.prefix, (suffix + 8191 * j) % KEYS);
                    let offset = (j + suffix) % slots * 256;
                    batch.put(&k, &corpus[offset..offset + 256]);
                }
                begin = Instant::now();
                db.write_with_options(&opts, batch)
            }
            Operation::Read | Operation::FreshSnapshot | Operation::HeldSnapshot => {
                begin = Instant::now();
                let v = match &held {
                    Some(view) => view.get_pinned(&k),
                    None if s.operation == Operation::FreshSnapshot => db.snapshot().get_pinned(&k),
                    None => db.get_pinned(&k),
                };
                v.map(|v| assert_eq!(v.unwrap().len(), 128))
            }
        };
        let end = Instant::now();
        if let Err(error) = result {
            assert_eq!(error.kind(), mmdb::ErrorKind::Busy, "{error}");
            samples.rejected += 1;
            continue;
        }
        let service = nanos(end.duration_since(begin));
        samples.rows.push(Sample {
            service_ns: service,
            latency_ns: if s.period.is_zero() {
                nanos(end.duration_since(dispatch))
            } else {
                nanos(end.duration_since(scheduled))
            },
            finish_ns: nanos(end.duration_since(start)),
        });
        samples.bytes += match s.operation {
            Operation::SmallPut => 144,
            Operation::Batch => (s.entries * (16 + 256)) as u64,
            _ => 0,
        };
    }
    if !s.period.is_zero() {
        let outstanding = |offered: usize| {
            let cut = nanos(s.period * offered as u32);
            offered.saturating_sub(samples.rows.partition_point(|r| r.finish_ns <= cut))
        };
        samples.outstanding_half = outstanding(s.count / 2);
        samples.outstanding_end = outstanding(s.count);
    }
    #[cfg(feature = "bench-metrics")]
    {
        samples.profile = mmdb::take_thread_write_profile();
    }
    samples
}

#[derive(Clone, Copy, Debug, Default)]
struct Observation {
    active_bytes: u64,
    immutable_bytes: u64,
    generations: u64,
    pin_bytes: u64,
    l0_files: u64,
    segmented_bytes: u64,
    overflow_bytes: u64,
    rss_kib: u64,
}

impl Observation {
    fn observe_peak(&mut self, sample: Self) {
        self.active_bytes = self.active_bytes.max(sample.active_bytes);
        self.immutable_bytes = self.immutable_bytes.max(sample.immutable_bytes);
        self.generations = self.generations.max(sample.generations);
        self.pin_bytes = self.pin_bytes.max(sample.pin_bytes);
        self.l0_files = self.l0_files.max(sample.l0_files);
        self.segmented_bytes = self.segmented_bytes.max(sample.segmented_bytes);
        self.overflow_bytes = self.overflow_bytes.max(sample.overflow_bytes);
        self.rss_kib = self.rss_kib.max(sample.rss_kib);
    }
}

fn sample(dbs: &[DB], pool: &BlockCachePool) -> Observation {
    let usage = pool.usage();
    let mut totals = Observation {
        segmented_bytes: usage.segmented_weighted_bytes,
        overflow_bytes: usage.overflow_weighted_bytes,
        rss_kib: rss_kib("VmRSS:").unwrap_or_default(),
        ..Observation::default()
    };
    for db in dbs {
        let m = db.memory_usage();
        totals.active_bytes += m.active_memtable_bytes as u64;
        totals.immutable_bytes += m.immutable_memtable_bytes as u64;
        totals.generations += m.immutable_memtable_count as u64;
        totals.pin_bytes += m.l0_pin_bytes;
        totals.l0_files += property(db, "num-files-at-level0");
    }
    totals
}

fn print_percentiles(values: &mut [u64]) -> (f64, f64) {
    values.sort_unstable();
    let percentile = |p: usize| values[(values.len() * p).div_ceil(100) - 1] as f64 / 1000.0;
    (percentile(50), percentile(99))
}

pub(super) fn run(config: &Config, mode: &str, path: &Path) {
    let memory = mode == "memory";
    let placement = setting("PLACEMENT", String::from("hot"));
    assert!(
        placement == "hot" || (memory && placement == "all") || (!memory && placement == "split")
    );
    let pool = Arc::new(BlockCachePool::new(config.pool_mib * MIB as u64));
    let count = if memory {
        16
    } else if placement == "split" {
        2
    } else {
        1
    };
    let mut dbs = Vec::new();
    for id in 0..count {
        let db = DB::open(config.options(&pool), path.join(format!("shard-{id}"))).unwrap();
        preload(&db, config);
        dbs.push(db);
    }
    let before = sample(&dbs, &pool);
    let prop_sum = |name| dbs.iter().map(|db| property(db, name)).sum::<u64>();
    let flush_before = prop_sum("stats.flushes_completed");
    let compact_before = prop_sum("stats.compactions_completed");
    let writes_before = prop_sum("stats.compaction_bytes_written");
    let streams = streams(config, memory, &placement);
    let barrier = Barrier::new(streams.len() + 1);
    let clock = OnceLock::new();
    let (runs, peak, mem) = thread::scope(|scope| {
        let (tx, rx) = mpsc::channel::<()>();
        let sources = (&dbs[..], &*pool);
        let sampler = config.sample.then(|| {
            scope.spawn(move || {
                let mut maxima = before;
                while rx
                    .recv_timeout(Duration::from_millis(50))
                    .is_err_and(|e| e == mpsc::RecvTimeoutError::Timeout)
                {
                    maxima.observe_peak(sample(sources.0, sources.1));
                }
                maxima
            })
        });
        let mut threads = Vec::new();
        for s in streams {
            let (db, barrier, clock) = (&dbs[s.db], &barrier, &clock);
            let op = s.operation;
            threads.push((
                op,
                scope.spawn(move || run_stream(db, config, s, barrier, clock)),
            ));
        }
        barrier.wait();
        clock.set(Instant::now()).unwrap();
        barrier.wait();
        let runs: Vec<_> = threads
            .into_iter()
            .map(|(op, t)| (op, t.join().unwrap()))
            .collect();
        drop(tx);
        let mem = sampler.map(|t| t.join().unwrap());
        let peak = rss_kib("VmHWM:").unwrap_or_default();
        (runs, peak, mem)
    });
    let wall = runs
        .iter()
        .filter_map(|(_, run)| run.rows.last())
        .map(|row| row.finish_ns)
        .max()
        .expect("no successful requests") as f64
        / 1e9;
    let mut by_class: BTreeMap<Operation, Samples> = BTreeMap::new();
    for (op, mut run) in runs {
        #[cfg(feature = "bench-metrics")]
        println!("stage operation={op:?} {:?}", run.profile);
        let all = by_class.entry(op).or_default();
        all.offered += run.offered;
        all.rejected += run.rejected;
        all.bytes += run.bytes;
        all.outstanding_half += run.outstanding_half;
        all.outstanding_end += run.outstanding_end;
        all.rows.append(&mut run.rows);
    }
    let after = sample(&dbs, &pool);
    println!(
        "run placement={placement} dbs={count} seconds={wall:.6} rss_after_preload_kib={} peak_kib={peak} before_l0={} after_l0={} after_immutables={} flushes={} compactions={} compaction_output_bytes={} instrumented={}",
        before.rss_kib,
        before.l0_files,
        after.l0_files,
        after.generations,
        prop_sum("stats.flushes_completed") - flush_before,
        prop_sum("stats.compactions_completed") - compact_before,
        prop_sum("stats.compaction_bytes_written") - writes_before,
        cfg!(feature = "bench-metrics")
    );
    if let Some(sampled) = mem {
        println!("sampled_peaks {sampled:?}");
    }
    for (op, all) in by_class {
        let mut times: Vec<_> = all.rows.iter().map(|r| r.latency_ns).collect();
        let (p50, p99) = print_percentiles(&mut times);
        for (t, row) in times.iter_mut().zip(&all.rows) {
            *t = row.service_ns;
        }
        let (_, svc) = print_percentiles(&mut times);
        println!(
            "class op={op:?} offered={} completed={} rejected={} bytes={} bytes_s={:.0} ops_s={:.0} arrival_p50_us={p50:.3} arrival_p99_us={p99:.3} service_p99_us={svc:.3} p99_qualified={} outstanding_half={} outstanding_end={}",
            all.offered,
            all.rows.len(),
            all.rejected,
            all.bytes,
            all.bytes as f64 / wall,
            all.rows.len() as f64 / wall,
            all.rows.len() >= 10_000,
            all.outstanding_half,
            all.outstanding_end
        );
        assert_eq!(
            all.rows.len(),
            all.offered,
            "all offered requests must complete"
        );
    }
    for db in dbs {
        db.close().unwrap();
    }
}
