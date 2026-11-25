//! Time lookup through return of all pins. Both arms verify and drop them after the timer.
//! Prepare once, then open the immutable store in a fresh process per trial.
//! `grouped_reads prepare DIR [ROWS]` or `grouped_reads DIR warm|page|device
//! scalar|grouped clustered|scattered FANOUT REQUESTS SEED`. Linux device
//! verification uses /proc/self/io and Python's file-scoped posix_fadvise.
use std::{env, fs, io::Read, path::Path, process::Command, time::Instant};

use mmdb::{DB, DbOptions, PinnedSlice, ReadOptions, WriteBatch};
use smallvec::SmallVec;

const DEFAULT_ROWS: u64 = 1 << 18;
const VALUE_BYTES: usize = 128;
const MAX_KEYS: usize = 32;
const PIN_BUDGET: usize = MAX_KEYS * (4 * 1024 + VALUE_BYTES + 256);

fn mix(mut x: u64) -> u64 {
    x = (x ^ (x >> 30)).wrapping_mul(0xbf58476d1ce4e5b9);
    x = (x ^ (x >> 27)).wrapping_mul(0x94d049bb133111eb);
    x ^ (x >> 31)
}

fn next(rng: &mut u64) -> u64 {
    *rng = rng.wrapping_add(0x9e3779b97f4a7c15);
    mix(*rng)
}

fn options(cache: bool) -> DbOptions {
    DbOptions::default()
        .with_block_cache_capacity(if cache { 128 << 20 } else { 0 })
        .with_block_size(4 << 10)
        .with_target_file_size_base(1 << 30)
        .with_pin_l0_first_block_and_warm_index(false)
}

fn fixture_value(key: u64) -> [u8; VALUE_BYTES] {
    let mut value = [0u8; VALUE_BYTES];
    let mut stream = key;
    for word in value.as_chunks_mut::<8>().0 {
        word.copy_from_slice(&next(&mut stream).to_be_bytes());
    }
    value[..8].copy_from_slice(&key.to_be_bytes());
    value
}

fn prepare(dir: &Path, rows: u64, overlap: bool) {
    assert!(rows >= 32 && rows.is_power_of_two());
    let opts = options(true)
        .with_error_if_exists(true)
        .with_write_buffer_size(128 << 20);
    let db = DB::open(opts, dir).unwrap();
    let records = (0..rows).map(|key| (key.to_be_bytes(), fixture_value(key)));
    assert_eq!(db.ingest_sorted(records).unwrap(), rows);
    if overlap {
        let mut batch = WriteBatch::new();
        for key in (1..rows).step_by(2) {
            let mut value = fixture_value(key);
            value[8] ^= 0xff;
            batch.put(&key.to_be_bytes(), &value);
            if batch.len() == 2048 {
                db.write(std::mem::take(&mut batch)).unwrap();
            }
        }
        db.write(batch).unwrap();
        db.flush().unwrap();
        assert_eq!(db.get_property("num-files-at-level0").as_deref(), Some("1"));
        assert_eq!(db.get_property("num-files-at-level6").as_deref(), Some("1"));
    }
    db.close().unwrap();
    fs::write(dir.with_extension("rows"), rows.to_string()).unwrap();
}

fn warm_os_cache(files: &[fs::DirEntry]) {
    let mut buffer = vec![0u8; 1 << 20];
    for file in files {
        let mut file = fs::File::open(file.path()).unwrap();
        while file.read(&mut buffer).unwrap() != 0 {}
    }
}

fn evict_os_cache(dir: &Path) {
    let success = Command::new("python3")
        .args(["-c", "import glob,os,sys\nfor name in glob.glob(os.path.join(sys.argv[1],'*.sst')):\n with open(name,'rb') as f: os.posix_fadvise(f.fileno(),0,0,os.POSIX_FADV_DONTNEED)"])
        .arg(dir).status().unwrap().success();
    assert!(success, "could not request file-scoped page eviction");
}

fn storage_reads() -> u64 {
    fs::read_to_string("/proc/self/io")
        .expect("cache-regime verification needs Linux process I/O accounting")
        .lines()
        .find_map(|line| line.strip_prefix("read_bytes:"))
        .expect("missing Linux read_bytes counter")
        .trim()
        .parse()
        .expect("invalid read_bytes counter")
}

fn block_probes(db: &DB) -> u64 {
    ["stats.block_cache_hits", "stats.block_cache_misses"]
        .iter()
        .map(|name| db.get_property(name).unwrap().parse::<u64>().unwrap())
        .sum()
}

fn requests(
    db: &DB,
    opts: &ReadOptions<'_>,
    grouped: bool,
    keys: &[&[u8]],
) -> Vec<Option<PinnedSlice>> {
    if grouped {
        db.get_many_pinned_with_options(opts, keys).unwrap()
    } else {
        let mut values = Vec::with_capacity(keys.len());
        for key in keys {
            values.push(db.get_pinned_with_options(opts, key).unwrap());
        }
        values
    }
}

fn check_pins(values: &[Option<PinnedSlice>], keys: &[&[u8]]) -> usize {
    assert_eq!(values.len(), keys.len());
    let mut owners: SmallVec<[(usize, usize); MAX_KEYS]> = SmallVec::new();
    for (value, key) in values.iter().zip(keys) {
        let value = value.as_ref().unwrap();
        assert_eq!(value.len(), VALUE_BYTES);
        assert_eq!(&value[..8], *key);
        let backing = value.sst_backing_charge().expect("fixture uses SST values");
        if !owners.contains(&backing) {
            owners.push(backing);
        }
    }
    owners.iter().map(|&(_, charge)| charge).sum()
}

fn main() {
    let a: Vec<_> = env::args().skip(1).filter(|a| a != "--bench").collect();
    let preparing = matches!(
        a.first().map(String::as_str),
        Some("prepare" | "prepare-overlap")
    );
    if a.is_empty() || (a.len() == 1 && !preparing) {
        println!(
            "grouped_reads: use 'prepare DIR', then 'DIR warm|page|device scalar|grouped clustered|scattered FANOUT REQUESTS SEED'"
        );
        return;
    }
    if preparing {
        assert!((2..=3).contains(&a.len()), "prepare DIR [ROWS]");
        prepare(
            Path::new(&a[1]),
            a.get(2).map_or(DEFAULT_ROWS, |s| s.parse().unwrap()),
            a[0] == "prepare-overlap",
        );
        return;
    }
    assert_eq!(
        a.len(),
        7,
        "DIR warm|page|device scalar|grouped clustered|scattered FANOUT REQUESTS SEED"
    );
    let dir = Path::new(&a[0]);
    let regime = a[1].as_str();
    assert!(matches!(regime, "warm" | "page" | "device"));
    let grouped = match a[2].as_str() {
        "scalar" => false,
        "grouped" => true,
        _ => panic!("unknown method"),
    };
    let clustered = match a[3].as_str() {
        "clustered" => true,
        "scattered" => false,
        _ => panic!("unknown locality"),
    };
    let fanout: usize = a[4].parse().unwrap();
    assert!(matches!(fanout, 1 | 8 | 32));
    let samples: usize = a[5].parse().unwrap();
    assert!(samples > 0);
    let mut rng: u64 = a[6].parse().unwrap();
    let rows: u64 = fs::read_to_string(dir.with_extension("rows"))
        .unwrap()
        .trim()
        .parse()
        .unwrap();
    let mask = rows - 1;

    let db = DB::open_read_only_with_options(options(regime == "warm"), dir).unwrap();
    let snapshot = db.snapshot();
    let opts = snapshot.read_options();
    let files: Vec<_> = fs::read_dir(dir)
        .unwrap()
        .map(Result::unwrap)
        .filter(|f| f.path().extension().is_some_and(|ext| ext == "sst"))
        .collect();
    assert_eq!(files.len(), 1, "one-SST experiment");
    match regime {
        "warm" => {
            for id in 0..rows {
                assert!(snapshot.get_pinned(&id.to_be_bytes()).unwrap().is_some());
            }
        }
        "page" => warm_os_cache(&files),
        "device" => {}
        _ => unreachable!(),
    }
    let before_blocks = block_probes(&db);
    let before_tables = mmdb::point_table_probes();
    let mut before_storage = storage_reads();
    let mut device_elapsed = Vec::new();
    let mut io_bytes = 0;
    let mut peak_pin_bytes = 0;
    let mut elapsed = Vec::with_capacity(samples);
    for n in 0..samples {
        if regime == "device" && n % 128 == 0 {
            evict_os_cache(dir);
            before_storage = storage_reads();
        }
        let base = (next(&mut rng) & mask) & !((fanout - 1) as u64);
        let mut encoded = [[0u8; 8]; MAX_KEYS];
        for (slot, bytes) in encoded[..fanout].iter_mut().enumerate() {
            let id = if clustered {
                base + ((slot * 13) % fanout) as u64
            } else {
                next(&mut rng) & mask
            };
            *bytes = id.to_be_bytes();
        }
        let views = encoded.each_ref().map(|k| k.as_slice());
        let keys = &views[..fanout];
        let start = Instant::now();
        let values = requests(&db, &opts, grouped, keys);
        elapsed.push(start.elapsed().as_nanos() as u64);
        peak_pin_bytes = peak_pin_bytes.max(check_pins(&values, keys));
        if regime == "device" {
            let after = storage_reads();
            let delta = after - before_storage;
            if delta >= 4096 {
                device_elapsed.push(*elapsed.last().unwrap());
            }
            io_bytes += delta;
            before_storage = after;
        }
    }
    if regime != "device" {
        io_bytes = storage_reads() - before_storage;
    }
    assert!(
        peak_pin_bytes <= PIN_BUDGET,
        "returned SST allocation budget exceeded: {peak_pin_bytes}"
    );
    let blocks = block_probes(&db) - before_blocks;
    let tables = mmdb::point_table_probes() - before_tables;
    if !grouped || clustered {
        assert_eq!(tables, (samples * fanout) as u64);
    } else {
        // The scattered trace can repeat a key within a single request.
        assert!((samples as u64..=(samples * fanout) as u64).contains(&tables));
    }
    elapsed.sort_unstable();
    device_elapsed.sort_unstable();
    let io_requests = device_elapsed.len();
    let device_p99 = if io_requests == 0 {
        0
    } else {
        device_elapsed[(io_requests * 99).div_ceil(100) - 1]
    };
    let p99 = elapsed[(samples * 99).div_ceil(100) - 1];
    let p50 = elapsed[samples.div_ceil(2) - 1];
    println!(
        "regime={regime} method={} locality={} fanout={fanout} samples={samples} p50_ns={p50} p99_ns={p99} blocks={blocks} tables={tables} storage_bytes={io_bytes} device_requests={io_requests} device_p99_ns={device_p99} peak_pin_bytes={peak_pin_bytes} pin_budget={PIN_BUDGET}",
        a[2], a[3]
    );
}
