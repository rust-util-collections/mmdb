//! External binary with a normal MMDB dependency and no engine instrumentation.
//! The tiny guard crate's optional "grouped-api" feature enables calling the new API.
//! Usage: FIXTURE REQUESTS SEED [scalar32|grouped32].
use mmdb::{DB, DbOptions, PinnedSlice, ReadOptions};
use std::{env, fs, path::Path, time::Instant};
const STEP: u64 = 0x9e3779b97f4a7c15;

fn mixed(mut x: u64) -> u64 {
    x = (x ^ (x >> 30)).wrapping_mul(0xbf58476d1ce4e5b9);
    x = (x ^ (x >> 27)).wrapping_mul(0x94d049bb133111eb);
    x ^ (x >> 31)
}

fn check(value: &PinnedSlice, key: &[u8; 8], overlap: bool) {
    let num = u64::from_be_bytes(*key);
    let base = mixed(num.wrapping_add(STEP.wrapping_mul(2))).to_be_bytes()[0];
    assert_eq!(value.len(), 128);
    assert_eq!(&value[..8], key);
    assert_eq!(value[8], if overlap && num % 2 == 1 { !base } else { base });
}

fn many(db: &DB, ro: &ReadOptions<'_>, keys: &[&[u8]], grouped: bool) -> Vec<Option<PinnedSlice>> {
    if grouped {
        #[cfg(feature = "grouped-api")]
        return db.get_many_pinned_with_options(ro, keys).unwrap();
        #[cfg(not(feature = "grouped-api"))]
        panic!("the baseline guard supports only scalar calls");
    }
    let mut results = Vec::with_capacity(keys.len());
    for key in keys {
        results.push(db.get_pinned_with_options(ro, key).unwrap());
    }
    results
}

fn main() {
    let args: Vec<_> = env::args().collect();
    assert!(
        (4..=5).contains(&args.len()),
        "FIXTURE REQUESTS SEED [scalar32|grouped32]"
    );
    let fixture = Path::new(&args[1]);
    let samples: usize = args[2].parse().unwrap();
    let mut state: u64 = args[3].parse().unwrap();
    let mode = args.get(4).map_or("single", String::as_str);
    assert!(matches!(mode, "single" | "scalar32" | "grouped32"));
    let rows: u64 = fs::read_to_string(fixture.with_extension("rows"))
        .unwrap()
        .parse()
        .unwrap();
    assert!(samples > 0 && rows.is_power_of_two());
    let options = DbOptions::default()
        .with_block_cache_capacity(128 << 20)
        .with_block_size(4096)
        .with_pin_l0_first_block_and_warm_index(false);
    let db = DB::open_read_only_with_options(options, fixture).unwrap();
    let overlap = db.get_property("num-files-at-level0").as_deref() == Some("1");
    assert_eq!(db.get_property("num-files-at-level6").as_deref(), Some("1"));
    let snapshot = db.snapshot();
    let read = snapshot.read_options();
    for id in 0..rows {
        assert!(
            db.get_pinned_with_options(&read, &id.to_be_bytes())
                .unwrap()
                .is_some()
        );
    }
    let mut durations = Vec::with_capacity(samples);
    for _ in 0..samples {
        if mode == "single" {
            state = state.wrapping_add(STEP.wrapping_mul(2));
            let key = (mixed(state) & (rows - 1)).to_be_bytes();
            let start = Instant::now();
            let value = db.get_pinned_with_options(&read, &key).unwrap().unwrap();
            durations.push(start.elapsed().as_nanos() as u64);
            check(&value, &key, overlap);
        } else {
            state = state.wrapping_add(STEP);
            let base = (mixed(state) & (rows - 1)) & !31;
            let encoded: [[u8; 8]; 32] =
                std::array::from_fn(|i| (base + ((i * 13) % 32) as u64).to_be_bytes());
            let keys = encoded.each_ref().map(|key| key.as_slice());
            let start = Instant::now();
            let held = many(&db, &read, &keys, mode == "grouped32");
            durations.push(start.elapsed().as_nanos() as u64);
            assert_eq!(held.len(), encoded.len());
            for (value, key) in held.iter().zip(&encoded) {
                check(value.as_ref().unwrap(), key, overlap);
            }
        }
    }
    durations.sort_unstable();
    println!(
        "samples={samples} p50_ns={} p99_ns={}",
        durations[samples.div_ceil(2) - 1],
        durations[(samples * 99).div_ceil(100) - 1]
    );
}
