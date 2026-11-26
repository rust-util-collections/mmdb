//! Fresh-process trials for sorted SSTs and WAL acknowledgement policies.
use std::{
    env, fs,
    hint::black_box,
    io::{self, Read},
    os::unix::fs::MetadataExt,
    path::Path,
    process,
    time::{Duration, Instant},
};

use mmdb::{CompressionType, DB, DbOptions, DurabilityMode, WriteOptions};

const ROWS: u64 = 1 << 18;
const LOOKUPS: usize = 40_000;
const PUTS: u64 = 10_000;

fn mix(mut x: u64) -> u64 {
    x = (x ^ (x >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    x = (x ^ (x >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    x ^ (x >> 31)
}

fn value<const N: usize>(id: u64, seed: u64, patterned: bool) -> [u8; N] {
    let mut data = [0; N];
    let base = mix(seed ^ id);
    for (i, word) in data.as_chunks_mut::<8>().0.iter_mut().enumerate() {
        let offset = if patterned { i % 4 } else { i };
        word.copy_from_slice(&mix(base.wrapping_add(offset as u64)).to_le_bytes());
    }
    data
}

fn p99(ns: &mut [u64]) -> u64 {
    ns.sort_unstable();
    ns[(ns.len() * 99).div_ceil(100) - 1]
}

#[cfg(target_os = "linux")]
fn disk_reads() -> u64 {
    fs::read_to_string("/proc/self/io")
        .unwrap()
        .lines()
        .find_map(|line| line.strip_prefix("read_bytes:"))
        .unwrap()
        .trim()
        .parse()
        .unwrap()
}

// Linux whole-device sectors are supplementary evidence. On ZFS the process
// counter alone can also rise on ARC hits.
fn device_read_sectors() -> Option<u64> {
    let names = env::var("MMDB_TRIAL_DISKS").ok()?;
    Some(
        names
            .split(',')
            .map(|name| {
                assert!(
                    !name.is_empty()
                        && name.bytes().all(|c| c.is_ascii_alphanumeric() || c == b'-')
                );
                let stats = fs::read_to_string(format!("/sys/class/block/{name}/stat")).unwrap();
                stats
                    .split_whitespace()
                    .nth(2)
                    .unwrap()
                    .parse::<u64>()
                    .unwrap()
            })
            .sum(),
    )
}

#[cfg(target_os = "macos")]
fn disk_reads() -> u64 {
    let mut usage = std::mem::MaybeUninit::<libc::rusage_info_v2>::uninit();
    // SAFETY: the output structure matches the v2 flavor; success fills it.
    let status = unsafe {
        libc::proc_pid_rusage(
            process::id() as libc::pid_t,
            libc::RUSAGE_INFO_V2,
            usage.as_mut_ptr().cast(),
        )
    };
    assert_eq!(status, 0, "{}", io::Error::last_os_error());
    // SAFETY: the kernel filled the requested v2 structure.
    unsafe { usage.assume_init() }.ri_diskio_bytesread
}

// Prewarm immutable table files, then report their logical and allocated bytes.
fn warm_tables(dir: &Path) -> io::Result<(usize, u64, u64)> {
    let mut buffer = vec![0u8; 1 << 20];
    let (mut files, mut size, mut alloc) = (0, 0, 0);
    for entry in fs::read_dir(dir)? {
        let path = entry?.path();
        if path.extension().is_some_and(|ext| ext == "sst") {
            let mut file = fs::File::open(path)?;
            let meta = file.metadata()?;
            size += meta.len();
            alloc += meta.blocks() * 512;
            files += 1;
            while file.read(&mut buffer)? != 0 {}
        }
    }
    assert!(files > 0 && size > 0);
    Ok((files, size, alloc))
}

fn sst(dir: &Path, compression: CompressionType, patterned: bool, seed: u64) {
    let options = DbOptions::default()
        .with_compression(compression)
        .with_block_size(4096)
        .with_block_cache_capacity(0)
        .with_pin_l0_first_block_and_warm_index(false);
    let db = DB::open(options.clone(), dir).unwrap();
    let start = Instant::now();
    let entries = (0..ROWS).map(|id| (id.to_be_bytes(), value::<256>(id, seed, patterned)));
    assert_eq!(db.ingest_sorted(entries).unwrap(), ROWS);
    let ingest = start.elapsed();
    db.close().unwrap();
    drop(db);

    let db = DB::open_read_only_with_options(options, dir).unwrap();
    let (files, bytes, allocated) = warm_tables(dir).unwrap();
    let view = db.snapshot();
    let opts = view.read_options().with_fill_cache(false);
    let before_sectors = device_read_sectors();
    let before = disk_reads();
    let start = Instant::now();
    let mut scan = db.iter_with_options(&opts).unwrap();
    let mut scanned = 0;
    while let Some(row) = scan.step_next().unwrap() {
        black_box(row);
        scanned += 1;
    }
    let scan_time = start.elapsed();
    assert_eq!(scanned, ROWS);
    drop(scan);

    let mut ticks = Vec::with_capacity(LOOKUPS);
    for n in 0..LOOKUPS {
        let id = mix(seed.wrapping_add(n as u64)) % ROWS;
        let key = id.to_be_bytes();
        let start = Instant::now();
        let found = db.get_pinned_with_options(&opts, &key).unwrap().unwrap();
        ticks.push(start.elapsed().as_nanos() as u64);
        assert_eq!(&*found, &value::<256>(id, seed, patterned));
    }
    let io = disk_reads() - before;
    let sector_delta = before_sectors
        .zip(device_read_sectors())
        .map(|(a, b)| b - a);
    let mut scan = db.iter_with_options(&opts).unwrap();
    for id in 0..ROWS {
        let (key, bytes) = scan.step_next().unwrap().unwrap();
        assert_eq!(key, id.to_be_bytes());
        assert_eq!(bytes, &value::<256>(id, seed, patterned));
    }
    assert!(scan.step_next().unwrap().is_none());
    println!(
        "sst rows={ROWS} lookups={LOOKUPS} files={files} sst_bytes={bytes} allocated_bytes={allocated} ingest_us={} scan_us={} read_p99_ns={} process_read_bytes={io} device_read_sectors={}",
        ingest.as_micros(),
        scan_time.as_micros(),
        p99(&mut ticks),
        sector_delta.map_or_else(|| "unavailable".to_owned(), |n| n.to_string()),
    );
    drop(scan);
    drop(view);
    db.close().unwrap();
}

fn wal_child(dir: &Path, mode: DurabilityMode, seed: u64) -> ! {
    let db = DB::open(DbOptions::default().with_write_buffer_size(64 << 20), dir).unwrap();
    let opts = WriteOptions::default().with_durability(mode);
    let mut ticks = Vec::with_capacity(PUTS as usize);
    let start = Instant::now();
    for id in 0..PUTS {
        let key = id.to_be_bytes();
        let data = value::<128>(id, seed, false);
        let at = Instant::now();
        db.put_with_options(&opts, &key, &data).unwrap();
        ticks.push(at.elapsed().as_nanos() as u64);
    }
    let puts: Duration = start.elapsed();
    let start = Instant::now();
    db.sync_wal().unwrap();
    let fence = start.elapsed();
    println!(
        "wal puts={PUTS} put_p99_ns={} puts_us={} fence_us={}",
        p99(&mut ticks),
        puts.as_micros(),
        fence.as_micros()
    );
    process::exit(0);
}

fn main() {
    let args: Vec<_> = env::args().collect();
    match args[1].as_str() {
        "sst" => {
            let codec = match args[2].as_str() {
                "none" => CompressionType::None,
                "lz4" => CompressionType::Lz4,
                _ => panic!("expected none or lz4"),
            };
            let patterned = match args[3].as_str() {
                "pattern" => true,
                "entropy" => false,
                _ => panic!("expected pattern or entropy"),
            };
            let dir = tempfile::tempdir().unwrap();
            sst(dir.path(), codec, patterned, args[4].parse().unwrap());
        }
        "wal" => {
            let dir = tempfile::tempdir().unwrap();
            let seed = args[3].parse().unwrap();
            let status = process::Command::new(env::current_exe().unwrap())
                .args(["wal-child", &args[2], &args[3]])
                .arg(dir.path())
                .status()
                .unwrap();
            assert!(status.success());
            let db = DB::open_read_only(dir.path()).unwrap();
            for id in 0..PUTS {
                assert_eq!(
                    &*db.get_pinned(&id.to_be_bytes()).unwrap().unwrap(),
                    &value::<128>(id, seed, false)
                );
            }
            db.close().unwrap();
            println!("verified rows={PUTS}");
        }
        "wal-child" => {
            let mode = match args[2].as_str() {
                "async" => DurabilityMode::WalAsync,
                "sync" => DurabilityMode::WalSync,
                _ => panic!("expected async or sync"),
            };
            wal_child(Path::new(&args[4]), mode, args[3].parse().unwrap());
        }
        _ => panic!("expected sst or wal"),
    }
}
