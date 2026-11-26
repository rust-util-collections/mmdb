//! MMDB-only profiles. Each invocation owns a fresh temporary directory.
mod concurrent;
#[cfg(target_os = "macos")]
mod macos;
mod reopen;

#[cfg(target_os = "macos")]
use macos::{cpu_ticks, rss_kib};
use std::{env, fmt::Display, path::Path, str::FromStr, sync::Arc};

use mmdb::{BlockCachePool, DB, DbOptions, DurabilityMode, WriteOptions};

const MIB: usize = 1 << 20;
const KEYS: usize = 32_768;

fn setting<T: FromStr>(name: &str, default: T) -> T
where
    T::Err: Display,
{
    let name = format!("MMDB_PROFILE_{name}");
    match env::var(&name) {
        Ok(value) => value.parse().unwrap_or_else(|e| panic!("{name}: {e}")),
        Err(env::VarError::NotPresent) => default,
        Err(e) => panic!("{name}: {e}"),
    }
}

struct Config {
    operations: usize,
    rate: u64,
    seconds: u64,
    buffer: usize,
    depth: usize,
    pool_mib: u64,
    seed: u64,
    entropy: bool,
    sync: bool,
    sample: bool,
}

impl Config {
    fn load() -> Self {
        let config = Self {
            operations: setting("OPS", 10_000),
            rate: setting("RATE", 0),
            seconds: setting("SECONDS", 0),
            buffer: setting("BUFFER_MIB", 16),
            depth: setting("IMMUTABLES", 4),
            pool_mib: setting("POOL_MIB", 64),
            seed: setting("SEED", 1),
            entropy: setting("ENTROPY", true),
            sync: setting("SYNC", false),
            sample: setting("SAMPLE", false),
        };
        assert!(
            config.rate == 0 || config.seconds == 0,
            "set either RATE or SECONDS"
        );
        assert!((1..=u32::MAX as usize).contains(&config.operations));
        assert!(config.buffer > 0 && config.depth > 0);
        config
    }

    fn options(&self, pool: &Arc<BlockCachePool>) -> DbOptions {
        DbOptions::default()
            .with_prefix_len(8)
            .with_write_buffer_size(self.buffer * MIB)
            .with_max_immutable_memtables(self.depth)
            .with_block_cache(Some(Arc::clone(pool)))
    }

    fn write_options(&self) -> WriteOptions {
        WriteOptions::default().with_durability(if self.sync {
            DurabilityMode::WalSync
        } else {
            DurabilityMode::WalAsync
        })
    }
}

fn key(prefix: u64, suffix: usize) -> [u8; 16] {
    let mut bytes = [0; 16];
    bytes[..8].copy_from_slice(&prefix.to_be_bytes());
    bytes[8..].copy_from_slice(&(suffix as u64).to_be_bytes());
    bytes
}

// Stateless SplitMix64 finalizer. Reproduce the same payload in every arm.
fn mix(mut x: u64) -> u64 {
    x = (x ^ (x >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    x = (x ^ (x >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    x ^ (x >> 31)
}

fn value(seed: u64, entropy: bool, bytes: &mut [u8]) {
    if entropy {
        for (i, chunk) in bytes.chunks_mut(8).enumerate() {
            let word = mix(seed.wrapping_add(i as u64)).to_le_bytes();
            chunk.copy_from_slice(&word[..chunk.len()]);
        }
    } else {
        bytes.fill(0x42);
    }
}

fn property(db: &DB, name: &str) -> u64 {
    db.get_property(name).unwrap().parse().unwrap()
}

#[cfg(not(target_os = "macos"))]
fn rss_kib(field: &str) -> Option<u64> {
    std::fs::read_to_string("/proc/self/status")
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

// Linux process user + system clock ticks. Sampling stays outside each request.
#[cfg(not(target_os = "macos"))]
fn cpu_ticks() -> u64 {
    let Ok(stat) = std::fs::read_to_string("/proc/self/stat") else {
        return 0;
    };
    let Some((_, fields)) = stat.rsplit_once(") ") else {
        return 0;
    };
    fields
        .split_whitespace()
        .skip(11)
        .take(2)
        .map(|n| n.parse::<u64>().unwrap())
        .sum()
}

fn main() {
    let mode = env::args()
        .nth(1)
        .filter(|arg| arg != "--bench")
        .unwrap_or_else(|| "mixed".to_owned());
    let config = Config::load();
    if mode == "prepare-reopen" {
        let dir = env::args().nth(2).expect("expected fixture directory");
        reopen::prepare(&config, Path::new(&dir));
        return;
    }
    let dir = tempfile::tempdir().unwrap();
    println!(
        "config mode={mode} ops={} rate={} seconds={} buffer_mib={} depth={} pool_mib={} seed={} entropy={} sync={} sample={} arch={}",
        config.operations,
        config.rate,
        config.seconds,
        config.buffer,
        config.depth,
        config.pool_mib,
        config.seed,
        config.entropy,
        config.sync,
        config.sample,
        env::consts::ARCH
    );
    match mode.as_str() {
        "mixed" | "memory" => concurrent::run(&config, &mode, dir.path()),
        "reopen" => reopen::run(&config, dir.path()),
        _ => panic!("unknown profile {mode}"),
    }
}
