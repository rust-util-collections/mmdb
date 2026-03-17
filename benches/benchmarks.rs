//! MMDB Benchmarks — comparable to RocksDB's db_bench report format.
//!
//! Run:
//!   cargo bench                    # all benchmarks
//!   cargo bench -- "fillseq"       # specific group
//!   cargo bench -- "cold"          # cold-cache scenarios only
//!   cargo bench -- "warm"          # warm-cache scenarios only
//!
//! Two configurations are tested for I/O-sensitive operations:
//! - **warm**: Large cache (256MB), data fits in memory — measures CPU/algorithm cost
//! - **cold**: Tiny cache (256KB), data exceeds cache — measures I/O + cache-miss cost
//!
//! Cold-cache tests use smaller datasets to keep total bench time reasonable.

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use std::path::Path;
use std::time::Duration;

/// Custom criterion config: 10 samples, 3s measurement — keeps total bench under 5 min.
fn bench_config() -> Criterion {
    Criterion::default()
        .sample_size(10)
        .measurement_time(Duration::from_secs(3))
}

// ── Helpers ─────────────────────────────────────────────────────────────

fn make_key(i: u64) -> Vec<u8> {
    format!("key_{:016}", i).into_bytes()
}

fn make_value(size: usize) -> Vec<u8> {
    vec![0x42u8; size]
}

/// Open DB with warm cache (256MB) — data stays in memory.
fn open_warm(path: &Path) -> mmdb::DB {
    mmdb::DB::open(
        mmdb::DbOptions {
            create_if_missing: true,
            write_buffer_size: 64 * 1024 * 1024,
            block_cache_capacity: 256 * 1024 * 1024,
            ..Default::default()
        },
        path,
    )
    .unwrap()
}

/// Open DB with cold cache (256KB) — forces disk I/O on reads.
fn open_cold(path: &Path) -> mmdb::DB {
    mmdb::DB::open(
        mmdb::DbOptions {
            create_if_missing: true,
            write_buffer_size: 4 * 1024 * 1024, // 4MB — frequent flushes
            block_cache_capacity: 256 * 1024,   // 256KB — almost nothing cached
            l0_compaction_trigger: 4,
            ..Default::default()
        },
        path,
    )
    .unwrap()
}

fn open_with(path: &Path, opts: mmdb::DbOptions) -> mmdb::DB {
    mmdb::DB::open(opts, path).unwrap()
}

// ── fillseq: Sequential Write ───────────────────────────────────────────

fn bench_fillseq(c: &mut Criterion) {
    let mut group = c.benchmark_group("fillseq");

    let value = make_value(100);

    for &count in &[1_000u64, 10_000] {
        group.throughput(Throughput::Elements(count));
        group.bench_with_input(BenchmarkId::from_parameter(count), &count, |b, &count| {
            b.iter_with_setup(
                || {
                    let dir = tempfile::tempdir().unwrap();
                    let db = open_warm(dir.path());
                    (dir, db)
                },
                |(dir, db)| {
                    for i in 0..count {
                        db.put(&make_key(i), &value).unwrap();
                    }
                    db.close().unwrap();
                    drop(dir);
                },
            );
        });
    }
    group.finish();
}

// ── fillrandom: Random Write ────────────────────────────────────────────

fn bench_fillrandom(c: &mut Criterion) {
    let mut group = c.benchmark_group("fillrandom");

    let value = make_value(100);

    for &count in &[1_000u64, 10_000] {
        group.throughput(Throughput::Elements(count));
        group.bench_with_input(BenchmarkId::from_parameter(count), &count, |b, &count| {
            b.iter_with_setup(
                || {
                    let dir = tempfile::tempdir().unwrap();
                    let db = open_warm(dir.path());
                    let keys: Vec<Vec<u8>> = (0..count)
                        .map(|i| make_key(i.wrapping_mul(6364136223846793005).wrapping_add(1)))
                        .collect();
                    (dir, db, keys)
                },
                |(dir, db, keys)| {
                    for key in &keys {
                        db.put(key, &value).unwrap();
                    }
                    db.close().unwrap();
                    drop(dir);
                },
            );
        });
    }
    group.finish();
}

// ── fillbatch: Batch Write ──────────────────────────────────────────────

fn bench_fillbatch(c: &mut Criterion) {
    let mut group = c.benchmark_group("fillbatch");

    let value = make_value(100);

    for &batch_size in &[10usize, 100, 1000] {
        let total_ops = 100 * batch_size as u64;
        group.throughput(Throughput::Elements(total_ops));
        group.bench_with_input(
            BenchmarkId::from_parameter(batch_size),
            &batch_size,
            |b, &batch_size| {
                b.iter_with_setup(
                    || {
                        let dir = tempfile::tempdir().unwrap();
                        let db = open_warm(dir.path());
                        (dir, db)
                    },
                    |(dir, db)| {
                        for round in 0..100u64 {
                            let mut batch = mmdb::WriteBatch::new();
                            for i in 0..batch_size as u64 {
                                batch.put(&make_key(round * 1000 + i), &value);
                            }
                            db.write(batch).unwrap();
                        }
                        db.close().unwrap();
                        drop(dir);
                    },
                );
            },
        );
    }
    group.finish();
}

// ── overwrite ───────────────────────────────────────────────────────────

fn bench_overwrite(c: &mut Criterion) {
    let mut group = c.benchmark_group("overwrite");

    let value = make_value(100);
    let count = 10_000u64;

    group.throughput(Throughput::Elements(count));
    group.bench_function("10k", |b| {
        b.iter_with_setup(
            || {
                let dir = tempfile::tempdir().unwrap();
                let db = open_warm(dir.path());
                for i in 0..count {
                    db.put(&make_key(i), &value).unwrap();
                }
                (dir, db)
            },
            |(_dir, db)| {
                let new_value = make_value(100);
                for i in 0..count {
                    db.put(&make_key(i), &new_value).unwrap();
                }
            },
        );
    });
    group.finish();
}

// ── readrandom: warm vs cold ────────────────────────────────────────────

fn bench_readrandom(c: &mut Criterion) {
    let mut group = c.benchmark_group("readrandom");

    let value = make_value(100);
    let read_count = 1_000u64;

    group.throughput(Throughput::Elements(read_count));

    // Warm: memtable only (no SST I/O)
    group.bench_function("warm/memtable_10k", |b| {
        b.iter_with_setup(
            || {
                let dir = tempfile::tempdir().unwrap();
                let db = open_warm(dir.path());
                for i in 0..10_000u64 {
                    db.put(&make_key(i), &value).unwrap();
                }
                let keys: Vec<Vec<u8>> = (0..read_count)
                    .map(|i| make_key((i * 7) % 10_000))
                    .collect();
                (dir, db, keys)
            },
            |(_dir, db, keys)| {
                for key in &keys {
                    let _ = db.get(key).unwrap();
                }
            },
        );
    });

    // Warm: SST with large cache (all blocks cached)
    group.bench_function("warm/sst_10k", |b| {
        b.iter_with_setup(
            || {
                let dir = tempfile::tempdir().unwrap();
                let db = open_warm(dir.path());
                for i in 0..10_000u64 {
                    db.put(&make_key(i), &value).unwrap();
                }
                db.flush().unwrap();
                // Pre-warm cache by reading all keys once
                for i in 0..10_000u64 {
                    let _ = db.get(&make_key(i));
                }
                let keys: Vec<Vec<u8>> = (0..read_count)
                    .map(|i| make_key((i * 7) % 10_000))
                    .collect();
                (dir, db, keys)
            },
            |(_dir, db, keys)| {
                for key in &keys {
                    let _ = db.get(key).unwrap();
                }
            },
        );
    });

    // Cold: SST with tiny cache (cache misses on most reads)
    // Smaller dataset (2K entries) to keep bench time reasonable
    group.bench_function("cold/sst_2k", |b| {
        b.iter_with_setup(
            || {
                let dir = tempfile::tempdir().unwrap();
                let db = open_cold(dir.path());
                for i in 0..2_000u64 {
                    db.put(&make_key(i), &value).unwrap();
                }
                db.flush().unwrap();
                db.compact().unwrap();
                let keys: Vec<Vec<u8>> = (0..500u64).map(|i| make_key((i * 7) % 2_000)).collect();
                (dir, db, keys)
            },
            |(_dir, db, keys)| {
                for key in &keys {
                    let _ = db.get(key).unwrap();
                }
            },
        );
    });

    group.finish();
}

// ── readseq (scan): warm vs cold ────────────────────────────────────────

fn bench_readseq(c: &mut Criterion) {
    let mut group = c.benchmark_group("readseq");

    let value = make_value(100);

    // Warm: 10K entries, large cache, data mostly in memtable/cache
    for &scan_count in &[100usize, 1_000, 10_000] {
        group.throughput(Throughput::Elements(scan_count as u64));
        group.bench_with_input(
            BenchmarkId::new("warm", scan_count),
            &scan_count,
            |b, &scan_count| {
                b.iter_with_setup(
                    || {
                        let dir = tempfile::tempdir().unwrap();
                        let db = open_warm(dir.path());
                        for i in 0..10_000u64 {
                            db.put(&make_key(i), &value).unwrap();
                        }
                        db.flush().unwrap();
                        (dir, db)
                    },
                    |(_dir, db)| {
                        let n = db.iter().unwrap().take(scan_count).count();
                        assert!(n > 0);
                    },
                );
            },
        );
    }

    // Cold: 5K entries (smaller to stay fast), tiny cache, multi-level SST
    for &scan_count in &[100usize, 1_000] {
        group.throughput(Throughput::Elements(scan_count as u64));
        group.bench_with_input(
            BenchmarkId::new("cold", scan_count),
            &scan_count,
            |b, &scan_count| {
                b.iter_with_setup(
                    || {
                        let dir = tempfile::tempdir().unwrap();
                        let db = open_cold(dir.path());
                        for i in 0..5_000u64 {
                            db.put(&make_key(i), &value).unwrap();
                        }
                        db.flush().unwrap();
                        db.compact().unwrap();
                        (dir, db)
                    },
                    |(_dir, db)| {
                        let n = db.iter().unwrap().take(scan_count).count();
                        assert!(n > 0);
                    },
                );
            },
        );
    }

    group.finish();
}

// ── prefix scan: warm vs cold ───────────────────────────────────────────

fn bench_prefix_scan(c: &mut Criterion) {
    let mut group = c.benchmark_group("prefix_scan");

    let value = make_value(100);
    let num_prefixes = 50u64;

    // Warm: prefix scan over 10K entries
    group.throughput(Throughput::Elements(100));
    group.bench_function("warm/100", |b| {
        b.iter_with_setup(
            || {
                let dir = tempfile::tempdir().unwrap();
                let opts = mmdb::DbOptions {
                    create_if_missing: true,
                    write_buffer_size: 64 * 1024 * 1024,
                    block_cache_capacity: 256 * 1024 * 1024,
                    prefix_len: 8,
                    ..Default::default()
                };
                let db = open_with(dir.path(), opts);
                for i in 0..10_000u64 {
                    let pfx = i % num_prefixes;
                    let key = format!("pfx_{:04}_{:010}", pfx, i);
                    db.put(key.as_bytes(), &value).unwrap();
                }
                db.flush().unwrap();
                (dir, db)
            },
            |(_dir, db)| {
                let n = db.iter_with_prefix(b"pfx_0025").unwrap().take(100).count();
                assert!(n > 0);
            },
        );
    });

    // Cold: prefix scan, tiny cache, 3K entries (fast enough)
    group.throughput(Throughput::Elements(100));
    group.bench_function("cold/100", |b| {
        b.iter_with_setup(
            || {
                let dir = tempfile::tempdir().unwrap();
                let opts = mmdb::DbOptions {
                    create_if_missing: true,
                    write_buffer_size: 4 * 1024 * 1024,
                    block_cache_capacity: 256 * 1024,
                    prefix_len: 8,
                    l0_compaction_trigger: 4,
                    ..Default::default()
                };
                let db = open_with(dir.path(), opts);
                for i in 0..3_000u64 {
                    let pfx = i % num_prefixes;
                    let key = format!("pfx_{:04}_{:010}", pfx, i);
                    db.put(key.as_bytes(), &value).unwrap();
                }
                db.flush().unwrap();
                db.compact().unwrap();
                (dir, db)
            },
            |(_dir, db)| {
                let n = db.iter_with_prefix(b"pfx_0025").unwrap().take(100).count();
                assert!(n > 0);
            },
        );
    });

    group.finish();
}

// ── seek ────────────────────────────────────────────────────────────────

fn bench_seek(c: &mut Criterion) {
    let mut group = c.benchmark_group("seek");

    let value = make_value(100);

    group.throughput(Throughput::Elements(500));
    group.bench_function("warm/random_500", |b| {
        b.iter_with_setup(
            || {
                let dir = tempfile::tempdir().unwrap();
                let db = open_warm(dir.path());
                for i in 0..50_000u64 {
                    db.put(&make_key(i), &value).unwrap();
                }
                db.flush().unwrap();
                let targets: Vec<Vec<u8>> = (0..500u64)
                    .map(|i| make_key((i.wrapping_mul(6364136223846793005)) % 50_000))
                    .collect();
                (dir, db, targets)
            },
            |(_dir, db, targets)| {
                for target in &targets {
                    let mut iter = db.iter().unwrap();
                    iter.seek(target);
                    let _ = iter.next();
                }
            },
        );
    });
    group.finish();
}

// ── delete + range delete ───────────────────────────────────────────────

fn bench_delete(c: &mut Criterion) {
    let mut group = c.benchmark_group("delete");

    let value = make_value(100);
    let count = 10_000u64;

    group.throughput(Throughput::Elements(count));

    group.bench_function("point_delete_10k", |b| {
        b.iter_with_setup(
            || {
                let dir = tempfile::tempdir().unwrap();
                let db = open_warm(dir.path());
                for i in 0..count {
                    db.put(&make_key(i), &value).unwrap();
                }
                (dir, db)
            },
            |(_dir, db)| {
                for i in 0..count {
                    db.delete(&make_key(i)).unwrap();
                }
            },
        );
    });

    group.bench_function("range_delete", |b| {
        b.iter_with_setup(
            || {
                let dir = tempfile::tempdir().unwrap();
                let db = open_warm(dir.path());
                for i in 0..count {
                    db.put(&make_key(i), &value).unwrap();
                }
                (dir, db)
            },
            |(_dir, db)| {
                db.delete_range(&make_key(0), &make_key(count / 2)).unwrap();
            },
        );
    });

    group.finish();
}

// ── value sizes ─────────────────────────────────────────────────────────

fn bench_value_sizes(c: &mut Criterion) {
    let mut group = c.benchmark_group("value_sizes");

    let count = 1_000u64;

    for &vsize in &[16usize, 100, 1024, 65536] {
        let value = make_value(vsize);
        let bytes_per_op = (24 + vsize) as u64;
        group.throughput(Throughput::Bytes(count * bytes_per_op));
        group.bench_with_input(
            BenchmarkId::new("write", format!("{}B", vsize)),
            &vsize,
            |b, _| {
                b.iter_with_setup(
                    || {
                        let dir = tempfile::tempdir().unwrap();
                        let db = open_warm(dir.path());
                        (dir, db)
                    },
                    |(dir, db)| {
                        for i in 0..count {
                            db.put(&make_key(i), &value).unwrap();
                        }
                        db.close().unwrap();
                        drop(dir);
                    },
                );
            },
        );
    }
    group.finish();
}

// ── compression ─────────────────────────────────────────────────────────

fn bench_compression(c: &mut Criterion) {
    let mut group = c.benchmark_group("compression");

    let count = 5_000u64;
    let value: Vec<u8> = (0..=255u8).cycle().take(256).collect();

    group.throughput(Throughput::Elements(count));

    for (name, compression) in [
        ("none", mmdb::CompressionType::None),
        ("lz4", mmdb::CompressionType::Lz4),
        ("zstd", mmdb::CompressionType::Zstd),
    ] {
        group.bench_with_input(
            BenchmarkId::from_parameter(name),
            &compression,
            |b, &compression| {
                b.iter_with_setup(
                    || {
                        let dir = tempfile::tempdir().unwrap();
                        let db = open_with(
                            dir.path(),
                            mmdb::DbOptions {
                                create_if_missing: true,
                                write_buffer_size: 64 * 1024 * 1024,
                                compression,
                                ..Default::default()
                            },
                        );
                        (dir, db)
                    },
                    |(dir, db)| {
                        for i in 0..count {
                            db.put(&make_key(i), &value).unwrap();
                        }
                        db.flush().unwrap();
                        db.close().unwrap();
                        drop(dir);
                    },
                );
            },
        );
    }
    group.finish();
}

// ── large scan: warm vs cold ────────────────────────────────────────────

fn bench_scan_large(c: &mut Criterion) {
    let mut group = c.benchmark_group("scan_large");

    let value = make_value(128);

    // Warm: 100K entries, large cache
    for &scan_count in &[100usize, 1_000, 10_000] {
        group.throughput(Throughput::Elements(scan_count as u64));
        group.bench_with_input(
            BenchmarkId::new("warm", scan_count),
            &scan_count,
            |b, &scan_count| {
                b.iter_with_setup(
                    || {
                        let dir = tempfile::tempdir().unwrap();
                        let db = open_warm(dir.path());
                        for i in 0..100_000u64 {
                            db.put(&make_key(i), &value).unwrap();
                        }
                        db.flush().unwrap();
                        (dir, db)
                    },
                    |(_dir, db)| {
                        let n = db.iter().unwrap().take(scan_count).count();
                        assert!(n > 0);
                    },
                );
            },
        );
    }

    // Cold: 10K entries (smaller!), tiny cache, multi-level SST
    for &scan_count in &[100usize, 1_000] {
        group.throughput(Throughput::Elements(scan_count as u64));
        group.bench_with_input(
            BenchmarkId::new("cold", scan_count),
            &scan_count,
            |b, &scan_count| {
                b.iter_with_setup(
                    || {
                        let dir = tempfile::tempdir().unwrap();
                        let db = open_cold(dir.path());
                        for i in 0..10_000u64 {
                            db.put(&make_key(i), &value).unwrap();
                        }
                        db.flush().unwrap();
                        db.compact().unwrap();
                        (dir, db)
                    },
                    |(_dir, db)| {
                        let n = db.iter().unwrap().take(scan_count).count();
                        assert!(n > 0);
                    },
                );
            },
        );
    }

    group.finish();
}

// ── mixed workload ──────────────────────────────────────────────────────

fn bench_mixed(c: &mut Criterion) {
    let mut group = c.benchmark_group("mixed");

    let value = make_value(100);
    let preload = 10_000u64;
    let ops = 5_000u64;

    group.throughput(Throughput::Elements(ops));

    group.bench_function("read90_write10", |b| {
        b.iter_with_setup(
            || {
                let dir = tempfile::tempdir().unwrap();
                let db = open_warm(dir.path());
                for i in 0..preload {
                    db.put(&make_key(i), &value).unwrap();
                }
                (dir, db)
            },
            |(_dir, db)| {
                for i in 0..ops {
                    if i % 10 == 0 {
                        db.put(&make_key(preload + i), &value).unwrap();
                    } else {
                        let _ = db.get(&make_key(i % preload)).unwrap();
                    }
                }
            },
        );
    });

    group.bench_function("read50_write50", |b| {
        b.iter_with_setup(
            || {
                let dir = tempfile::tempdir().unwrap();
                let db = open_warm(dir.path());
                for i in 0..preload {
                    db.put(&make_key(i), &value).unwrap();
                }
                (dir, db)
            },
            |(_dir, db)| {
                for i in 0..ops {
                    if i % 2 == 0 {
                        db.put(&make_key(preload + i), &value).unwrap();
                    } else {
                        let _ = db.get(&make_key(i % preload)).unwrap();
                    }
                }
            },
        );
    });

    group.finish();
}

criterion_group! {
    name = benches;
    config = bench_config();
    targets =
    bench_fillseq,
    bench_fillrandom,
    bench_fillbatch,
    bench_overwrite,
    bench_readrandom,
    bench_readseq,
    bench_prefix_scan,
    bench_seek,
    bench_delete,
    bench_value_sizes,
    bench_compression,
    bench_scan_large,
    bench_mixed,
}
criterion_main!(benches);
