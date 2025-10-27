//! MMDB Benchmarks — comparable to RocksDB's db_bench report format.
//!
//! Run: cargo bench
//! Results are saved in target/criterion/ with HTML reports.
//!
//! Benchmark categories (matching RocksDB db_bench):
//! - fillseq: Sequential write throughput
//! - fillrandom: Random write throughput
//! - readrandom: Random point read throughput
//! - readseq: Sequential scan throughput
//! - fillbatch: Batch write throughput
//! - overwrite: Overwrite existing keys
//! - Various value sizes and compression types

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use std::path::Path;

fn mmdb_open(path: &Path) -> mmdb::DB {
    mmdb::DB::open(
        mmdb::DbOptions {
            create_if_missing: true,
            write_buffer_size: 64 * 1024 * 1024,
            ..Default::default()
        },
        path,
    )
    .unwrap()
}

fn mmdb_open_opts(path: &Path, opts: mmdb::DbOptions) -> mmdb::DB {
    mmdb::DB::open(opts, path).unwrap()
}

fn make_key(i: u64) -> Vec<u8> {
    format!("key_{:016}", i).into_bytes()
}

fn make_value(size: usize) -> Vec<u8> {
    vec![0x42u8; size]
}

// ── fillseq: Sequential Write ────────────────────────────────────────────────

fn bench_fillseq(c: &mut Criterion) {
    let mut group = c.benchmark_group("fillseq");
    let value = make_value(100);

    for &count in &[1_000u64, 10_000, 100_000] {
        group.throughput(Throughput::Elements(count));
        group.bench_with_input(BenchmarkId::from_parameter(count), &count, |b, &count| {
            b.iter_with_setup(
                || {
                    let dir = tempfile::tempdir().unwrap();
                    let db = mmdb_open(dir.path());
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

// ── fillrandom: Random Write ─────────────────────────────────────────────────

fn bench_fillrandom(c: &mut Criterion) {
    let mut group = c.benchmark_group("fillrandom");
    let value = make_value(100);

    for &count in &[1_000u64, 10_000, 100_000] {
        group.throughput(Throughput::Elements(count));
        group.bench_with_input(BenchmarkId::from_parameter(count), &count, |b, &count| {
            b.iter_with_setup(
                || {
                    let dir = tempfile::tempdir().unwrap();
                    let db = mmdb_open(dir.path());
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

// ── readrandom: Random Point Read ────────────────────────────────────────────

fn bench_readrandom(c: &mut Criterion) {
    let mut group = c.benchmark_group("readrandom");
    let value = make_value(100);
    let read_count = 1_000u64;

    // Data in memtable only
    group.throughput(Throughput::Elements(read_count));
    group.bench_function("memtable_10k", |b| {
        b.iter_with_setup(
            || {
                let dir = tempfile::tempdir().unwrap();
                let db = mmdb_open(dir.path());
                for i in 0..10_000u64 {
                    db.put(&make_key(i), &value).unwrap();
                }
                let read_keys: Vec<Vec<u8>> = (0..read_count)
                    .map(|i| make_key((i * 7) % 10_000))
                    .collect();
                (dir, db, read_keys)
            },
            |(_dir, db, keys)| {
                for key in &keys {
                    let _ = db.get(key).unwrap();
                }
            },
        );
    });

    // Data in SST (after flush)
    group.bench_function("sst_10k", |b| {
        b.iter_with_setup(
            || {
                let dir = tempfile::tempdir().unwrap();
                let db = mmdb_open(dir.path());
                for i in 0..10_000u64 {
                    db.put(&make_key(i), &value).unwrap();
                }
                db.flush().unwrap();
                let read_keys: Vec<Vec<u8>> = (0..read_count)
                    .map(|i| make_key((i * 7) % 10_000))
                    .collect();
                (dir, db, read_keys)
            },
            |(_dir, db, keys)| {
                for key in &keys {
                    let _ = db.get(key).unwrap();
                }
            },
        );
    });

    // Data across multiple levels (after compaction)
    group.bench_function("compacted_10k", |b| {
        b.iter_with_setup(
            || {
                let dir = tempfile::tempdir().unwrap();
                let opts = mmdb::DbOptions {
                    create_if_missing: true,
                    write_buffer_size: 8 * 1024,
                    l0_compaction_trigger: 2,
                    ..Default::default()
                };
                let db = mmdb_open_opts(dir.path(), opts);
                for i in 0..10_000u64 {
                    db.put(&make_key(i), &value).unwrap();
                }
                db.flush().unwrap();
                db.compact().unwrap();
                let read_keys: Vec<Vec<u8>> = (0..read_count)
                    .map(|i| make_key((i * 7) % 10_000))
                    .collect();
                (dir, db, read_keys)
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

// ── readseq: Sequential Scan (Iterator) ──────────────────────────────────────

fn bench_readseq(c: &mut Criterion) {
    let mut group = c.benchmark_group("readseq");
    let value = make_value(100);
    let total = 10_000u64;

    for &scan_count in &[100usize, 1000, 10_000] {
        group.throughput(Throughput::Elements(scan_count as u64));
        group.bench_with_input(
            BenchmarkId::from_parameter(scan_count),
            &scan_count,
            |b, &scan_count| {
                b.iter_with_setup(
                    || {
                        let dir = tempfile::tempdir().unwrap();
                        let db = mmdb_open(dir.path());
                        for i in 0..total {
                            db.put(&make_key(i), &value).unwrap();
                        }
                        db.flush().unwrap();
                        (dir, db)
                    },
                    |(_dir, db)| {
                        let mut iter = db.iter().unwrap();
                        let mut n = 0;
                        while iter.next().is_some() {
                            n += 1;
                            if n >= scan_count {
                                break;
                            }
                        }
                    },
                );
            },
        );
    }
    group.finish();
}

// ── fillbatch: Batch Write ───────────────────────────────────────────────────

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
                        let db = mmdb_open(dir.path());
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

// ── overwrite: Overwrite Existing Keys ───────────────────────────────────────

fn bench_overwrite(c: &mut Criterion) {
    let mut group = c.benchmark_group("overwrite");
    let value = make_value(100);
    let count = 10_000u64;

    group.throughput(Throughput::Elements(count));
    group.bench_function("10k", |b| {
        b.iter_with_setup(
            || {
                let dir = tempfile::tempdir().unwrap();
                let db = mmdb_open(dir.path());
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

// ── Value Size Comparison ────────────────────────────────────────────────────

fn bench_value_sizes(c: &mut Criterion) {
    let mut group = c.benchmark_group("value_sizes");
    let count = 1_000u64;

    for &vsize in &[16usize, 100, 256, 1024, 4096, 65536] {
        let value = make_value(vsize);
        let bytes_per_op = (24 + vsize) as u64; // key ~24B + value
        group.throughput(Throughput::Bytes(count * bytes_per_op));
        group.bench_with_input(
            BenchmarkId::new("write", format!("{}B", vsize)),
            &vsize,
            |b, _| {
                b.iter_with_setup(
                    || {
                        let dir = tempfile::tempdir().unwrap();
                        let db = mmdb_open(dir.path());
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

// ── Compression Comparison ───────────────────────────────────────────────────

fn bench_compression(c: &mut Criterion) {
    let mut group = c.benchmark_group("compression");
    let count = 5_000u64;
    // Compressible data (repeated patterns)
    let value: Vec<u8> = (0..=255u8).cycle().take(256).collect();

    group.throughput(Throughput::Elements(count));

    for (name, compression) in [
        ("none", mmdb::sst::format::CompressionType::None),
        ("lz4", mmdb::sst::format::CompressionType::Lz4),
        ("zstd", mmdb::sst::format::CompressionType::Zstd),
    ] {
        group.bench_with_input(
            BenchmarkId::from_parameter(name),
            &compression,
            |b, &compression| {
                b.iter_with_setup(
                    || {
                        let dir = tempfile::tempdir().unwrap();
                        let db = mmdb_open_opts(
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

// ── Mixed Workload ───────────────────────────────────────────────────────────

fn bench_readwhilewriting(c: &mut Criterion) {
    let mut group = c.benchmark_group("readwhilewriting");
    let value = make_value(100);
    let preload = 10_000u64;
    let ops = 5_000u64;

    group.throughput(Throughput::Elements(ops));

    // 90% reads, 10% writes
    group.bench_function("read90_write10", |b| {
        b.iter_with_setup(
            || {
                let dir = tempfile::tempdir().unwrap();
                let db = mmdb_open(dir.path());
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

    // 50/50
    group.bench_function("read50_write50", |b| {
        b.iter_with_setup(
            || {
                let dir = tempfile::tempdir().unwrap();
                let db = mmdb_open(dir.path());
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

criterion_group!(
    benches,
    bench_fillseq,
    bench_fillrandom,
    bench_readrandom,
    bench_readseq,
    bench_fillbatch,
    bench_overwrite,
    bench_value_sizes,
    bench_compression,
    bench_readwhilewriting,
);
criterion_main!(benches);
