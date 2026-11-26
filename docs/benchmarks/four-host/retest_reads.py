"""Checkpoint grouped-read A/A and A/B pairs on one frozen engine and storage stack.

Run from the measurement checkout with PROBE SCRATCH OUTPUT --stack NAME
--revision FULL_SHA. On Linux also supply --devices disk[,disk]. SCRATCH holds
immutable fixtures; OUTPUT receives the original grouped-read CSV schema.
"""
import argparse
import csv
import io
import json
import os
from pathlib import Path
import platform
import plistlib
import random
import re
import subprocess
import sys
import tempfile

from retest_run import Journal, atomic_json, digest, run_child

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "grouped-reads"))
from read_evidence import expected_table_probes, has_cache_evidence, trial_command

PRIMARY = ("m5-apfs", "5900x-raid", "9474f-raid", "9950x-zfs")
STACKS = PRIMARY + ("5900x-single", "9474f-single", "9950x-ext4")
SEED = 20261125
COLUMNS = ("round", "pair", "order", "arm", "seed", "regime", "method", "locality", "fanout",
           "samples", "p50_ns", "p99_ns", "blocks", "tables", "storage_bytes", "device_requests",
           "device_p99_ns", "peak_pin_bytes", "pin_budget")
# Each tuple gives the row count, request count per process and access pattern.
CELLS = {"warm": (262144, 300000, "clustered"), "page": (262144, 10000, "clustered"),
         "device": (1048576, 10000, "scattered")}


def cases_for(stack):
    return [case for case in CELLS if case == "page" or stack in PRIMARY
            or (case == "device" and stack == "9950x-ext4")]


def schedule():
    """Match a single selected cell in the original trials.py, including arm order."""
    rng = random.Random(SEED)
    for i in range(5):
        pairs = ["AA", "AB"]
        rng.shuffle(pairs)
        for pair in pairs:
            arms = [("baseline", "scalar"), ("comparison", "scalar" if pair == "AA" else "grouped")]
            if (i + (pair == "AB")) % 2:
                arms.reverse()
            yield i + 1, pair, SEED + 1021 * i, arms


def capture(*argv):
    return subprocess.check_output(argv, text=True, env={**os.environ, "LC_ALL": "C"}).strip()


def environment(stack, scratch, devices):
    system = platform.system()
    if (system == "Darwin") != (stack == "m5-apfs") or system not in ("Darwin", "Linux"):
        raise ValueError(f"{stack} does not match the host operating system {system}")
    if system == "Darwin":
        if devices:
            raise ValueError("--devices takes Linux whole-disk names")
        mount = next(p for p in (scratch, *scratch.parents) if p.is_mount())
        volume = plistlib.loads(subprocess.check_output(["diskutil", "info", "-plist", mount]))
        host = dict(cpu_model=capture("sysctl", "-n", "machdep.cpu.brand_string"), cpu_affinity="OS scheduled",
                    filesystem=volume["FilesystemName"], storage_counter="ri_diskio_bytesread",
                    volume_device=volume["DeviceIdentifier"], physical_stores=volume.get("APFSPhysicalStores", []),
                    device_cache_control="file-scoped msync(MS_INVALIDATE) every 128 requests")
        fs_type = volume["FilesystemType"].lower()
    else:
        if not devices or len(devices) != len(set(devices)):
            raise ValueError("Linux trials require distinct --devices whole-disk names")
        for dev in devices:
            if not re.fullmatch(r"[A-Za-z0-9_-]+", dev) or not (Path("/sys/block") / dev / "stat").is_file():
                raise ValueError(f"unknown whole disk {dev}")
        fs_type = capture("findmnt", "-n", "-T", scratch, "-o", "FSTYPE")
        cpu_fields = json.loads(capture("lscpu", "-J"))["lscpu"]
        cpu_model = next(item["data"] for item in cpu_fields if item["field"].rstrip(":") == "Model name")
        host = dict(cpu_model=cpu_model, cpu_affinity="4",
                    filesystem=capture("findmnt", "-n", "-T", scratch, "-o", "SOURCE,FSTYPE,OPTIONS"),
                    storage_counter="linux_read_bytes", device_cache_control="file-scoped POSIX_FADV_DONTNEED every 128 requests")
        if fs_type == "zfs":
            dataset = capture("findmnt", "-n", "-T", scratch, "-o", "SOURCE")
            pool = dataset.split("/")[0]
            lines = capture("zpool", "status", "-LP", pool).splitlines()
            leaves = [line.split()[0] for line in lines if line.lstrip().startswith("/dev/")]
            physical = set()
            for leaf in leaves:
                for row in capture("lsblk", "-snr", "-o", "KNAME,TYPE", leaf).splitlines():
                    name, kind = row.split()
                    if kind == "disk":
                        physical.add(name)
            if physical != set(devices):
                raise ValueError(f"ZFS backing disks {sorted(physical)} differ from --devices {devices}")
            host["zfs_vdevs"] = leaves
            host["zfs_properties"] = capture("zfs", "get", "-Hp", "-o", "property,value",
                                              "compression,recordsize,sync,primarycache,secondarycache", dataset)
    expected_fs = {"m5-apfs": "apfs", "9950x-ext4": "ext4", "9950x-zfs": "zfs"}.get(stack, "btrfs")
    if fs_type != expected_fs:
        raise ValueError(f"{scratch} has filesystem {fs_type}; {stack} requires {expected_fs}")
    return dict(**host, stack=stack, devices=devices, kernel=platform.release(), architecture=platform.machine())


def counters(devices, zfs):
    result = {"disk": {}}
    for dev in devices:
        stats = (Path("/sys/block") / dev / "stat").read_text().split()
        result["disk"][dev] = {"read_ios": int(stats[0]), "read_sectors": int(stats[2])}
    if zfs:
        names = {"demand_data_hits", "demand_data_misses"}
        arc = [line.split() for line in Path("/proc/spl/kstat/zfs/arcstats").read_text().splitlines()]
        result["arc"] = {r[0]: int(r[-1]) for r in arc if r and r[0] in names}
        if result["arc"].keys() != names:
            raise ValueError("missing ZFS ARC demand data counters")
    return result


def fixture(args, count):
    bundle = args.scratch / f"grouped-{args.revision}-{count}"
    if not bundle.exists():
        with tempfile.TemporaryDirectory(dir=args.scratch, prefix=".grouped-") as stage:
            staging = Path(stage) / "fixture"
            staging.mkdir()
            run_child(trial_command([args.probe, "prepare", staging / "store", str(count)], args.cpus), check=True)
            staging.rename(bundle)
    path = bundle / "store"
    tables = list(path.glob("*.sst"))
    if len(tables) != 1 or int(path.with_suffix(".rows").read_text()) != count:
        raise ValueError(f"invalid {count}-row single-SST fixture in {path}")
    return path, tables[0]


def arm(args, meta, path, regime, method, seed):
    size, samples, locality = CELLS[regime]
    zfs = args.stack == "9950x-zfs"
    cmd = trial_command([args.probe, path, regime, method, locality, "32", str(samples), str(seed)], args.cpus)
    before = counters(args.devices, zfs)
    proc = run_child(cmd, capture_output=True, text=True, timeout=900)
    after = counters(args.devices, zfs)
    if proc.returncode:
        raise RuntimeError(f"{method} {regime} seed {seed} exited {proc.returncode}: {proc.stderr.strip()}")
    raw = dict(field.split("=", 1) for field in proc.stdout.split())
    if set(raw) != set(COLUMNS[5:]):
        raise ValueError(f"unexpected probe counters: {proc.stdout!r}")
    row = {k: v if k in ("regime", "method", "locality") else int(v) for k, v in raw.items()}
    if ((row["regime"], row["method"], row["locality"], row["fanout"], row["samples"])
            != (regime, method, locality, 32, samples) or meta["rows"] != size
            or row["tables"] != expected_table_probes({**row, "seed": seed}, meta)
            or not 0 < row["peak_pin_bytes"] <= row["pin_budget"]
            or not 0 < row["p50_ns"] <= row["p99_ns"] or row["blocks"] <= 0
            or row["storage_bytes"] < 0 or not 0 <= row["device_requests"] <= samples):
        raise ValueError(f"probe response does not match the requested workload: {raw}")
    deltas = [after["disk"][d]["read_sectors"] - before["disk"][d]["read_sectors"] for d in args.devices]
    misses = after["arc"]["demand_data_misses"] - before["arc"]["demand_data_misses"] if zfs else None
    hits = after["arc"]["demand_data_hits"] - before["arc"]["demand_data_hits"] if zfs else None
    if any(n < 0 for n in deltas) or (zfs and min(hits, misses) < 0):
        raise ValueError("device or ARC counter reset during observation")
    return dict(metrics=row, seed=seed, before=before, after=after, arc_demand_hits=hits, arc_demand_misses=misses,
                physical_device_read_bytes=512 * sum(deltas) if args.devices else None)


def qualification(arms, zfs):
    media = [r["physical_device_read_bytes"] for r in arms]
    misses = [r["arc_demand_misses"] for r in arms]
    hits = [r["arc_demand_hits"] for r in arms]
    process_ok = all(has_cache_evidence(r["metrics"]) for r in arms)
    kind = arms[0]["metrics"]["regime"]
    ok = process_ok
    if zfs and kind == "device":
        ok = ok and all(n >= 4096 * r["metrics"]["samples"] * .95 and m > 0
                        for r, n, m in zip(arms, media, misses))
    label = kind if ok else "unqualified"
    if zfs and not process_ok and kind == "page" and all(m == 0 for m in misses) and all(h > 0 for h in hits):
        label = "ARC-prewarmed"
    return {"classification": label, "qualified": ok, "process_counter_qualified": process_ok,
            "physical_device_read_bytes": media, "arc_demand_misses": misses, "arc_demand_hits": hits}


def export(path, items):
    buffer = io.StringIO()
    writer = csv.DictWriter(buffer, fieldnames=COLUMNS, lineterminator="\n")
    writer.writeheader()
    for item in items:
        writer.writerow({**item["metrics"], **{k: item[k] for k in COLUMNS[:5]}})
    with tempfile.NamedTemporaryFile(dir=path.parent, delete=False, mode="w", newline="") as f:
        temporary = Path(f.name)
        try:
            f.write(buffer.getvalue())
            f.flush()
            os.fsync(f.fileno())
            os.replace(temporary, path)
        finally:
            temporary.unlink(missing_ok=True)


def run_case(args, journal, regime, host, path, table):
    rows, samples, locality = CELLS[regime]
    meta = dict(**host, build_revision=args.revision, probe_sha256=args.probe_hash,
                sst_sha256=digest(table), sst_bytes=table.stat().st_size, rows=rows, key_bytes=8,
                value_bytes=128, block_bytes=4096, prefix_len=0, sst_compression="None",
                warm_cache_bytes=128 << 20, page_or_device_cache_bytes=0, reader_threads=1,
                samples_per_run=samples, base_seed=SEED, selected_fanout=32, selected_locality=locality,
                device_rule="at least 4096 process-accounted bytes on at least 95% of requests in every arm",
                physical_io_window="Linux disk and ARC counters cover each whole process, including open and warmup",
                coalesces_duplicates=True, matrix="primary" if regime == "warm" else regime)
    if host["storage_counter"] == "linux_read_bytes":
        meta["sst_attributes"] = capture("lsattr", "-d", table).split()[0]
    zfs = args.stack == "9950x-zfs"

    def block(key, seed, arms):
        spec = dict(regime=regime, seed=seed, arms=arms, fixture=meta["sst_sha256"],
                    rows=rows, samples=samples, locality=locality, fanout=32)
        def collect():
            return [dict(arm(args, meta, path, regime, method, seed), arm=role, order=n)
                    for n, (role, method) in enumerate(arms, 1)]
        return journal.block([regime, *key], spec, collect)

    if regime == "device" and zfs:
        pilots = [r for i in range(2) for r in block(["pilot", i], SEED + 1021 * i,
                  [("pilot", "scalar"), ("pilot", "grouped")])]
        proof = qualification(pilots, True)
        atomic_json(args.output / "device-pilots.json", pilots)
        if all(n == 0 for n in proof["physical_device_read_bytes"]) and all(m == 0 for m in proof["arc_demand_misses"]):
            label = "ARC-resident pilot" if all(h > 0 for h in proof["arc_demand_hits"]) else "unqualified pilot"
            atomic_json(args.output / "device.metadata.json", meta)
            atomic_json(args.output / "device-qualification.json", dict(proof, accepted_device_cell=False,
                        classification=f"{label}; no physical device reads", stage="pilot"))
            print(f"{args.stack} device stopped after four pilots with zero physical reads", flush=True)
            return
    completed = []
    for i, pair, seed, arms in schedule():
        for r in block([i, pair], seed, arms):
            completed.append(dict(r, round=i, pair=pair))
    proof = qualification(completed, zfs)
    if regime == "device":
        proof["accepted_device_cell"] = proof["qualified"]
    atomic_json(args.output / f"{regime}.metadata.json", meta)
    atomic_json(args.output / f"{regime}.host-io.json", [{k: v for k, v in r.items() if k != "metrics"} for r in completed])
    atomic_json(args.output / f"{regime}-qualification.json", proof)
    export(args.output / f"{regime}.csv", completed)
    print(f"{args.stack} {regime} completed {len(completed)} processes ({proof['classification']})", flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("probe", type=Path)
    parser.add_argument("scratch", type=Path)
    parser.add_argument("output", type=Path)
    parser.add_argument("--stack", choices=STACKS, required=True)
    parser.add_argument("--revision", required=True)
    parser.add_argument("--devices", default="", help="comma-separated Linux whole-disk names")
    parser.add_argument("--cpus", help="4 on Linux or native on macOS (default follows the stack)")
    parser.add_argument("--case", choices=["all", *CELLS], default="all")
    args = parser.parse_args()
    if not re.fullmatch(r"[0-9a-f]{40}", args.revision):
        parser.error("--revision must be a full 40-character commit hash")
    args.probe = args.probe.resolve(strict=True)
    args.probe_hash = digest(args.probe)
    args.scratch = args.scratch.resolve()
    args.scratch.mkdir(parents=True, exist_ok=True)
    expected_cpu = "native" if args.stack == "m5-apfs" else "4"
    if args.cpus is not None and args.cpus != expected_cpu:
        parser.error(f"{args.stack} requires --cpus {expected_cpu}")
    args.cpus = expected_cpu
    args.devices = sorted(s.strip() for s in args.devices.split(",") if s.strip())
    chosen = cases_for(args.stack)
    if args.case != "all":
        if args.case not in chosen:
            parser.error(f"{args.case} is outside the scheduled cells for {args.stack}")
        chosen = [args.case]
    folder = args.output / "checkpoints"
    if not (folder / "identity.json").exists() and any(args.output.glob("*.csv")):
        parser.error("existing CSVs belong to an earlier campaign; choose a new output directory")
    host = environment(args.stack, args.scratch, args.devices)
    here = Path(__file__).resolve().parent
    identity = dict(**host, build_revision=args.revision, probe_sha256=args.probe_hash,
                    driver_sha256=digest(Path(__file__)), journal_sha256=digest(here / "retest_run.py"),
                    evidence_sha256=digest(here.parent / "grouped-reads" / "read_evidence.py"),
                    scratch=str(args.scratch), seed=SEED)
    with Journal(folder, identity) as journal:
        for regime in chosen:
            path, table = fixture(args, CELLS[regime][0])
            run_case(args, journal, regime, host, path, table)


if __name__ == "__main__":
    main()
