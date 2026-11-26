"""Validate cache/device evidence for complete trial records."""
import csv
from functools import cache
import hashlib
import json
import platform
import plistlib
import subprocess


def file_sha256(path):
    digest = hashlib.sha256()
    with path.open("rb") as f:
        for block in iter(lambda: f.read(1 << 20), b""):
            digest.update(block)
    return digest.hexdigest()


def trial_command(command, cpus):
    if platform.system() == "Darwin":
        if cpus != "native":
            raise ValueError("macOS trials use --cpus native (OS scheduling)")
        return command
    return ["taskset", "-c", cpus, *command]


def has_cache_evidence(row):
    reads = int(row["storage_bytes"])
    if row["regime"] == "device":
        # Require evidence on nearly every request, not a single metadata read.
        samples, verified = int(row["samples"]), int(row["device_requests"])
        return (0 < verified <= samples and reads >= verified * 4096 and int(row["device_p99_ns"]) > 0
                and verified * 100 >= samples * 95)
    return row["regime"] in ("warm", "page") and reads == 0


def all_request_p99(row):
    if int(row["samples"]) < 10000:
        raise ValueError("a decision trial needs at least 10,000 completed requests")
    return int(row["p99_ns"])


@cache
def distinct_scattered_probes(rows, samples, fanout, seed):
    """Replay the deterministic scattered trace outside the timed process."""
    mask = (1 << 64) - 1
    step = 0x9E3779B97F4A7C15
    total = 0
    state = seed
    for _ in range(samples):
        state = (state + step) & mask  # cluster base drawn before generating keys
        keys = set()
        for _ in range(fanout):
            state = (state + step) & mask
            value = ((state ^ (state >> 30)) * 0xBF58476D1CE4E5B9) & mask
            value = ((value ^ (value >> 27)) * 0x94D049BB133111EB) & mask
            keys.add((value ^ (value >> 31)) & (rows - 1))
        total += len(keys)
    return total


def expected_table_probes(row, meta):
    samples, fanout = int(row["samples"]), int(row["fanout"])
    if (row["method"] == "grouped" and row["locality"] == "scattered"
            and fanout > 1 and meta.get("coalesces_duplicates", False)):
        return distinct_scattered_probes(meta["rows"], samples, fanout, int(row["seed"]))
    return samples * fanout


def require_cache_evidence(results):
    with results.open() as f:
        invalid = []
        for i, row in enumerate(csv.DictReader(f), 2):
            all_request_p99(row)
            if not has_cache_evidence(row):
                invalid.append(i)
    if invalid:
        raise ValueError(f"{results}: cache regime unverified on CSV lines {invalid}")


def verify_block_reuse(control, candidate):
    if (candidate["locality"] == "clustered" and int(candidate["fanout"]) > 1
            and int(candidate["blocks"]) >= int(control["blocks"])):
        raise ValueError("clustered candidate did not reduce physical block acquisitions")


def metadata(args):
    def capture(command):
        return subprocess.check_output(command, text=True).strip()

    tables = list(args.fixture.glob("*.sst"))
    assert len(tables) == 1
    # Use the same taskset launcher as timed children: a caller serializing
    # disk access through that launcher also serializes the fixture hash.
    if platform.system() == "Darwin":
        trial_command([], args.cpus)
        hashes = [file_sha256(path) for path in (args.probe, tables[0])]
        mount = next(p for p in tables[0].resolve().parents if p.is_mount())
        volume = plistlib.loads(subprocess.check_output(["diskutil", "info", "-plist", mount]))
        host = {
            "os_version": capture(["sw_vers", "-productVersion"]),
            "cpu_model": capture(["sysctl", "-n", "machdep.cpu.brand_string"]),
            "cpu_affinity": "OS scheduled",
            "filesystem": volume["FilesystemName"],
            "storage_counter": "ri_diskio_bytesread",
            "device_cache_control": "file-scoped msync(MS_INVALIDATE) every 128 requests",
        }
    else:
        hashes = [line.split()[0] for line in capture(trial_command(
            ["sha256sum", "--", str(args.probe), str(tables[0])], args.cpus)).splitlines()]
        host = {
            "cpu_affinity": args.cpus,
            "cpu_model": next(line.split(":", 1)[1].strip() for line in capture(["lscpu"]).splitlines() if line.startswith("Model name:")),
            "filesystem": capture(["findmnt", "-n", "-T", str(tables[0]), "-o", "SOURCE,FSTYPE,OPTIONS"]),
            "sst_attributes": capture(["lsattr", "-d", str(tables[0])]).split()[0],
            "storage_counter": "linux_read_bytes",
        }
    meta = {
        **host,
        "build_revision": capture(["git", "rev-parse", "--verify", args.revision + "^{commit}"]),
        "probe_sha256": hashes[0],
        "kernel": platform.release(),
        "architecture": platform.machine(),
        "rustc": capture(["rustc", "+stable", "-vV"]),
        "sst_bytes": tables[0].stat().st_size,
        "sst_sha256": hashes[1],
        "rows": int(args.fixture.with_suffix(".rows").read_text()),
        "value_bytes": 128, "key_bytes": 8, "block_bytes": 4096, "prefix_len": 0,
        "sst_compression": "None", "warm_cache_bytes": 128 << 20,
        "page_or_device_cache_bytes": 0, "reader_threads": 1,
        "fixture": "one bottom SST, high-entropy deterministic inline values",
        "cache_regimes": "warm full scalar pass, page full buffered SST read, device evict before each 128 requests",
        "coalesces_duplicates": True,
        "device_rule": "at least 4096 read_bytes on >=95% of requests",
        "matrix": args.matrix, "samples_per_run": args.samples, "base_seed": args.seed,
        "trace": "splitmix over power-of-two keys; clustered permutes within consecutive fanout keys, scattered independent",
    }
    meta.update({"selected_fanout": args.fanout, "selected_locality": args.locality})
    with args.output.with_suffix(".metadata.json").open("x") as f:
        json.dump(meta, f, indent=2)
        f.write("\n")
