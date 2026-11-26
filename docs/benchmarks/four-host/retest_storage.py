"""Measure SST codecs, WAL, reopened stores, and disk or revision differences in fresh processes."""
import argparse
from collections import defaultdict
import gzip
from itertools import groupby
import json
import os
from pathlib import Path
import platform
import re
import tempfile
import time

from retest_run import Journal, digest, run_child
from storage_trials import observations, schedule


BASE_SEED = 20261126
DISK_PACKET = "volume-pairs.jsonl.gz"
VERSION_PACKET = "version-bridge.jsonl.gz"


def job(stack, case, arm, argv, *, binary="storage", packet=None, settings=None):
    return dict(stack=stack, binary=binary, argv=argv, settings=settings or {},
                views={packet or f"{stack}/storage.jsonl.gz": [case, arm]})


def reopen_block(stack, iteration):
    modes = (False, True) if iteration % 2 else (True, False)
    return [job(stack, "reopen", "read-only" if ro else "writable", ["reopen"],
                binary="engine", packet=f"{stack}/reopen.jsonl.gz",
                settings=dict(OPS=100000, PREPARE_MIB=64, BUFFER_MIB=6, IMMUTABLES=4,
                              POOL_MIB=64, SEED=BASE_SEED + iteration, READ_ONLY=str(ro).lower()))
            for ro in modes]


def entropy_block(reference, candidate, iteration, seed):
    # Each local AB1 is adjacent to its codec B and to the other volume's AB1.
    # The two scratch volumes never share a comparison process within a single view.
    order = [(candidate, "A2"), (candidate, "A1"), (reference, "A2"), (reference, "A1"),
             (reference, "B"), (reference, "AB1"), (candidate, "AB1"), (candidate, "B")]
    block = []
    for stack, arm in order if iteration % 2 else reversed(order):
        trial = job(stack, "entropy", arm, ["sst", "lz4" if arm == "B" else "none", "entropy", str(seed)])
        if stack == reference and arm != "B":
            trial["views"][DISK_PACKET] = ["sst-none-entropy", arm]
        elif stack == candidate and arm == "AB1":
            trial["views"][DISK_PACKET] = ["sst-none-entropy", "B"]
        block.append(trial)
    return block


def wal_block(stacks, mode, iteration, seed):
    if len(stacks) == 1 or mode == "async":
        return [job(stack, "wal", mode, ["wal", mode, str(seed)])
                for stack in (stacks if iteration % 2 else reversed(stacks))]
    block = []
    for arm in (("A2", "A1", "AB1", "B") if iteration % 2 else ("B", "AB1", "A1", "A2")):
        stack = stacks[1] if arm == "B" else stacks[0]
        trial = job(stack, "wal-sync", arm, ["wal", "sync", str(seed)], packet=DISK_PACKET)
        if arm in ("AB1", "B"):
            trial["views"][f"{stack}/storage.jsonl.gz"] = ["wal", "sync"]
        block.append(trial)
    return block


def blocks(stacks, primary, suite):
    if suite == "version":
        for i in range(1, 6):
            order = ("A2", "A1", "AB1", "B") if i % 2 else ("B", "AB1", "A1", "A2")
            yield i, "revision-sentinel", [
                job(primary, "sst-none-entropy", arm, ["sst", "none", "entropy", str(BASE_SEED + i)],
                    binary="storage" if arm == "B" else "old-storage", packet=VERSION_PACKET)
                for arm in order]
        return

    # Validate the actual child configuration on the first reopen, before the longer sweeps.
    for i in range(1, 6):
        for stack in stacks if i % 2 else reversed(stacks):
            yield i, f"reopen-{stack}", reopen_block(stack, i)
    for (i, shape), source in groupby(schedule("both"), key=lambda item: (item[0], item[2])):
        arms = list(source)
        seed = BASE_SEED + i
        if shape == "wal":
            for _, _, _, mode, _ in arms:
                yield i, f"wal-{mode}", wal_block(stacks, mode, i, seed)
        elif len(stacks) == 2 and shape == "entropy":
            yield i, "entropy-codec-and-disk", entropy_block(*stacks, i, seed)
        else:
            for stack in stacks if i % 2 else reversed(stacks):
                yield i, f"{shape}-{stack}", [job(stack, shape, arm, argv) for _, _, _, arm, argv in arms]


def reopen_observation(raw, settings):
    fields = {line.split()[0]: dict(w.split("=", 1) for w in line.split()[1:]) for line in raw.splitlines()}
    config = fields["config"]
    for name, setting in (("ops", "OPS"), ("pool_mib", "POOL_MIB"), ("seed", "SEED"),
                          ("buffer_mib", "BUFFER_MIB"), ("depth", "IMMUTABLES")):
        if int(config[name]) != settings[setting]:
            raise ValueError(f"reopen config {name}={config[name]} differs from {settings[setting]}")
    result = {k: int(v) for k, v in fields["reopen"].items()}
    # The engine prints this row only after checking every recovered 128-byte value.
    if (config["mode"] != "reopen" or result["rows"] != settings["OPS"]
            or min(result["open_us"], result["wal_bytes"], result["open_rss_kib"], result["open_peak_kib"]) <= 0):
        raise ValueError("incomplete residual WAL or reopen observation")
    return dict(settings=settings, read_only=settings["READ_ONLY"] == "true", reopen=result)


def storage_observation(raw, mode):
    data = observations(raw)
    if mode == "wal":
        if data["verified"]["rows"] != 10000 or data["wal"]["puts"] != 10000:
            raise ValueError("incomplete WAL work or verification")
    else:
        sst = data["sst"]
        if sst["rows"] != 262144 or sst["lookups"] != 40000 or min(sst["files"], sst["sst_bytes"]) <= 0:
            raise ValueError("incomplete SST fixture, scan or point reads")
    return data


def host_counters(devices):
    """Whole-device and host ARC counters enclosing the child, including setup and verification."""
    if not devices or platform.system() != "Linux":
        return {}
    values = {}
    for device in devices.split(","):
        stats = (Path("/sys/class/block") / device / "stat").read_text().split()
        values[device + ".read_sectors"] = int(stats[2])
        values[device + ".write_sectors"] = int(stats[6])
    arc = Path("/proc/spl/kstat/zfs/arcstats")
    if arc.is_file():
        for line in arc.read_text().splitlines():
            fields = line.split()
            if fields and fields[0] in ("demand_data_hits", "demand_data_misses",
                                        "demand_metadata_hits", "demand_metadata_misses"):
                values["host_arc." + fields[0]] = int(fields[2])
    return values


def execute(trial, options):
    stack = options.stacks[trial["stack"]]
    env = {k: v for k, v in os.environ.items() if not k.startswith("MMDB_")}
    env["TMPDIR"] = stack["scratch"]
    if stack["devices"]:
        env["MMDB_TRIAL_DISKS"] = stack["devices"]
    env.update({"MMDB_PROFILE_" + k: str(v) for k, v in trial["settings"].items()})
    cmd = [*options.launcher, options.binaries[trial["binary"]], *trial["argv"]]
    before = host_counters(stack["devices"])
    started = time.time_ns()
    raw = run_child(cmd, check=True, capture_output=True, text=True, env=env).stdout
    ended = time.time_ns()
    after = host_counters(stack["devices"])
    data = (reopen_observation(raw, trial["settings"]) if trial["binary"] == "engine"
            else storage_observation(raw, trial["argv"][0]))
    return dict(**data, raw=raw, sample_started_ns=started, sample_ended_ns=ended,
                whole_child_host_counter_delta={k: after[k] - v for k, v in before.items()})


def write_packet(path, header, rows):
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(dir=path.parent, prefix=".writing-", delete=False) as f:
        temporary = Path(f.name)
    try:
        with temporary.open("wb") as f:
            with gzip.GzipFile(fileobj=f, filename="", mode="wb", mtime=0) as packet:
                for row in [{"metadata": header}, *rows]:
                    packet.write((json.dumps(row, sort_keys=True, separators=(",", ":")) + "\n").encode())
            f.flush()
            os.fsync(f.fileno())
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def collect(options, suite):
    stacks = list(options.stacks)
    roles = ("storage", "old-storage") if suite == "version" else ("engine", "storage")
    hashes = {role: digest(options.binaries[role]) for role in roles}
    identity = dict(driver_sha256=digest(__file__), parser_sha256=digest(Path(__file__).with_name("storage_trials.py")),
                    executor_sha256=digest(Path(__file__).with_name("retest_run.py")), suite=suite,
                    revision=options.revision, old_revision=options.old_revision if suite == "version" else None,
                    binary_sha256=hashes, cpus=options.cpus, volumes=options.stacks, volume_order=stacks,
                    primary=options.primary if suite == "version" else None, platform=platform.platform())
    packets = defaultdict(list)
    mappings = []
    journal_dir = options.output / (".storage-" + suite + "-journal")
    if suite == "version":
        outputs = [VERSION_PACKET]
    else:
        outputs = [f"{s}/{name}.jsonl.gz" for s in stacks for name in ("storage", "reopen")]
        if len(stacks) == 2:
            outputs.append(DISK_PACKET)
    outputs.append(suite + "-work-map.jsonl.gz")
    if not (journal_dir / "identity.json").exists() and any((options.output / p).exists() for p in outputs):
        raise FileExistsError("Existing results have no matching journal. Choose a fresh output directory.")
    with Journal(journal_dir, identity) as journal:
        for i, key, jobs in blocks(stacks, options.primary, suite):
            block = f"{suite}-r{i}-{key}"
            results = journal.block(block, jobs, lambda: [execute(j, options) for j in jobs])
            if len(results) != len(jobs):
                raise ValueError(f"incomplete checkpoint for {block}")
            for position, (trial, measured) in enumerate(zip(jobs, results), 1):
                stack, role = trial["stack"], trial["binary"]
                revision = options.old_revision if role == "old-storage" else options.revision
                work_id = f"{options.revision}/{stack}/{block}/{position}/{measured['sample_started_ns']}"
                mapping = dict(work_id=work_id, stack=stack, block=block, position=position,
                               revision=revision, binary_sha256=hashes[role], views=trial["views"])
                mappings.append(mapping)
                for packet, (case, arm) in trial["views"].items():
                    row = dict(**measured, work_id=work_id, round=i, seed=BASE_SEED + i, case=case,
                               arm=arm, arguments=trial["argv"], measurement_revision=revision, stack=stack)
                    if packet == DISK_PACKET:
                        row["volume"] = "reference" if stack == stacks[0] else "candidate"
                    if packet == VERSION_PACKET:
                        row["binary"] = "old" if role == "old-storage" else "new"
                    packets[packet].append(row)
            print(block, flush=True)

        header = dict(engine_revision=options.revision, cpus=options.cpus, platform=identity["platform"],
                      seed_base=BASE_SEED, stack_order=stacks, binary_sha256=hashes["storage"])
        for name, rows in packets.items():
            meta = dict(header)
            if name == VERSION_PACKET:
                meta.update(old_engine_revision=options.old_revision, new_engine_revision=options.revision,
                            old_binary_sha256=hashes["old-storage"], new_binary_sha256=hashes["storage"],
                            stack=options.primary, devices=options.stacks[options.primary]["devices"])
            elif name == DISK_PACKET:
                meta["disks"] = [options.stacks[s]["devices"] for s in stacks]
            else:
                stack = name.split("/")[0]
                meta.update(stack=stack, devices=options.stacks[stack]["devices"])
                if name.endswith("reopen.jsonl.gz"):
                    meta.update(binary_sha256=hashes["engine"], cpu_affinity=options.cpus)
                    if len({r["reopen"]["wal_bytes"] for r in rows}) != 1:
                        raise ValueError(f"{stack}: different WAL sizes across reopen trials")
                else:
                    meta["suite"] = "both"
            if len({r["work_id"] for r in rows}) != len(rows):
                raise ValueError(f"{name} counts the same physical observation twice")
            write_packet(options.output / name, meta, rows)
        write_packet(options.output / (suite + "-work-map.jsonl.gz"), header, mappings)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("output", type=Path)
    parser.add_argument("--storage", type=Path, required=True, help="storage driver linked to the new engine")
    parser.add_argument("--engine", type=Path, help="new engine_profile for residual-WAL checks")
    parser.add_argument("--revision", required=True)
    parser.add_argument("--old-storage", type=Path, help="same storage driver linked to the old engine")
    parser.add_argument("--old-revision")
    parser.add_argument("--suite", choices=["standard", "version", "all"], default="all")
    parser.add_argument("--cpus", required=True, help="Linux taskset list or native on macOS")
    parser.add_argument("--volume", nargs=3, action="append", required=True, metavar=("STACK", "SCRATCH", "DISKS"),
                        help="one or two stacks in reference, candidate order; use - for no disk counter on macOS")
    parser.add_argument("--primary", help="stack on which to run the separate revision sentinel")
    o = parser.parse_args()
    if not re.fullmatch("[0-9a-f]{40}", o.revision):
        parser.error("--revision requires a full commit SHA")
    if (platform.system() == "Darwin") != (o.cpus == "native"):
        parser.error("use native scheduling on macOS or an explicit CPU list on Linux")
    if len(o.volume) not in (1, 2) or (o.cpus == "native" and len(o.volume) != 1):
        parser.error("supply one or two Linux volumes, or one native macOS volume")
    o.stacks = {}
    for label, scratch, devices in o.volume:
        if label in o.stacks or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_-]*", label):
            parser.error("stack labels must be distinct, simple names")
        path = Path(scratch).resolve(strict=True)
        if not path.is_dir():
            parser.error("each scratch path must be a directory")
        if o.cpus != "native":
            names = devices.split(",")
            if (len(names) != len(set(names)) or
                    any(not re.fullmatch(r"[A-Za-z0-9_-][A-Za-z0-9_.-]*", name)
                        or not (Path("/sys/class/block") / name / "stat").is_file() for name in names)):
                parser.error("supply the distinct Linux backing block-device names for each volume")
        elif devices != "-":
            parser.error("use - for the macOS disk-counter field")
        o.stacks[label] = dict(scratch=str(path), devices=None if devices == "-" else devices,
                               mount_device=path.stat().st_dev, directory_inode=path.stat().st_ino)
    if len(o.stacks) == 2:
        a, b = o.stacks.values()
        if a["mount_device"] == b["mount_device"] or set(a["devices"].split(",")) & set(b["devices"].split(",")):
            parser.error("the volume comparison needs separate mounts and disjoint physical disks")
    if len(o.stacks) == 2 and o.suite != "standard" and not o.primary:
        parser.error("name the revision sentinel's primary volume with --primary")
    o.primary = o.primary or next(iter(o.stacks))
    if o.primary not in o.stacks:
        parser.error("--primary must name a supplied stack")
    o.binaries = {"storage": o.storage.resolve(strict=True)}
    if o.suite != "version":
        if not o.engine:
            parser.error("--engine is required for the standard measurements")
        o.binaries["engine"] = o.engine.resolve(strict=True)
    if o.suite != "standard":
        if not o.old_storage or not o.old_revision or not re.fullmatch("[0-9a-f]{40}", o.old_revision):
            parser.error("--old-storage and a full --old-revision SHA are required")
        o.binaries["old-storage"] = o.old_storage.resolve(strict=True)
        if o.old_revision == o.revision or o.binaries["old-storage"] == o.binaries["storage"]:
            parser.error("the revision sentinel requires distinct old and new builds")
    o.launcher = [] if o.cpus == "native" else ["taskset", "-c", o.cpus]
    for suite in (("standard", "version") if o.suite == "all" else (o.suite,)):
        collect(o, suite)


if __name__ == "__main__":
    main()
