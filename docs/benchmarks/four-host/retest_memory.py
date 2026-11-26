"""Replay the fixed shard trace across volumes and run a matched revision sentinel."""
import argparse
import gzip
import json
import os
from pathlib import Path
import platform
import subprocess
import sys

from retest_run import Journal, digest, run_child

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent / "apple-m5"))
from summarize_engine import counters, observation


def publish(path, header, rows):
    # The journal is the source of truth if export was interrupted.
    temporary = path.with_suffix(".tmp")
    with temporary.open("wb") as stream, gzip.GzipFile(
            filename="", fileobj=stream, mode="wb", mtime=0) as f:
        for row in [{"metadata": header}, *rows]:
            f.write((json.dumps(row) + "\n").encode())
    os.replace(temporary, path)


def mount(path):
    if platform.system() == "Darwin":
        return subprocess.check_output(["df", "-P", str(path)], text=True).splitlines()[-1].split()[0]
    return subprocess.check_output(["findmnt", "-n", "-o", "SOURCE,FSTYPE,OPTIONS", "-T", str(path)],
                                   text=True).strip()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("output", type=Path)
    parser.add_argument("--engine", required=True, type=Path)
    parser.add_argument("--old-engine", type=Path)
    parser.add_argument("--revision", required=True)
    parser.add_argument("--old-revision", default="828e47a1d019e3fd0854a3d9a0d98b72e873350c")
    parser.add_argument("--volume", nargs=2, action="append", required=True, metavar=("LABEL", "SCRATCH"))
    parser.add_argument("--primary", required=True)
    parser.add_argument("--cpus", required=True)
    parser.add_argument("--suite", choices=("settings", "version", "all"), default="all")
    args = parser.parse_args()
    executable = args.engine.resolve(strict=True)
    if (platform.system() == "Darwin") != (args.cpus == "native"):
        parser.error("use native on macOS or a physical CPU list on Linux")
    if args.suite != "settings" and not args.old_engine:
        parser.error("the version sentinel requires --old-engine")
    locations = {name: Path(path).resolve(strict=True) for name, path in args.volume}
    if (len(locations) != len(args.volume) or args.primary not in locations
            or len({p.stat().st_dev for p in locations.values()}) != len(locations)):
        parser.error("expected distinct volumes and a valid primary label")
    launcher = [] if args.cpus == "native" else ["taskset", "-c", args.cpus]
    if launcher:
        affinity = subprocess.check_output([*launcher, "taskset", "-pc", "0"], text=True).strip()
        affinity = affinity.rsplit(":", 1)[-1].strip()
    else:
        affinity = "native"
    common = dict(engine_revision=args.revision, binary_sha256={"control": digest(executable)},
                  cpu_affinity=affinity, platform=platform.platform(), runner_sha256=digest(__file__),
                  journal_sha256=digest(HERE / "retest_run.py"), plan_sha256=digest(HERE / "memory-plan.jsonl.gz"),
                  post_window="flush then compact each DB, serially", volumes={
                      k: {"path": str(v), "device_id": v.stat().st_dev, "mount": mount(v)}
                      for k,v in locations.items()})
    env = {k:v for k,v in os.environ.items() if not k.startswith("MMDB_PROFILE_")}
    with gzip.open(HERE / "memory-plan.jsonl.gz", "rt") as f:
        next(f)
        schedule = [json.loads(line) for line in f]
    if len(schedule) != 80:
        raise ValueError("expected the frozen 80-process schedule")
    args.output.mkdir(parents=True, exist_ok=True)

    def trial(row, name, binary):
        settings = row["settings"]
        trial_env = {**env, **{"MMDB_PROFILE_" + k:str(v) for k,v in settings.items()},
                     "MMDB_PROFILE_MEASURE_DRAIN":"true", "TMPDIR":str(locations[name])}
        process = run_child([*launcher, str(binary), "memory"], env=trial_env,
                            text=True, check=True, capture_output=True)
        result = {**row, "raw":process.stdout}
        # Reject truncated output or a wrongly built harness before committing
        # the pair. A missed offered rate remains an observable outcome.
        _, kept = observation(result)
        terminal = counters(next(line for line in process.stdout.splitlines() if line.startswith("drain ")))
        run = counters(next(line for line in process.stdout.splitlines() if line.startswith("run ")))
        if (min(terminal["micros"], terminal["sst_bytes"]) <= 0 or terminal["final_l0"] or terminal["final_immutables"]
                or terminal["terminal_us"] + 1 < 1e6 * run["seconds"]
                or terminal["total_compaction_bytes"] < run["compaction_output_bytes"]):
            raise ValueError(f"unqualified terminal state: {terminal}")
        result["offered_load_kept"] = kept
        return result

    if args.suite in ("settings", "all"):
        stores = {name: [] for name in locations}
        journal_dir = args.output / ".memory-journal"
        if not (journal_dir / "identity.json").exists() and any(
                (args.output / name / "memory.jsonl.gz").exists() for name in locations):
            raise FileExistsError("an existing memory packet has no matching journal")
        with Journal(journal_dir, common) as journal:
            for block_no in range(0, len(schedule), 2):
                rows = schedule[block_no:block_no+2]
                arms = {r["arm"] for r in rows}
                if arms not in ({"A1","A2"}, {"AB1","B"}):
                    raise ValueError("a checkpoint must contain one complete matched pair")
                for name in (list(locations) if block_no % 4 == 0 else reversed(locations)):
                    key = [name, rows[0]["round"], rows[0]["case"], sorted(arms)]
                    finished = journal.block(key, rows,
                                             lambda: [trial(row, name, executable) for row in rows])
                    stores[name].extend(finished)
                    print("memory", name, len(stores[name]), "/80", flush=True)
            for name, rows in stores.items():
                folder = args.output / name
                folder.mkdir(exist_ok=True)
                publish(folder / "memory.jsonl.gz", {**common, "stack":name}, rows)

    if args.suite in ("version", "all"):
        old = args.old_engine.resolve(strict=True)
        version_header = {**common, "old_revision":args.old_revision, "old_binary_sha256":digest(old)}
        results = []
        journal_dir = args.output / ".memory-version-journal"
        if not (journal_dir / "identity.json").exists() and (args.output / "version-memory.jsonl.gz").exists():
            raise FileExistsError("an existing sentinel packet has no matching journal")
        with Journal(journal_dir, version_header) as journal:
            for i in range(1,6):
                settings = dict(SEED=20261126+i, OPS=20000, RATE=0, SECONDS=8,
                                BUFFER_MIB=6, IMMUTABLES=4, POOL_MIB=256, PLACEMENT="hot", BATCH_ENTRIES=32)
                for pair in ([("A2","A1"),("AB1","B")] if i % 2 else [("B","AB1"),("A1","A2")]):
                    specs = [dict(round=i, mode="memory", case="version-hot-saturation",
                                  arm=arm, binary="new" if arm == "B" else "old", settings=settings)
                             for arm in pair]
                    results.extend(journal.block([i, sorted(pair)], specs, lambda: [
                        trial(row, args.primary, executable if row["binary"] == "new" else old) for row in specs]))
                print("version memory", i, "/5", flush=True)
            publish(args.output / "version-memory.jsonl.gz", version_header, results)


if __name__ == "__main__":
    main()
