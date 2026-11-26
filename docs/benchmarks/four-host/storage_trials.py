"""Run paired SST compression trials and descriptive WAL trials on one volume."""
import argparse
import gzip
import hashlib
import json
import os
from pathlib import Path
import platform
import random
import subprocess


def observations(text):
    rows = {}
    for line in text.splitlines():
        label, *counters = line.split()
        if label in rows:
            raise ValueError(f"duplicate observation: {label}")
        metrics = {}
        for token in counters:
            key, value = token.split("=")
            metrics[key] = None if value == "unavailable" else int(value)
        rows[label] = metrics
    return rows


def schedule(suite):
    for i in range(1, 6):
        seed = 20261126 + i
        if suite in ("both", "sst"):
            shapes = ["pattern", "entropy"]
            random.Random(seed).shuffle(shapes)
            for shape in shapes:
                aa = [("A1", "none"), ("A2", "none")]
                ab = [("AB1", "none"), ("B", "lz4")]
                arms = aa[::-1] + ab if i % 2 else ab[::-1] + aa
                for arm, codec in arms:
                    yield i, seed, shape, arm, ["sst", codec, shape, str(seed)]
        if suite in ("both", "wal"):
            for mode in (("async", "sync") if i % 2 else ("sync", "async")):
                yield i, seed, "wal", mode, ["wal", mode, str(seed)]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("binary", type=Path)
    parser.add_argument("output", type=Path)
    parser.add_argument("--scratch", type=Path, required=True)
    parser.add_argument("--stack", required=True)
    parser.add_argument("--revision", required=True)
    parser.add_argument("--cpus", required=True, help="Linux CPU list or native on macOS")
    parser.add_argument("--devices", help="comma-separated Linux backing block devices")
    parser.add_argument("--suite", choices=("both", "sst", "wal"), default="both")
    args = parser.parse_args()
    binary = args.binary.resolve(strict=True)
    scratch = args.scratch.resolve(strict=True)
    if not scratch.is_dir():
        parser.error("--scratch must be a directory")
    if (platform.system() == "Darwin") != (args.cpus == "native"):
        parser.error("use a CPU list on Linux or native on macOS")
    launcher = [] if args.cpus == "native" else ["taskset", "-c", args.cpus]
    env = {k: v for k, v in os.environ.items() if not k.startswith("MMDB_")}
    env["TMPDIR"] = str(scratch)
    if args.devices:
        env["MMDB_TRIAL_DISKS"] = args.devices
    header = dict(engine_revision=args.revision, binary_sha256=hashlib.sha256(binary.read_bytes()).hexdigest(),
                  stack=args.stack, platform=platform.platform(), cpus=args.cpus, devices=args.devices,
                  suite=args.suite, seed_base=20261126)
    with gzip.open(args.output, "xt") as out:
        print(json.dumps(dict(metadata=header)), file=out, flush=True)
        for iteration, seed, case, arm, argv in schedule(args.suite):
            run = subprocess.run([*launcher, str(binary), *argv], env=env, check=True,
                                 capture_output=True, text=True)
            data = observations(run.stdout)
            if case == "wal":
                if data["verified"]["rows"] != 10_000 or data["wal"]["puts"] != 10_000:
                    raise ValueError("incomplete WAL fixture")
            else:
                obs = data["sst"]
                if obs["rows"] != 262_144 or obs["lookups"] != 40_000 or obs["sst_bytes"] <= 0:
                    raise ValueError("incomplete SST fixture")
            print(json.dumps(dict(round=iteration, seed=seed, case=case, arm=arm,
                                  arguments=argv, **data)), file=out, flush=True)
            print(iteration, case, arm, flush=True)


if __name__ == "__main__":
    main()
