"""Repeat only the primary hot-DB handoff comparison with 100,000 scalar requests."""
import argparse
import gzip
import hashlib
import json
import os
from pathlib import Path
import subprocess

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument("control", type=Path)
parser.add_argument("candidate", type=Path)
parser.add_argument("output", type=Path)
parser.add_argument("--tmpdir", required=True, type=Path)
parser.add_argument("--cpus", default="4-7")
parser.add_argument("--rate", default=18_911, type=int)
args = parser.parse_args()
if args.rate <= 0:
    parser.error("rate must be positive")
binaries = {"control": args.control.resolve(), "candidate": args.candidate.resolve()}
hashes = {k: hashlib.sha256(p.read_bytes()).hexdigest() for k, p in binaries.items()}
env = {k: v for k, v in os.environ.items() if not k.startswith("MMDB_PROFILE_")}
env["TMPDIR"] = str(args.tmpdir.resolve())
args.tmpdir.mkdir(parents=True, exist_ok=True)
with gzip.open(args.output, "xt") as out:
    out.write(json.dumps({"metadata": {"suite": "handoff-primary-extension", "cpus": args.cpus,
                                       "binary_sha256": hashes, "seed_base": 20361125, "rounds": 5}}) + "\n")
    for iteration in range(1, 6):
        settings = {"OPS": 100_000, "RATE": args.rate, "SEED": 20361125 + iteration,
                    "SYNC": "false", "ENTROPY": "true", "PLACEMENT": "hot"}
        env.update({"MMDB_PROFILE_" + k: str(v) for k, v in settings.items()})
        pairs = ["AA", "AB"] if iteration % 2 else ["AB", "AA"]
        for pair in pairs:
            arms = [("reference", "control"), ("comparison", "control" if pair == "AA" else "candidate")]
            if (iteration + (pair == "AB")) % 2:
                arms.reverse()
            for order, (arm, binary) in enumerate(arms, 1):
                cmd = ["taskset", "-c", args.cpus, str(binaries[binary]), "mixed"]
                result = subprocess.run(cmd, env=env, check=True, capture_output=True, text=True)
                out.write(json.dumps({"round": iteration, "pair": pair, "arm": arm, "order": order,
                                      "binary": binary, "settings": settings, "output": result.stdout}) + "\n")
                out.flush()
        print("primary extension round", iteration, flush=True)
