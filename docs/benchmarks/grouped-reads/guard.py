"""Compare uninstrumented public scalar gets from two release executables."""
import argparse
import csv
import subprocess
from pathlib import Path

from read_evidence import file_sha256, trial_command

parser = argparse.ArgumentParser()
parser.add_argument("baseline", type=Path)
parser.add_argument("candidate", type=Path)
parser.add_argument("fixture", type=Path)
parser.add_argument("output", type=Path)
parser.add_argument("--cpus", required=True, help="Linux CPU list, or native for macOS scheduling")
parser.add_argument("--samples", type=int, default=300000)
parser.add_argument("--fanout", type=int, choices=[1, 32], default=1)
args = parser.parse_args()
if args.samples < 10000:
    parser.error("decision trials need at least 10,000 completed requests")
with args.output.open("x") as f:
    writer = csv.DictWriter(f, fieldnames=["round", "pair", "order", "arm", "cpus",
                                          "sha256", "fixture", "fanout", "method", "seed", "samples", "p50_ns", "p99_ns"],
                            lineterminator="\n")
    writer.writeheader()
    hashes = {binary: file_sha256(binary) for binary in (args.baseline, args.candidate)}
    for r in range(5):
        seed = 20261125 + 1021 * r
        for pair in (["AA", "AB"] if r % 2 == 0 else ["AB", "AA"]):
            arms = [("baseline", args.baseline),
                    ("comparison", args.candidate if pair == "AB" else args.baseline)]
            if (r + (pair == "AB")) % 2:
                arms.reverse()
            for order, (arm, binary) in enumerate(arms, 1):
                method = "grouped32" if pair == "AB" and arm == "comparison" else "scalar32"
                command = [binary, args.fixture, str(args.samples), str(seed)]
                if args.fanout == 32:
                    command.append(method)
                else:
                    method = "single"
                out = subprocess.check_output(trial_command(command, args.cpus), text=True)
                metrics = dict(t.split("=", 1) for t in out.split())
                writer.writerow(dict(round=r+1, pair=pair, order=order, arm=arm,
                                     cpus=args.cpus, sha256=hashes[binary], seed=seed, fixture=args.fixture.name,
                                     method=method, fanout=args.fanout, **metrics))
                f.flush()
        print(f"Finished scalar guard round {r+1}", flush=True)
