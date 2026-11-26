"""Run interleaved A/A and A/B trials from fresh processes."""
import argparse
import csv
import itertools
import random
import subprocess
from pathlib import Path

from read_evidence import metadata, require_cache_evidence, trial_command

parser = argparse.ArgumentParser()
parser.add_argument("probe", type=Path)
parser.add_argument("fixture", type=Path)
parser.add_argument("output", type=Path)
parser.add_argument("--cpus", required=True, help="Linux CPU list, or native for macOS scheduling")
parser.add_argument("--revision", required=True, help="commit used to build the supplied release binary")
parser.add_argument("--matrix", choices=["primary", "warm", "page", "device"], required=True)
parser.add_argument("--fanout", type=int, choices=[1, 8, 32], help="restrict a repeated calibration to one fanout")
parser.add_argument("--locality", choices=["clustered", "scattered"], help="restrict a repeated calibration to one locality")
parser.add_argument("--samples", type=int, default=10000)
parser.add_argument("--seed", type=int, default=20261125)
args = parser.parse_args()
if args.samples < 10000:
    parser.error("decision trials need at least 10,000 completed requests; use the probe directly for a smoke check")
primary = [("warm", "clustered", 32), ("warm", "clustered", 1)]
if args.matrix == "primary":
    cells = primary
else:
    cells = [
        cell
        for cell in itertools.product([args.matrix], ["clustered", "scattered"], [1, 8, 32])
        if cell not in primary
    ]

cells = [
    (regime, locality, fanout)
    for regime, locality, fanout in cells
    if (args.locality is None or args.locality == locality)
    and (args.fanout is None or args.fanout == fanout)
]
if not cells:
    parser.error("selection does not contain a workload")
fields = ["round", "pair", "order", "arm", "seed", "regime", "method", "locality",
          "fanout", "samples", "p50_ns", "p99_ns", "blocks", "tables",
          "storage_bytes", "device_requests", "device_p99_ns", "peak_pin_bytes", "pin_budget"]
metadata(args)
rng = random.Random(args.seed)
with args.output.open("x") as f:
    writer = csv.DictWriter(f, fieldnames=fields, lineterminator="\n")
    writer.writeheader()
    for iteration in range(5):
        trials = list(itertools.product(cells, ["AA", "AB"]))
        rng.shuffle(trials)
        for (regime, locality, fanout), pair in trials:
            arms = [("baseline", "scalar"), ("comparison", "scalar" if pair == "AA" else "grouped")]
            if (iteration + (pair == "AB")) % 2:
                arms.reverse()
            trace_seed = args.seed + 1021 * iteration
            for order, (arm, method) in enumerate(arms, 1):
                result = subprocess.run(trial_command([
                    args.probe, args.fixture, regime,
                    method, locality, str(fanout), str(args.samples), str(trace_seed),
                ], args.cpus), check=True, capture_output=True, text=True, timeout=900)
                measurements = dict(s.split("=", 1) for s in result.stdout.strip().split())
                writer.writerow(dict(round=iteration + 1, pair=pair, order=order, arm=arm,
                                     seed=trace_seed, **measurements))
                f.flush()
        print(f"Finished {args.matrix} round {iteration + 1}", flush=True)

require_cache_evidence(args.output)
