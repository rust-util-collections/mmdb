"""Run the gap fixture with one serial L0 job; only optional flush cuts differ."""
import argparse
import csv
import importlib.util
from pathlib import Path

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument("--baseline", type=Path, required=True)
parser.add_argument("--candidate", type=Path, required=True)
parser.add_argument("--l0-replay", type=Path, required=True, help="Existing L0 replay.py")
parser.add_argument("--tmpdir", type=Path, required=True)
parser.add_argument("--cpus", default="4-7")
parser.add_argument("--operations", type=int, default=16_384)
parser.add_argument("--rate", type=int, default=20_000)
parser.add_argument("--output", type=Path, required=True)
args = parser.parse_args()
spec = importlib.util.spec_from_file_location("l0_replay", args.l0_replay)
replay = importlib.util.module_from_spec(spec)
spec.loader.exec_module(replay)
with args.output.open("x") as output:
    writer = None
    for pair in range(1, 6):
        comparisons = ["aa", "ab"] if pair % 2 else ["ab", "aa"]
        for comparison in comparisons:
            arms = [("A", args.baseline), ("B", args.candidate if comparison == "ab" else args.baseline)]
            if (pair + (comparison == "ab")) % 2:
                arms.reverse()
            for order, (arm, binary) in enumerate(arms, 1):
                metrics = replay.execute(args, binary, "gap", 1, True, 6 + pair)
                cut = comparison == "ab" and arm == "B"
                assert (int(metrics["flushed_ssts"]) > int(metrics["flushes"])) == cut
                assert int(metrics["flushed_ssts"]) <= 2 * int(metrics["flushes"])
                if not cut:
                    assert int(metrics["logical_cold_emitted_bytes"]) > 0 and int(metrics["l0_stall_ns"]) > 0
                row = {"pair": pair, "comparison": comparison, "arm": arm, "order": order, **metrics}
                if writer is None:
                    writer = csv.DictWriter(output, fieldnames=row, lineterminator="\n")
                    writer.writeheader()
                writer.writerow(row)
                output.flush()
        print("prefix-cut round", pair, flush=True)
