"""Reproduce the L0 paired comparisons using separately compiled harness binaries."""
import argparse
import csv
import os
from pathlib import Path
import subprocess
import sys


def execute(args, binary, mode, subranges, cold, seed):
    command = ["taskset", "-c", args.cpus, str(binary.resolve()), mode]
    env = dict(
        os.environ,
        TMPDIR=str(args.tmpdir.resolve()),
        MMDB_L0_OPS=str(args.operations),
        MMDB_L0_RATE=str(args.rate),
        MMDB_L0_SEED=str(seed),
        MMDB_L0_SUBS=str(subranges),
        MMDB_L0_COLD_ROWS=str(int(cold)),
    )
    proc = subprocess.run(command, env=env, capture_output=True, text=True, timeout=90)
    if proc.returncode:
        raise RuntimeError(f"{command} exited {proc.returncode}: {proc.stderr}")
    metrics = dict(word.split("=", 1) for word in proc.stdout.split() if "=" in word)
    assert int(metrics["write_ops"]) == int(metrics["read_ops"]) == args.operations
    assert int(metrics["l1_start"]) == 24
    assert int(metrics["multi_target_jobs"]) > 0
    assert (int(metrics["parallel_jobs"]) > 0) == (subranges > 1)
    assert 1 <= int(metrics["peak_ranges"]) <= subranges
    if subranges > 1:
        assert int(metrics["peak_ranges"]) > 1
    return metrics


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--baseline", required=True, type=Path)
    parser.add_argument("--candidate", required=True, type=Path)
    parser.add_argument("--cpus", required=True)
    parser.add_argument("--tmpdir", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--operations", type=int, default=16384)
    parser.add_argument("--rate", type=int, default=20000, help="Offered requests/s per class")
    parser.add_argument("comparisons", nargs="+", choices=["aa", "ab", "fence", "observer"])
    args = parser.parse_args()
    base = ("baseline", "gap", 1, False)
    arms = {
        "aa": (base, base),
        "ab": (base, ("candidate", "gap", 2, False)),
        "fence": (("baseline", "native", 1, True), ("baseline", "sentinel", 1, True)),
        "observer": (base, ("baseline", "gap", 1, True)),
    }
    binaries = {"baseline": args.baseline, "candidate": args.candidate}
    with args.output.open("x", newline="") as output:
        writer = None
        for pair in range(1, 6):
            for kind in args.comparisons:
                end_states = []
                reverse = (pair + int(kind in ("ab", "observer"))) % 2
                for order, arm in enumerate((1, 0) if reverse else (0, 1), 1):
                    name, mode, subranges, cold = arms[kind][arm]
                    metrics = execute(args, binaries[name], mode, subranges, cold, 6 + pair)
                    if kind == "aa" or (kind == "ab" and arm == 0):
                        assert int(metrics["l0_stall_ns"]) > 0
                    row = {
                        "pair": pair, "order": order, "comparison": kind,
                        "arm": "B" if arm else "A", "binary": name, **metrics,
                    }
                    if writer is None:
                        writer = csv.DictWriter(output, fieldnames=row, lineterminator="\n")
                        writer.writeheader()
                    writer.writerow(row)
                    output.flush()
                    end_states.append((metrics["l0_end"], metrics["rewrite_jobs"]))
                if end_states[0] != end_states[1]:
                    print(f"Pair {pair} ({kind}) ended with different L0 counts or rewrite jobs: "
                          f"{end_states}; compare in-window metrics separately.", file=sys.stderr)


if __name__ == "__main__":
    main()
