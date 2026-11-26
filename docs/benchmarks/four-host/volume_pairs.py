"""Interleave matched trials across two separate volumes of one Linux host."""
import argparse
import gzip
import hashlib
import json
import os
from pathlib import Path
import subprocess

from storage_trials import observations


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("binary", type=Path)
    p.add_argument("output", type=Path)
    p.add_argument("--reference", type=Path, required=True)
    p.add_argument("--candidate", type=Path, required=True)
    p.add_argument("--reference-disks", required=True)
    p.add_argument("--candidate-disks", required=True)
    p.add_argument("--cpus", required=True)
    p.add_argument("--revision", required=True)
    args = p.parse_args()
    volumes = [(args.reference.resolve(strict=True), args.reference_disks),
               (args.candidate.resolve(strict=True), args.candidate_disks)]
    if volumes[0][0].stat().st_dev == volumes[1][0].stat().st_dev:
        p.error("the two scratch directories need separate mounts")
    if set(args.reference_disks.split(",")) & set(args.candidate_disks.split(",")):
        p.error("the two panels need distinct physical devices")
    binary = args.binary.resolve(strict=True)
    metadata = dict(engine_revision=args.revision, cpus=args.cpus,
                    disks=[d for _, d in volumes], binary_sha256=hashlib.sha256(binary.read_bytes()).hexdigest())
    with gzip.open(args.output, "xt") as out:
        print(json.dumps(dict(metadata=metadata)), file=out, flush=True)
        for i in range(1, 6):
            seed = 20261126 + i
            cases = ("wal-sync", "sst-none-entropy")
            for case in cases if i % 2 else reversed(cases):
                arms = ("A2", "A1", "AB1", "B") if i % 2 else ("B", "AB1", "A1", "A2")
                for arm in arms:
                    path, disks = volumes[int(arm == "B")]
                    env = dict(os.environ, TMPDIR=str(path), MMDB_TRIAL_DISKS=disks)
                    trial = ["wal", "sync"] if case == "wal-sync" else ["sst", "none", "entropy"]
                    cmd = ["taskset", "-c", args.cpus, str(binary), *trial, str(seed)]
                    raw = subprocess.check_output(cmd, text=True, env=env)
                    data = observations(raw)
                    if case == "wal-sync" and data["verified"]["rows"] != 10_000:
                        raise ValueError("incomplete WAL verification")
                    if case == "sst-none-entropy" and data["sst"]["rows"] != 262144:
                        raise ValueError("incomplete SST verification")
                    print(json.dumps(dict(round=i, seed=seed, arm=arm, case=case,
                                          volume="candidate" if arm == "B" else "reference", **data)),
                          file=out, flush=True)
                    print(i, case, arm, flush=True)


if __name__ == "__main__":
    main()
