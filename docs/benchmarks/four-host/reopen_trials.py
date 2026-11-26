"""Measure and verify a fixed residual WAL with both open modes."""
import argparse
import gzip
import hashlib
import json
import os
from pathlib import Path
import subprocess


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("binary", type=Path)
    parser.add_argument("output", type=Path)
    parser.add_argument("--scratch", type=Path, required=True)
    parser.add_argument("--cpus", required=True)
    parser.add_argument("--revision", required=True)
    args = parser.parse_args()
    binary = args.binary.resolve(strict=True)
    env = {k: v for k, v in os.environ.items() if not k.startswith("MMDB_PROFILE_")}
    env["TMPDIR"] = str(args.scratch.resolve(strict=True))
    cmd = ([] if args.cpus == "native" else ["taskset", "-c", args.cpus]) + [str(binary), "reopen"]
    header = dict(engine_revision=args.revision, binary_sha256=hashlib.sha256(binary.read_bytes()).hexdigest(),
                  cpu_affinity=args.cpus)
    with gzip.open(args.output, "xt") as f:
        print(json.dumps(dict(metadata=header)), file=f, flush=True)
        for i in range(1, 6):
            for ro in ((False, True) if i % 2 else (True, False)):
                settings = dict(OPS=100000, PREPARE_MIB=64, BUFFER_MIB=6, IMMUTABLES=4,
                                POOL_MIB=64, SEED=20261126+i, READ_ONLY=str(ro).lower())
                params = {"MMDB_PROFILE_" + k: str(v) for k, v in settings.items()}
                raw = subprocess.check_output(cmd, env={**env, **params}, text=True)
                print(json.dumps(dict(round=i, read_only=ro, settings=settings, raw=raw)), file=f, flush=True)


if __name__ == "__main__":
    main()
