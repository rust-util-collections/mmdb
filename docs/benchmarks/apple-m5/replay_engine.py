"""Replay the native paired engine trials in their recorded process order."""
import argparse
import gzip
import hashlib
import json
import os
from pathlib import Path
import platform
import subprocess


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("packet", type=Path)
    parser.add_argument("output", type=Path)
    parser.add_argument("--tmpdir", type=Path, required=True)
    parser.add_argument("--binary", action="append", nargs=2, metavar=("ROLE", "PATH"),
                        required=True, help="map each recorded binary role to a local executable")
    parser.add_argument("--case", help="replay only the named case")
    args = parser.parse_args()
    roles = {role: Path(path).resolve() for role, path in args.binary}
    args.tmpdir.mkdir(parents=True, exist_ok=True)
    with gzip.open(args.packet, "rt") as packet:
        header = json.loads(next(packet))["metadata"]
        rows = [json.loads(line) for line in packet]
    if args.case:
        rows = [row for row in rows if row["case"] == args.case]
    if not rows:
        parser.error("no trials match that case")
    missing = {row["binary"] for row in rows} - roles.keys()
    if missing:
        parser.error("missing --binary for " + ", ".join(sorted(missing)))
    metadata = {
        "original_binary_sha256": header["binary_sha256"],
        "binary_sha256": {role: hashlib.sha256(path.read_bytes()).hexdigest() for role, path in roles.items()},
        "platform": platform.platform(),
        "scheduler": "native",
        "tmpdir": str(args.tmpdir.resolve()),
        "filesystem": subprocess.check_output(["df", "-P", str(args.tmpdir)], text=True).strip(),
    }
    with gzip.open(args.output, "xt") as output:
        print(json.dumps({"metadata": metadata}), file=output)
        for row in rows:
            env = {k: v for k, v in os.environ.items()
                   if not k.startswith(("MMDB_PROFILE_", "MMDB_L0_"))}
            env["TMPDIR"] = str(args.tmpdir.resolve())
            prefix = "MMDB_L0_" if row["mode"] in ("gap", "native", "sentinel") else "MMDB_PROFILE_"
            env.update({prefix + key: str(value) for key, value in row["settings"].items()})
            trial = subprocess.run([str(roles[row["binary"]]), row["mode"]], env=env,
                                   check=True, text=True, stdout=subprocess.PIPE)
            result = {key: row[key] for key in ("case", "arm", "round", "binary", "mode", "settings")}
            print(json.dumps({**result, "raw": trial.stdout}), file=output, flush=True)


if __name__ == "__main__":
    main()
