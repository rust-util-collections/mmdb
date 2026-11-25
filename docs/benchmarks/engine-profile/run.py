"""Run deterministic release-build MMDB engine profiles in fresh Linux processes."""
import argparse
import gzip
import hashlib
import itertools
import json
import os
from pathlib import Path
import platform
import random
import re
import subprocess

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument("suite", choices=["memory", "mixed", "handoff", "reopen"])
parser.add_argument("binary", type=Path)
parser.add_argument("output", type=Path)
parser.add_argument("--candidate", type=Path, help="handoff candidate compiled with the same driver")
parser.add_argument("--metrics", type=Path, help="instrumented copy of the control")
parser.add_argument("--cpus", required=True, help="Linux CPU list passed to taskset")
parser.add_argument("--tmpdir", type=Path, required=True, help="filesystem for fresh database directories")
parser.add_argument("--ops", type=int, default=10_000, help="requests per primary foreground stream")
parser.add_argument("--rounds", type=int, default=5)
parser.add_argument("--seed", type=int, default=20261125)
args = parser.parse_args()
if args.rounds < 1 or args.ops < 1:
    parser.error("ops and rounds must be positive")
if args.suite == "handoff" and args.candidate is None:
    parser.error("handoff requires --candidate")

binaries = {"control": args.binary.resolve()}
if args.candidate:
    binaries["candidate"] = args.candidate.resolve()
if args.metrics:
    binaries["metrics"] = args.metrics.resolve()
rng = random.Random(args.seed)


def number(value):
    try:
        return int(value)
    except ValueError:
        try:
            return float(value)
        except ValueError:
            return value


def counters(line):
    return {key: number(value) for key, value in re.findall(r"(\w+)[:=] *(\w+(?:\.\d+)?)", line)}


def trial(out, mode, case, arm, settings, round_no=0, binary="control"):
    env = {k: v for k, v in os.environ.items() if not k.startswith("MMDB_PROFILE_")}
    params = {"OPS": args.ops, "SEED": args.seed + round_no, **settings}
    env.update({"MMDB_PROFILE_" + k: str(v) for k, v in params.items()})
    env["TMPDIR"] = str(args.tmpdir.resolve())
    cmd = ["taskset", "-c", args.cpus, str(binaries[binary]), mode]
    result = subprocess.run(cmd, env=env, text=True, capture_output=True)
    if result.returncode:
        raise RuntimeError(result.stdout + result.stderr)
    record = {"mode": mode, "case": case, "arm": arm, "binary": binary,
              "round": round_no, "settings": params, "classes": {}, "stages": []}
    for line in result.stdout.splitlines():
        label = line.split(" ", 1)[0]
        data = counters(line)
        if label == "class":
            record["classes"][data["op"]] = data
        elif label == "stage":
            record["stages"].append(data)
        else:
            record[label] = data
    out.write(json.dumps(record) + "\n")
    out.flush()
    return record


def rate(record):
    # All foreground streams in the fixed trace finish equal request counts
    # over one common makespan; the calibrated rate is per scalar stream.
    return max(1, int(0.7 * record["classes"]["SmallPut"]["ops_s"]))


def memory(out):
    # First vary queue depth at fixed buffer size, then hold the nominal
    # (1+depth)*buffer envelope at 30 MiB per member with integer MiB buffers.
    variants = [(4,16), (1,16), (2,16), (1,15), (2,10), (4,6)]
    controls = {}
    for placement in ["hot", "all"]:
        fixed = {"PLACEMENT": placement, "POOL_MIB": 256, "BATCH_ENTRIES": 32}
        base = {"BUFFER_MIB": 16, "IMMUTABLES": 4, **fixed}
        controls[placement] = (fixed, rate(trial(out, "memory", "calibrate", "q4-b16", base)))
    for i in range(1, args.rounds + 1):
        # One A/A pair per placement/load block. Reuse the bracketing control
        # as the paired reference for all candidate sizes in that block.
        blocks = list(itertools.product(controls, ["paced","saturation"]))
        rng.shuffle(blocks)
        for placement, load in blocks:
            fixed, offered = controls[placement]
            fixed = {**fixed, "RATE": offered if load == "paced" else 0}
            case = f"{placement}-{load}"
            variants_in_order = variants[1:]
            rng.shuffle(variants_in_order)
            arms = [("A1",4,16)] + [(f"q{q}-b{b}",q,b) for q,b in variants_in_order] + [("A2",4,16)]
            if i % 2 == 0:
                arms.reverse()
            for arm,q,b in arms:
                trial(out, "memory", case, arm, {**fixed,"BUFFER_MIB":b,"IMMUTABLES":q}, i)
        print("memory round",i,flush=True)
    for placement,(fixed,offered) in controls.items():
        trial(out,"memory","gauge-sample","q4-b16",{**fixed,"RATE":offered,"SAMPLE":"true"})


def mixed(out):
    kinds = list(itertools.product(["false","true"],["false","true"]))
    rates = {}
    for sync,entropy in kinds:
        fixed = {"SYNC":sync,"ENTROPY":entropy}
        rates[sync,entropy] = rate(trial(out,"mixed","calibrate","hot",fixed))
    for i in range(1,args.rounds+1):
        rng.shuffle(kinds)
        for sync,entropy in kinds:
            fixed = {"SYNC":sync,"ENTROPY":entropy,"RATE":rates[sync,entropy]}
            case = f"sync-{sync}-entropy-{entropy}"
            arms = [("A1","hot","control"),("split","split","control"),("A2","hot","control")]
            if args.metrics and i == 1:
                arms.append(("stage-timing","hot","metrics"))
            if i%2==0:
                arms.reverse()
            for arm,placement,binary in arms:
                trial(out,"mixed",case,arm,{**fixed,"PLACEMENT":placement},i,binary)
        print("mixed round",i,flush=True)
    for control in ["writer","reader"]:
        trial(out,"mixed","single-stream",control,{"CONTROL":control})


def handoff(out):
    # Primary: incompressible, WalAsync, one DB. Separately check controls.
    for sync in ["false","true"]:
        offered = rate(trial(out,"mixed","calibrate",sync,{"SYNC":sync}))
        for i in range(1,args.rounds+1):
            arms = [("A1","control"),("B","candidate"),("A2","control")]
            if i%2==0:
                arms.reverse()
            for arm,binary in arms:
                trial(out,"mixed","same-db-"+sync,arm,{"SYNC":sync,"RATE":offered},i,binary)
            for control in ["writer","reader"]:
                for arm,binary in arms:
                    trial(out,"mixed",control+"-"+sync,arm,{"SYNC":sync,"CONTROL":control},i,binary)


def reopen(out):
    for size in [10_000,100_000]:
        for i in range(1,args.rounds+1):
            trial(out,"reopen","durable-residual-wal",str(size),{"OPS":size,"BUFFER_MIB":64},i)


args.tmpdir.mkdir(parents=True,exist_ok=True)
with gzip.open(args.output,"xt") as output:
    fs = subprocess.check_output(["findmnt","-T",str(args.tmpdir),"-no","SOURCE,FSTYPE,OPTIONS"],text=True).strip()
    meta = {"suite":args.suite,"cpus":args.cpus,"platform":platform.platform(),
            "fs":fs,"binary_sha256":{k:hashlib.sha256(p.read_bytes()).hexdigest() for k,p in binaries.items()}}
    output.write(json.dumps({"metadata":meta})+"\n")
    {"memory":memory,"mixed":mixed,"handoff":handoff,"reopen":reopen}[args.suite](output)
