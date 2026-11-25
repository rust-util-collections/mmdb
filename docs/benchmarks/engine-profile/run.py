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
import shutil
import subprocess

from paced_load import load_kept

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument("suite", choices=["memory", "mixed", "handoff", "reopen"])
parser.add_argument("binary", type=Path)
parser.add_argument("output", type=Path)
parser.add_argument("--handoff-rates", nargs=2, type=int, metavar=("WAL_ASYNC", "WAL_SYNC"),
                    help="reuse the high-entropy rates from the mixed control calibration")
parser.add_argument("--candidate", type=Path, help="handoff candidate compiled with the same driver")
parser.add_argument("--metrics", type=Path, help="instrumented copy of the control")
parser.add_argument("--cpus", required=True, help="Linux CPU list passed to taskset")
parser.add_argument("--tmpdir", type=Path, required=True, help="filesystem for fresh database directories")
parser.add_argument("--ops", type=int, default=10_000, help="requests per primary foreground stream")
parser.add_argument("--rounds", type=int, default=5)
parser.add_argument("--seed", type=int, default=20261125)
parser.add_argument("--saturation-seconds", type=int, default=8,
                    help="common closed-loop measurement window with every stream active")
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


def calibrate(out, mode, case, fixed):
    # A finite trace gives an initial rate estimate. Confirm the sustainable
    # boundary at the actual request mix with all arrivals on the shared clock.
    guess = trial(out, mode, "initial-rate-" + case, "finite-trace", fixed)
    estimate = max(1, int(guess["classes"]["SmallPut"]["ops_s"]))
    low, high = 0, None
    for _ in range(8):
        test = trial(out, mode, "paced-capacity-" + case, str(estimate), {**fixed, "RATE": estimate})
        sustained = test["classes"]["SmallPut"]["ops_s"] >= 0.98 * estimate and load_kept(test)
        if sustained:
            low = estimate
        else:
            high = estimate
        if low and high and high <= low * 1.2:
            break
        if not low and estimate == 1:
            raise RuntimeError("could not sustain one scheduled foreground request per second")
        if high is None:
            estimate = max(low + 1, int(low * 1.25))
        elif not low:
            estimate = max(1, int(high * 0.75))
        else:
            estimate = (low + high) // 2
    if not low:
        raise RuntimeError("repeat calibration using a lower starting rate")
    chosen = max(1, int(0.70 * low))
    print("paced", case, "rate", chosen, "control sustained", low, "next failing", high, flush=True)
    return chosen


def memory(out):
    # First vary queue depth at fixed buffer size, then hold the nominal
    # (1+depth)*buffer envelope at 30 MiB per member with integer MiB buffers.
    sweeps = {"buffer": [(4, 16), (1, 16), (2, 16)],
              "envelope": [(4, 6), (1, 15), (2, 10)]}
    controls = {}
    for sweep, variants in sweeps.items():
        if sweep == "envelope":
            assert {(q + 1) * b for q, b in variants} == {30}
        q, b = variants[0]
        for placement in ["hot", "all"]:
            fixed = {"PLACEMENT": placement, "POOL_MIB": 256, "BATCH_ENTRIES": 32}
            base = {"BUFFER_MIB": b, "IMMUTABLES": q, **fixed}
            controls[sweep, placement] = fixed, calibrate(out, "memory", sweep + "-" + placement, base)
    for i in range(1, args.rounds + 1):
        # Reuse bracketing controls only within the same sweep, layout and load.
        blocks = list(itertools.product(controls, ["paced", "saturation"]))
        rng.shuffle(blocks)
        for (sweep, placement), load in blocks:
            fixed, offered = controls[sweep, placement]
            fixed = {**fixed, "RATE": offered if load == "paced" else 0,
                     "SECONDS": args.saturation_seconds if load == "saturation" else 0}
            case = f"{sweep}-{placement}-{load}"
            (cq, cb), *variants = sweeps[sweep]
            rng.shuffle(variants)
            arms = [("A1", cq, cb)] + [(f"q{q}-b{b}", q, b) for q, b in variants] + [("A2", cq, cb)]
            if i % 2 == 0:
                arms.reverse()
            for arm, q, b in arms:
                trial(out, "memory", case, arm, {**fixed, "BUFFER_MIB": b, "IMMUTABLES": q}, i)
        print("memory round", i, flush=True)
    for (sweep, placement), (fixed, offered) in controls.items():
        for q, b in sweeps[sweep]:
            trial(out, "memory", "gauge-" + sweep + "-" + placement, f"q{q}-b{b}",
                  {**fixed, "RATE": offered, "SAMPLE": "true", "BUFFER_MIB": b,
                   "IMMUTABLES": q, "SEED": args.seed + 1}, binary="metrics" if args.metrics else "control")


def mixed(out):
    kinds = list(itertools.product(["false", "true"], ["true", "false"]))
    rates = {}
    for sync,entropy in kinds:
        fixed = {"SYNC":sync,"ENTROPY":entropy}
        rates[sync,entropy] = calibrate(out, "mixed", f"sync-{sync}-entropy-{entropy}", fixed)
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
    for index, sync in enumerate(["false", "true"]):
        offered = (args.handoff_rates[index] if args.handoff_rates
                   else calibrate(out, "mixed", "handoff-" + sync, {"SYNC":sync}))
        if offered < 1:
            raise ValueError("handoff rate must be positive")
        for i in range(1,args.rounds+1):
            arms = [("A1","control"),("B","candidate"),("A2","control")]
            if i%2==0:
                arms.reverse()
            for arm,binary in arms:
                trial(out,"mixed","same-db-"+sync,arm,{"SYNC":sync,"RATE":offered},i,binary)
            for control in ["writer", "reader"]:
                for arm, binary in arms:
                    trial(out, "mixed", control + "-" + sync, arm, {"SYNC":sync, "CONTROL":control}, i, binary)
                    trial(out, "mixed", control + "-paced-" + sync, arm,
                          {"SYNC":sync, "CONTROL":control, "RATE":offered}, i, binary)


def reopen(out):
    for size in [10_000, 100_000]:
        for i in range(1, args.rounds + 1):
            profiles = [(4,16), (2,16), (1,16), (1,15), (2,10), (4,6)]
            rng.shuffle(profiles)
            for q, b in profiles:
                trial(out, "reopen", "durable-residual-wal", f"q{q}-b{b}",
                      {"OPS":size, "BUFFER_MIB":b, "IMMUTABLES":q, "PREPARE_MIB":64}, i)
            trial(out, "reopen", "read-only", str(size),
                  {"OPS":size, "BUFFER_MIB":6, "PREPARE_MIB":64, "READ_ONLY":"true"}, i)


args.tmpdir.mkdir(parents=True,exist_ok=True)
with gzip.open(args.output,"xt") as output:
    fs = subprocess.check_output(["findmnt","-T",str(args.tmpdir),"-no","SOURCE,FSTYPE,OPTIONS"],text=True).strip()
    meta = {"suite":args.suite,"cpus":args.cpus,"platform":platform.platform(),
            "fs":fs,"binary_sha256":{k:hashlib.sha256(p.read_bytes()).hexdigest() for k,p in binaries.items()}}
    meta["ticks_per_second"] = os.sysconf("SC_CLK_TCK")
    meta["saturation_seconds"] = args.saturation_seconds
    meta["operations"] = args.ops
    meta["rounds"] = args.rounds
    meta["seed"] = args.seed
    if shutil.which("btrfs") and "btrfs" in fs:
        meta["directory_compression"] = subprocess.check_output(
            ["btrfs", "property", "get", str(args.tmpdir), "compression"], text=True).strip()
    output.write(json.dumps({"metadata":meta})+"\n")
    {"memory":memory,"mixed":mixed,"handoff":handoff,"reopen":reopen}[args.suite](output)
