"""Check and summarize the six matched rounds in the durable-write packet."""

import argparse
import csv
import gzip
import json
import math
import re
import statistics
import sys
from pathlib import Path


ARMS = {"aa1": "control", "aa2": "control", "ab1": "control", "ab2": "candidate"}
SEEDS = range(2110, 2116)
P99_CLASSES = ("SmallPut", "FreshSnapshot", "HeldSnapshot", "Read")
METRICS = ("arrival_p99_us", "service_p99_us")


def require(condition, message):
    if not condition:
        raise ValueError(message)


def rows(output, tag):
    return [dict(re.findall(r"(\w+)=([^\s]+)", line)) for line in output.splitlines()
            if line.startswith(tag + " ")]


def primary(record):
    settings = record["settings"]
    arm = record["label"].removeprefix("primary-")
    seed = settings["SEED"]
    location = f"seed {seed}, {arm}"
    require(seed in SEEDS and ARMS.get(arm) == record["binary"], f"unexpected arm at {location}")
    output = record["output"]
    config, = rows(output, "config")
    run, = rows(output, "run")
    expected = dict(mode="mixed", ops="10000", buffer_mib="4", depth="2", sync="true",
                    seconds="0", rate=str(settings["RATE"]), seed=str(seed))
    require(all(config[k] == v for k, v in expected.items()), f"wrong configuration at {location}")
    require(settings["RATE"] > 0 and settings["CONTROL"] == "mixed"
            and settings["PLACEMENT"] == run["placement"] == "hot"
            and settings["BUFFER_MIB"] == 4 and settings["IMMUTABLES"] == 2
            and settings["OPS"] == 10000 and settings["SYNC"] == "true"
            and run["instrumented"] == "false", f"wrong primary workload at {location}")
    classes = rows(output, "class")
    require(sorted(row["op"] for row in classes) == sorted((*P99_CLASSES, "Batch")),
            f"missing or duplicate operation class at {location}")
    result = {}
    for row in classes:
        op = row["op"]
        count = 79 if op == "Batch" else 10000
        bytes_per_call = {"SmallPut": 144, "Batch": 4096 * 272}.get(op, 0)
        require(all(int(row[k]) == count for k in ("offered", "completed", "sampled"))
                and int(row["rejected"]) == 0 and int(row["bytes"]) == count * bytes_per_call,
                f"incomplete or mismatched {op} at {location}")
        if op in P99_CLASSES:
            require(row["p99_qualified"] == "true", f"unqualified {op} p99 at {location}")
            for metric in METRICS:
                value = float(row[metric])
                require(math.isfinite(value) and value > 0, f"invalid {op} {metric} at {location}")
                result[op, metric] = value
    workload = ({k: v for k, v in settings.items() if k != "SEED"},
                {k: v for k, v in config.items() if k != "seed"})
    return seed, arm, workload, result


def diagnostics(record):
    output = record["output"]
    stages = {}
    for line in output.splitlines():
        if line.startswith("stage "):
            op = rows(line, "stage")[0]["leader_stream"]
            counters = dict(re.findall(r"(\w+): (\d+)", line))
            totals = stages.setdefault(op, [0, 0, 0, 0])
            for i, k in enumerate(("freezes", "freeze_ns", "wal_seal_syncs", "wal_seal_sync_ns")):
                totals[i] += int(counters[k])
    trace = rows(output, "seal_trace")
    traced = record["settings"].get("SEAL_TRACE") == "true"
    require(not traced or stages, f"missing thread profiles in {record['label']}")
    require(all(row["leader_stream"] in stages for row in trace), "rotation missing its profile")
    for op, (freezes, freeze_ns, attempts, seal_ns) in stages.items():
        events = [row for row in trace if row["leader_stream"] == op]
        if traced:
            require((len(events), sum(int(r["freeze_ns"]) for r in events),
                     sum(int(r["seal_calls"]) for r in events), sum(int(r["seal_ns"]) for r in events))
                    == (freezes, freeze_ns, attempts, seal_ns),
                    f"rotation trace disagrees with {op} counters in {record['label']}")
        if op in ("SmallPut", "Batch") or freezes:
            service = sum(int(row["service_ns"]) for row in events) if traced else ""
            peak_seal = max((int(r["seal_ns"]) for r in events), default="")
            yield record["label"], op, freezes, freeze_ns, attempts, seal_ns, service, peak_seal


def summarize(records):
    arms = {}
    hashes = {}
    common_settings = None
    diagnostic_rows = []
    for record in records:
        require(record["returncode"] == 0, f"unsuccessful run {record['label']}")
        digest = record["sha256"]
        binary = record["binary"]
        require(re.fullmatch("[0-9a-f]{64}", digest) and hashes.setdefault(binary, digest) == digest,
                f"unstable or invalid fingerprint for {binary}")
        if record["label"].startswith("primary-"):
            seed, arm, settings, data = primary(record)
            if common_settings is None:
                common_settings = settings
            require(common_settings == settings, f"mismatched settings at {seed}, {arm}")
            require((seed, arm) not in arms, f"duplicate arm at {seed}, {arm}")
            arms[seed, arm] = data
        else:
            diagnostic_rows.extend(diagnostics(record))
    require(set(arms) == {(seed, arm) for seed in SEEDS for arm in ARMS},
            f"expected 24 primary runs, found {len(arms)}")
    require(hashes["control"] != hashes["candidate"], "control and candidate share a fingerprint")

    writer = csv.writer(sys.stdout, lineterminator="\n")
    writer.writerow(("op", "metric", "seed", *ARMS))
    for op in P99_CLASSES:
        for metric in METRICS:
            for seed in SEEDS:
                writer.writerow((op, metric, seed, *(arms[seed, a][op, metric] for a in ARMS)))

    writer.writerow(("op", "metric", "aa_max_abs_fraction", "ab_geomean", "ab_lower_one_sided_95",
                     "ab_upper_one_sided_95", "favorable_pairs"))
    # One-sided Student t critical value, df = six independent seed pairs minus one.
    critical = 2.0150483733330233
    for op in P99_CLASSES:
        for metric in METRICS:
            aa = [arms[s, "aa2"][op, metric] / arms[s, "aa1"][op, metric] for s in SEEDS]
            log_ab = [math.log(arms[s, "ab2"][op, metric] / arms[s, "ab1"][op, metric])
                      for s in SEEDS]
            mean = statistics.mean(log_ab)
            margin = critical * statistics.stdev(log_ab) / math.sqrt(len(log_ab))
            values = (max(abs(r - 1) for r in aa), math.exp(mean),
                      math.exp(mean - margin), math.exp(mean + margin))
            writer.writerow((op, metric, *(f"{v:.6f}" for v in values), sum(x < 0 for x in log_ab)))

    writer.writerow(("diagnostic", "leader_stream", "freezes", "freeze_ns", "seal_wrapper_attempts",
                     "seal_wrapper_ns", "traced_rotating_calls_service_ns", "max_traced_seal_ns"))
    writer.writerows(diagnostic_rows)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("packet", type=Path, nargs="?", default=Path(__file__).with_name("runs.jsonl.gz"))
    args = parser.parse_args()
    opener = gzip.open if args.packet.suffix == ".gz" else open
    with opener(args.packet, "rt", encoding="utf-8") as source:
        summarize(json.loads(line) for line in source)
