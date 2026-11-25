"""Validate complete engine_profile trials and summarize paired outcomes as CSV."""
import argparse
from collections import defaultdict
import csv
import gzip
import json
from statistics import median
import sys


MEMORY_ARMS = {"buffer": [(4, 16), (1, 16), (2, 16)], "envelope": [(4, 6), (1, 15), (2, 10)]}


def require(condition, message):
    if not condition:
        raise ValueError(message)


def expected_pairs(suite):
    if suite == "handoff":
        return {f"{case}-{sync}": {"B"} for case in ("same-db", "writer", "reader", "writer-paced", "reader-paced")
                for sync in ("false", "true")}
    if suite == "mixed":
        return {f"sync-{sync}-entropy-{entropy}": {"split"}
                for sync in ("false", "true") for entropy in ("false", "true")}
    if suite == "memory":
        return {f"{sweep}-{placement}-{load}": {f"q{q}-b{b}" for q, b in configs[1:]}
                for sweep, configs in MEMORY_ARMS.items() for placement in ("hot", "all")
                for load in ("paced", "saturation")}
    raise ValueError(f"unknown paired suite {suite!r}")


def sample_count(record, values):
    if "sampled" in values:
        return values["sampled"]
    # Earlier fixed-count profiles retained every successful latency.
    require(record["settings"].get("SECONDS", 0) == 0, "missing saturation reservoir size")
    return values["completed"]


def metrics(record):
    run = record["run"]
    classes = record["classes"]
    logical_bytes = sum(c["bytes"] for c in classes.values())
    data = {
        "peak_kib": run["peak_kib"],
        "user_bytes_s": logical_bytes / run["seconds"],
        "compaction_bytes": run["compaction_output_bytes"],
        "sst_output_per_logical_byte": run["compaction_output_bytes"] / logical_bytes if logical_bytes else None,
    }
    for op, c in classes.items():
        # A fixed-duration stream can complete more calls than its bounded
        # reservoir retains. The decision uses the actual percentile sample.
        data[op + "_p99_us"] = c["arrival_p99_us"] if sample_count(record, c) >= 10_000 else None
        data[op + "_ops_s"] = c["completed"] / run["seconds"]
        data[op + "_outstanding_half"] = c["outstanding_half"]
        data[op + "_outstanding_end"] = c["outstanding_end"]
    return data


def validate_record(record):
    require(record["run"]["seconds"] > 0, f"invalid duration in {record['case']}")
    require(record["run"]["peak_kib"] > 0, f"missing measured Linux peak RSS in {record['case']}")
    classes = record["classes"]
    control = record["settings"].get("CONTROL", "mixed")
    if control == "reader":
        expected = {"Read"}
    elif control == "writer":
        expected = {"SmallPut"}
    elif record["mode"] == "memory":
        expected = {"SmallPut", "Batch", "Read"}
    else:
        expected = {"SmallPut", "Batch", "Read", "FreshSnapshot", "HeldSnapshot"}
    require(set(classes) == expected, f"missing or unexpected classes in {record['case']}")
    for op, c in classes.items():
        require(c["offered"] == c["completed"] > 0 and c["rejected"] == 0
                and 0 < sample_count(record, c) <= c["completed"], f"incomplete {record['case']} {op}")


def load_kept(record):
    if not record["settings"].get("RATE", 0):
        return True
    for c in record["classes"].values():
        allowance = max(2, c["offered"] // 100)
        if c["outstanding_end"] > allowance or c["outstanding_end"] - c["outstanding_half"] > allowance:
            return False
    return True


def check_pair(control, other):
    case = control["case"]
    varying = set()
    if other["arm"] == "split":
        varying.add("PLACEMENT")
    if case.startswith(("buffer-", "envelope-")):
        varying.update(("IMMUTABLES", "BUFFER_MIB"))
        expected_control = MEMORY_ARMS[case.split("-", 1)[0]][0]
        for r in (control, other):
            actual = r["settings"]["IMMUTABLES"], r["settings"]["BUFFER_MIB"]
            if r["arm"] in ("A1", "A2"):
                require(actual == expected_control, f"unexpected control geometry {actual} in {case}")
            else:
                require(r["arm"] == f"q{actual[0]}-b{actual[1]}", f"mislabelled geometry in {case}")
    for key in (control["settings"].keys() | other["settings"].keys()) - varying:
        require(control["settings"].get(key) == other["settings"].get(key),
                f"mismatched {key} in {case} round {control['round']}")
    require(control["classes"].keys() == other["classes"].keys(), "mismatched measured classes")
    if control["case"].startswith("envelope-"):
        for r in (control, other):
            s = r["settings"]
            require((s["IMMUTABLES"] + 1) * s["BUFFER_MIB"] == 30, "unequal buffer envelope")


def guard(suite, case, label, values, gains, noise, qualified_load):
    if suite == "memory" and label == "peak_kib":
        return "within-1-GiB" if max(values) <= 1 << 20 else "over-1-GiB"
    primary = suite == "handoff" and case == "same-db-false" and label == "SmallPut_p99_us"
    control = ((suite == "memory" and
                ((case.endswith("-paced") and label == "SmallPut_p99_us") or
                 (case.endswith("-saturation") and label == "user_bytes_s"))) or
               (suite == "handoff" and
                ((case.startswith("writer-") and label in ("SmallPut_p99_us", "user_bytes_s")) or
                 (case.startswith("reader-") and label in ("Read_p99_us", "Read_ops_s")) or
                 (case.startswith("same-db-") and label in ("user_bytes_s", "sst_output_per_logical_byte")))))
    if not (primary or control):
        return ""
    if not qualified_load:
        return "offered-load-unqualified"
    if len(gains) < 5:
        return "fewer-than-five-pairs"
    gain = median(gains)
    if primary:
        return ("primary-met" if gain >= 15 and gain > noise and sum(g > 0 for g in gains[:5]) >= 4
                else "primary-unmet")
    if noise > 5:
        return "guard-inconclusive-AA"
    return "guard-met" if gain >= -5 else "guard-regression"


def summarize_reopen(records, rounds, writer):
    geometries = {g for members in MEMORY_ARMS.values() for g in members}
    expected = {(size, "writable", q, b) for size in (10_000, 100_000) for q, b in geometries}
    expected |= {(size, "read-only", 4, 6) for size in (10_000, 100_000)}
    cells = defaultdict(dict)
    wal_bytes = {}
    for r in records:
        settings = r["settings"]
        size = settings["OPS"]
        q, b = settings.get("IMMUTABLES", 4), settings["BUFFER_MIB"]
        readonly = str(settings.get("READ_ONLY", "false")).lower() == "true"
        role = "read-only" if readonly else "writable"
        expected_label = ("read-only", str(size)) if readonly else ("durable-residual-wal", f"q{q}-b{b}")
        require((r["case"], r["arm"]) == expected_label, "mismatched reopen arm")
        key = size, role, q, b
        iteration = r["round"]
        result = r["reopen"]
        require(iteration not in cells[key], f"duplicate reopen round {key} {iteration}")
        require(settings["PREPARE_MIB"] == 64 and result["rows"] == size and result["wal_bytes"] > 0,
                f"invalid recovery fixture {key}")
        require(result["open_peak_kib"] >= result["open_rss_kib"] > 0, f"invalid open-time memory {key}")
        cells[key][iteration] = result
        fixture = (size, iteration)
        require(wal_bytes.setdefault(fixture, result["wal_bytes"]) == result["wal_bytes"],
                f"unequal residual WAL lengths for {fixture}")
    require(cells.keys() == expected, f"missing or unexpected reopen geometries: {set(cells) ^ expected}")
    writer.writerow(["rows", "mode", "immutable_limit", "buffer_mib", "prepare_mib", "repetitions",
                     "open_us_median", "open_us_min", "open_us_max", "wal_bytes_median",
                     "open_rss_kib_median", "open_peak_kib_max", "after_verify_peak_kib_max"])
    for key, trials in sorted(cells.items()):
        require(set(trials) == set(rounds), f"incomplete reopen {key}: {set(trials)}")
        results = trials.values()
        times = [r["open_us"] for r in results]
        writer.writerow([*key, 64, len(results), median(times), min(times), max(times),
                         median(r["wal_bytes"] for r in results), median(r["open_rss_kib"] for r in results),
                         max(r["open_peak_kib"] for r in results),
                         max(r["after_verify_peak_kib"] for r in results)])


def summarize(meta, records, writer):
    suite = meta["suite"]
    rounds = range(1, meta["rounds"] + 1)
    if suite == "reopen":
        summarize_reopen(records, rounds, writer)
        return
    groups = defaultdict(lambda: defaultdict(dict))
    for record in records:
        validate_record(record)
        if record["round"]:
            group = groups[record["case"]][record["round"]]
            require(record["arm"] not in group, f"duplicate {record['case']} round {record['round']}")
            group[record["arm"]] = record
    if suite == "mixed":
        controls = [r["arm"] for r in records if r["case"] == "single-stream" and r["round"] == 0]
        require(sorted(controls) == ["reader", "writer"], "missing or duplicate single-stream controls")
    if suite == "memory":
        required_gauges = {(f"gauge-{sweep}-{placement}", f"q{q}-b{b}")
                           for sweep, configs in MEMORY_ARMS.items()
                           for placement in ("hot", "all") for q, b in configs}
        gauges = [r for r in records if r["case"].startswith("gauge-")]
        require(len(gauges) == len(required_gauges)
                and {(r["case"], r["arm"]) for r in gauges} == required_gauges
                and all(r.get("sampled_peaks") for r in gauges), "incomplete memory gauge sweep")
    expected = expected_pairs(suite)
    require(groups.keys() == expected.keys(), f"missing or unexpected cases: {set(groups) ^ set(expected)}")
    writer.writerow(["case", "arm", "metric", "pairs", "control_median", "arm_median", "paired_gain_pct",
                     "aa_max_abs_pct", "improved_pairs", "gain_min_pct", "gain_max_pct", "arm_min",
                     "arm_max", "eligible_pairs", "load_kept_pairs", "status", "reference_max",
                     "reference_rss_status", "arm_rss_status", "matched_rss_budget"])
    for case, arms in sorted(expected.items()):
        trials = groups[case]
        require(set(trials) == set(rounds), f"incomplete rounds in {case}")
        for i, trial in trials.items():
            wanted = {"A1", "A2"} | arms
            if suite == "mixed" and i == 1 and "metrics" in meta["binary_sha256"]:
                wanted.add("stage-timing")
            require(set(trial) == wanted, f"{case} round {i}, missing or unexpected arms: {set(trial) ^ wanted}")
        for arm in sorted(arms):
            observed = []
            load_ok = []
            for i in rounds:
                a, b, a2 = (trials[i][key] for key in ("A1", arm, "A2"))
                check_pair(a, b)
                check_pair(a, a2)
                observed.append(tuple(metrics(r) for r in (a, b, a2)))
                load_ok.append(all(load_kept(r) for r in (a, b, a2)))
            for key in observed[0][0]:
                triples = [(a[key], b[key], a2[key]) for a, b, a2 in observed]
                eligible = sum(all(x is not None for x in row) for row in triples)
                columns = [""] * 9
                reference_max = reference_budget = candidate_budget = matched_budget = ""
                if eligible < len(rounds):
                    status = "insufficient-p99-samples" if key.endswith("_p99_us") else "undefined-denominator"
                else:
                    baseline, values, duplicates = zip(*triples)
                    reference_max = round(max(max(baseline), max(duplicates)), 3)
                    if suite == "memory" and key == "peak_kib":
                        budget_kib = 1 << 20
                        reference_budget = "within" if reference_max <= budget_kib else "over"
                        candidate_budget = "within" if max(values) <= budget_kib else "over"
                        matched_budget = "both-within" if reference_budget == candidate_budget == "within" else "limited"
                    columns[:2] = [round(median(baseline), 3), round(median(values), 3)]
                    columns[7:9] = [round(min(values), 3), round(max(values), 3)]
                    if any(x == 0 for x in baseline):
                        status = "zero-reference"
                    else:
                        sign = 1 if key.endswith(("_bytes_s", "_ops_s")) else -1
                        gains = [sign * 100 * (b-a)/a for a, b in zip(baseline, values)]
                        noise = max(100 * abs(a2-a)/a for a, a2 in zip(baseline, duplicates))
                        columns[2:7] = [round(median(gains), 3), round(noise, 3), sum(g > 0 for g in gains),
                                        round(min(gains), 3), round(max(gains), 3)]
                        status = guard(suite, case, key, values, gains, noise, all(load_ok))
                writer.writerow([case, arm, key, len(rounds), *columns, eligible, sum(load_ok), status, reference_max,
                                 reference_budget, candidate_budget, matched_budget])


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("trials")
    args = parser.parse_args()
    with gzip.open(args.trials, "rt") as source:
        metadata = json.loads(next(source))["metadata"]
        summarize(metadata, [json.loads(line) for line in source], csv.writer(sys.stdout, lineterminator="\n"))
