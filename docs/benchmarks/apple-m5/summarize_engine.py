"""Summarize five independent native A/A and A/B pairs from raw benchmark output."""
import argparse
from collections import defaultdict
import csv
import gzip
import json
from pathlib import Path
from statistics import median
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "engine-profile"))
from paced_load import load_kept


def counters(line):
    values = {}
    for token in line.split():
        key, sep, value = token.partition("=")
        if sep:
            try:
                values[key] = int(value)
            except ValueError:
                try:
                    values[key] = float(value)
                except ValueError:
                    values[key] = value
    return values


def observation(row):
    settings = row["settings"]
    if row["mode"] in ("gap", "native", "sentinel"):
        data = counters(row["raw"])
        if not (data["write_ops"] == data["read_ops"] == settings["OPS"]
                and data["l1_start"] == 24 and data["multi_target_jobs"] > 0
                and 1 <= data["peak_ranges"] <= settings["SUBS"]
                and (data["parallel_jobs"] > 0) == (data["peak_ranges"] > 1) == (settings["SUBS"] > 1)):
            raise ValueError("L0 input, completion or concurrency check failed")
        result = {
            "stall_ms_per_GiB": data["l0_stall_ns"] / 1e6 / (data["user_bytes"] / 2**30),
            "read_p99_us": data["read_arrival_p99_ns"] / 1000,
            "write_p99_us": data["write_arrival_p99_ns"] / 1000,
        }
        for key in ("initial_cold_consumed_bytes", "output_file_bytes", "peak_ranges",
                    "l0_end", "rewrite_jobs", "logical_cold_emitted_bytes"):
            if isinstance(data[key], int):
                result[key] = data[key]
        return result, None

    lines = row["raw"].splitlines()
    run = counters(next(line for line in lines if line.startswith("run ")))
    classes = {}
    for line in lines:
        if line.startswith("class "):
            data = counters(line)
            if not (data["offered"] == data["completed"] > 0 and data["rejected"] == 0):
                raise ValueError("incomplete request stream")
            classes[data["op"]] = data
    required = {"SmallPut", "Read", "Batch"}
    if row["mode"] == "mixed":
        required.update(("FreshSnapshot", "HeldSnapshot"))
    if set(classes) != required or min(run["seconds"], run["peak_kib"], run["rss_after_preload_kib"]) <= 0:
        raise ValueError("missing stream, positive RSS or timed duration")
    memory = row["mode"] == "memory"
    entries = settings.get("BATCH_ENTRIES", 64 if memory else 4096)
    batch = classes["Batch"]
    put = classes["SmallPut"]
    if (batch["bytes"] != batch["completed"] * entries * (16 + 256)
            or put["bytes"] != put["completed"] * (16 + 128)
            or any(data["bytes"] for op, data in classes.items() if op not in ("Batch", "SmallPut"))):
        raise ValueError("logical bytes differ from the completed writes")
    issuers = 16 if memory else 1
    if (batch["streams"] != issuers or any(data["streams"] != 1 for op, data in classes.items() if op != "Batch")
            or memory and (run["dbs"] != 16 or entries != 32)):
        raise ValueError("unexpected stream, DB or batch-entry count")
    if not settings.get("SECONDS", 0):
        ops = settings["OPS"]
        ratio = settings.get("BATCH_RATIO", 128)
        expected_batches = issuers * ops if memory else (ops + ratio - 1) // ratio
        if (batch["completed"] != expected_batches
                or any(data["completed"] != ops for op, data in classes.items() if op != "Batch")):
            raise ValueError("fixed-count trace did not complete every requested operation")
    result = {}
    for op in ("SmallPut", "Read"):
        data = classes[op]
        result[op.lower() + "_p99_us"] = data["arrival_p99_us"] if data["sampled"] >= 10_000 else None
    logical_bytes = sum(c["bytes"] for c in classes.values())
    result.update({
        "peak_rss_MiB": run["peak_kib"] / 1024,
        "logical_MiB_s": logical_bytes / (run["seconds"] * 2**20),
        "compaction_output_MiB": run["compaction_output_bytes"] / 2**20,
        "compaction_output_per_logical_byte": run["compaction_output_bytes"] / logical_bytes,
        "final_l0": run["after_l0"],
        "final_immutables": run["after_immutables"],
    })
    return result, load_kept({**row, "classes": classes})


def summarize(packet, output):
    cases = defaultdict(lambda: defaultdict(dict))
    with gzip.open(packet, "rt") as source:
        json.loads(next(source))["metadata"]
        for line in source:
            row = json.loads(line)
            arm = row["arm"]
            if arm not in {"A1", "A2", "AB1", "B"}:
                continue
            group = cases[row["case"]][row["round"]]
            if arm in group:
                raise ValueError(f"duplicate {arm} in {row['case']} round {row['round']}")
            group[arm] = (row["settings"]["SEED"], *observation(row))
    if not cases:
        raise ValueError("no paired trials in packet")
    output.writerow(("case", "metric", "n_pairs", "ab_reference", "candidate", "gain_pct",
                     "max_AA_pct", "pairs_favoring_candidate", "kept_pairs", "min_value", "max_value"))
    for case, rounds in sorted(cases.items()):
        if len(rounds) != 5:
            raise ValueError(f"expected five rounds for {case}")
        for group in rounds.values():
            if set(group) != {"A1", "A2", "AB1", "B"} or len({v[0] for v in group.values()}) != 1:
                raise ValueError(f"incomplete or mismatched pairs in {case}")
        for metric in next(iter(rounds.values()))["B"][1]:
            values = [{arm: trial[1][metric] for arm, trial in group.items()} for group in rounds.values()]
            if any(None in v.values() for v in values):
                continue
            # Concurrency and ending debt describe the work performed in an arm.
            diagnostic = metric in ("peak_ranges", "l0_end", "rewrite_jobs", "final_l0", "final_immutables")
            direction = 1 if metric == "logical_MiB_s" else -1
            gains = [direction * 100 * (v["B"] / v["AB1"] - 1) for v in values if v["AB1"] and not diagnostic]
            noise = [100 * abs(v["A2"] / v["A1"] - 1) for v in values if v["A1"]]
            kept = [all(v[2] for v in group.values()) for group in rounds.values()
                    if group["B"][2] is not None]
            output.writerow([case, metric, len(values), round(median(v["AB1"] for v in values), 3),
                             round(median(v["B"] for v in values), 3),
                             round(median(gains), 2) if len(gains) == len(values) else "",
                             round(max(noise), 2) if len(noise) == len(values) else "",
                             sum(gain > 0 for gain in gains) if len(gains) == len(values) else "",
                             sum(kept) if kept else "",
                             round(min(min(v.values()) for v in values), 3),
                             round(max(max(v.values()) for v in values), 3)])


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("packet", type=Path)
    summarize(parser.parse_args().packet, csv.writer(sys.stdout, lineterminator="\n"))
