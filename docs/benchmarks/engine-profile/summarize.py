"""Summarize paired engine_profile JSONL files and report the A/A noise bands."""
import argparse
from collections import defaultdict
import csv
import gzip
import json
from statistics import median
import sys

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument("trials")
args = parser.parse_args()
with gzip.open(args.trials, "rt") as source:
    meta = json.loads(next(source))["metadata"]
    records = [json.loads(line) for line in source]
if meta["suite"] == "reopen":
    times = defaultdict(list)
    for row in records:
        times[row["settings"]["OPS"]].append(row["reopen"]["open_us"])
    for n, values in times.items():
        print(f"rows={n} median_us={median(values):.0f} range={min(values)}..{max(values)}")
    sys.exit()

groups = defaultdict(lambda: defaultdict(dict))
for record in records:
    for op, values in record["classes"].items():
        assert values["offered"] == values["completed"] and values["rejected"] == 0, (record["case"], op)
    if record["round"]:
        group = groups[record["case"]][record["round"]]
        assert record["arm"] not in group
        group[record["arm"]] = record


def metrics(row):
    classes = row["classes"]
    output = {"peak_kib": row["run"]["peak_kib"],
              "user_bytes_s": sum(v["bytes_s"] for v in classes.values()),
              "compaction_bytes": row["run"]["compaction_output_bytes"]}
    for op, v in classes.items():
        if v["completed"] >= 10_000:
            output[op + "_p99_us"] = v["arrival_p99_us"]
        output[op + "_ops_s"] = v["ops_s"]
        output[op + "_outstanding_half"] = v["outstanding_half"]
        output[op + "_outstanding_end"] = v["outstanding_end"]
    return output


writer = csv.writer(sys.stdout, lineterminator="\n")
writer.writerow(["case","arm","metric","pairs","control_median","arm_median","paired_gain_pct","aa_max_abs_pct","improved_pairs"])
for case, rounds in sorted(groups.items()):
    candidates = set(next(iter(rounds.values()))) - {"A1","A2","stage-timing"}
    for arm in sorted(candidates):
        controls, alternatives, duplicates = [], [], []
        for trial in rounds.values():
            control, candidate = trial["A1"], trial[arm]
            assert control["settings"]["SEED"] == candidate["settings"]["SEED"]
            assert control["settings"].get("RATE", 0) == candidate["settings"].get("RATE", 0)
            if case.startswith("envelope-"):
                envelope = lambda r: (1 + r["settings"]["IMMUTABLES"]) * r["settings"]["BUFFER_MIB"]
                assert envelope(control) == envelope(candidate) == 30
            controls.append(metrics(control))
            duplicates.append(metrics(trial["A2"]))
            alternatives.append(metrics(candidate))
        for label in controls[0]:
            baseline = [row[label] for row in controls]
            values = [row[label] for row in alternatives]
            gain, noise, better = "", "", ""
            if all(baseline):
                sign = 1 if label.endswith(("_bytes_s","_ops_s")) else -1
                deltas = [sign * 100 * (b-a)/a for a,b in zip(baseline,values)]
                gain = round(median(deltas), 3)
                noise = round(max(100 * abs(row[label]-a)/a for a,row in zip(baseline,duplicates)), 3)
                better = sum(d > 0 for d in deltas)
            writer.writerow([case,arm,label,len(rounds),round(median(baseline),3),round(median(values),3),gain,noise,better])
