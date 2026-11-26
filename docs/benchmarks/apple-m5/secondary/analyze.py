"""Recompute native prefix-cut pairs and ordered write-stage projections from the recorded rows."""
import csv
import gzip
import hashlib
import json
from pathlib import Path
from statistics import median
import sys
import subprocess

root = Path(subprocess.check_output(["git", "rev-parse", "--show-toplevel"], text=True).strip())
sys.path.insert(0, str(root / "docs/benchmarks/engine-profile/writer-trace"))
from replay import load, replay


def prefix(path):
    with path.open() as f:
        rows = list(csv.DictReader(f))
    assert len(rows) == 20
    rounds = []
    for rep in range(1, 6):
        pair = {(r["comparison"], r["arm"]): r for r in rows if int(r["pair"]) == rep}
        assert set(pair) == {(kind, arm) for kind in ["aa", "ab"] for arm in ["A", "B"]}
        for (kind, arm), row in pair.items():
            cut = kind == "ab" and arm == "B"
            assert int(row["seed"]) == 6 + rep and int(row["rate"]) == 20000
            assert int(row["subs"]) == int(row["peak_ranges"]) == 1 and row["trace_cold"] == "true"
            assert int(row["write_ops"]) == int(row["read_ops"]) == 16384
            assert int(row["fence_ops"]) == 64 and int(row["user_bytes"]) == 4456448
            assert int(row["l1_start"]) == 24 and int(row["initial_cold_files"]) == 18
            assert int(row["multi_target_jobs"]) > 0 and int(row["parallel_jobs"]) == 0
            assert int(row["preload_rss_kib"]) > 0 and int(row["peak_rss_kib"]) > 0
            assert (int(row["flushed_ssts"]) > int(row["flushes"])) == cut
        rounds.append(pair)

    def metric(row, field):
        if field == "stall_ms_per_GiB":
            return int(row["l0_stall_ns"]) / 1e6 / (int(row["user_bytes"]) / 2**30)
        return int(row[field])

    result = {}
    for field in ["flushes", "flushed_ssts", "rewrite_jobs", "logical_cold_emitted_bytes",
                  "output_file_bytes", "output_bytes_after_workers", "initial_cold_consumed_bytes",
                  "stall_ms_per_GiB", "read_arrival_p99_ns", "write_arrival_p99_ns", "l0_end"]:
        av = [metric(p["ab", "A"], field) for p in rounds]
        bv = [metric(p["ab", "B"], field) for p in rounds]
        gains = [100 * (1 - b / a) for a, b in zip(av, bv) if a]
        noise = [100 * abs(1 - metric(p["aa", "B"], field) / metric(p["aa", "A"], field))
                 for p in rounds if metric(p["aa", "A"], field)]
        result[field] = {"baseline": median(av), "one_cut": median(bv),
                         "baseline_range": [min(av), max(av)], "one_cut_range": [min(bv), max(bv)]}
        if len(gains) == 5 and len(noise) == 5:
            result[field].update(gain_pct=median(gains), range_pct=[min(gains), max(gains)],
                                 max_abs_aa_pct=max(noise))
    return result


def writer(folder):
    rows = [json.loads(line) for line in (folder / "summary.jsonl").read_text().splitlines()]
    assert len(rows) == 10 and len({r["executable_sha256"] for r in rows}) == 1
    pairs = []
    for rep in range(1, 6):
        subset = [r for r in rows if r["rep"] == rep]
        assert [r["order"] for r in subset] == [1, 2]
        assert [r["traced"] for r in subset] == ([True, False] if rep % 2 else [False, True])
        for r in subset:
            m = r["measurements"]
            assert r["seed"] == 20261125 + rep and r["rate"] == 18911 and r["cpus"] == "native"
            assert m["run"]["dbs"] == "1" and m["run"]["placement"] == "hot"
            for name in ["SmallPut", "Batch", "Read", "FreshSnapshot", "HeldSnapshot"]:
                n = 79 if name == "Batch" else 10000
                expected_bytes = {"SmallPut": 1440000, "Batch": 88014848}.get(name, 0)
                assert int(m[name]["offered"]) == int(m[name]["completed"]) == int(m[name]["sampled"]) == n
                assert int(m[name]["rejected"]) == 0 and int(m[name]["bytes"]) == expected_bytes
        traced, control = sorted(subset, key=lambda r: not r["traced"])
        data = folder / f"trace-{rep}.csv.gz"
        with gzip.open(data, "rb") as f:
            assert hashlib.file_digest(f, "sha256").hexdigest() == traced["trace_sha256"]
        calls, groups = load(data)
        assert len(groups) == traced["groups"]
        for name, mult in [("SmallPut", 1), ("Batch", 128)]:
            assert all(c.scheduled == i * (10**9 // 18911) * mult for i, c in enumerate(calls[name]))
        obs = replay(calls, groups, 4096, False)
        ideal = replay(calls, groups, 4096, True)
        sensitivity = replay(calls, groups, 4096, True, True)
        assert [obs, ideal, sensitivity] == [traced[name] for name in ["observed", "projection", "sensitivity"]]
        peak = obs["small_p99_ns"]
        assert abs(peak - float(traced["measurements"]["SmallPut"]["arrival_p99_us"]) * 1000) <= 1
        gain = 100 * (1 - ideal["small_p99_ns"] / peak)
        assert gain == traced["ideal_p99_gain_percent"]
        pairs.append({"rep": rep, "observed_ms": peak / 1e6, "modeled_ms": ideal["small_p99_ns"] / 1e6,
                      "control_ms": float(control["measurements"]["SmallPut"]["arrival_p99_us"])/1e3,
                      "ideal_gain_pct": gain, "sensitivity_gain_pct": traced["sensitivity_p99_gain_percent"],
                      "overlaps": ideal["overlap_groups"], "overlap_ms": ideal["overlapping_ns"] / 1e6})
    gains = [p["ideal_gain_pct"] for p in pairs]
    passing = sum(g >= 15 for g in gains)
    return {"paired": pairs, "median_gain_pct": median(gains), "range_pct": [min(gains), max(gains)],
            "passing_15pct": passing, "qualifies": median(gains) >= 15 and passing >= 4}


if __name__ == "__main__":
    data = Path(sys.argv[1])
    print(json.dumps({"prefix": prefix(data / "prefix.csv"), "writer": writer(data / "writer")}, indent=2))
