"""Verify the warmed writer traces, their schedules and the recorded screen."""
import gzip
import hashlib
import json
from pathlib import Path
import statistics

from replay import load, replay

HERE = Path(__file__).resolve().parent


def verify():
    summary = [
        json.loads(line)
        for line in (HERE / "summary.jsonl").read_text().splitlines()
    ]
    result = json.loads((HERE / "analysis.json").read_text())
    context = result["contexts"]
    assert hashlib.sha256((HERE / "trace.patch").read_bytes()).hexdigest() == context["trace_patch_sha256"]
    assert len(summary) == 10 and len(result["paired"]) == 5
    gains = []
    for number, pair in enumerate(result["paired"], 1):
        rows = [row for row in summary if row["rep"] == number]
        assert [r["traced"] for r in rows] == ([True, False] if number % 2 else [False, True])
        assert [r["order"] for r in rows] == [1, 2]
        assert pair["rep"] == number
        traced = next(r for r in rows if r["traced"])
        control = next(r for r in rows if not r["traced"])
        for run in rows:
            assert run["seed"] == 20261125 + number and run["rate"] == context["rate"]
            assert run["executable_sha256"] == context["executable_sha256"]
            assert run["cpus"] == "4-7"
            measurements = run["measurements"]
            assert measurements["run"]["placement"] == "hot"
            assert measurements["run"]["dbs"] == "1"
            for operation in ("SmallPut", "Batch", "Read", "FreshSnapshot", "HeldSnapshot"):
                expected = context["bulk"] if operation == "Batch" else context["small"]
                row = measurements[operation]
                assert int(row["offered"]) == int(row["completed"]) == int(row["sampled"]) == expected
                assert int(row["rejected"]) == 0
                assert int(row["bytes"]) == {"SmallPut": 1440000, "Batch": 88014848}.get(operation, 0)
        trace_file = HERE / f"trace-{number}.csv.gz"
        with gzip.open(trace_file, "rb") as stream:
            assert hashlib.sha256(stream.read()).hexdigest() == traced["trace_sha256"]
        calls, groups = load(trace_file)
        period_ns = 1_000_000_000 // context["rate"]
        for operation, interval in (("SmallPut", period_ns), ("Batch", period_ns * 128)):
            assert all(call.scheduled == index * interval for index, call in enumerate(calls[operation]))
        observed = replay(calls, groups, context["burst"], False)
        projected = replay(calls, groups, context["burst"], True)
        sensitivity = replay(calls, groups, context["burst"], True, True)
        assert traced["observed"] == observed
        assert traced["projection"] == projected
        assert traced["sensitivity"] == sensitivity
        base = observed["small_p99_ns"]
        assert abs(base / 1000 - float(traced["measurements"]["SmallPut"]["arrival_p99_us"])) <= 0.001
        gain = 100 * (1 - projected["small_p99_ns"] / base)
        assert pair["groups"] == len(groups)
        assert pair["observed_ms"] == base / 1e6 and pair["modeled_ms"] == projected["small_p99_ns"] / 1e6
        assert pair["gain"] == gain
        assert pair["sensitivity_gain"] == 100 * (1 - sensitivity["small_p99_ns"] / base)
        assert pair["overlaps"] == projected["overlap_groups"]
        assert pair["overlap_ms"] == projected["overlapping_ns"] / 1e6
        assert pair["wal_ms"] == sum(g.wal_end - g.wal_start for g in groups) / 1e6
        assert pair["apply_ms"] == sum(g.apply_end - g.apply_start for g in groups) / 1e6
        control_p99 = float(control["measurements"]["SmallPut"]["arrival_p99_us"])
        assert pair["observer_delta"] == 100 * (base / 1000 / control_p99 - 1)
        assert pair["warm_control_cache_misses"] == int(control["measurements"]["run"]["cache_misses"])
        gains.append(gain)
    passing = sum(value >= 15 for value in gains)
    median = statistics.median(gains)
    assert (median, passing, median >= 15 and passing >= 4) == (
        result["median_gain"], result["passing_15pct"], result["qualifies"]
    )
    print(f"Five matched pairs verified. Median modeled gain {median:.3f}%, {passing}/5 at 15%.")


if __name__ == "__main__":
    verify()
