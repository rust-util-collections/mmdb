"""Replay a recorded two-issuer workload with serial or zero-handoff-cost WAL/apply lanes."""
import argparse
import csv
import gzip
import json
import lzma
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class Call:
    scheduled: int
    enqueued: int
    acknowledged: int


@dataclass(frozen=True)
class Group:
    sequence: int
    requests: int
    entries: int
    generation: int
    start: int
    wal_start: int
    wal_end: int
    apply_start: int
    apply_end: int
    released: int


def load(path):
    calls = {"SmallPut": [], "Batch": []}
    groups = []
    opener = {".gz": gzip.open, ".xz": lzma.open}.get(path.suffix, open)
    with opener(path, "rt") as stream:
        for record in csv.reader(stream):
            if record[0] == "R":
                _, kind, number, *times = record
                assert int(number) == len(calls[kind])
                call = Call(*map(int, times))
                assert call.scheduled <= call.enqueued <= call.acknowledged
                calls[kind].append(call)
            else:
                assert record[0] == "G"
                group = Group(*map(int, record[1:]))
                times = [group.start, group.wal_start, group.wal_end, group.apply_start,
                         group.apply_end, group.released]
                assert times == sorted(times)
                groups.append(group)
    groups.sort(key=lambda g: g.sequence)
    return calls, groups


def p99(values):
    assert values
    return sorted(values)[(len(values)*99 + 99)//100 - 1]


def replay(calls, groups, bulk_entries, pipeline, free_neighbor=False):
    index = dict.fromkeys(calls, 0)
    last_predicted = dict.fromkeys(calls, 0)
    last_actual = dict.fromkeys(calls, 0)
    latencies = {k: [] for k in calls}
    previous = None
    old_wal_end = old_apply_start = old_apply_end = old_release = 0
    queued_before_last_apply_end = modeled_overlaps = overlapping_ns = barriers = 0
    mixed_groups = free_neighbors = 0
    for pos, g in enumerate(groups):
        # Each stream has one outstanding operation at most. The payload and
        # sequence count distinguish the single put from the multi-entry batch.
        n_bulk, remainder = divmod(g.entries - g.requests, bulk_entries-1)
        n_small = g.requests - n_bulk
        assert remainder == 0 and n_small in (0, 1) and n_bulk in (0, 1) and g.requests > 0
        if n_small and n_bulk:
            mixed_groups += 1
        members = [k for k, n in [("SmallPut", n_small), ("Batch", n_bulk)] if n]
        actual_ready = predicted_ready = 0
        for k in members:
            c = calls[k][index[k]]
            # Replay the recorded issue/preparation cost after the worker can
            # legally submit its next request: both the clock and prior ack apply.
            extra = c.enqueued - max(last_actual[k], c.scheduled)
            assert extra >= 0 and c.enqueued <= g.start
            release = max(last_predicted[k], c.scheduled) + extra
            actual_ready = max(actual_ready, c.enqueued)
            predicted_ready = max(predicted_ready, release)

        if previous:
            assert g.sequence == previous.sequence + previous.entries
        may_overlap = previous and g.generation == previous.generation
        if previous and not may_overlap:
            barriers += 1
        if may_overlap and actual_ready < previous.apply_end:
            queued_before_last_apply_end += 1

        # Preserve scheduler/handoff and admission delays. A new WAL generation
        # cannot log until its predecessor has been applied and the old one sealed.
        gap = g.start - max(previous.released if previous else 0, actual_ready)
        assert gap >= 0
        pre = g.wal_start - g.start
        wal = g.wal_end - g.wal_start
        between = g.apply_start - g.wal_end
        apply = g.apply_end - g.apply_start
        post = g.released - g.apply_end
        if free_neighbor and members == ["SmallPut"]:
            # Sensitivity only: allow the entire adjacent small group to cost
            # zero even without real readiness or a common WAL generation.
            neighbors = groups[max(0,pos-1):pos] + groups[pos+1:pos+2]
            if any(other.requests == 1 and other.entries == bulk_entries for other in neighbors):
                free_neighbors += 1
                gap = pre = wal = between = apply = post = 0
        start = max(predicted_ready, old_wal_end if pipeline and may_overlap else old_release) + gap
        wal_start = start + pre
        wal_end = wal_start + wal
        apply_start = max(wal_end+between, old_release)
        apply_end = apply_start+apply
        release = apply_end+post
        overlap = max(0, min(wal_end, old_apply_end) - max(wal_start, old_apply_start))
        modeled_overlaps += int(overlap > 0)
        overlapping_ns += overlap

        for k in members:
            c = calls[k][index[k]]
            tail = c.acknowledged - g.released
            assert tail >= 0
            ack = release + tail
            assert c.scheduled <= ack <= c.acknowledged
            if not pipeline:
                assert ack == c.acknowledged, (k, index[k], ack, c.acknowledged)
            last_predicted[k], last_actual[k] = ack, c.acknowledged
            latencies[k].append(ack - c.scheduled)
            index[k] += 1
        previous = g
        old_wal_end, old_apply_start, old_apply_end, old_release = wal_end, apply_start, apply_end, release
    assert all(index[k]==len(calls[k]) for k in calls)
    return {
        "small_p99_ns":p99(latencies["SmallPut"]), "bulk_completion_ns":last_predicted["Batch"],
        "enqueued_before_prior_apply_end":queued_before_last_apply_end,
        "overlap_groups":modeled_overlaps,"overlapping_ns":overlapping_ns,
        "generation_barriers":barriers, "mixed_groups":mixed_groups, "free_small_neighbors":free_neighbors,
    }


def main():
    ap=argparse.ArgumentParser(description=__doc__)
    ap.add_argument("trace",type=Path)
    ap.add_argument("--bulk-entries",type=int,default=4096)
    args=ap.parse_args()
    calls,groups=load(args.trace)
    observed=replay(calls,groups,args.bulk_entries,False)
    projected=replay(calls,groups,args.bulk_entries,True)
    free = replay(calls,groups,args.bulk_entries,True,True)
    print(json.dumps({"groups":len(groups),"small":len(calls["SmallPut"]),"bulk":len(calls["Batch"]),
                      "observed":observed, "projection":projected, "free_neighbor_sensitivity":free,
                      "free_neighbor_p99_gain_percent":100*(1-free["small_p99_ns"]/observed["small_p99_ns"]),
                      "projected_small_p99_gain_percent":100*(1-projected["small_p99_ns"]/observed["small_p99_ns"])}))


if __name__ == "__main__":
    main()
