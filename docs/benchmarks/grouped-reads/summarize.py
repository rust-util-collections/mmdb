"""Summarize complete A/A and A/B cells, checking the measured cache regime."""
import csv
import json
import sys
from collections import defaultdict
from pathlib import Path
from statistics import median

from read_evidence import all_request_p99, expected_table_probes, require_cache_evidence, verify_block_reuse


def summarize_file(path):
    path = Path(path)
    lines = []
    require_cache_evidence(path)
    meta = json.loads(path.with_suffix(".metadata.json").read_text())
    cells = defaultdict(lambda: defaultdict(dict))
    with path.open() as f:
        for row in csv.DictReader(f):
            if int(row["tables"]) != expected_table_probes(row, meta):
                raise ValueError(f"{path}: incorrect SST probe count for trace {row['seed']}")
            round_num = int(row["round"])
            if row["pair"] not in ("AA", "AB") or round_num not in range(1, 6) or row["arm"] not in ("baseline", "comparison"):
                raise ValueError(f"{path}: unexpected schedule coordinates {row['pair']} round {row['round']} arm {row['arm']}")
            if "samples_per_run" in meta and int(row["samples"]) != meta["samples_per_run"]:
                raise ValueError(f"{path}: samples {row['samples']} != metadata samples_per_run {meta['samples_per_run']}")
            if "base_seed" in meta and int(row["seed"]) != meta["base_seed"] + (round_num - 1) * 1021:
                raise ValueError(f"{path}: seed {row['seed']} disagrees with round {round_num} schedule")
            if meta.get("selected_locality") is not None and row["locality"] != meta["selected_locality"]:
                raise ValueError(f"{path}: locality {row['locality']} != selected {meta['selected_locality']}")
            if meta.get("selected_fanout") is not None and int(row["fanout"]) != int(meta["selected_fanout"]):
                raise ValueError(f"{path}: fanout {row['fanout']} != selected {meta['selected_fanout']}")
            cell = (row["regime"], row["locality"], int(row["fanout"]))
            pair = (row["pair"], round_num)
            if row["arm"] in cells[cell][pair]:
                raise ValueError(f"duplicate observation in {path}: {cell} {pair}")
            cells[cell][pair][row["arm"]] = row

    if not cells:
        raise ValueError(f"{path}: no cells in benchmark CSV")
    if meta.get("retained_cells") is not None:
        expected_retained = {(c["locality"], int(c["fanout"])) for c in meta["retained_cells"]}
        observed_retained = {(loc, fan) for _, loc, fan in cells}
        if observed_retained != expected_retained:
            raise ValueError(f"{path}: cells {observed_retained} != retained_cells {expected_retained}")

    expected_pairs = {(kind, repeat) for kind in ("AA", "AB") for repeat in range(1, 6)}
    for cell, pairs in sorted(cells.items()):
        if set(pairs) != expected_pairs:
            raise ValueError(f"{path}: incomplete or extra pairs in {cell}")
        aa, ab, control_us, grouped_us, control_probes, grouped_probes = [], [], [], [], [], []
        device_a_us, device_b_us = [], []
        qualified_fractions, storage_a, storage_b = [], [], []
        for kind in ["AA", "AB"]:
            for repeat in range(1, 6):
                arms = pairs[kind, repeat]
                if set(arms) != {"baseline", "comparison"}:
                    raise ValueError(f"{path}: incomplete arms in {cell} {(kind, repeat)}")
                a, b = arms["baseline"], arms["comparison"]
                if a["seed"] != b["seed"] or a["samples"] != b["samples"]:
                    raise ValueError(f"{path}: mismatched seed or samples in {cell} {(kind, repeat)}")
                expected_b = "grouped" if kind == "AB" else "scalar"
                if a["method"] != "scalar" or b["method"] != expected_b:
                    raise ValueError(f"{path}: mislabeled method in {cell} {(kind, repeat)}")
                old, new = all_request_p99(a), all_request_p99(b)
                gain = 100 * (old - new) / old
                if kind == "AA":
                    aa.append(abs(gain))
                else:
                    verify_block_reuse(a, b)
                    ab.append(gain)
                    control_us.append(old / 1000)
                    grouped_us.append(new / 1000)
                    control_probes.append(int(a["blocks"]) / int(a["samples"]))
                    grouped_probes.append(int(b["blocks"]) / int(b["samples"]))
                    if cell[0] == "device":
                        device_a_us.append(int(a["device_p99_ns"]) / 1000)
                        device_b_us.append(int(b["device_p99_ns"]) / 1000)
                        storage_a.append(int(a["storage_bytes"]) / int(a["samples"]))
                        storage_b.append(int(b["storage_bytes"]) / int(b["samples"]))
                if cell[0] == "device":
                    qualified_fractions.extend(int(row["device_requests"]) / int(row["samples"])
                                               for row in (a, b))
        device_details = ""
        if device_a_us:
            counter = "darwin_diskio" if meta.get("storage_counter") == "ri_diskio_bytesread" else "linux_read"
            device_details = (f" {counter}_ge_4kib_p99_us={median(device_a_us):.2f}/"
                              f"{median(device_b_us):.2f} "
                              f"min_io_qualified={min(qualified_fractions):.3f} "
                              f"{counter}_bytes_per_request={median(storage_a):.0f}/{median(storage_b):.0f}")
        lines.append(f"{path.name} {cell} p99_us={median(control_us):.2f}/{median(grouped_us):.2f} "
                     f"p99_ranges_us={min(control_us):.2f}..{max(control_us):.2f}/"
                     f"{min(grouped_us):.2f}..{max(grouped_us):.2f} "
                     f"median_gain={median(ab):.2f}% gain_range={min(ab):.2f}..{max(ab):.2f}% "
                     f"max_AA={max(aa):.2f}% "
                     f"positive_pairs={sum(gain > 0 for gain in ab)}/5 "
                     f"blocks_per_request={median(control_probes):.2f}/{median(grouped_probes):.2f}{device_details}")
    return lines


if __name__ == "__main__":
    for filename in sys.argv[1:]:
        for line in summarize_file(filename):
            print(line)
