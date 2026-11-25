"""Summarize complete A/A and A/B cells, checking the measured cache regime."""
import csv
import json
import sys
from collections import defaultdict
from pathlib import Path
from statistics import median

from read_evidence import all_request_p99, expected_table_probes, require_cache_evidence, verify_block_reuse

for filename in sys.argv[1:]:
    path = Path(filename)
    require_cache_evidence(path)
    meta = json.loads(path.with_suffix(".metadata.json").read_text())
    cells = defaultdict(lambda: defaultdict(dict))
    with path.open() as f:
        for row in csv.DictReader(f):
            if int(row["tables"]) != expected_table_probes(row, meta):
                raise ValueError(f"{path}: incorrect SST probe count for trace {row['seed']}")
            cell = (row["regime"], row["locality"], int(row["fanout"]))
            pair = (row["pair"], int(row["round"]))
            if row["arm"] in cells[cell][pair]:
                raise ValueError(f"duplicate observation in {path}: {cell} {pair}")
            cells[cell][pair][row["arm"]] = row

    for cell, pairs in sorted(cells.items()):
        aa, ab, control_us, grouped_us, control_probes, grouped_probes = [], [], [], [], [], []
        device_a_us, device_b_us = [], []
        qualified_fractions, storage_a, storage_b = [], [], []
        for kind in ["AA", "AB"]:
            for repeat in range(1, 6):
                arms = pairs[kind, repeat]
                a, b = arms["baseline"], arms["comparison"]
                assert a["seed"] == b["seed"] and a["samples"] == b["samples"]
                assert a["method"] == "scalar" and b["method"] == ("grouped" if kind == "AB" else "scalar")
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
            device_details = (f" io_positive_only_p99_us={median(device_a_us):.2f}/"
                              f"{median(device_b_us):.2f} "
                              f"min_io_qualified={min(qualified_fractions):.3f} "
                              f"bytes_per_request={median(storage_a):.0f}/{median(storage_b):.0f}")
        print(f"{path.name} {cell} p99_us={median(control_us):.2f}/{median(grouped_us):.2f} "
              f"p99_ranges_us={min(control_us):.2f}..{max(control_us):.2f}/"
              f"{min(grouped_us):.2f}..{max(grouped_us):.2f} "
              f"median_gain={median(ab):.2f}% gain_range={min(ab):.2f}..{max(ab):.2f}% "
              f"max_AA={max(aa):.2f}% "
              f"positive_pairs={sum(gain > 0 for gain in ab)}/5 "
              f"blocks_per_request={median(control_probes):.2f}/{median(grouped_probes):.2f}{device_details}")
