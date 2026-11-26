"""Validate and summarize the seven frozen-engine result directories."""
import argparse
from collections import defaultdict
import csv
import gzip
import json
from pathlib import Path
from statistics import median
import subprocess
import sys

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent / "apple-m5"))
sys.path.insert(0, str(HERE.parent / "grouped-reads"))
from read_evidence import expected_table_probes, has_cache_evidence
from summarize_engine import counters, observation

REVISION = "828e47a1d019e3fd0854a3d9a0d98b72e873350c"
ARMS = {"A1", "A2", "AB1", "B"}
LOCATIONS = ("m5-apfs", "5900x-raid", "5900x-single", "9474f-raid",
             "9474f-single", "9950x-zfs", "9950x-ext4")
PAIRED_FIELDS = ("location", "case", "metric", "reference", "candidate",
                 "gain_pct", "max_aa_pct", "positive_pairs", "min_gain_pct",
                 "max_gain_pct", "eligible_pairs", "rounds", "evidence")
SINGLE_FIELDS = ("location", "case", "metric", "median", "min", "max", "observations")


def require(condition, message):
    if not condition:
        raise ValueError(message)


def packet(path):
    with gzip.open(path, "rt") as f:
        header = json.loads(next(f))["metadata"]
        rows = [json.loads(line) for line in f]
    return header, rows


def add(group, row, metrics, eligible=True):
    index = row["round"], row["arm"]
    require(index not in group, f"duplicate trial {index}")
    group[index] = (metrics, eligible)


def paired(location, case, group, *, higher=()):
    expected = {(r, a) for r in range(1, 6) for a in ARMS}
    require(group.keys() == expected, f"{location} {case}: five complete A/A and A/B pairs required")
    result = []
    for metric in group[1, "B"][0]:
        data = [{arm: group[i, arm][0][metric] for arm in ARMS} for i in range(1, 6)]
        if any(None in values.values() for values in data):
            continue
        sign = 1 if metric in higher else -1
        gains = [sign * 100 * (v["B"] / v["AB1"] - 1) for v in data if v["AB1"] > 0]
        noise = [100 * abs(v["A2"] / v["A1"] - 1) for v in data if v["A1"] > 0]
        ok = sum(all(group[i, a][1] for a in ARMS) for i in range(1, 6))
        def number(x):
            return round(x, 4)
        # Saturated processes may admit different amounts of work. Ending
        # file/queue counts and total compaction output describe that debt.
        # st_blocks can also lag an SST install on ZFS. Compare encoded
        # file length (sst_bytes) for the portable space result.
        diagnostic = metric in ("final_l0", "final_immutables", "compaction_output_MiB",
                                "allocated_bytes")
        ratios = len(gains) == len(noise) == 5 and not diagnostic
        status = "descriptive"
        if ok < 5:
            status = "unqualified"
        elif ratios and metric not in ("peak_rss_MiB", "compaction_output_per_logical_byte",
                                        "block_probes_per_request", "fence_us"):
            mid, band = median(gains), max(noise)
            target = 20 if metric == "sst_bytes" else 10
            if abs(mid) <= band:
                status = "inside-control-spread"
            elif mid >= target and sum(g > 0 for g in gains) >= 4:
                status = "clears-benefit-screen"
            elif mid < -5 and sum(g < 0 for g in gains) >= 4:
                status = "repeatable-loss-over-5pct"
            else:
                status = "outside-spread-below-screen-or-inconsistent"
        result.append((location, case, metric,
                       number(median(v["AB1"] for v in data)), number(median(v["B"] for v in data)),
                       number(median(gains)) if ratios else "", number(max(noise)) if ratios else "",
                       sum(g > 0 for g in gains) if ratios else "", number(min(gains)) if ratios else "",
                       number(max(gains)) if ratios else "", ok, 5, status))
    return result


def single(location, case, metric, values):
    return (location, case, metric, round(median(values), 4), min(values), max(values), len(values))


def drain_metrics(row, values):
    lines = row["raw"].splitlines()
    run = counters(next(line for line in lines if line.startswith("run ")))
    terminal = counters(next(line for line in lines if line.startswith("drain ")))
    classes = [counters(line) for line in lines if line.startswith("class ")]
    logical = sum(c["bytes"] for c in classes)
    require(terminal["terminal_us"] >= run["seconds"] * 1e6 - 1
            and 0 < terminal["micros"] <= terminal["terminal_us"]
            and terminal["total_compaction_bytes"] >= run["compaction_output_bytes"]
            and terminal["final_l0"] == terminal["final_immutables"] == 0
            and terminal["sst_bytes"] > 0, "invalid post-window compact state")
    values.update(drain_ms=terminal["micros"] / 1000,
                  through_compact_MiB_s=(logical / 2**20) / (terminal["terminal_us"] / 1e6),
                  compacted_output_per_logical_byte=terminal["total_compaction_bytes"] / logical)
    for classes_row in classes:
        if classes_row["op"] == "SmallPut":
            qualified = classes_row["sampled"] >= 10000
            values["smallput_service_p99_us"] = classes_row["service_p99_us"] if qualified else None
            values["smallput_issue_delay_p99_us"] = classes_row["issue_delay_p99_us"] if qualified else None


def shard_results(location, path, *, drain=False):
    _, rows = packet(path)
    _, plan = packet(HERE / "memory-plan.jsonl.gz")
    require(len(rows) == len(plan) == 80, f"{location}: incomplete memory matrix")
    groups = defaultdict(dict)
    peak = []
    for row, scheduled in zip(rows, plan):
        for key in ("case", "mode", "arm", "round", "settings"):
            require(row[key] == scheduled[key], f"{location}: wrong memory schedule: {row['case']} {key}")
        values, keeps_load = observation(row)
        if drain:
            drain_metrics(row, values)
        add(groups[row["case"]], row, values, bool(keeps_load))
        peak.append(values["peak_rss_MiB"])
    pairs = [r for case, g in sorted(groups.items())
             for r in paired(location, "shards-" + case, g, higher=("logical_MiB_s", "through_compact_MiB_s"))]
    return pairs, [single(location, "shards-all-arms", "peak_rss_MiB", peak)]


def input_receipt(folder, revision=REVISION, *, warm_measured=True):
    page = json.loads((folder / "grouped" / "page.metadata.json").read_text())
    warm = json.loads((folder / "grouped" / "warm.metadata.json").read_text()) if warm_measured else page
    require(warm["sst_sha256"] == page["sst_sha256"] and warm["probe_sha256"] == page["probe_sha256"],
            f"{folder}: small fixture or probe changed between cache regimes")
    mem_meta, _ = packet(folder / "memory.jsonl.gz")
    open_meta, reopens = packet(folder / "reopen.jsonl.gz")
    storage_meta, ssts = packet(folder / "storage.jsonl.gz")
    require(open_meta["engine_revision"] == revision and
            open_meta["binary_sha256"] == mem_meta["binary_sha256"]["control"],
            f"{folder}: mismatched engine executables")
    lengths = {}
    for r in ssts:
        if "sst" in r:
            key = f"{r['seed']}/{r['case']}/{r['arguments'][1]}"
            n = r["sst"]["sst_bytes"]
            require(lengths.setdefault(key, n) == n, f"{folder}: varying encoded size on one trace")
    wal_bytes = {counters(line)["wal_bytes"] for r in reopens for line in r["raw"].splitlines()
                 if line.startswith("reopen ")}
    require(len(wal_bytes) == 1, f"{folder}: unequal residual WAL sizes")
    device_path = folder / "grouped" / "device.metadata.json"
    large = json.loads(device_path.read_text()) if device_path.exists() else None
    if large:
        require(large["probe_sha256"] == warm["probe_sha256"], f"{folder}: device probe differs")
    return dict(grouped_small_sst_sha256=warm["sst_sha256"],
                grouped_large_sst_sha256=large["sst_sha256"] if large else None,
                residual_wal_bytes=next(iter(wal_bytes)), grouped_probe_sha256=warm["probe_sha256"],
                engine_profile_sha256=open_meta["binary_sha256"], storage_binary_sha256=storage_meta["binary_sha256"],
                sst_lengths=lengths)


def read_results(location, path, kind, revision=REVISION):
    meta = json.loads(path.with_suffix(".metadata.json").read_text())
    with path.open() as f:
        rows = list(csv.DictReader(f))
    shape, samples, size = {
        "warm": ("clustered", 300000, 262144),
        "page": ("clustered", 10000, 262144),
        "device": ("scattered", 10000, 1048576),
    }[kind]
    require(meta["build_revision"] == revision and meta["rows"] == size, f"{path}: incorrect source or fixture")
    if location == "9950x-zfs" and kind == "device":
        proof = json.loads((path.parent / "device-qualification.json").read_text())
        sectors = proof["physical_device_read_bytes"]
        require(proof["accepted_device_cell"] is True and len(sectors) == len(rows)
                and all(b >= 4096 * samples * 0.95 for b in sectors),
                f"{path}: ZFS process accounting needs corroborating physical I/O in every arm")
    group = {}
    coverage = []
    for row in rows:
        round_no = int(row["round"])
        require(row["regime"] == kind and row["locality"] == shape and int(row["fanout"]) == 32
                and int(row["samples"]) == samples and int(row["seed"]) == 20261125 + (round_no-1)*1021,
                f"{path}: different request trace")
        require(int(row["tables"]) == expected_table_probes(row, meta), f"{path}: incorrect SST probes")
        require(int(row["peak_pin_bytes"]) <= int(row["pin_budget"]), f"{path}: pin budget exceeded")
        arms = {("AA", "baseline"): "A1", ("AA", "comparison"): "A2",
                ("AB", "baseline"): "AB1", ("AB", "comparison"): "B"}
        arm = arms[row["pair"], row["arm"]]
        require(row["method"] == ("grouped" if arm == "B" else "scalar"), f"{path}: wrong method")
        metrics = dict(request_p99_us=int(row["p99_ns"]) / 1000,
                       block_probes_per_request=int(row["blocks"]) / samples)
        add(group, dict(round=round_no, arm=arm), metrics, has_cache_evidence(row))
        if kind == "device":
            coverage.append(100 * int(row["device_requests"]) / samples)
    name = "arc-prewarmed" if location == "9950x-zfs" and kind == "page" else kind
    return (paired(location, "grouped-" + name, group),
            [single(location, "grouped-device", "io_qualified_pct", coverage)] if coverage else [])


def storage_results(location, path, revision=REVISION):
    header, rows = packet(path)
    require(header["engine_revision"] == revision and len(rows) == 50, f"{path}: wrong version or size")
    groups = defaultdict(dict)
    wal = defaultdict(dict)
    for row in rows:
        case, arm, i = row["case"], row["arm"], row["round"]
        require(row["seed"] == 20261126 + i, f"{path}: seed mismatch")
        if case == "wal":
            require(arm in ("sync", "async") and i not in wal[arm] and row["arguments"] ==
                    ["wal", arm, str(row["seed"])], f"{path}: wrong WAL arm")
            require(row["wal"]["puts"] == row["verified"]["rows"] == 10000, f"{path}: incomplete WAL work")
            wal[arm][i] = row["wal"]
            continue
        require(case in ("entropy", "pattern") and row["arguments"] ==
                ["sst", "lz4" if arm == "B" else "none", case, str(row["seed"])], f"{path}: wrong SST arm")
        s = row["sst"]
        require(s["rows"] == 262144 and s["lookups"] == 40000 and s["files"] > 0 and s["sst_bytes"] > 0,
                f"{path}: incomplete SST work")
        storage_ok = s["process_read_bytes"] == 0
        if location == "9950x-zfs":
            # On this stack ARC hits can increment process read_bytes. Use the
            # physical sector count enclosing exactly the timed scan/point phase.
            storage_ok = s["device_read_sectors"] == 0
        for phase, fields, ok in (
            ("build", ("ingest_us", "sst_bytes", "allocated_bytes"), True),
            ("prewarmed", ("scan_us", "read_p99_ns"), storage_ok),
        ):
            add(groups[case, phase], row, {key: s[key] for key in fields}, ok)
    require(set(groups) == {(c,p) for c in ("entropy","pattern") for p in ("build","prewarmed")},
            f"{path}: missing SST workload")
    pairs = [r for (case, phase), g in sorted(groups.items())
             for r in paired(location, f"lz4-{case}-{phase}", g)]
    desc = []
    for arm in ("async", "sync"):
        require(set(wal[arm]) == set(range(1,6)), f"{path}: incomplete WAL runs")
        for key in ("put_p99_ns", "puts_us", "fence_us"):
            desc.append(single(location, "wal-" + arm, key, [r[key] for r in wal[arm].values()]))
    return pairs, desc


def reopen_results(location, path):
    _, rows = packet(path)
    require(len(rows) == 10, f"{path}: missing reopen runs")
    cells = {}
    for r in rows:
        i, ro = r["round"], r["read_only"]
        require(type(ro) is bool and (i, ro) not in cells, f"{path}: invalid reopen arm")
        s = r["settings"]
        require(int(s["OPS"]) == 100000 and int(s["PREPARE_MIB"]) == 64 and
                int(s["BUFFER_MIB"]) == 6 and int(s["IMMUTABLES"]) == 4 and
                int(s["SEED"]) == 20261126 + i and str(s["READ_ONLY"]).lower() == str(ro).lower(),
                f"{path}: mismatched reopen configuration")
        vals = counters(next(l for l in r["raw"].splitlines() if l.startswith("reopen ")))
        conf = counters(next(l for l in r["raw"].splitlines() if l.startswith("config ")))
        require(conf["pool_mib"] == 64 and conf["seed"] == int(s["SEED"]), f"{path}: wrong cache pool or trace")
        require(vals["rows"] == 100000 and min(vals["wal_bytes"], vals["open_us"], vals["open_peak_kib"]) > 0,
                f"{path}: invalid WAL fixture")
        cells[i, ro] = vals
    require(len({v["wal_bytes"] for v in cells.values()}) == 1, f"{path}: unequal WAL input")
    return [single(location, "reopen-" + ("ro" if ro else "rw"), key,
                   [cells[i,ro][key] for i in range(1,6)])
            for ro in (True,False) for key in ("open_us", "open_peak_kib", "wal_bytes")]


def volume_results(location, path, revision=REVISION):
    meta, rows = packet(path)
    require(meta["engine_revision"] == revision and len(rows) == 40, f"{path}: incomplete disk comparison")
    groups = defaultdict(dict)
    for r in rows:
        require(r["seed"] == 20261126 + r["round"] and
                r["volume"] == ("candidate" if r["arm"] == "B" else "reference"),
                f"{path}: volume or seed mismatch")
        case = r["case"]
        if case == "wal-sync":
            require(r["wal"]["puts"] == r["verified"]["rows"] == 10000, f"{path}: invalid WAL work")
            add(groups[case], r, {k: r["wal"][k] for k in ("put_p99_ns","puts_us","fence_us")})
        else:
            s = r["sst"]
            require(case == "sst-none-entropy" and s["rows"] == 262144 and s["lookups"] == 40000,
                    f"{path}: invalid SST work")
            add(groups[case], r, {k: s[k] for k in ("ingest_us","sst_bytes","allocated_bytes")})
            zfs = location == "9950x-ext4-to-zfs" and r["arm"] == "B"
            ok = s["device_read_sectors"] == 0 if zfs else s["process_read_bytes"] == 0
            add(groups["prewarmed-reads"], r, {k: s[k] for k in ("scan_us","read_p99_ns")}, ok)
    require(len(groups) == 3, f"{path}: missing volume workload")
    return [row for case,g in sorted(groups.items()) for row in paired(location, "volume-" + case, g)]


def version_results(location, folder, freeze):
    result = []
    for workload, filename in (("memory", "version-memory.jsonl.gz"), ("storage", "version-bridge.jsonl.gz")):
        header, rows = packet(folder / filename)
        if workload == "memory":
            old, new = header["old_revision"], header["engine_revision"]
            old_binary, new_binary = header["old_binary_sha256"], header["binary_sha256"]["control"]
        else:
            old, new = header["old_engine_revision"], header["new_engine_revision"]
            old_binary, new_binary = header["old_binary_sha256"], header["new_binary_sha256"]
        require((old, new) == (freeze["baseline_engine"], freeze["engine"]) and
                old_binary != new_binary and len(rows) == 20, f"{folder}: invalid version comparison")
        groups = defaultdict(dict)
        for r in rows:
            candidate = r["arm"] == "B"
            if workload == "memory":
                expected = dict(SEED=20261126+r["round"], OPS=20000, RATE=0, SECONDS=8,
                                BUFFER_MIB=6, IMMUTABLES=4, POOL_MIB=256, PLACEMENT="hot", BATCH_ENTRIES=32)
                require(r["settings"] == expected and r["binary"] == ("new" if candidate else "old"),
                        f"{folder}: mismatched version memory work")
                values, kept = observation(r)
                drain_metrics(r, values)
                add(groups["hot-saturation"], r, values, bool(kept))
            else:
                require(r["measurement_revision"] == (new if candidate else old)
                        and r["arguments"] == ["sst","none","entropy",str(20261126+r["round"])]
                        and r["sst"]["rows"] == 262144 and r["sst"]["lookups"] == 40000,
                        f"{folder}: mismatched version SST work")
                s = r["sst"]
                ok = (s["device_read_sectors"] == 0 if location == "9950x-zfs"
                      else s["process_read_bytes"] == 0)
                add(groups["sorted-ingest"], r, {"ingest_us":s["ingest_us"], "sst_bytes":s["sst_bytes"]})
                add(groups["prewarmed-reads"], r, {"scan_us":s["scan_us"], "read_p99_ns":s["read_p99_ns"]}, ok)
        for case, group in sorted(groups.items()):
            result.extend(paired(location, "version-" + case, group, higher=("logical_MiB_s","through_compact_MiB_s")))
    return result


def capacity_result(location, path, revision=REVISION, lock=None):
    rows, small, large, min_charge, max_charge = {
        "9474f-raid": (8388608, 4096, 98304, 49152, 5120),
        "9950x-ext4": (1048576, 512, 12288, 6144, 640),
    }[location]
    build = json.loads((path.parent / "build.json").read_text())
    fixture = json.loads((path.parent / "fixture.json").read_text())
    with path.open() as f:
        meta = json.loads(next(f))["metadata"]
    if lock is None:
        lock = json.loads((HERE / "freeze.json").read_text())["supplements"]["capacity_external_lock_sha256"]
    require(build["engine_revision"] == fixture["engine_revision"] == revision
            and build["binary_sha256"] == meta["binary_sha256"] and build["cargo_lock_sha256"] == lock
            and int(fixture["rows"]) == rows and int(fixture["value_bytes"]) == 8192,
            f"{path}: mismatched capacity build or fixture")
    cmd = [sys.executable, "-B", str(HERE.parent / "cache-capacity" / "summarize.py"), str(path),
           "--minimum-large-charge-mib", str(min_charge), "--maximum-small-charge-mib", str(max_charge)]
    result = json.loads(subprocess.check_output(cmd, text=True))
    require((result["rows"], result["small_mib"], result["large_mib"]) == (rows, small, large)
            and result["rounds"] == 5 and result["samples_per_arm"] == 100000,
            f"{path}: incorrect capacity comparison")
    result["clears_campaign_benefit_screen"] = (
        result["p99_benefit_demonstrated"] and result["median_p99_gain_pct"] >= 10)
    return result


def shard_decision(rows, peak):
    by_key = {(r[1], r[2]): dict(zip(PAIRED_FIELDS, r)) for r in rows}
    placements = {}
    for placement in ("hot", "all"):
        paced = "shards-" + placement + "-paced"
        rates = by_key["shards-" + placement + "-saturation", "logical_MiB_s"]
        guards = {}
        for metric in ("smallput_p99_us", "read_p99_us"):
            item = by_key[paced, metric]
            if item["eligible_pairs"] != 5:
                guard = "load-unqualified"
            elif item["min_gain_pct"] >= -5:
                guard = "within-5pct-in-every-pair"
            elif item["evidence"] == "repeatable-loss-over-5pct":
                guard = "exceeds-guard"
            else:
                guard = "uncertain"
            guards[metric] = guard
        placements[placement] = dict(saturation=rates["evidence"], paced_guards=guards)
    supported = peak <= 1024 and all(
        result["saturation"] == "clears-benefit-screen"
        and all(g == "within-5pct-in-every-pair" for g in result["paced_guards"].values())
        for result in placements.values())
    return dict(max_process_rss_mib=peak, placements=placements, passes_optional_preset_screen=supported)


def write_csv(path, header, rows):
    with path.open("w") as f:
        writer = csv.writer(f, lineterminator="\n")
        writer.writerow(header)
        writer.writerows(rows)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("results", type=Path)
    parser.add_argument("output", type=Path)
    parser.add_argument("--locations", nargs="+", choices=LOCATIONS, default=LOCATIONS)
    parser.add_argument("--retest-freeze", type=Path, help="optimized campaign manifest; omit for the original study")
    args = parser.parse_args()
    freeze = json.loads(args.retest_freeze.read_text()) if args.retest_freeze else {}
    revision = freeze["engine"] if freeze else REVISION
    capacity_lock = freeze.get("capacity_lock_sha256")
    pairs, descriptive, capacity, receipts, shard_screens = [], [], {}, {}, {}
    for location in args.locations:
        folder = args.results / location
        warm_measured = not freeze or location in freeze["primary_locations"]
        receipts[location] = input_receipt(folder, revision, warm_measured=warm_measured)
        memory = shard_results(location, folder / "memory.jsonl.gz", drain=bool(freeze))
        peak = memory[1][0][SINGLE_FIELDS.index("max")]
        shard_screens[location] = shard_decision(memory[0], peak)
        checks = [memory, storage_results(location, folder / "storage.jsonl.gz", revision)]
        if location in ("9950x-ext4", "9474f-raid"):
            capacity[location] = capacity_result(location, folder / "capacity" / "trials.jsonl", revision, capacity_lock)
        descriptive.extend(reopen_results(location, folder / "reopen.jsonl.gz"))
        for kind in ("warm", "page", "device"):
            if kind == "warm" and not warm_measured:
                continue
            path = folder / "grouped" / (kind + ".csv")
            if kind == "device" and not path.exists() and location in ("5900x-single", "9474f-single", "9950x-zfs"):
                if location == "9950x-zfs":
                    proof = json.loads((path.parent / "device-qualification.json").read_text())
                    require(proof["accepted_device_cell"] is False and
                            proof["physical_device_read_bytes"] == [0]*4 and
                            proof["arc_demand_misses"] == [0]*4, "ZFS pilot evidence missing")
                continue
            checks.append(read_results(location, path, kind, revision))
        for p, d in checks:
            pairs.extend(p)
            descriptive.extend(d)
        if freeze and location in freeze["primary_locations"]:
            pairs.extend(version_results(location, folder, freeze))
    for label, ref, cand in (("5900x-single-to-raid", "5900x-single","5900x-raid"),
                             ("9474f-single-to-raid", "9474f-single","9474f-raid"),
                             ("9950x-ext4-to-zfs", "9950x-ext4","9950x-zfs")):
        if {ref,cand}.issubset(args.locations):
            path = args.results / (label + ".jsonl.gz")
            header, _ = packet(path)
            for key in ("engine_profile_sha256", "grouped_probe_sha256", "storage_binary_sha256"):
                require(receipts[ref][key] == receipts[cand][key], f"{label}: different {key} on two volumes")
            require(header["binary_sha256"] == receipts[ref]["storage_binary_sha256"], f"{label}: wrong binary")
            pairs.extend(volume_results(label, path, revision))
    reference = receipts[args.locations[0]]
    for loc, receipt in receipts.items():
        for key in ("grouped_small_sst_sha256", "sst_lengths", "residual_wal_bytes"):
            require(receipt[key] == reference[key], f"{loc}: inconsistent {key} across hosts")
    require(len({r["grouped_large_sst_sha256"] for r in receipts.values()
                 if r["grouped_large_sst_sha256"]}) <= 1, "different scattered-device fixtures")
    args.output.mkdir(parents=True, exist_ok=True)
    write_csv(args.output / "paired.csv", PAIRED_FIELDS, pairs)
    write_csv(args.output / "descriptive.csv", SINGLE_FIELDS, descriptive)
    (args.output / "capacity.json").write_text(json.dumps(capacity, indent=2) + "\n")
    (args.output / "inputs.json").write_text(json.dumps(receipts, indent=2) + "\n")
    (args.output / "shard-screens.json").write_text(json.dumps(shard_screens, indent=2) + "\n")


if __name__ == "__main__":
    main()
