"""Small deterministic tests for the evidence summaries, without timed engine runs."""
import csv
import io
import unittest

from summarize import summarize


def summary(suite, records, rounds=5):
    out = io.StringIO()
    summarize({"suite": suite, "rounds": rounds, "binary_sha256": {}}, records, csv.writer(out))
    return list(csv.DictReader(io.StringIO(out.getvalue())))


def handoff_trials():
    records = []
    for sync in ("false", "true"):
        for case in ("same-db", "writer", "reader", "writer-paced", "reader-paced"):
            kind = case.split("-")[0]
            ops = {"writer": ["SmallPut"], "reader": ["Read"]}.get(
                kind, ["SmallPut", "Batch", "Read", "FreshSnapshot", "HeldSnapshot"])
            for i in range(1, 6):
                for arm, p99 in (("A1", 100), ("B", 80), ("A2", 102)):
                    classes = {}
                    for op in ops:
                        classes[op] = {"offered": 10_000, "completed": 10_000,
                                       "rejected": 0, "sampled": 10_000,
                                       "bytes": 1000 if op in ("SmallPut", "Batch") else 0,
                                       "outstanding_half": 1, "outstanding_end": 1, "arrival_p99_us": p99}
                    records.append({"mode": "mixed", "case": f"{case}-{sync}", "round": i, "arm": arm,
                                    "settings": {"OPS": 10_000, "SEED": i, "SYNC": sync,
                                                 "CONTROL": kind if kind in ("writer", "reader") else "mixed"},
                                    "run": {"seconds": 1, "peak_kib": 600, "compaction_output_bytes": 500},
                                    "classes": classes})
    return records


class SummaryTest(unittest.TestCase):
    def test_paired_gain_noise_and_logical_bytes(self):
        rows = {(r["case"], r["metric"]): r for r in summary("handoff", handoff_trials())}
        primary = rows["same-db-false", "SmallPut_p99_us"]
        self.assertEqual(primary["status"], "primary-met")
        self.assertEqual(float(primary["paired_gain_pct"]), 20)
        self.assertEqual(float(primary["aa_max_abs_pct"]), 2)
        self.assertEqual(float(primary["gain_min_pct"]), 20)
        self.assertEqual(float(primary["gain_max_pct"]), 20)
        # 500 newly written SST bytes over 2,000 logical write bytes.
        self.assertEqual(float(rows["same-db-false", "sst_output_per_logical_byte"]["arm_median"]), 0.25)
        self.assertEqual(rows["reader-false", "sst_output_per_logical_byte"]["status"], "undefined-denominator")

    def test_reservoir_size_controls_p99_qualification(self):
        records = handoff_trials()
        candidate = next(r for r in records if (r["case"], r["round"], r["arm"]) == ("same-db-false", 3, "B"))
        candidate["classes"]["SmallPut"]["sampled"] = 9_999
        rows = summary("handoff", records)
        p99 = next(r for r in rows if (r["case"], r["metric"]) == ("same-db-false", "SmallPut_p99_us"))
        self.assertEqual((p99["eligible_pairs"], p99["paired_gain_pct"], p99["status"]),
                         ("4", "", "insufficient-p99-samples"))

    def test_reject_missing_pair_or_inconsistent_settings(self):
        records = handoff_trials()
        for excluded in (lambda r: r["case"] == "reader-paced-false",
                         lambda r: r["round"] == 5,
                         lambda r: (r["case"], r["round"], r["arm"]) == ("reader-true", 3, "B")):
            with self.assertRaises(ValueError):
                summary("handoff", [r for r in records if not excluded(r)])
        candidate = next(r for r in records if r["arm"] == "B")
        candidate["settings"]["SEED"] += 1
        with self.assertRaisesRegex(ValueError, "mismatched SEED"):
            summary("handoff", records)

    def test_memory_reports_candidate_and_both_reference_rss_peaks_separately(self):
        records = []
        sweeps = {"buffer": ((4, 16), (1, 16), (2, 16)), "envelope": ((4, 6), (1, 15), (2, 10))}
        for sweep, (control, *candidates) in sweeps.items():
            for placement in ("hot", "all"):
                for q, b in (control, *candidates):
                    for name in (["A1", "A2"] if (q, b) == control else [f"q{q}-b{b}"]):
                        for load in ("paced", "saturation"):
                            # Only the second control run crosses 1 GiB.
                            rss = (1 << 20) + 1 if name == "A2" else (1 << 20) - 1
                            classes = {op: {"offered": 10000, "completed": 10000, "sampled": 10000,
                                            "bytes": 1000, "rejected": 0, "outstanding_half": 0,
                                            "outstanding_end": 0, "arrival_p99_us": 100}
                                       for op in ("SmallPut", "Batch", "Read")}
                            rec = {"case": f"{sweep}-{placement}-{load}", "arm": name, "round": 1,
                                   "mode": "memory", "settings": {"IMMUTABLES": q, "BUFFER_MIB": b},
                                   "run": {"seconds": 1, "peak_kib": rss, "compaction_output_bytes": 50},
                                   "classes": classes}
                            records.append(rec)
                    records.append({**rec, "round": 0, "case": f"gauge-{sweep}-{placement}",
                                    "arm": f"q{q}-b{b}", "sampled_peaks": {"active_bytes": 10}})
        rss_rows = [r for r in summary("memory", records, rounds=1) if r["metric"] == "peak_kib"]
        self.assertEqual(len(rss_rows), 16)
        for r in rss_rows:
            self.assertEqual((int(r["reference_max"]), int(r["arm_max"])), ((1 << 20) + 1, (1 << 20) - 1))
            self.assertEqual((r["reference_rss_status"], r["arm_rss_status"], r["matched_rss_budget"]),
                             ("over", "within", "limited"))

    def test_reopen_separates_geometry_and_mode(self):
        records = []
        for size in (10_000, 100_000):
            for q, b, read_only in [(4, 16, False), (2, 16, False), (1, 16, False),
                                    (1, 15, False), (2, 10, False), (4, 6, False), (4, 6, True)]:
                for i in range(1, 6):
                    records.append({"case": "read-only" if read_only else "durable-residual-wal",
                                    "arm": str(size) if read_only else f"q{q}-b{b}", "round": i,
                                    "settings": {"OPS": size, "BUFFER_MIB": b, "IMMUTABLES": q,
                                                 "PREPARE_MIB": 64, "READ_ONLY": str(read_only).lower()},
                                    "reopen": {"rows": size, "wal_bytes": size * 160, "open_us": b + i,
                                               "open_rss_kib": 512, "open_peak_kib": 600,
                                               "after_verify_peak_kib": 800}})
        rows = summary("reopen", records)
        self.assertEqual(len(rows), 14)
        self.assertTrue(all(int(r["repetitions"]) == 5 for r in rows))
        for r in rows:
            self.assertEqual(float(r["open_us_median"]), int(r["buffer_mib"]) + 3)
        with self.assertRaisesRegex(ValueError, "incomplete reopen"):
            summary("reopen", records[:-1])
        records[-1]["reopen"]["wal_bytes"] += 1
        with self.assertRaisesRegex(ValueError, "unequal residual WAL"):
            summary("reopen", records)
