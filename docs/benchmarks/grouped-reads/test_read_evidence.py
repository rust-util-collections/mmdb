import unittest

from read_evidence import all_request_p99, expected_table_probes, has_cache_evidence, verify_block_reuse


class CacheRegimeTests(unittest.TestCase):
    def test_advisory_eviction_needs_per_request_evidence(self):
        r = dict(regime="device", samples=100, device_requests=0, device_p99_ns=0, storage_bytes=0)
        self.assertFalse(has_cache_evidence(r))
        r.update(storage_bytes=32_768, device_requests=94)
        self.assertFalse(has_cache_evidence(r))
        r.update(device_requests=95, device_p99_ns=1200000)
        self.assertFalse(has_cache_evidence(r))  # 95 requests could not each have read 4 KiB.
        r.update(storage_bytes=95 * 4096)
        self.assertTrue(has_cache_evidence(r))
        r.update(device_requests=101)
        self.assertFalse(has_cache_evidence(r))
        r.update(storage_bytes=0, device_requests=95)
        self.assertFalse(has_cache_evidence(r))

    def test_resident_arms_exclude_storage_reads(self):
        for regime in ["warm", "page"]:
            self.assertTrue(has_cache_evidence(dict(regime=regime, storage_bytes=0)))
            self.assertFalse(has_cache_evidence(dict(regime=regime, storage_bytes=4096)))


class DecisionMetricTests(unittest.TestCase):
    def test_same_complete_trace_with_different_io_subsets(self):
        a = dict(samples=10000, p99_ns=1234, device_requests=9500, device_p99_ns=4000)
        b = dict(samples=10000, p99_ns=1234, device_requests=10000, device_p99_ns=1000)
        self.assertEqual(all_request_p99(a), all_request_p99(b))
        with self.assertRaises(ValueError):
            all_request_p99(dict(a, samples=9999))

    def test_same_scattered_keys_are_probed_once(self):
        row = dict(method="grouped", locality="scattered", fanout=8, samples=1, seed=0)
        # The first eight sampled IDs on a two-row fixture include both 0 and 1.
        self.assertEqual(expected_table_probes(row, dict(rows=2, coalesces_duplicates=True)), 2)
        self.assertEqual(expected_table_probes(row, dict(rows=2, coalesces_duplicates=False)), 8)


    def test_clustered_timing_alone_cannot_prove_block_reuse(self):
        control = dict(blocks=64)
        candidate = dict(blocks=64, locality="clustered", fanout=32)
        with self.assertRaises(ValueError):
            verify_block_reuse(control, candidate)
        candidate.update(blocks=4)
        verify_block_reuse(control, candidate)


class SummarizeCsvTests(unittest.TestCase):
    def test_summarize_rejects_empty_rogue_or_mismatched_schedule(self):
        import csv
        import json
        from pathlib import Path
        import shutil
        import tempfile
        from summarize import summarize_file

        src_csv = Path(__file__).resolve().parent / "results/final/primary.csv"
        src_meta = src_csv.with_suffix(".metadata.json")
        self.assertEqual(len(summarize_file(src_csv)), 1)

        def mutate_and_run(mutate_rows=None, mutate_meta=None):
            with tempfile.TemporaryDirectory() as d:
                dst_csv = Path(d) / "primary.csv"
                dst_meta = dst_csv.with_suffix(".metadata.json")
                shutil.copyfile(src_csv, dst_csv)
                shutil.copyfile(src_meta, dst_meta)
                if mutate_meta is not None:
                    meta = json.loads(dst_meta.read_text())
                    mutate_meta(meta)
                    dst_meta.write_text(json.dumps(meta))
                if mutate_rows is not None:
                    with dst_csv.open() as f:
                        reader = csv.DictReader(f)
                        fieldnames = reader.fieldnames
                        rows = list(reader)
                    rows = mutate_rows(rows)
                    with dst_csv.open("w", newline="") as f:
                        writer = csv.DictWriter(f, fieldnames=fieldnames)
                        writer.writeheader()
                        writer.writerows(rows)
                return summarize_file(dst_csv)

        # Empty CSV must fail instead of succeeding with zero output.
        with self.assertRaises(ValueError):
            mutate_and_run(mutate_rows=lambda rows: [])

        # Extra unrequested arm or round must fail instead of being silently ignored.
        with self.assertRaises(ValueError):
            mutate_and_run(mutate_rows=lambda rows: rows + [{**rows[0], "arm": "rogue"}])
        with self.assertRaises(ValueError):
            mutate_and_run(mutate_rows=lambda rows: rows + [{**rows[0], "round": "6"}])

        # Samples differing from metadata samples_per_run must fail.
        with self.assertRaises(ValueError):
            mutate_and_run(mutate_meta=lambda m: m.update(samples_per_run=500_000))

        # Swapped/wrong method must raise ValueError, not AssertionError.
        def bad_method(rows):
            target = next(r for r in rows if r["pair"] == "AB" and r["round"] == "1" and r["arm"] == "comparison")
            target["method"] = "scalar"
            target["tables"] = next(
                r["tables"] for r in rows if r["pair"] == "AB" and r["round"] == "1" and r["arm"] == "baseline"
            )
            return rows

        with self.assertRaises(ValueError):
            mutate_and_run(mutate_rows=bad_method)
