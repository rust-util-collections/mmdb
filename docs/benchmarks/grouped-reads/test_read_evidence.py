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
