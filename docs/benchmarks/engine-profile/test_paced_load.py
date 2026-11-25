"""Paced backlog checks for the actual stream topology."""
import unittest

from summarize import load_kept


class PacedLoadTests(unittest.TestCase):
    def test_short_memory_run_counts_one_just_arrived_batch_per_stream(self):
        for calls_per_stream in (1, 32):
            batch = {"offered": 16 * calls_per_stream, "outstanding_end": 16,
                     "outstanding_half": 0 if calls_per_stream == 1 else 16}
            record = {"mode": "memory", "settings": {"RATE": 10},
                      "classes": {"Batch": batch}}
            self.assertTrue(load_kept(record))
            batch["streams"] = 16
            self.assertTrue(load_kept(record))
            batch["outstanding_end"] = 17
            self.assertFalse(load_kept(record))

    def test_single_stream_and_long_trace_keep_their_existing_limits(self):
        for mode, offered in (("mixed", 79), ("memory", 160_000)):
            limit = max(2, offered // 100)
            batch = {"offered": offered, "outstanding_half": 0, "outstanding_end": limit}
            record = {"mode": mode, "settings": {"RATE": 10}, "classes": {"Batch": batch}}
            self.assertTrue(load_kept(record))
            batch["outstanding_end"] += 1
            self.assertFalse(load_kept(record))
