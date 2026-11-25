import unittest
from replay import Call, Group, replay


class StageReplayTest(unittest.TestCase):
    def test_small_wal_can_hide_behind_bulk_apply_only_within_one_generation(self):
        calls = {
            "Batch": [Call(0, 5, 90)],
            "SmallPut": [Call(10, 12, 116), Call(150, 151, 160)],
        }
        bulk = Group(1, 1, 4096, 1, 10, 10, 20, 20, 90, 90)
        for generation, overlap in [(1, 25), (2, 0)]:
            small = Group(4097, 1, 1, generation, 90, 90, 115, 115, 116, 116)
            later = Group(4098, 1, 1, generation, 151, 151, 158, 158, 160, 160)
            groups = [bulk, small, later]
            serial = replay(calls, groups, 4096, False)
            ideal = replay(calls, groups, 4096, True)
            self.assertEqual(serial["small_p99_ns"],106)
            self.assertEqual(ideal["small_p99_ns"],106-overlap)
            self.assertEqual(ideal["overlapping_ns"],overlap)

    def test_group_with_both_issuers_cannot_hide_later_calls(self):
        calls = {
            "Batch": [Call(0, 2, 90)],
            "SmallPut": [Call(0, 1, 92), Call(50, 93, 103)],
        }
        joint = Group(1, 2, 4097, 1, 2, 2, 62, 62, 90, 90)
        later = Group(4098, 1, 1, 1, 93, 93, 99, 99, 103, 103)
        serial = replay(calls, [joint, later], 4096, False)
        ideal = replay(calls, [joint, later], 4096, True)
        self.assertEqual(serial["small_p99_ns"],ideal["small_p99_ns"])
        self.assertEqual(ideal["overlap_groups"],0)


if __name__ == '__main__':
    unittest.main()
