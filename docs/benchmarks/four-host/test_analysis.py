import csv
import gzip
import json
from pathlib import Path
import tempfile
import unittest

import analyze
import derive_locks as locks
from retest_run import Journal
from retest_storage import blocks
from storage_trials import schedule


class FourHostChecks(unittest.TestCase):
    def test_checkpoint_retries_the_whole_failed_block_and_rejects_drift(self):
        with tempfile.TemporaryDirectory() as where:
            folder = Path(where)
            with Journal(folder, {"binary":"v1"}) as j:
                self.assertEqual(j.block("aa", [1,2], lambda:[10,11]), [10,11])
                with self.assertRaisesRegex(RuntimeError, "interrupted"):
                    j.block("ab", [3,4], lambda: (_ for _ in ()).throw(RuntimeError("interrupted")))
                with self.assertRaises(RuntimeError):
                    with Journal(folder, {"binary":"v1"}):
                        self.fail("a concurrent writer entered the same journal")
            with Journal(folder, {"binary":"v1"}) as j:
                self.assertEqual(j.block("aa", [1,2], lambda:self.fail("re-executed a completed pair")), [10,11])
                self.assertEqual(j.block("ab", [3,4], lambda:[12,13]), [12,13])
            with Journal(folder, {"binary":"v1"}) as j:
                with self.assertRaisesRegex(ValueError, "mismatched checkpoint"):
                    j.block("aa", [1,3], lambda:[1,3])
            with self.assertRaisesRegex(ValueError, "identity differs"):
                with Journal(folder, {"binary":"v2"}):
                    self.fail("resumed another binary")
            self.assertEqual(len(list((folder/"attempts").glob("*.failed.json"))), 1)

    def test_dual_volume_views_keep_distinct_controls_and_reduce_process_count(self):
        schedule_rows = list(blocks(["single", "dual"], "dual", "standard"))
        self.assertEqual(sum(len(work) for _,_,work in schedule_rows), 130)
        views = {}
        for i, block, jobs in schedule_rows:
            for pos, job in enumerate(jobs):
                for packet, (case, arm) in job["views"].items():
                    key = (packet,i,case,arm)
                    self.assertNotIn(key, views)
                    views[key] = (block,pos)
        for i in range(1,6):
            for case in ("sst-none-entropy","wal-sync"):
                ids = [views["volume-pairs.jsonl.gz",i,case,arm] for arm in ("A1","A2","AB1","B")]
                self.assertEqual(len(set(ids)), 4)
        self.assertEqual(sum(len(j) for _,_,j in blocks(["single"], "single", "version")), 20)

    def test_reopen_checks_actual_pool_against_requested_geometry(self):
        with tempfile.TemporaryDirectory() as d:
            path = Path(d) / "reopen.jsonl.gz"
            with gzip.open(path, "wt") as f:
                print(json.dumps({"metadata": {}}), file=f)
                for i in range(1, 6):
                    for ro in (False, True):
                        seed = 20261126 + i
                        params = dict(OPS=100000, PREPARE_MIB=64, BUFFER_MIB=6, IMMUTABLES=4,
                                      POOL_MIB=64, READ_ONLY=str(ro).lower(), SEED=seed)
                        raw = (f"config seed={seed} pool_mib=256\n"
                               "reopen rows=100000 wal_bytes=17203612 open_us=1 open_peak_kib=1000")
                        print(json.dumps(dict(round=i, read_only=ro, settings=params, raw=raw)), file=f)
            with self.assertRaisesRegex(ValueError, "wrong cache pool"):
                analyze.reopen_results("host", path)

    def test_paired_gain_uses_independent_ab_reference(self):
        group = {}
        for i in range(1, 6):
            for arm, ns in (("A1", 100), ("A2", 110), ("AB1", 200), ("B", 160)):
                analyze.add(group, dict(round=i, arm=arm), {"read_p99_ns": ns}, i != 2 or arm != "A2")
        row = dict(zip(analyze.PAIRED_FIELDS, analyze.paired("host", "read", group)[0]))
        self.assertEqual(row["gain_pct"], 20)
        self.assertEqual(row["max_aa_pct"], 10)
        self.assertEqual(row["eligible_pairs"], 4)
        del group[2, "A1"]
        with self.assertRaises(ValueError):
            analyze.paired("host", "read", group)

    def test_io_accounting_alone_cannot_accept_zfs_device(self):
        with tempfile.TemporaryDirectory() as d:
            folder = Path(d)
            probe = folder / "device.csv"
            with probe.open("w") as f:
                csv.writer(f).writerow(["storage_bytes"])
            probe.with_suffix(".metadata.json").write_text(json.dumps(
                dict(build_revision=analyze.REVISION, rows=1048576)))
            (folder / "device-qualification.json").write_text(json.dumps(
                dict(accepted_device_cell=False, physical_device_read_bytes=[0]*4)))
            with self.assertRaisesRegex(ValueError, "corroborating physical I/O"):
                analyze.read_results("9950x-zfs", probe, "device")

    def test_seed_and_work_checked_for_each_codec_and_wal_arm(self):
        with tempfile.TemporaryDirectory() as d:
            path = Path(d) / "storage.jsonl.gz"
            data = []
            for i, seed, case, arm, argv in schedule("both"):
                row = dict(round=i, seed=seed, case=case, arm=arm, arguments=argv)
                if case == "wal":
                    row.update(verified={"rows": 10000}, wal=dict(puts=10000, put_p99_ns=12,
                                                                  puts_us=20, fence_us=1))
                else:
                    row["sst"] = dict(rows=262144,lookups=40000,files=1,sst_bytes=10,
                                      allocated_bytes=20,ingest_us=4,scan_us=5,read_p99_ns=6,
                                      process_read_bytes=0,device_read_sectors=0)
                data.append(row)
            def save():
                with gzip.open(path, "wt") as f:
                    print(json.dumps(dict(metadata=dict(engine_revision=analyze.REVISION))), file=f)
                    for row in data:
                        print(json.dumps(row), file=f)
            save()
            paired, desc = analyze.storage_results("host", path)
            self.assertEqual(len(paired), 10)
            self.assertEqual(len(desc), 6)
            data[0]["seed"] += 1
            save()
            with self.assertRaisesRegex(ValueError, "seed mismatch"):
                analyze.storage_results("host", path)


class FrozenLockTests(unittest.TestCase):
    def test_exporter_keeps_its_registry_mmdb_package(self):
        original = gzip.decompress((Path(__file__).parent / 'exporter.lock.gz').read_bytes())
        derived = locks.derive(original, '5.0.1')
        old_blocks = original.decode().split(locks.PACKAGE)[1:]
        new_blocks = derived.decode().split(locks.PACKAGE)[1:]
        index = next(i for i, b in enumerate(old_blocks) if locks.package_fields(b).get('version') == '4.3.15'
                     and locks.package_fields(b)['name'] == 'mmdb')
        self.assertEqual(old_blocks[index], new_blocks[index])
        self.assertEqual(derived.count(b' "mmdb 4.3.15",'), 1)
        self.assertEqual(derived.count(b' "mmdb 5.0.1",'), 1)
        self.assertNotIn(b' "mmdb 5.0.0",', derived)
        self.assertEqual(locks.derive(derived, '5.0.0'), original)
        with self.assertRaisesRegex(ValueError, 'collide'):
            locks.derive(original, '4.3.15')


if __name__ == "__main__":
    unittest.main()
