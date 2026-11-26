import csv
import gzip
import json
from pathlib import Path
import tempfile
import unittest

import analyze
from storage_trials import schedule


class FourHostChecks(unittest.TestCase):
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


if __name__ == "__main__":
    unittest.main()
