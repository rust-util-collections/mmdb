"""Capacity checkpoint tests using tiny fixtures and deterministic fake child processes."""
import argparse
import contextlib
import io
import json
import os
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest import mock

import run
from summarize import summarize


class ResumeTests(unittest.TestCase):
    def setUp(self):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.root = Path(tmp.name)
        self.fixture = self.root / 'fixture'
        self.fixture.mkdir()
        (self.fixture / '1.sst').write_bytes(b'abc')
        self.fixture.with_suffix('.rows').write_text('8')
        self.args = argparse.Namespace(engine_dir=self.root / 'engine', checkpoint_dir=self.root / 'journal',
                                       lockfile=self.root / 'Cargo.lock', output=self.root / 'out.jsonl',
                                       rounds=2, seed=7, small_mib=2, large_mib=4, reserve_mib=5, cpu=0, samples=2)
        self.args.lockfile.write_text('fixture lock')
        subprocess.run(['git', 'init', '-q', self.args.engine_dir], check=True)
        subprocess.run(['git', '-c', 'user.name=Test', '-c', 'user.email=test@example.invalid',
                        '-C', self.args.engine_dir, 'commit', '-q', '--allow-empty', '-m', 'fixture'], check=True)
        self.fincore = self.script('fincore', 'printf "0 3\\n"')
        self.script('taskset', 'shift 2; exec "$@"')
        self.probe = self.script('probe', '''n=$(cat "$COUNT")
    n=$((n+1)); echo "$n" >"$COUNT"
    if [ "$n" = "$FAIL_AT" ]; then echo 'injected failure' >&2; exit 9; fi
    for phase in open warm end; do echo "$phase sst_files=1 sst_bytes=3 sst_resident_bytes=3 segmented_bytes=1 overflow_bytes=0 active_bytes=0 immutable_bytes=0 l0_pin_bytes=0 rss_kib=1 peak_kib=1 anon_kib=1 file_rss_kib=0 available_kib=1000 host_cached_kib=25"; done
    echo "measured capacity_mib=$3 samples=$4 seed=$5 rows=8 hit_probes=2 miss_probes=0 read_bytes=0 warm_ms=1 service_ns=7 p50_ns=3 p99_ns=4"''')
        self.count = self.root / 'count'
        self.count.write_text('0')
        self.addCleanup(mock.patch.stopall)
        mock.patch.dict(os.environ, {'PATH': str(self.root) + os.pathsep + os.environ['PATH'],
                                     'COUNT': str(self.count), 'FAIL_AT': '4',
                                     'MMDB_TRIAL_HOST_LOCK': str(self.root / 'host.lock')}).start()
        mock.patch.object(run, 'gauges', return_value={'MemAvailable': 1 << 30}).start()

    def script(self, name, source):
        path = self.root / name
        path.write_text('#!/bin/sh\n' + source + '\n')
        path.chmod(0o755)
        return path

    def invoke(self):
        meta = {'binary': str(self.probe), 'fixture': str(self.fixture), 'rows': 8,
                'binary_sha256': run.hashlib.sha256(self.probe.read_bytes()).hexdigest(),
                **{k: getattr(self.args, k) for k in ('rounds', 'seed', 'small_mib', 'large_mib', 'samples', 'reserve_mib', 'cpu')}}
        with contextlib.redirect_stdout(io.StringIO()):
            run.checkpoint_run(self.args, meta, self.fixture, self.fincore)

    def test_resume_retries_both_arms_of_failed_pair(self):
        with self.assertRaisesRegex(RuntimeError, 'injected failure'):
            self.invoke()
        self.assertFalse(self.args.output.exists())
        self.assertEqual(len(list((self.args.checkpoint_dir / 'blocks').glob('*.json'))), 1)
        attempt, = (self.args.checkpoint_dir / 'attempts').glob('*.arms.jsonl')
        self.assertEqual([json.loads(s)['exit_code'] for s in attempt.read_text().splitlines()], [0, 9])
        self.invoke()
        self.assertEqual(int(self.count.read_text()), 10)
        packet = self.args.output.read_bytes()
        rows = [json.loads(s) for s in packet.splitlines()][1:]
        self.assertEqual(len(rows), 8)
        self.assertTrue(all(row['exit_code'] == 0 for row in rows))
        self.invoke()
        self.assertEqual(int(self.count.read_text()), 10)
        self.assertEqual(self.args.output.read_bytes(), packet)
        for key in ('cpu', 'seed', 'large_mib', 'reserve_mib', 'samples', 'rounds'):
            with self.subTest(key=key), mock.patch.object(self.args, key, getattr(self.args, key) + 1):
                with self.assertRaises(ValueError):
                    self.invoke()
        self.args.lockfile.write_text('new lock')
        with self.assertRaises(ValueError):
            self.invoke()
        self.assertEqual(int(self.count.read_text()), 10)

    def test_fincore_preflight_and_fixture_identity(self):
        self.fincore = self.script('fincore', 'echo "RES unavailable"')
        with self.assertRaises(ValueError):
            self.invoke()
        self.assertEqual(int(self.count.read_text()), 0)
        sst = self.fixture / '1.sst'
        first = run.fixture_identity(self.fixture, lambda p: p.read_bytes().hex())
        original = sst.stat()
        sst.write_bytes(b'def')
        os.utime(sst, ns=(original.st_atime_ns, original.st_mtime_ns))
        self.assertNotEqual(first, run.fixture_identity(self.fixture, lambda p: p.read_bytes().hex()))

    def test_summary_rejects_invalid_or_mislabeled_arms(self):
        with mock.patch.dict(os.environ, {'FAIL_AT': '-1'}):
            self.invoke()
        packet = [json.loads(line) for line in self.args.output.read_text().splitlines()]
        self.assertEqual(summarize(self.args.output, 0, 2)['rounds'], self.args.rounds)
        for field, value in [('invalid_output', 'unexpected phase'), ('capacity_mib', 7), ('seed', -1)]:
            with self.subTest(field=field):
                rows = [*packet[:-1], {**packet[-1], field: value}]
                self.args.output.write_text(''.join(json.dumps(row) + '\n' for row in rows))
                with self.assertRaises(ValueError):
                    summarize(self.args.output, 0, 2)


if __name__ == '__main__':
    unittest.main()
