"""Run cache-capacity A/A controls and A/B comparisons on a prepared, immutable Linux fixture."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import platform
import shutil
import subprocess
import sys
import tempfile
import time
import uuid


def gauges(path):
    return {k: int(v.split()[0]) for line in path.read_text().splitlines()
            if ':' in line for k, v in [line.split(':', 1)] if v.strip().split()[0].isdigit()}


def schedule(args):
    for rnd in range(1, args.rounds + 1):
        seed = args.seed + (rnd - 1) * 1021
        for pair in (['AA', 'AB'] if rnd % 2 else ['AB', 'AA']):
            arms = [('control', args.small_mib), ('comparison', args.large_mib if pair == 'AB' else args.small_mib)]
            if (rnd + int(pair == 'AB')) % 2 == 0:
                arms.reverse()
            yield rnd, seed, pair, arms


def trial(args, meta, descriptor, run_child):
    rnd, pair, arm, order, seed, capacity = descriptor
    available = gauges(Path('/proc/meminfo'))['MemAvailable']
    if available < (capacity + args.reserve_mib) * 1024:
        raise RuntimeError(f'Need {capacity + args.reserve_mib} MiB available to start the next arm')
    command = ['taskset', '-c', str(args.cpu), meta['binary'], 'trial', meta['fixture'],
               str(capacity), str(args.samples), str(seed), str(args.reserve_mib)]
    start = time.monotonic()
    result = run_child(command, capture_output=True, text=True)
    row = {'round': rnd, 'pair': pair, 'arm': arm, 'order': order, 'seed': seed,
           'capacity_mib': capacity, 'available_before_kib': available,
           'wall_seconds': time.monotonic() - start, 'exit_code': result.returncode,
           'stdout': result.stdout, 'stderr': result.stderr}
    if result.returncode == 0:
        try:
            for line in result.stdout.splitlines():
                phase, *fields = line.split()
                if phase not in ('open', 'warm', 'end', 'measured') or phase in row:
                    raise ValueError(f'unexpected phase {phase}')
                row[phase] = {k: int(v) for k, v in (field.split('=') for field in fields)}
            gauge_fields = {'sst_files', 'sst_bytes', 'sst_resident_bytes', 'segmented_bytes',
                            'overflow_bytes', 'active_bytes', 'immutable_bytes', 'l0_pin_bytes',
                            'rss_kib', 'peak_kib', 'anon_kib', 'file_rss_kib', 'available_kib', 'host_cached_kib'}
            for phase in ('open', 'warm', 'end'):
                if not gauge_fields <= row[phase].keys() or row[phase]['sst_files'] <= 0 or row[phase]['sst_bytes'] <= 0:
                    raise ValueError(f'incomplete {phase} gauges')
            measured = row['measured']
            timing_fields = {'hit_probes', 'miss_probes', 'read_bytes', 'warm_ms', 'service_ns', 'p50_ns', 'p99_ns'}
            if (not timing_fields <= measured.keys() or measured['samples'] != args.samples
                    or measured['seed'] != seed or measured['rows'] != meta['rows']
                    or measured['capacity_mib'] != capacity):
                raise ValueError('trial output differs from scheduled work')
        except (KeyError, ValueError) as error:
            row['invalid_output'] = str(error)
    return row


def run_pair(args, meta, block, run_child, emit):
    rnd, seed, pair, arms = block
    rows = []
    try:
        for order, (arm, capacity) in enumerate(arms, 1):
            row = trial(args, meta, (rnd, pair, arm, order, seed, capacity), run_child)
            rows.append(row)
            if emit:
                emit(row)
            if row['exit_code'] or 'invalid_output' in row:
                raise ValueError('unsuccessful or incomplete capacity trial')
            print(f'round {rnd} {pair} {arm} {capacity} MiB in {row["wall_seconds"]:.1f}s', flush=True)
    except Exception as error:
        # The journal retains this attempt, including a first arm that already succeeded.
        raise RuntimeError(json.dumps({'round': rnd, 'pair': pair, 'error': str(error), 'attempted': rows})) from error
    return rows


def preflight(fincore, fixture):
    first_sst = next(fixture.glob('*.sst'), None)
    if first_sst is None:
        raise ValueError('capacity fixture contains no SST files')
    result = subprocess.check_output([fincore, '--bytes', '--noheadings', '--output', 'RES,SIZE', first_sst], text=True)
    resident, size = map(int, result.split())
    if resident < 0 or size <= 0 or size != first_sst.stat().st_size:
        raise ValueError('fincore did not return resident bytes and the SST file length')


def fixture_identity(fixture, digest):
    # A data rewrite changes ctime even if the caller restores the original mtime.
    # This identifies the immutable local files without prewarming a 64 GiB fixture by hashing its data.
    state = []
    store_files = [p for p in fixture.iterdir() if p.name in ('CURRENT', 'LOCK')
                   or p.name.startswith('MANIFEST-') or p.suffix in ('.wal', '.sst')]
    for path in sorted([*store_files, fixture.with_suffix('.rows')]):
        stat = path.stat()
        entry = [path.name, stat.st_dev, stat.st_ino, stat.st_size, stat.st_mtime_ns, stat.st_ctime_ns]
        if path.name == 'CURRENT' or path.name.startswith('MANIFEST-') or path.suffix == '.rows':
            entry.append(digest(path))
        state.append(entry)
    return hashlib.sha256(json.dumps(state, separators=(',', ':')).encode()).hexdigest()


def publish(output, metadata, rows):
    # Publish only complete blocks. Repeating the command after publication is idempotent.
    with tempfile.NamedTemporaryFile(mode='w', dir=output.parent, delete=False) as out:
        temporary = Path(out.name)
        try:
            for value in [{'metadata': metadata}, *rows]:
                print(json.dumps(value, sort_keys=True), file=out)
            out.flush()
            os.fsync(out.fileno())
            try:
                os.link(temporary, output)
            except FileExistsError:
                if temporary.read_bytes() != output.read_bytes():
                    raise ValueError(f'{output} already contains a different packet') from None
            directory = os.open(output.parent, os.O_RDONLY)
            try:
                os.fsync(directory)
            finally:
                os.close(directory)
        finally:
            temporary.unlink(missing_ok=True)


def checkpoint_run(args, meta, fixture, fincore):
    sys.path.append(str(Path(__file__).resolve().parents[1] / 'four-host'))
    from retest_run import Journal, __file__ as journal_source, digest, run_child

    revision = subprocess.check_output(['git', '-C', args.engine_dir, 'rev-parse', 'HEAD'], text=True).strip()
    subprocess.run(['git', '-C', args.engine_dir, 'diff', '--quiet', 'HEAD', '--', 'src', 'Cargo.toml'], check=True)
    cpuinfo = Path('/proc/cpuinfo')
    cpu_lines = cpuinfo.read_text().splitlines() if cpuinfo.exists() else []
    identity = {**meta, 'engine_revision': revision, 'cargo_lock_sha256': digest(args.lockfile),
                'runner_sha256': digest(__file__), 'journal_sha256': digest(journal_source),
                'fixture_state_sha256': fixture_identity(fixture, digest),
                'cpu_model': next((s.split(':', 1)[1].strip() for s in cpu_lines
                                   if s.startswith('model name')), platform.machine()),
                'fincore_sha256': digest(fincore), 'page_size': os.sysconf('SC_PAGESIZE'),
                'hostname': platform.node()}
    meta.update({k: identity[k] for k in ('engine_revision', 'cargo_lock_sha256', 'fixture_state_sha256')})
    with Journal(args.checkpoint_dir, identity) as journal:
        preflight(fincore, fixture)
        rows = []
        for block in schedule(args):
            rnd, seed, pair, arms = block
            spec = {'round': rnd, 'seed': seed, 'pair': pair, 'arms': arms}
            # Keep completed arm output if the controller stops before the second arm or the commit.
            attempt = args.checkpoint_dir / 'attempts' / f'{rnd}-{pair}-{uuid.uuid4().hex}.arms.jsonl'

            def execute_pair():
                with attempt.open('x') as stream:
                    def observe(row):
                        print(json.dumps(row), file=stream, flush=True)
                        os.fsync(stream.fileno())
                    return run_pair(args, meta, block, run_child, observe)

            rows.extend(journal.block([rnd, pair], spec, execute_pair))
            attempt.unlink(missing_ok=True)
        if fixture_identity(fixture, digest) != identity['fixture_state_sha256']:
            raise ValueError('capacity fixture changed during the run')
        publish(args.output, meta, rows)


def main():
    cli = argparse.ArgumentParser(description=__doc__)
    cli.add_argument('binary', type=Path)
    cli.add_argument('fixture', type=Path)
    cli.add_argument('output', type=Path)
    cli.add_argument('--small-mib', type=int, default=4096)
    cli.add_argument('--large-mib', type=int, default=98304)
    cli.add_argument('--reserve-mib', type=int, default=32768)
    cli.add_argument('--samples', type=int, default=100000)
    cli.add_argument('--rounds', type=int, default=5)
    cli.add_argument('--seed', type=int, default=20261125)
    cli.add_argument('--cpu', type=int, default=4)
    cli.add_argument('--checkpoint-dir', type=Path, help='resume completed A/A and A/B pairs from this directory')
    cli.add_argument('--engine-dir', type=Path, help='linked MMDB checkout for checkpoint provenance')
    cli.add_argument('--lockfile', type=Path, help='Cargo.lock used to build the capacity binary')
    args = cli.parse_args()
    if not 0 < args.small_mib < args.large_mib or min(args.samples, args.rounds, args.reserve_mib) <= 0:
        cli.error('capacities must be ordered and counts positive')
    if any((args.checkpoint_dir, args.engine_dir, args.lockfile)) and not all((args.checkpoint_dir, args.engine_dir, args.lockfile)):
        cli.error('use --checkpoint-dir, --engine-dir and --lockfile together')
    binary, fixture = args.binary.resolve(strict=True), args.fixture.resolve(strict=True)
    metadata = {k: str(v) if isinstance(v, Path) else v for k, v in vars(args).items()
                if k not in ('checkpoint_dir', 'engine_dir', 'lockfile')}
    metadata.update({'binary': str(binary), 'fixture': str(fixture),
                     'binary_sha256': hashlib.sha256(binary.read_bytes()).hexdigest(),
                     'kernel': platform.release(), 'rows': int(fixture.with_suffix('.rows').read_text()),
                     'filesystem': subprocess.check_output(['findmnt', '-n', '-T', fixture, '-o', 'SOURCE,TARGET,FSTYPE,OPTIONS'], text=True).strip()})
    fincore = shutil.which('fincore')
    if fincore is None:
        cli.error('fincore is required to measure SST page residency')
    if args.checkpoint_dir:
        checkpoint_run(args, metadata, fixture, fincore)
    else:
        preflight(fincore, fixture)
        with args.output.open('x', buffering=1) as out:
            def emit(row):
                print(json.dumps(row), file=out)
            emit({'metadata': metadata})
            for block in schedule(args):
                run_pair(args, metadata, block, subprocess.run, emit)


if __name__ == '__main__':
    main()
