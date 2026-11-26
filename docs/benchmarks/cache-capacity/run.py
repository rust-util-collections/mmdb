"""Run cache-capacity A/A controls and A/B comparisons on a prepared, immutable Linux fixture."""
import argparse
import hashlib
import json
from pathlib import Path
import platform
import subprocess
import time


def gauges(path):
    return {k: int(v.split()[0]) for line in path.read_text().splitlines()
            if ':' in line for k, v in [line.split(':', 1)] if v.strip().split()[0].isdigit()}


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
    args = cli.parse_args()
    if not 0 < args.small_mib < args.large_mib or min(args.samples, args.rounds, args.reserve_mib) <= 0:
        cli.error('capacities must be ordered and counts positive')
    binary, fixture = args.binary.resolve(), args.fixture.resolve()
    metadata = {**vars(args), 'binary': binary, 'fixture': fixture,
                'binary_sha256': hashlib.sha256(binary.read_bytes()).hexdigest(),
                'kernel': platform.release(), 'rows': int(fixture.with_suffix('.rows').read_text()),
                'filesystem': subprocess.check_output(['findmnt', '-n', '-T', fixture, '-o', 'SOURCE,TARGET,FSTYPE,OPTIONS'], text=True).strip()}
    with args.output.open('x', buffering=1) as out:
        def emit(row):
            print(json.dumps(row, default=str), file=out)
        emit({'metadata': metadata})
        for rnd in range(1, args.rounds + 1):
            seed = args.seed + (rnd - 1) * 1021
            pairs = ['AA', 'AB'] if rnd % 2 else ['AB', 'AA']
            for pair in pairs:
                arms = [('control', args.small_mib), ('comparison', args.large_mib if pair == 'AB' else args.small_mib)]
                if (rnd + int(pair == 'AB')) % 2 == 0:
                    arms.reverse()
                for order, (arm, capacity) in enumerate(arms, 1):
                    available = gauges(Path('/proc/meminfo'))['MemAvailable']
                    if available < (capacity + args.reserve_mib) * 1024:
                        raise RuntimeError(f'Need {capacity + args.reserve_mib} MiB available to start the next arm')
                    command = ['taskset', '-c', str(args.cpu), str(binary), 'trial', str(fixture),
                               str(capacity), str(args.samples), str(seed), str(args.reserve_mib)]
                    start = time.monotonic()
                    result = subprocess.run(command, capture_output=True, text=True)
                    row = {'round': rnd, 'pair': pair, 'arm': arm, 'order': order, 'seed': seed,
                           'capacity_mib': capacity, 'available_before_kib': available,
                           'wall_seconds': time.monotonic() - start, 'exit_code': result.returncode,
                           'stdout': result.stdout, 'stderr': result.stderr}
                    if not result.returncode:
                        for line in result.stdout.splitlines():
                            phase, *fields = line.split()
                            row[phase] = {k: int(v) for field in fields for k, v in [field.split('=')]}
                        measured = row['measured']
                        assert measured['samples'] == args.samples and measured['seed'] == seed
                        assert measured['rows'] == metadata['rows'] and measured['capacity_mib'] == capacity
                    emit(row)
                    result.check_returncode()
                    print(f'round {rnd} {pair} {arm} {capacity} MiB in {row["wall_seconds"]:.1f}s', flush=True)


if __name__ == '__main__':
    main()
