"""Summarize independent cache-capacity pairs and check their evidence gates."""
import argparse
import json
from pathlib import Path
from statistics import median


def summarize(path, min_large_mib, max_small_mib):
    with path.open() as f:
        meta = json.loads(next(f))['metadata']
        trials = [json.loads(line) for line in f]
    rounds = meta['rounds']
    pairs = {(t['round'], t['pair'], t['arm']): t for t in trials}
    expected = {(i, pair, arm) for i in range(1, rounds + 1)
                for pair in ('AA', 'AB') for arm in ('control', 'comparison')}
    if len(trials) != len(expected) or set(pairs) != expected:
        raise ValueError('incomplete or duplicate comparison arms')
    def charge(t):
        return t['warm']['segmented_bytes'] + t['warm']['overflow_bytes']
    def hits(t):
        m = t['measured']
        return m['hit_probes'] / (m['hit_probes'] + m['miss_probes'])
    for i, pair, arm in sorted(expected):
        t = pairs[i, pair, arm]
        if t['exit_code'] or 'invalid_output' in t:
            raise ValueError(f'unsuccessful result for round {i} {pair} {arm}')
        m = t['measured']
        cap = meta['large_mib'] if (pair, arm) == ('AB', 'comparison') else meta['small_mib']
        seed = meta['seed'] + (i - 1) * 1021
        if (t['seed'] != seed or m['seed'] != seed or t['capacity_mib'] != cap or m['capacity_mib'] != cap
                or m['samples'] != meta['samples'] or m['rows'] != meta['rows']):
            raise ValueError(f'unmatched result for round {i} {pair} {arm}')
        other = pairs[i, pair, 'comparison' if arm == 'control' else 'control']
        if sorted([t['order'], other['order']]) != [1, 2]:
            raise ValueError('invalid process order')
    def gain(r, pair):
        a, b = (pairs[r, pair, arm]['measured']['p99_ns'] for arm in ('control', 'comparison'))
        return 100 * (1 - b / a)
    gains = [gain(i, 'AB') for i in range(1, rounds + 1)]
    noise = max(abs(gain(i, 'AA')) for i in range(1, rounds + 1))
    kinds = {'small': [r for r in trials if r['capacity_mib'] == meta['small_mib']],
             'large': [r for r in trials if r['capacity_mib'] == meta['large_mib']]}
    stats = {}
    for kind, records in kinds.items():
        ab = [r for r in records if r['pair'] == 'AB']
        stats[kind] = {
            'median_ab_p99_ns': median(r['measured']['p99_ns'] for r in ab),
            'median_ab_p50_ns': median(r['measured']['p50_ns'] for r in ab),
            'median_ab_warm_charge_bytes': median(charge(r) for r in ab),
            'min_warm_charge_bytes': min(map(charge, records)),
            'max_warm_charge_bytes': max(map(charge, records)),
            'median_ab_hit_fraction': median(map(hits, ab)),
            'min_hit_fraction': min(map(hits, records)),
            'max_random_read_bytes': max(r['measured']['read_bytes'] for r in records),
            'min_sst_resident_fraction': min(min(1, r[p]['sst_resident_bytes'] / r[p]['sst_bytes'])
                                             for p in ('warm', 'end') for r in records),
            'median_ab_end_rss_kib': median(r['end']['rss_kib'] for r in ab),
            'max_process_peak_rss_kib': max(r['end']['peak_kib'] for r in records),
            'min_observed_host_available_kib': min(r[p]['available_kib'] for r in records for p in ('open','warm','end')),
        }
    checks = {
        'all_file_residencies_at_least_99pct': all(s['min_sst_resident_fraction'] >= .99 for s in stats.values()),
        'no_measured_disk_reads': all(s['max_random_read_bytes'] == 0 for s in stats.values()),
        'large_warm_charge_at_least_threshold': stats['large']['min_warm_charge_bytes'] >= min_large_mib << 20,
        'small_warm_charge_within_bound': stats['small']['max_warm_charge_bytes'] <= max_small_mib << 20,
        'large_hit_fraction_at_least_95pct': stats['large']['min_hit_fraction'] >= .95,
        'median_latency_gain_at_least_10pct': median(gains) >= 10,
        'five_pairs_same_direction_exceed_aa': rounds == 5 and all(g > 0 for g in gains) and median(gains) > noise,
    }
    return {'rows': meta['rows'], 'samples_per_arm': meta['samples'], 'rounds': rounds,
            'small_mib': meta['small_mib'], 'large_mib': meta['large_mib'], 'minimum_large_charge_mib': min_large_mib,
            'maximum_small_charge_mib': max_small_mib, 'arms': stats, 'paired_p99_gain_pct': gains,
            'median_p99_gain_pct': median(gains), 'maximum_absolute_aa_pct': noise, 'checks': checks,
            'p99_benefit_demonstrated': all(checks.values())}


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('packet', type=Path)
    parser.add_argument('--minimum-large-charge-mib', type=int, default=49152)
    parser.add_argument('--maximum-small-charge-mib', type=int, default=5120)
    args = parser.parse_args()
    print(json.dumps(summarize(args.packet, args.minimum_large_charge_mib, args.maximum_small_charge_mib), indent=2))
