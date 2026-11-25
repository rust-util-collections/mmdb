"""Collect five release-matched shadow traces and tracing-disabled controls."""
import argparse
import gzip
import hashlib
import json
import os
from pathlib import Path
import subprocess
from replay import load, replay


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--binary", required=True, type=Path)
    ap.add_argument("--cpus", required=True)
    ap.add_argument("--tmpdir", required=True, type=Path)
    ap.add_argument("--out", required=True, type=Path)
    ap.add_argument("--rate", required=True, type=int)
    ap.add_argument("--seed", default=20261125, type=int)
    args = ap.parse_args()
    assert args.rate > 0
    args.out.mkdir(exist_ok=True,parents=True)
    binary=args.binary.resolve()
    sha=hashlib.sha256(binary.read_bytes()).hexdigest()
    with (args.out/'summary.jsonl').open('x') as logs:
        for rep in range(1,6):
            for order,traced in enumerate((True,False) if rep%2 else (False,True),1):
                env={k:v for k,v in os.environ.items() if not k.startswith('MMDB_PROFILE_') and k!='MMDB_GROUP_TRACE'}
                env.update(TMPDIR=str(args.tmpdir.resolve()),MMDB_PROFILE_RATE=str(args.rate),
                           MMDB_PROFILE_SEED=str(args.seed+rep),MMDB_PROFILE_POOL_MIB='64',
                           MMDB_PROFILE_BUFFER_MIB='16', MMDB_PROFILE_IMMUTABLES='4',
                           MMDB_PROFILE_OPS='10000',MMDB_PROFILE_BATCH_ENTRIES='4096',
                           MMDB_PROFILE_BATCH_RATIO='128',MMDB_PROFILE_SYNC='false',
                           MMDB_PROFILE_ENTROPY='true',MMDB_PROFILE_PLACEMENT='hot',
                           MMDB_PROFILE_SAMPLE='false',MMDB_PROFILE_CONTROL='mixed')
                raw=args.out/f'trace-{rep}.csv'
                if traced:env['MMDB_GROUP_TRACE']=str(raw.resolve())
                proc=subprocess.run(['taskset','-c',args.cpus,str(binary),'mixed'],
                                    env=env,text=True,capture_output=True,timeout=300,check=True)
                outputs={}
                for line in proc.stdout.splitlines():
                    if line.startswith(('class ','run ')):
                        fields=dict(word.split('=',1) for word in line.split() if '=' in word)
                        outputs[fields.get('op','run')]=fields
                record={'rep':rep,'order':order,'traced':traced,'seed':args.seed+rep,
                        'rate':args.rate,'executable_sha256':sha,'cpus':args.cpus,'measurements':outputs}
                if traced:
                    calls,groups=load(raw)
                    assert len(calls['SmallPut'])==10000 and len(calls['Batch'])==79
                    actual=replay(calls,groups,4096,False)
                    ideal=replay(calls,groups,4096,True)
                    sensitivity=replay(calls,groups,4096,True,True)
                    assert abs(actual['small_p99_ns']-1000*float(outputs['SmallPut']['arrival_p99_us']))<=1
                    record.update(groups=len(groups),observed=actual,projection=ideal,sensitivity=sensitivity,
                                  ideal_p99_gain_percent=100*(1-ideal['small_p99_ns']/actual['small_p99_ns']),
                                  sensitivity_p99_gain_percent=100*(1-sensitivity['small_p99_ns']/actual['small_p99_ns']),
                                  trace_sha256=hashlib.sha256(raw.read_bytes()).hexdigest())
                    with gzip.open(str(raw)+'.gz','wb',compresslevel=3) as f:f.write(raw.read_bytes())
                    raw.unlink()
                logs.write(json.dumps(record)+'\n');logs.flush()
                print(rep,order,traced,outputs['SmallPut']['arrival_p99_us'],record.get('ideal_p99_gain_percent'),flush=True)


if __name__=='__main__':
    main()
