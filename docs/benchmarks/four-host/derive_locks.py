"""Derive reproducible Cargo locks from the four-host freeze for a new MMDB version."""
import argparse
from collections import Counter
import gzip
import hashlib
import json
from pathlib import Path
import re
import tempfile


HERE = Path(__file__).resolve().parent
PACKAGE = '\n[[package]]\n'
FIELD = re.compile(r'^(name|version|source|checksum) = "([^"\n]+)"$', re.M)


def package_fields(block):
    return dict(FIELD.findall(block))


def registry_packages(blocks):
    packages = Counter()
    for block in blocks:
        fields = package_fields(block)
        if fields.get('source', '').startswith('registry+'):
            packages[fields['name'], fields['version'], fields['checksum']] += 1
    return packages


def derive(original, version):
    header, *blocks = original.decode().split(PACKAGE)
    fields = [package_fields(b) for b in blocks]
    engine = [i for i, f in enumerate(fields) if f.get('name') == 'mmdb']
    if len(engine) != 1 or 'source' in fields[engine[0]]:
        raise ValueError('expected exactly one local mmdb package')
    idx = engine[0]
    old = fields[idx]['version']
    changed = list(blocks)
    changed[idx] = changed[idx].replace(f'\nversion = "{old}"\n', f'\nversion = "{version}"\n', 1)
    if registry_packages(changed) != registry_packages(blocks):
        raise ValueError('registry package names, versions or checksums changed')
    return PACKAGE.join([header, *changed]).encode()


def main():
    cli = argparse.ArgumentParser(description=__doc__)
    cli.add_argument('output', type=Path, help='directory for root.lock, storage.lock and capacity.lock')
    cli.add_argument('--manifest', type=Path, default=HERE.parents[2] / 'Cargo.toml',
                     help='MMDB Cargo.toml supplying the target package version')
    cli.add_argument('--check', action='store_true', help='verify the three existing output files')
    args = cli.parse_args()
    section = re.search(r'^\[package\][ \t]*\n(.*?)(?=^\[|\Z)', args.manifest.read_text(), re.M | re.S)
    fields = package_fields(section[1]) if section else {}
    if fields.get('name') != 'mmdb' or 'version' not in fields:
        cli.error('expected a Cargo.toml with a local mmdb package and explicit version')
    freeze = json.loads((HERE / 'freeze.json').read_text())
    archives = {
        'root': (HERE.parent / 'apple-m5/bench.Cargo.lock.gz', freeze['engine_lock_sha256']),
        'storage': (HERE / 'storage.lock.gz', freeze['standalone_storage_lock_sha256']),
        'capacity': (HERE / 'capacity.lock.gz', freeze['supplements']['capacity_external_lock_sha256']),
    }
    outputs = {}
    for name, (source, expected) in archives.items():
        original = gzip.decompress(source.read_bytes())
        if hashlib.sha256(original).hexdigest() != expected:
            raise ValueError(f'archived lock digest differs from freeze.json: {source}')
        outputs[args.output / f'{name}.lock'] = derive(original, fields['version'])
    if not args.check:
        args.output.mkdir(parents=True, exist_ok=True)
    for dest, data in outputs.items():
        if args.check:
            if dest.read_bytes() != data:
                raise ValueError(f'derived lock does not match {dest}')
        else:
            with tempfile.NamedTemporaryFile(dir=args.output, delete=False) as f:
                f.write(data)
            Path(f.name).replace(dest)
        print(f'{hashlib.sha256(data).hexdigest()}  {dest.name}')


if __name__ == '__main__':
    main()
