"""Prepare, build or measure the historical prefix-cut and ordered writer trials on macOS."""
import argparse
from concurrent.futures import ThreadPoolExecutor
import gzip
import hashlib
import json
import os
from pathlib import Path
import platform
import shutil
import subprocess
import sys
import tarfile

REPO = Path(subprocess.check_output(["git", "rev-parse", "--show-toplevel"], text=True).strip())
DOCS = REPO / "docs/benchmarks"
CUT = DOCS / "l0-profile/prefix-cuts"
TRACE = DOCS / "engine-profile/writer-trace"
REVS = {"control": "552895056040d660d32f0e447cb3b6cb8d91a4cf",
        "cut": "552895056040d660d32f0e447cb3b6cb8d91a4cf",
        "writer": "7b4644369d65b4d7a0a09a582840b99991518e16"}
CASES = {"control": "l0_profile", "cut": "l0_profile", "writer": "engine_profile"}


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def replace_once(path, old, new):
    source = path.read_text()
    assert source.count(old) == 1, (path, old)
    path.write_text(source.replace(old, new))


def prepare(area):
    scratch = area / "scratch"
    scratch.mkdir(exist_ok=True)
    for name, rev in REVS.items():
        tree = area / name
        tree.mkdir()
        files = ["Cargo.toml", "LICENSE", "README.md", "src", "benches", "tests", "examples"]
        with subprocess.Popen(["git", "archive", rev, *files], cwd=REPO, stdout=subprocess.PIPE) as proc:
            with tarfile.open(fileobj=proc.stdout, mode="r|") as archive:
                archive.extractall(tree, filter="data")
            assert proc.wait() == 0
        with gzip.open(DOCS / "apple-m5/bench.Cargo.lock.gz", "rb") as src, (tree / "Cargo.lock").open("wb") as dst:
            shutil.copyfileobj(src, dst)
        patch = TRACE / "trace.patch" if name == "writer" else CUT / "one-cut.patch"
        includes = ["--include=" + entry for entry in
                    ["benches/l0_profile.rs", "src/db/flush.rs", "src/compaction/profile.rs"]] if name == "control" else []
        # Run outside the containing checkout so git applies all paths to the archived tree.
        env = dict(os.environ, GIT_CEILING_DIRECTORIES=str(area))
        subprocess.run(["git", "apply", *includes, "--check", str(patch)], cwd=tree, env=env, check=True)
        subprocess.run(["git", "apply", *includes, str(patch)], cwd=tree, env=env, check=True)
        if name == "writer":
            driver = tree / "benches/engine_profile/main.rs"
            shutil.copy2(REPO / "benches/engine_profile/macos.rs", driver.parent)
            replace_once(driver, "fn rss_kib(", '#[cfg(target_os = "macos")]\nmod macos;\n'
                         '#[cfg(target_os = "macos")]\nuse macos::{rss_kib, cpu_ticks};\n'
                         '#[cfg(not(target_os = "macos"))]\nfn rss_kib(')
            replace_once(driver, "fn cpu_ticks(", '#[cfg(not(target_os = "macos"))]\nfn cpu_ticks(')
            assert (tree / "src/write_trace.rs").is_file()
        else:
            driver = tree / "benches/l0_profile.rs"
            module = (REPO / "benches/engine_profile/macos.rs").read_text().split("pub(super) fn cpu_ticks()")[0]
            (driver.parent / "macos_rss.rs").write_text(module)
            replace_once(driver, "fn rss_kib(", '#[cfg(target_os = "macos")]\nmod macos_rss;\n'
                         '#[cfg(target_os = "macos")]\nuse macos_rss::rss_kib;\n'
                         '#[cfg(not(target_os = "macos"))]\nfn rss_kib(')
            assert "flushed_ssts" in driver.read_text()
            assert ("one_prefix_cut_preserves" in (tree / "src/db.rs").read_text()) == (name == "cut")
    drivers = area / "drivers"
    drivers.mkdir()
    for original, name in ((CUT / "trials.py", "prefix.py"), (CUT.parent / "replay.py", "l0.py"),
                           (TRACE / "record.py", "record.py"), (TRACE / "replay.py", "replay.py")):
        shutil.copy2(original, drivers / name)
    replace_once(drivers / "l0.py", '["taskset", "-c", args.cpus, str(binary.resolve()), mode]',
                 '[str(binary.resolve()), mode]\n    assert args.cpus == "native"')
    replace_once(drivers / "record.py", "proc=subprocess.run(['taskset','-c',args.cpus,str(binary),'mixed'],",
                 "assert args.cpus == 'native'\n                proc=subprocess.run([str(binary),'mixed'],")
    print("Prepared matched sources and native-scheduling drivers", flush=True)


def build_one(area, label):
    target = CASES[label]
    tree = area / label
    out = area / "bin" / label
    cmd = ["cargo", "+stable", "bench", "--no-run", "--offline", "--locked", "--jobs", "3", "--bench", target,
           "--features", "bench-metrics", "--target-dir", str(area / ("build-" + label)),
           "--message-format", "json-render-diagnostics"]
    with (area / (label + "-build.log")).open("w") as log:
        result = subprocess.run(cmd, cwd=tree, stdout=subprocess.PIPE, stderr=log, text=True, check=True)
    files = [json.loads(line) for line in result.stdout.splitlines()]
    path, = [row["executable"] for row in files if row.get("reason") == "compiler-artifact"
             and row["target"]["name"] == target and row.get("executable")]
    shutil.copy2(path, out)
    print(f"{label}: {sha(out)}", flush=True)


def build(area):
    (area / "bin").mkdir(exist_ok=True)
    with ThreadPoolExecutor(max_workers=3) as pool:
        list(pool.map(lambda label: build_one(area, label), REVS))
    # Run the two crossing-boundary/selection contract tests, using their full names
    # after querying the freshly built test binary to avoid a zero-test pass.
    tree = area / "cut"
    command = ["cargo", "+stable", "test", "--release", "--offline", "--locked", "--jobs", "3", "--lib",
               "--features", "bench-metrics", "--target-dir", str(area / "build-cut")]
    names = ["full_l0_claim_selects_the_same_cold_gap_after_disjoint_flushes",
             "one_prefix_cut_preserves_stripes_and_crossing_range_deletion"]
    with (area / "cut-tests.log").open("w") as log:
        result = subprocess.check_output(command + ["--", "--list"], cwd=tree, stderr=log, text=True)
        tests = [line.split(": test")[0] for line in result.splitlines() if line.endswith(": test")]
        for short in names:
            full, = [name for name in tests if name.endswith("::" + short)]
            ran = subprocess.check_output(command + [full, "--", "--exact"], cwd=tree, stderr=log, text=True)
            assert "1 passed" in ran, ran
            log.write(ran)


def run(area):
    drivers = area / "drivers"
    results = area / "results"
    results.mkdir(exist_ok=True)
    subprocess.run([sys.executable, "-B", str(drivers / "prefix.py"), "--baseline", str(area / "bin/control"),
                    "--candidate", str(area / "bin/cut"), "--l0-replay", str(drivers / "l0.py"), "--tmpdir",
                    str(area / "scratch"), "--cpus", "native", "--operations", "16384", "--rate", "20000",
                    "--output", str(results / "prefix.csv")], check=True)
    subprocess.run([sys.executable, "-B", str(drivers / "record.py"), "--binary", str(area / "bin/writer"),
                    "--tmpdir", str(area / "scratch"), "--cpus", "native", "--rate", "18911", "--seed", "20261125",
                    "--out", str(results / "writer")], check=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=["prepare", "build", "run"])
    parser.add_argument("area", type=Path)
    args = parser.parse_args()
    assert platform.system() == "Darwin"
    area = args.area.resolve()
    area.mkdir(parents=True, exist_ok=True)
    {"prepare": prepare, "build": build, "run": run}[args.action](area)


if __name__ == "__main__":
    main()
