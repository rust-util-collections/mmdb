"""Durable matched-block checkpoints shared by the Unix measurement drivers."""
import contextlib
import fcntl
import hashlib
import json
import os
from pathlib import Path
import subprocess
import tempfile
import time
import uuid


def encoded(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()


def digest(path):
    h = hashlib.sha256()
    with Path(path).open("rb") as f:
        for block in iter(lambda: f.read(1024 * 1024), b""):
            h.update(block)
    return h.hexdigest()


def atomic_json(path, data):
    fd, temporary = tempfile.mkstemp(dir=path.parent, prefix=".writing-")
    try:
        with os.fdopen(fd, "wb") as f:
            f.write(encoded(data) + b"\n")
            f.flush()
            os.fsync(f.fileno())
        os.replace(temporary, path)
        directory = os.open(path.parent, os.O_RDONLY)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)
    finally:
        Path(temporary).unlink(missing_ok=True)


_leases = []


def run_child(command, **kwargs):
    """Keep other runs off the host until a surviving timed child also exits."""
    return subprocess.run(command, pass_fds=tuple(_leases), **kwargs)


class Journal:
    """One immutable output identity; publish a whole matched block or retry it."""

    def __init__(self, output_dir, identity):
        self.folder = Path(output_dir)
        self.identity = json.loads(encoded(identity))
        self.fingerprint = hashlib.sha256(encoded(self.identity)).hexdigest()
        self.stack = contextlib.ExitStack()
        self.used = set()

    def __enter__(self):
        self.folder.mkdir(parents=True, exist_ok=True)
        try:
            locks = [self.folder / ".lock"]
            if os.environ.get("MMDB_TRIAL_HOST_LOCK"):
                locks.insert(0, Path(os.environ["MMDB_TRIAL_HOST_LOCK"]))
            for path in dict.fromkeys(p.resolve() for p in locks):
                f = self.stack.enter_context(path.open("a+b"))
                try:
                    fcntl.flock(f, fcntl.LOCK_EX | fcntl.LOCK_NB)
                except BlockingIOError as error:
                    raise RuntimeError(f"another measurement holds {path}") from error
                _leases.append(f.fileno())
                self.stack.callback(_leases.remove, f.fileno())
            identity_file = self.folder / "identity.json"
            if identity_file.exists():
                if json.loads(identity_file.read_bytes()) != self.identity:
                    raise ValueError(f"measurement identity differs from {identity_file}")
            else:
                atomic_json(identity_file, self.identity)
            for name in ("blocks", "attempts"):
                (self.folder / name).mkdir(exist_ok=True)
        except BaseException:
            self.stack.close()
            raise
        return self

    def __exit__(self, *exc_info):
        self.stack.close()

    def block(self, key, specification, run):
        tag = hashlib.sha256(encoded(key)).hexdigest()
        if tag in self.used:
            raise ValueError(f"duplicate block in schedule: {key}")
        self.used.add(tag)
        spec = hashlib.sha256(encoded(specification)).hexdigest()
        path = self.folder / "blocks" / (tag + ".json")
        if path.exists():
            record = json.loads(path.read_bytes())
            checksum = record.pop("sha256")
            if (hashlib.sha256(encoded(record)).hexdigest() != checksum
                    or record["identity"] != self.fingerprint or record["spec"] != spec):
                raise ValueError(f"invalid or mismatched checkpoint for {key}")
            return record["rows"]

        attempts = self.folder / "attempts"
        pending = attempts / (tag + ".running.json")
        if pending.exists():
            pending.rename(attempts / f"{tag}.{uuid.uuid4().hex}.interrupted.json")
        record = dict(key=key, identity=self.fingerprint, spec=spec, started_ns=time.time_ns())
        atomic_json(pending, record)
        try:
            rows = run()
            if not isinstance(rows, list) or not rows:
                raise ValueError("matched block returned no observations")
            result = dict(record, ended_ns=time.time_ns(), rows=rows)
            atomic_json(path, dict(result, sha256=hashlib.sha256(encoded(result)).hexdigest()))
        except BaseException as error:
            atomic_json(attempts / f"{tag}.{uuid.uuid4().hex}.failed.json",
                        dict(record, ended_ns=time.time_ns(), error=repr(error)))
            pending.unlink(missing_ok=True)
            raise
        pending.unlink(missing_ok=True)
        return rows
