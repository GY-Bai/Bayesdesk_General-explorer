"""Durable isolated job wrapper: reports actual exit status through an atomic manifest.

Called by systemd (recommended) or detached local process (development only).
"""
from __future__ import annotations
import argparse
import json
import os
from pathlib import Path
import subprocess
import sys
import time
from datetime import datetime, timezone


def atomic_json(path: Path, content: dict):
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + f".{os.getpid()}.tmp")
    with tmp.open("w", encoding="utf-8") as out:
        json.dump(content, out, sort_keys=True)
        out.flush()
        os.fsync(out.fileno())
    os.replace(tmp, path)


def main(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument("--entry-spec", required=True)
    args = parser.parse_args(argv)
    spec = json.loads(Path(args.entry_spec).read_text())
    folder = Path(spec["job_dir"])
    atomic_json(folder / "started.json", {"pid": os.getpid(), "started_at": time.time(), "job_id": spec["job_id"]})
    started = time.time()
    exit_code = None
    error_code = None
    try:
        with (folder / "stdout.log").open("ab", buffering=0) as stdout, (folder / "stderr.log").open("ab", buffering=0) as stderr:
            try:
                proc = subprocess.run(spec["argv"], cwd=spec["cwd"], stdout=stdout, stderr=stderr,
                                      timeout=spec["timeout_seconds"], check=False, start_new_session=False)
                exit_code = proc.returncode
            except subprocess.TimeoutExpired:
                exit_code = 124
                error_code = "JOB_TIMEOUT"
            except OSError as exc:
                exit_code = 127
                error_code = "EXEC_FAILED"
                stderr.write((str(exc) + "\n").encode())
    except Exception as exc:
        exit_code = 125
        error_code = "WRAPPER_ERROR"
        (folder / "wrapper_error.txt").write_text(repr(exc))
    state = "SUCCEEDED" if exit_code == 0 else "FAILED"
    atomic_json(folder / "result.json", {"schema_version": 1, "job_id": spec["job_id"],
                                         "state": state, "exit_code": exit_code,
                                         "error_code": error_code, "duration_seconds": round(time.time()-started, 3),
                                         "finished_at": datetime.now(timezone.utc).isoformat()})
    return 0 if exit_code == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
