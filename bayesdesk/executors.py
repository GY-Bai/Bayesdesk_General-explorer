"""OS process ownership. Systemd user services for durable use; local is dev-only."""
from __future__ import annotations
import json
import os
from pathlib import Path
import subprocess
import sys
import threading

from .errors import ContractError


class DetachedExecutor:
    name = "local"

    def __init__(self):
        self._children = {}

    def start(self, unit: str, entry_spec: Path, cpu_units: int, memory_mib: int) -> dict:
        """Dev fallback. Does not guarantee cgroup isolation or durable recovery."""
        with (entry_spec.parent / "launcher.log").open("ab", buffering=0) as log:
            child = subprocess.Popen([sys.executable, "-m", "bayesdesk.job_entry", "--entry-spec", str(entry_spec)],
                                     stdin=subprocess.DEVNULL, stdout=log, stderr=log, start_new_session=True,
                                     close_fds=True)
        self._children[unit] = child
        threading.Thread(target=child.wait, daemon=True).start()
        return {"pid": child.pid, "unit": None}

    def reap(self, unit: str):
        child = self._children.get(unit)
        if child is not None and child.poll() is not None:
            self._children.pop(unit, None)

    def alive(self, unit: str, job_dir: Path) -> bool | None:
        self.reap(unit)
        marker = job_dir / "started.json"
        if not marker.exists():
            return None
        pid = json.loads(marker.read_text())["pid"]
        # Linux /proc cmdline identity check avoids treating a reused PID as our process.
        cmdline = Path(f"/proc/{pid}/cmdline")
        try:
            raw = cmdline.read_bytes()
            return str(job_dir / "entry.json").encode() in raw and b"bayesdesk.job_entry" in raw
        except (OSError, ValueError):
            return False


class SystemdExecutor:
    name = "systemd-user"

    def reap(self, unit: str):
        pass

    def start(self, unit: str, entry_spec: Path, cpu_units: int, memory_mib: int) -> dict:
        cmd = ["systemd-run", "--user", "--unit", unit, "--collect",
               "--property", "Type=exec", "--property", f"CPUQuota={cpu_units * 100}%",
               "--property", f"MemoryMax={memory_mib}M",
               sys.executable, "-m", "bayesdesk.job_entry", "--entry-spec", str(entry_spec)]
        result = subprocess.run(cmd, capture_output=True, text=True, check=False, timeout=25)
        if result.returncode != 0:
            raise ContractError("EXEC_START_FAILED", f"systemd-run failed: {result.stderr[:400]}", retryable=True)
        return {"pid": None, "unit": unit}

    def alive(self, unit: str, job_dir: Path) -> bool | None:
        try:
            p = subprocess.run(["systemctl", "--user", "show", unit, "--property=ActiveState", "--value"],
                               capture_output=True, text=True, timeout=10, check=False)
        except (OSError, subprocess.TimeoutExpired):
            return None
        if p.returncode != 0:
            return False if (job_dir / "started.json").exists() else None
        state = p.stdout.strip()
        if state in ("active", "activating", "reloading", "deactivating"):
            return True
        if state in ("inactive", "failed"):
            return False
        return None


def get_executor(name: str):
    if name == "local":
        return DetachedExecutor()
    if name == "systemd-user":
        return SystemdExecutor()
    raise ContractError("UNKNOWN_EXECUTOR", name)
