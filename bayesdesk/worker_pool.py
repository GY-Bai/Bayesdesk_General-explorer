"""Optional short-lived external Worker launcher.

A Worker is an external CLI process, not a permanently sleeping LLM. The supervisor
claims a Task and launches a finite session; external CLI must perform its own
handoff/complete/escalate and periodic lease renewal. This code does no research.
"""
from __future__ import annotations
import json
import os
from pathlib import Path
import subprocess
import threading
from typing import Any

from .errors import ContractError, require
from .job_entry import atomic_json
from .leader import Leader

# The process that signs Broker permits is privileged. The LLM worker is not.
# Do not copy the supervisor's arbitrary environment into an Agent process.
# Operators may explicitly add provider-specific credentials via env_allowlist.
DEFAULT_WORKER_ENV = frozenset({
    "PATH", "HOME", "LANG", "LC_ALL", "LC_CTYPE", "TMPDIR", "TEMP", "TMP",
    "USER", "LOGNAME", "SHELL", "TERM", "PYTHONPATH", "VIRTUAL_ENV",
})
FORBIDDEN_WORKER_ENV = frozenset({"BAYESDESK_SHARED_SECRET", "BAYESDESK_OPERATOR_TOKEN"})


class WorkerPool:
    def __init__(self, leader: Leader, config: dict, capsules_root: str | Path):
        require(isinstance(config, dict) and {"workers", "max_concurrent"} <= set(config)
                and set(config) <= {"workers", "max_concurrent", "env_allowlist"},
                "INVALID_WORKER_CONFIG", "expected workers and max_concurrent")
        require(type(config["max_concurrent"]) is int and config["max_concurrent"] >= 1,
                "INVALID_WORKER_CONFIG", "max_concurrent must be positive")
        self.leader = leader
        self.config = config
        extra = config.get("env_allowlist", [])
        require(isinstance(extra, list) and all(isinstance(x, str) and x.isidentifier() for x in extra)
                and not (set(extra) & FORBIDDEN_WORKER_ENV),
                "INVALID_WORKER_CONFIG", "invalid or privileged worker environment variable")
        self.env_keys = DEFAULT_WORKER_ENV | set(extra)
        self.capsules_root = Path(capsules_root).resolve()
        self.capsules_root.mkdir(parents=True, exist_ok=True)
        self.children: dict[str, subprocess.Popen] = {}

    def _reap(self):
        for key, proc in list(self.children.items()):
            if proc.poll() is not None:
                self.children.pop(key)

    def tick(self) -> list[dict]:
        self._reap()
        self.leader.requeue_expired()
        started = []
        for worker in self.config["workers"]:
            if len(self.children) >= self.config["max_concurrent"]:
                break
            require(isinstance(worker, dict) and set(worker) == {"worker_id", "argv", "cwd"},
                    "INVALID_WORKER_CONFIG", "each worker needs worker_id/argv/cwd")
            name = worker["worker_id"]
            if name in self.children:
                continue
            argv = worker["argv"]
            require(isinstance(argv, list) and argv and all(isinstance(a, str) and a for a in argv),
                    "INVALID_WORKER_CONFIG", "Worker argv must be fixed strings")
            cwd = Path(worker["cwd"]).resolve(strict=True)
            require(cwd.is_dir(), "INVALID_WORKER_CONFIG", "Worker cwd must be a directory")
            claim = self.leader.assign(name)
            if claim is None:
                break
            task = self.leader.task(claim["task_id"])
            capsule = self.capsules_root / f"{claim['task_id']}-g{claim['generation']}.json"
            # Context Capsule is data only. The worker command never comes from this capsule.
            relevant_lessons = self.leader.knowledge_search(task["knowledge_scope"]) if task["knowledge_scope"] else []
            atomic_json(capsule, {
                "knowledge_scope": task["knowledge_scope"], "lessons": relevant_lessons[:15],
                "execution_policy": task["execution_policy"],
                "schema_version": 1, "task_id": task["id"], "worker_id": name,
                "generation": claim["generation"], "decision_id": task["decision_id"],
                "objective": task["objective"], "constraints": task["constraints"],
                "acceptance": task["acceptance"], "dependencies": task["dependencies"],
                "last_job_id": task["last_job_id"], "last_event": json.loads(task["last_event_json"]) if task["last_event_json"] else None,
                "instructions": "Use structured CLI contracts; never wait in a model sleep loop. Renew assignment lease; handoff, complete, or escalate."})
            command = [a.replace("{capsule}", str(capsule)) for a in argv]
            try:
                with (self.capsules_root / f"{claim['task_id']}-g{claim['generation']}.launcher.log").open("ab") as log:
                    # Signing and operator secrets belong to trusted control services, NEVER the LLM Worker.
                    safe_env = {k:v for k,v in os.environ.items() if k in self.env_keys}
                    proc = subprocess.Popen(command, cwd=str(cwd), stdin=subprocess.DEVNULL,
                                            stdout=log, stderr=log, close_fds=True, env=safe_env)
                self.children[name] = proc
                threading.Thread(target=proc.wait, daemon=True).start()
                started.append({"worker_id": name, "task_id": task["id"], "generation": claim["generation"], "pid": proc.pid})
            except OSError as err:
                # The lease will be requeued after expiry; do not falsely report success.
                started.append({"worker_id": name, "task_id": task["id"], "start_error": str(err)})
        return started
