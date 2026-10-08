"""Host-owned multi-resource queue and at-least-once event outbox.

The broker only executes trusted recipes with typed input arguments.
No NLP, no LLM, no untrusted argv, no source-code editing.
"""
from __future__ import annotations

import json
import os
from pathlib import Path
import sys
import time
import uuid

from .contracts import validate_job, verify_permit, payload_fingerprint, resource_profile, sign_event
from .errors import ContractError, require
from .executors import get_executor
from .job_entry import atomic_json
from .recipes import render_recipe
from .storage import connect, dumps, utcnow, write_tx

SCHEMA = """
CREATE TABLE IF NOT EXISTS jobs(
  job_id TEXT PRIMARY KEY, handoff_id TEXT UNIQUE NOT NULL, payload_hash TEXT NOT NULL,
  task_id TEXT NOT NULL, attempt_id TEXT NOT NULL, decision_id TEXT NOT NULL,
  recipe_id TEXT NOT NULL, spec_json TEXT NOT NULL,
  state TEXT NOT NULL, priority INTEGER NOT NULL DEFAULT 0,
  cpu_units INTEGER NOT NULL, memory_mib INTEGER NOT NULL, gpu_count INTEGER NOT NULL,
  unit_name TEXT NOT NULL, executor_meta TEXT, last_error TEXT,
  created_at TEXT NOT NULL, updated_at TEXT NOT NULL, started_at TEXT, finished_at TEXT
);
CREATE INDEX IF NOT EXISTS idx_jobs_state ON jobs(state,priority DESC,created_at);
CREATE TABLE IF NOT EXISTS event_outbox(
  event_id TEXT PRIMARY KEY, job_id TEXT NOT NULL REFERENCES jobs(job_id),
  payload_json TEXT NOT NULL, delivered_at TEXT, created_at TEXT NOT NULL
);
"""
RESERVED_STATES = ("STARTING", "RUNNING", "UNKNOWN")
TERMINAL_STATES = ("SUCCEEDED", "FAILED", "LOST", "CANCELLED")


class Broker:
    def __init__(self, db_path: str | Path, node_config: dict, recipes: dict, secret: str, jobs_root: str | Path,
                 executor_name: str = "local"):
        require(isinstance(secret, str) and len(secret) >= 24, "INVALID_SECRET", "require >=24-character shared secret")
        require(isinstance(node_config, dict) and set(node_config) == {"node_id", "allocatable"},
                "INVALID_NODE", "node config requires node_id and allocatable")
        self.capacity = resource_profile(node_config["allocatable"])
        self.node_id = node_config["node_id"]
        self.recipes = recipes
        self.secret = secret
        self.path = str(db_path)
        self.jobs_root = Path(jobs_root).expanduser().resolve()
        self.jobs_root.mkdir(parents=True, exist_ok=True)
        self.executor = get_executor(executor_name)
        with connect(self.path) as db:
            db.executescript(SCHEMA)

    def _usage(self, db) -> dict:
        row = db.execute("SELECT COALESCE(SUM(cpu_units),0) AS cpu, COALESCE(SUM(memory_mib),0) AS mem, COALESCE(SUM(gpu_count),0) AS gpu FROM jobs WHERE state IN ('STARTING','RUNNING','UNKNOWN')").fetchone()
        return {"cpu_units": row["cpu"], "memory_mib": row["mem"], "gpu_count": row["gpu"]}

    def inspect(self) -> dict:
        with connect(self.path) as db:
            used = self._usage(db)
            return {"node": self.node_id, "allocatable": self.capacity, "reserved": used,
                    "available": {k: self.capacity[k] - used[k] for k in self.capacity},
                    "queued": db.execute("SELECT COUNT(*) FROM jobs WHERE state='QUEUED'").fetchone()[0]}

    def quote(self, profile: dict) -> dict:
        profile = resource_profile(profile)
        info = self.inspect()
        blocking = [k for k in self.capacity if profile[k] > info["available"][k]]
        never_fit = [k for k in self.capacity if profile[k] > self.capacity[k]]
        return {"admission_now": not blocking, "blocking_resources": blocking,
                "never_fits_node": never_fit, "profile": profile,
                "available_snapshot": info["available"], "binding": False}

    def submit(self, job: dict) -> dict:
        validate_job(job)
        require(job["recipe_id"] in self.recipes, "RECIPE_NOT_REGISTERED", "unknown trusted recipe")
        # Type check arguments/path *before* enqueue: a job must be executable when it reaches the front.
        render_recipe(self.recipes[job["recipe_id"]], job["inputs"])
        for k, v in job["profile"].items():
            require(v <= self.capacity[k], "JOB_NEVER_FITS", f"requested {k} exceeds allocatable")
        fingerprint = payload_fingerprint(job)
        with connect(self.path) as db, write_tx(db):
            prev = db.execute("SELECT * FROM jobs WHERE handoff_id=?", (job["handoff_id"],)).fetchone()
            if prev:
                require(prev["payload_hash"] == fingerprint, "IDEMPOTENCY_CONFLICT", "handoff ID used with different job")
                return {"job_id": prev["job_id"], "state": prev["state"], "deduplicated": True}
            verify_permit(self.secret, job)
            job_id = "JOB-" + uuid.uuid4().hex[:20]
            unit = "bayesdesk-" + job_id.lower()
            p = job["profile"]
            db.execute("INSERT INTO jobs(job_id,handoff_id,payload_hash,task_id,attempt_id,decision_id,recipe_id,spec_json,state,cpu_units,memory_mib,gpu_count,unit_name,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                       (job_id, job["handoff_id"], fingerprint, job["task_id"], job["attempt_id"], job["decision_id"],
                        job["recipe_id"], dumps(job), "QUEUED", p["cpu_units"], p["memory_mib"], p["gpu_count"], unit, utcnow(), utcnow()))
            return {"job_id": job_id, "state": "QUEUED", "deduplicated": False}

    def status(self, job_id: str) -> dict:
        with connect(self.path) as db:
            row = db.execute("SELECT * FROM jobs WHERE job_id=?", (job_id,)).fetchone()
            require(row is not None, "JOB_NOT_FOUND", job_id)
            return dict(row)

    def _job_dir(self, job_id: str) -> Path:
        return self.jobs_root / job_id

    def _record_terminal(self, db, row, state: str, manifest: dict):
        require(state in TERMINAL_STATES, "INVALID_STATE", "not terminal")
        db.execute("UPDATE jobs SET state=?,finished_at=?,updated_at=?,last_error=? WHERE job_id=? AND state IN ('STARTING','RUNNING','UNKNOWN')",
                   (state, utcnow(), utcnow(), dumps(manifest) if state != "SUCCEEDED" else None, row["job_id"]))
        event = {"event_id": "EVT-" + uuid.uuid4().hex[:20], "job_id": row["job_id"],
                 "type": "JOB_SUCCEEDED" if state == "SUCCEEDED" else "JOB_FAILED" if state == "FAILED" else "JOB_LOST",
                 "task_id": row["task_id"], "handoff_id": row["handoff_id"],
                 "payload": {"result": manifest, "node_id": self.node_id,
                             "evidence_ref": str(self._job_dir(row["job_id"]) / "result.json")}}
        event = sign_event(self.secret, event)
        db.execute("INSERT INTO event_outbox VALUES(?,?,?,?,?)", (event["event_id"], row["job_id"], dumps(event), None, utcnow()))

    def reconcile(self) -> list[dict]:
        """Result manifest is authoritative; unknown live state is quarantined, never re-launched."""
        changes = []
        with connect(self.path) as db, write_tx(db):
            rows = db.execute("SELECT * FROM jobs WHERE state IN ('STARTING','RUNNING','UNKNOWN')").fetchall()
            for row in rows:
                folder = self._job_dir(row["job_id"])
                result = folder / "result.json"
                if result.exists():
                    try:
                        manifest = json.loads(result.read_text())
                        require(manifest["job_id"] == row["job_id"] and manifest["state"] in ("SUCCEEDED", "FAILED"),
                                "INVALID_MANIFEST", "job result inconsistent")
                    except (ValueError, KeyError, ContractError):
                        db.execute("UPDATE jobs SET state='UNKNOWN',last_error=?,updated_at=? WHERE job_id=?",
                                   ("INVALID_RESULT_MANIFEST", utcnow(), row["job_id"]))
                        continue
                    self.executor.reap(row["unit_name"])
                    self._record_terminal(db, row, manifest["state"], manifest)
                    changes.append({"job_id": row["job_id"], "state": manifest["state"]})
                    continue
                live = self.executor.alive(row["unit_name"], folder)
                if live is True:
                    if row["state"] != "RUNNING":
                        db.execute("UPDATE jobs SET state='RUNNING',started_at=COALESCE(started_at,?),updated_at=? WHERE job_id=?",
                                   (utcnow(), utcnow(), row["job_id"]))
                elif live is False:
                    # Cannot prove no descendants remain; quarantine and require human inspection.
                    db.execute("UPDATE jobs SET state='UNKNOWN',last_error='MISSING_RESULT_REQUIRES_INSPECTION',updated_at=? WHERE job_id=?",
                               (utcnow(), row["job_id"]))
                elif row["state"] != "UNKNOWN":
                    db.execute("UPDATE jobs SET state='UNKNOWN',last_error='EXECUTION_STATE_UNCERTAIN',updated_at=? WHERE job_id=?",
                               (utcnow(), row["job_id"]))
        return changes

    def dispatch(self, limit: int = 16) -> list[dict]:
        """Backfill under atomic reservation. Protect old queue head's CPU/RAM capacity."""
        require(type(limit) is int and 0 <= limit <= 1000, "INVALID_LIMIT", "invalid dispatch limit")
        launches = []
        for _ in range(limit):
            with connect(self.path) as db, write_tx(db):
                rows = db.execute("SELECT * FROM jobs WHERE state='QUEUED' ORDER BY priority DESC,rowid").fetchall()
                if not rows:
                    break
                used = self._usage(db)
                available = {k: self.capacity[k] - used[k] for k in self.capacity}
                head = rows[0]
                blocked_head = any(head[k] > available[k] for k in self.capacity)
                chosen = None
                for idx, row in enumerate(rows):
                    if any(row[k] > available[k] for k in self.capacity):
                        continue
                    if idx > 0 and blocked_head:
                        # Preserve CPU and memory for the oldest blocked request,
                        # so short backfill does not starve an exclusive GPU job.
                        if (row["cpu_units"] > max(0, available["cpu_units"] - head["cpu_units"]) or
                            row["memory_mib"] > max(0, available["memory_mib"] - head["memory_mib"])):
                            continue
                    chosen = row
                    break
                if chosen is None:
                    break
                db.execute("UPDATE jobs SET state='STARTING',updated_at=? WHERE job_id=? AND state='QUEUED'",
                           (utcnow(), chosen["job_id"]))
            spec = json.loads(chosen["spec_json"])
            folder = self._job_dir(chosen["job_id"])
            folder.mkdir(parents=True, exist_ok=True)
            argv, cwd = render_recipe(self.recipes[chosen["recipe_id"]], spec["inputs"])
            entry = folder / "entry.json"
            atomic_json(entry, {"job_id": chosen["job_id"], "job_dir": str(folder),
                                "argv": argv, "cwd": cwd, "timeout_seconds": spec["timeout_seconds"]})
            try:
                meta = self.executor.start(chosen["unit_name"], entry, chosen["cpu_units"], chosen["memory_mib"])
                with connect(self.path) as db, write_tx(db):
                    db.execute("UPDATE jobs SET executor_meta=?,state='RUNNING',started_at=?,updated_at=? WHERE job_id=? AND state='STARTING'",
                               (dumps(meta), utcnow(), utcnow(), chosen["job_id"]))
                launches.append({"job_id": chosen["job_id"], "state": "RUNNING"})
            except Exception as exc:
                # Do not automatically retry: side effect may have occurred before acknowledgment.
                with connect(self.path) as db, write_tx(db):
                    db.execute("UPDATE jobs SET state='UNKNOWN',last_error=?,updated_at=? WHERE job_id=?",
                               (f"LAUNCH_UNCERTAIN:{exc}", utcnow(), chosen["job_id"]))
                launches.append({"job_id": chosen["job_id"], "state": "UNKNOWN"})
        return launches

    def tick(self):
        finished = self.reconcile()
        launched = self.dispatch()
        return {"finished": finished, "launched": launched}

    def outbox(self, limit: int = 100) -> list[dict]:
        with connect(self.path) as db:
            return [json.loads(r["payload_json"]) for r in db.execute(
                "SELECT payload_json FROM event_outbox WHERE delivered_at IS NULL ORDER BY created_at,event_id LIMIT ?", (limit,))]

    def mark_delivered(self, event_id: str):
        with connect(self.path) as db, write_tx(db):
            db.execute("UPDATE event_outbox SET delivered_at=COALESCE(delivered_at,?) WHERE event_id=?", (utcnow(), event_id))

    def release_unknown(self, job_id: str, inspected_by: str, reason: str):
        """Manual safety gate: only an operator who confirmed all descendants ended."""
        require(inspected_by.strip() and reason.strip(), "OPERATOR_REQUIRED", "inspection record required")
        with connect(self.path) as db, write_tx(db):
            row = db.execute("SELECT * FROM jobs WHERE job_id=?", (job_id,)).fetchone()
            require(row is not None and row["state"] == "UNKNOWN", "INVALID_TRANSITION", "job not UNKNOWN")
            self._record_terminal(db, row, "LOST", {"error_code": "OPERATOR_CONFIRMED_LOST", "inspected_by": inspected_by, "reason": reason})
        return {"job_id": job_id, "state": "LOST"}
