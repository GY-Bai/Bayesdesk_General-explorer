"""Engineer Leader control plane: routing, fencing, handoff, and human escalation.

No LLM calls, code modification, experiment execution, or research decisions live here.
"""
from __future__ import annotations

import json
import time
import uuid
from pathlib import Path

from .contracts import resource_profile, sign_permit, validate_id, verify_event
from .errors import ContractError, require
from .storage import connect, dumps, utcnow, write_tx

SCHEMA = """
CREATE TABLE IF NOT EXISTS decisions(
  id TEXT PRIMARY KEY, approved_by TEXT NOT NULL, rationale TEXT NOT NULL, created_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS tasks(
  id TEXT PRIMARY KEY, decision_id TEXT NOT NULL REFERENCES decisions(id),
  objective TEXT NOT NULL, constraints_json TEXT NOT NULL, acceptance_json TEXT NOT NULL,
  dependencies_json TEXT NOT NULL, status TEXT NOT NULL DEFAULT 'READY',
  generation INTEGER NOT NULL DEFAULT 0, worker_id TEXT, lease_expires_at REAL,
  last_job_id TEXT, last_event_json TEXT, priority INTEGER NOT NULL DEFAULT 0,
  created_at TEXT NOT NULL, updated_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS handoffs(
  id TEXT PRIMARY KEY, task_id TEXT NOT NULL REFERENCES tasks(id),
  generation INTEGER NOT NULL, attempt_id TEXT NOT NULL, recipe_id TEXT NOT NULL,
  job_id TEXT UNIQUE, status TEXT NOT NULL, permitted_json TEXT NOT NULL,
  source_commit TEXT NOT NULL, created_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS escalations(
  id TEXT PRIMARY KEY, task_id TEXT NOT NULL REFERENCES tasks(id),
  question TEXT NOT NULL, observations_json TEXT NOT NULL,
  evidence_json TEXT NOT NULL, status TEXT NOT NULL, resolution_decision TEXT,
  created_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS event_inbox(
  event_id TEXT PRIMARY KEY, job_id TEXT NOT NULL, type TEXT NOT NULL,
  payload_json TEXT NOT NULL, arrived_at TEXT NOT NULL,
  processed_at TEXT
);
CREATE TABLE IF NOT EXISTS task_events(
  id INTEGER PRIMARY KEY AUTOINCREMENT, task_id TEXT NOT NULL,
  kind TEXT NOT NULL, details_json TEXT NOT NULL, at TEXT NOT NULL
);
"""


class Leader:
    def __init__(self, db_path: str | Path, secret: str):
        require(isinstance(secret, str) and len(secret) >= 24, "INVALID_SECRET", "require >=24-character shared HMAC secret")
        self.path = str(db_path)
        self.secret = secret
        with connect(self.path) as db:
            db.executescript(SCHEMA)

    def approve_decision(self, decision_id: str, approved_by: str, rationale: str):
        """Explicit operator action. Not called from leader scheduling policy."""
        validate_id(decision_id, "decision_id")
        require(bool(approved_by.strip()) and bool(rationale.strip()), "INVALID_DECISION", "human identity and rationale required")
        with connect(self.path) as db, write_tx(db):
            db.execute("INSERT INTO decisions VALUES(?,?,?,?)", (decision_id, approved_by, rationale, utcnow()))
        return {"decision_id": decision_id, "approved": True}

    def create_task(self, task_id: str, decision_id: str, objective: str, dependencies: list[str],
                    constraints: list[str], acceptance: dict, priority: int = 0):
        validate_id(task_id, "task_id")
        require(isinstance(objective, str) and objective.strip(), "INVALID_TASK", "objective required")
        require(isinstance(dependencies, list) and isinstance(constraints, list) and
                all(isinstance(x, str) for x in dependencies + constraints) and
                isinstance(acceptance, dict) and type(priority) is int, "INVALID_TASK", "malformed task")
        require(task_id not in dependencies and len(set(dependencies)) == len(dependencies), "INVALID_DAG", "invalid dependencies")
        with connect(self.path) as db, write_tx(db):
            require(db.execute("SELECT 1 FROM decisions WHERE id=?", (decision_id,)).fetchone() is not None,
                    "DECISION_REQUIRED", "task must reference approved decision")
            for dep in dependencies:
                require(db.execute("SELECT 1 FROM tasks WHERE id=?", (dep,)).fetchone() is not None,
                        "MISSING_DEPENDENCY", f"unknown prerequisite task: {dep}")
            db.execute("INSERT INTO tasks(id,decision_id,objective,constraints_json,acceptance_json,dependencies_json,priority,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?,?)",
                       (task_id, decision_id, objective, dumps(constraints), dumps(acceptance), dumps(dependencies), priority, utcnow(), utcnow()))
        return self.task(task_id)

    def task(self, task_id: str) -> dict:
        with connect(self.path) as db:
            r = db.execute("SELECT * FROM tasks WHERE id=?", (task_id,)).fetchone()
            require(r is not None, "TASK_NOT_FOUND", task_id)
            return {**dict(r), "constraints": json.loads(r["constraints_json"]),
                    "acceptance": json.loads(r["acceptance_json"]),
                    "dependencies": json.loads(r["dependencies_json"])}

    def list_tasks(self) -> list[dict]:
        with connect(self.path) as db:
            return [dict(x) for x in db.execute("SELECT id,decision_id,objective,status,worker_id,generation,last_job_id,priority FROM tasks ORDER BY priority DESC,created_at,id")]

    def assign(self, worker_id: str):
        """Atomic assignment; no subjective planning or problem solving."""
        validate_id(worker_id, "worker_id")
        with connect(self.path) as db, write_tx(db):
            rows = db.execute("SELECT * FROM tasks WHERE status IN ('READY','RESULT_READY') ORDER BY priority DESC,created_at,id").fetchall()
            for row in rows:
                deps = json.loads(row["dependencies_json"])
                if any((d := db.execute("SELECT status FROM tasks WHERE id=?", (dep,)).fetchone()) is None
                       or d["status"] != "DONE" for dep in deps):
                    continue
                gen = row["generation"] + 1
                db.execute("UPDATE tasks SET status='ASSIGNED',worker_id=?,generation=?,lease_expires_at=?,updated_at=? WHERE id=?",
                           (worker_id, gen, time.time() + 900, utcnow(), row["id"]))
                self._audit(db, row["id"], "WORKER_ASSIGNED", {"worker_id": worker_id, "generation": gen})
                return {"task_id": row["id"], "generation": gen, "worker_id": worker_id}
        return None

    @staticmethod
    def _audit(db, task_id, kind, details):
        db.execute("INSERT INTO task_events(task_id,kind,details_json,at) VALUES(?,?,?,?)",
                   (task_id, kind, dumps(details), utcnow()))

    @staticmethod
    def _require_assignment(db, task_id, worker_id, generation):
        row = db.execute("SELECT * FROM tasks WHERE id=?", (task_id,)).fetchone()
        require(row is not None, "TASK_NOT_FOUND", task_id)
        require(row["status"] == "ASSIGNED" and row["worker_id"] == worker_id and
                row["generation"] == generation and (row["lease_expires_at"] or 0) > time.time(),
                "STALE_ASSIGNMENT", "assignment expired or not active")
        return row

    def renew(self, task_id: str, worker_id: str, generation: int, ttl_seconds: int = 900):
        require(type(ttl_seconds) is int and 60 <= ttl_seconds <= 3600,
                "INVALID_TTL", "lease TTL must be 60-3600 seconds")
        with connect(self.path) as db, write_tx(db):
            self._require_assignment(db, task_id, worker_id, generation)
            db.execute("UPDATE tasks SET lease_expires_at=?,updated_at=? WHERE id=?",
                       (time.time() + ttl_seconds, utcnow(), task_id))
        return {"task_id": task_id, "lease_seconds": ttl_seconds}

    def requeue_expired(self):
        with connect(self.path) as db, write_tx(db):
            rows = db.execute("SELECT id,generation FROM tasks WHERE status='ASSIGNED' AND lease_expires_at<?",
                              (time.time(),)).fetchall()
            for row in rows:
                db.execute("UPDATE tasks SET status='READY',worker_id=NULL,lease_expires_at=NULL,updated_at=? WHERE id=?",
                           (utcnow(), row["id"]))
                self._audit(db, row["id"], "ASSIGNMENT_EXPIRED", {"generation": row["generation"]})
            # HANDOFF_PREPARED is NOT automatically requeued: remote broker may have accepted it.
            return {"requeued": [r["id"] for r in rows]}

    def prepare_handoff(self, task_id: str, worker_id: str, generation: int,
                        recipe_id: str, source_commit: str, allowed_profiles: dict,
                        max_timeout_seconds: int = 86400, permit_ttl_seconds: int = 3600):
        from .contracts import HEX40
        validate_id(recipe_id, "recipe_id")
        require(isinstance(source_commit, str) and HEX40.fullmatch(source_commit), "INVALID_SHA", "full 40-digit commit required")
        require(isinstance(allowed_profiles, dict) and bool(allowed_profiles), "INVALID_PROFILES", "at least one profile required")
        for k, v in allowed_profiles.items():
            validate_id(k, "profile_name")
            resource_profile(v)
        require(0 < permit_ttl_seconds <= 86400 and 0 < max_timeout_seconds <= 7 * 86400,
                "INVALID_BUDGET", "permit/timeout out of range")
        with connect(self.path) as db, write_tx(db):
            task = self._require_assignment(db, task_id, worker_id, generation)
            handoff_id = "HOF-" + uuid.uuid4().hex[:20]
            attempt_id = "ATT-" + uuid.uuid4().hex[:20]
            claims = {"handoff_id": handoff_id, "task_id": task_id, "attempt_id": attempt_id,
                      "decision_id": task["decision_id"], "generation": generation, "recipe_id": recipe_id,
                      "source_commit": source_commit, "allowed_profiles": allowed_profiles, "max_timeout_seconds": max_timeout_seconds,
                      "expires_at": int(time.time()) + permit_ttl_seconds}
            permit = sign_permit(self.secret, claims)
            db.execute("INSERT INTO handoffs VALUES(?,?,?,?,? ,NULL,?,?,?,?)",
                       (handoff_id, task_id, generation, attempt_id, recipe_id, "PREPARED", dumps(permit), source_commit, utcnow()))
            db.execute("UPDATE tasks SET status='HANDOFF_PREPARED',updated_at=? WHERE id=?", (utcnow(), task_id))
            self._audit(db, task_id, "HANDOFF_PREPARED", {"handoff_id": handoff_id, "attempt_id": attempt_id})
        return {"handoff_id": handoff_id, "attempt_id": attempt_id, "task_id": task_id,
                "decision_id": task["decision_id"], "generation": generation, "recipe_id": recipe_id,
                "source_commit": source_commit, "permit": permit}

    def acknowledge_handoff(self, handoff_id: str, job_id: str):
        validate_id(job_id, "job_id")
        with connect(self.path) as db, write_tx(db):
            handoff = db.execute("SELECT * FROM handoffs WHERE id=?", (handoff_id,)).fetchone()
            require(handoff is not None, "HANDOFF_NOT_FOUND", handoff_id)
            if handoff["status"] == "ACCEPTED":
                require(handoff["job_id"] == job_id, "IDEMPOTENCY_CONFLICT", "handoff acknowledged with another job")
                return {"task_id": handoff["task_id"], "job_id": job_id, "status": "WAITING_JOB"}
            require(handoff["status"] == "PREPARED", "INVALID_TRANSITION", "handoff cannot be acknowledged")
            db.execute("UPDATE handoffs SET status='ACCEPTED',job_id=? WHERE id=?", (job_id, handoff_id))
            db.execute("UPDATE tasks SET status='WAITING_JOB',worker_id=NULL,last_job_id=?,updated_at=? WHERE id=?",
                       (job_id, utcnow(), handoff["task_id"]))
            self._audit(db, handoff["task_id"], "HANDOFF_ACCEPTED", {"job_id": job_id})
            self._apply_events(db, job_id, handoff["task_id"])
        return {"task_id": handoff["task_id"], "job_id": job_id, "status": "WAITING_JOB"}

    @staticmethod
    def _apply_events(db, job_id: str, task_id: str):
        events = db.execute("SELECT * FROM event_inbox WHERE job_id=? AND processed_at IS NULL ORDER BY arrived_at,event_id", (job_id,)).fetchall()
        for event in events:
            if event["type"] in ("JOB_SUCCEEDED", "JOB_FAILED", "JOB_LOST"):
                # Do not overwrite escalated/done tasks when late events arrive.
                db.execute("UPDATE tasks SET status='RESULT_READY',last_event_json=?,updated_at=? WHERE id=? AND status='WAITING_JOB'",
                           (event["payload_json"], utcnow(), task_id))
            db.execute("UPDATE event_inbox SET processed_at=? WHERE event_id=?", (utcnow(), event["event_id"]))
            Leader._audit(db, task_id, "JOB_EVENT", {"event_id": event["event_id"], "type": event["type"]})

    def ingest_event(self, event: dict):
        required = {"event_id", "job_id", "type", "task_id", "handoff_id", "payload", "signature"}
        verify_event(self.secret, event)
        require(isinstance(event, dict) and set(event) == required, "INVALID_EVENT", "invalid event envelope")
        for k in ("event_id", "job_id", "task_id", "handoff_id"):
            validate_id(event[k], k)
        require(event["type"] in ("JOB_SUCCEEDED", "JOB_FAILED", "JOB_LOST"), "INVALID_EVENT", "unknown event type")
        require(isinstance(event["payload"], dict), "INVALID_EVENT", "payload must be object")
        with connect(self.path) as db, write_tx(db):
            old = db.execute("SELECT payload_json FROM event_inbox WHERE event_id=?", (event["event_id"],)).fetchone()
            if old:
                require(json.loads(old["payload_json"]) == event, "IDEMPOTENCY_CONFLICT", "event ID reused with another payload")
                return {"deduplicated": True}
            h = db.execute("SELECT * FROM handoffs WHERE id=?", (event["handoff_id"],)).fetchone()
            require(h is not None and h["task_id"] == event["task_id"], "INVALID_EVENT", "unrecognized handoff")
            require(h["job_id"] in (None, event["job_id"]), "INVALID_EVENT", "job ID mismatch")
            db.execute("INSERT INTO event_inbox VALUES(?,?,?,?,?,NULL)",
                       (event["event_id"], event["job_id"], event["type"], dumps(event), utcnow()))
            if h["status"] == "ACCEPTED":
                self._apply_events(db, event["job_id"], h["task_id"])
            return {"deduplicated": False, "pending_handoff": h["status"] != "ACCEPTED"}

    def complete(self, task_id: str, worker_id: str, generation: int, evidence: list[str]):
        require(isinstance(evidence, list) and evidence and all(isinstance(v, str) and v for v in evidence),
                "EVIDENCE_REQUIRED", "completion requires references to acceptance evidence")
        with connect(self.path) as db, write_tx(db):
            self._require_assignment(db, task_id, worker_id, generation)
            db.execute("UPDATE tasks SET status='DONE',worker_id=NULL,updated_at=? WHERE id=?", (utcnow(), task_id))
            self._audit(db, task_id, "TASK_COMPLETED", {"evidence": evidence})
        return self.task(task_id)

    def escalate(self, task_id: str, worker_id: str, generation: int, question: str,
                 observations: list[str], evidence: list[str]):
        require(isinstance(question, str) and question.strip() and isinstance(observations, list) and
                isinstance(evidence, list) and bool(evidence), "EVIDENCE_REQUIRED", "question and evidence required")
        with connect(self.path) as db, write_tx(db):
            self._require_assignment(db, task_id, worker_id, generation)
            esc_id = "ESC-" + uuid.uuid4().hex[:20]
            db.execute("INSERT INTO escalations VALUES(?,?,?,?,?,'OPEN',NULL,?)",
                       (esc_id, task_id, question, dumps(observations), dumps(evidence), utcnow()))
            db.execute("UPDATE tasks SET status='BLOCKED',worker_id=NULL,updated_at=? WHERE id=?", (utcnow(), task_id))
            self._audit(db, task_id, "ESCALATED", {"escalation_id": esc_id, "question": question})
            return {"escalation_id": esc_id, "task_id": task_id, "status": "OPEN"}

    def resolve_escalation(self, escalation_id: str, decision_id: str, approved_by: str, rationale: str):
        """Requires explicit human command; scheduler never invokes this automatically."""
        validate_id(decision_id, "decision_id")
        require(approved_by.strip() and rationale.strip(), "INVALID_DECISION", "human approval required")
        with connect(self.path) as db, write_tx(db):
            esc = db.execute("SELECT * FROM escalations WHERE id=?", (escalation_id,)).fetchone()
            require(esc is not None and esc["status"] == "OPEN", "INVALID_ESCALATION", "not an open escalation")
            db.execute("INSERT INTO decisions VALUES(?,?,?,?)", (decision_id, approved_by, rationale, utcnow()))
            db.execute("UPDATE escalations SET status='RESOLVED',resolution_decision=? WHERE id=?", (decision_id, escalation_id))
            db.execute("UPDATE tasks SET status='READY',decision_id=?,updated_at=? WHERE id=?", (decision_id, utcnow(), esc["task_id"]))
            self._audit(db, esc["task_id"], "HUMAN_DECISION", {"decision_id": decision_id, "escalation_id": escalation_id})
        return {"task_id": esc["task_id"], "decision_id": decision_id, "status": "READY"}

    def events(self, task_id: str) -> list[dict]:
        with connect(self.path) as db:
            return [{**dict(r), "details": json.loads(r["details_json"])} for r in db.execute("SELECT * FROM task_events WHERE task_id=? ORDER BY id", (task_id,))]
