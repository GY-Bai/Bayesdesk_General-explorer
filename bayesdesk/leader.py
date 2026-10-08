"""Engineer Leader control plane: routing, fencing, handoff, and human escalation.

No LLM calls, code modification, experiment execution, or research decisions live here.
"""
from __future__ import annotations

import json
import hashlib
import os
import time
import uuid
from pathlib import Path

from .contracts import resource_profile, sign_permit, validate_id, verify_event, verify_receipt, payload_fingerprint
from .errors import ContractError, require
from .storage import connect, dumps, utcnow, write_tx
from .policies import (validate_execution_policy, validate_acceptance, authorize_handoff, KINDS)

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
  created_at TEXT NOT NULL, updated_at TEXT NOT NULL,
  policy_json TEXT NOT NULL DEFAULT '{"recipes":{}}',
  knowledge_scope_json TEXT NOT NULL DEFAULT '{}'
);
CREATE TABLE IF NOT EXISTS handoffs(
  id TEXT PRIMARY KEY, task_id TEXT NOT NULL REFERENCES tasks(id),
  generation INTEGER NOT NULL, attempt_id TEXT NOT NULL, recipe_id TEXT NOT NULL,
  job_id TEXT UNIQUE, status TEXT NOT NULL, permitted_json TEXT NOT NULL,
  source_commit TEXT NOT NULL, created_at TEXT NOT NULL,
  receipt_json TEXT
);
CREATE TABLE IF NOT EXISTS escalations(
  id TEXT PRIMARY KEY, task_id TEXT NOT NULL REFERENCES tasks(id),
  question TEXT NOT NULL, observations_json TEXT NOT NULL,
  evidence_json TEXT NOT NULL, status TEXT NOT NULL, resolution_decision TEXT,
  created_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS evidence(
  id TEXT PRIMARY KEY, task_id TEXT NOT NULL REFERENCES tasks(id),
  attempt_id TEXT, kind TEXT NOT NULL, sha256 TEXT NOT NULL,
  producer TEXT NOT NULL, generation INTEGER NOT NULL, metadata_json TEXT NOT NULL,
  created_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS lessons(
  id TEXT PRIMARY KEY, task_id TEXT NOT NULL REFERENCES tasks(id),
  title TEXT NOT NULL, summary TEXT NOT NULL, scope_json TEXT NOT NULL,
  supporting_json TEXT NOT NULL, state TEXT NOT NULL,
  author_worker TEXT NOT NULL, verified_by TEXT, verification_json TEXT,
  created_at TEXT NOT NULL, verified_at TEXT
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
        self.evidence_root = Path(self.path).resolve().parent / "evidence-blobs"
        self.evidence_root.mkdir(parents=True, exist_ok=True)
        with connect(self.path) as db:
            db.executescript(SCHEMA)
            # In-place upgrades of V0.1 SQLite state; never destroy existing jobs or tasks.
            cols = {r["name"] for r in db.execute("PRAGMA table_info(tasks)")}
            if "policy_json" not in cols:
                db.execute("ALTER TABLE tasks ADD COLUMN policy_json TEXT NOT NULL DEFAULT '{\"recipes\":{}}'")
            if "knowledge_scope_json" not in cols:
                db.execute("ALTER TABLE tasks ADD COLUMN knowledge_scope_json TEXT NOT NULL DEFAULT '{}'")
            cols = {r["name"] for r in db.execute("PRAGMA table_info(handoffs)")}
            if "receipt_json" not in cols:
                db.execute("ALTER TABLE handoffs ADD COLUMN receipt_json TEXT")

    def approve_decision(self, decision_id: str, approved_by: str, rationale: str):
        """Explicit operator action. Not called from leader scheduling policy."""
        validate_id(decision_id, "decision_id")
        require(bool(approved_by.strip()) and bool(rationale.strip()), "INVALID_DECISION", "human identity and rationale required")
        with connect(self.path) as db, write_tx(db):
            db.execute("INSERT INTO decisions VALUES(?,?,?,?)", (decision_id, approved_by, rationale, utcnow()))
        return {"decision_id": decision_id, "approved": True}

    def create_task(self, task_id: str, decision_id: str, objective: str, dependencies: list[str],
                    constraints: list[str], acceptance: dict, priority: int = 0,
                    execution_policy: dict | None = None, knowledge_scope: dict | None = None):
        validate_id(task_id, "task_id")
        require(isinstance(objective, str) and objective.strip(), "INVALID_TASK", "objective required")
        require(isinstance(dependencies, list) and isinstance(constraints, list) and
                all(isinstance(x, str) for x in dependencies + constraints) and
                isinstance(acceptance, dict) and type(priority) is int, "INVALID_TASK", "malformed task")
        validate_acceptance(acceptance)
        policy = validate_execution_policy(execution_policy if execution_policy is not None else {"recipes": {}})
        scope = knowledge_scope or {}
        require(isinstance(scope, dict) and all(isinstance(k, str) and isinstance(v, str) for k,v in scope.items()),
                "INVALID_SCOPE", "knowledge scope must contain string keys/values")
        require(task_id not in dependencies and len(set(dependencies)) == len(dependencies), "INVALID_DAG", "invalid dependencies")
        with connect(self.path) as db, write_tx(db):
            require(db.execute("SELECT 1 FROM decisions WHERE id=?", (decision_id,)).fetchone() is not None,
                    "DECISION_REQUIRED", "task must reference approved decision")
            for dep in dependencies:
                require(db.execute("SELECT 1 FROM tasks WHERE id=?", (dep,)).fetchone() is not None,
                        "MISSING_DEPENDENCY", f"unknown prerequisite task: {dep}")
            db.execute("INSERT INTO tasks(id,decision_id,objective,constraints_json,acceptance_json,dependencies_json,priority,created_at,updated_at,policy_json,knowledge_scope_json) VALUES(?,?,?,?,?,?,?,?,?,?,?)",
                       (task_id, decision_id, objective, dumps(constraints), dumps(acceptance), dumps(dependencies), priority, utcnow(), utcnow(), dumps(policy), dumps(scope)))
        return self.task(task_id)

    def task(self, task_id: str) -> dict:
        with connect(self.path) as db:
            r = db.execute("SELECT * FROM tasks WHERE id=?", (task_id,)).fetchone()
            require(r is not None, "TASK_NOT_FOUND", task_id)
            return {**dict(r), "constraints": json.loads(r["constraints_json"]),
                    "acceptance": json.loads(r["acceptance_json"]),
                    "dependencies": json.loads(r["dependencies_json"]),
                    "execution_policy": json.loads(r["policy_json"]),
                    "knowledge_scope": json.loads(r["knowledge_scope_json"])}

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
                        max_timeout_seconds: int = 86400, permit_ttl_seconds: int = 3600,
                        inputs: dict | None = None):
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
            approved_inputs = inputs if inputs is not None else {}
            prior = db.execute("SELECT COUNT(*) FROM handoffs WHERE task_id=? AND recipe_id=? AND status!='ABORTED'",
                               (task_id, recipe_id)).fetchone()[0]
            authorize_handoff(json.loads(task["policy_json"]), recipe_id, source_commit,
                              allowed_profiles, approved_inputs, max_timeout_seconds, prior)
            handoff_id = "HOF-" + uuid.uuid4().hex[:20]
            attempt_id = "ATT-" + uuid.uuid4().hex[:20]
            claims = {"handoff_id": handoff_id, "task_id": task_id, "attempt_id": attempt_id,
                      "decision_id": task["decision_id"], "generation": generation, "recipe_id": recipe_id,
                      "source_commit": source_commit, "allowed_profiles": allowed_profiles, "max_timeout_seconds": max_timeout_seconds,
                      "inputs_fingerprint": payload_fingerprint(approved_inputs),
                      "expires_at": int(time.time()) + permit_ttl_seconds}
            permit = sign_permit(self.secret, claims)
            db.execute("""INSERT INTO handoffs(id,task_id,generation,attempt_id,recipe_id,job_id,status,permitted_json,source_commit,created_at)
                        VALUES(?,?,?,?,?,NULL,?,?,?,?)""",
                       (handoff_id, task_id, generation, attempt_id, recipe_id, "PREPARED", dumps(permit), source_commit, utcnow()))
            db.execute("UPDATE tasks SET status='HANDOFF_PREPARED',updated_at=? WHERE id=?", (utcnow(), task_id))
            self._audit(db, task_id, "HANDOFF_PREPARED", {"handoff_id": handoff_id, "attempt_id": attempt_id})
        return {"handoff_id": handoff_id, "attempt_id": attempt_id, "task_id": task_id,
                "decision_id": task["decision_id"], "generation": generation, "recipe_id": recipe_id,
                "source_commit": source_commit, "permit": permit}

    def acknowledge_handoff(self, handoff_id: str, receipt: dict):
        """Only authenticated Node Broker receipts can transfer ownership."""
        verify_receipt(self.secret, receipt)
        job_id = receipt["job_id"]
        require(receipt["handoff_id"] == handoff_id, "INVALID_RECEIPT", "handoff mismatch")
        with connect(self.path) as db, write_tx(db):
            handoff = db.execute("SELECT * FROM handoffs WHERE id=?", (handoff_id,)).fetchone()
            require(handoff is not None, "HANDOFF_NOT_FOUND", handoff_id)
            claims = json.loads(handoff["permitted_json"])["claims"]
            for key in ("task_id", "attempt_id", "decision_id", "source_commit"):
                require(receipt[key] == claims[key], "INVALID_RECEIPT", f"receipt {key} mismatch")
            if handoff["status"] == "ACCEPTED":
                require(handoff["job_id"] == job_id, "IDEMPOTENCY_CONFLICT", "handoff acknowledged with another job")
                return {"task_id": handoff["task_id"], "job_id": job_id, "status": "WAITING_JOB"}
            require(handoff["status"] == "PREPARED", "INVALID_TRANSITION", "handoff cannot be acknowledged")
            task = db.execute("SELECT status FROM tasks WHERE id=?", (handoff["task_id"],)).fetchone()
            require(task and task["status"] == "HANDOFF_PREPARED", "INVALID_TRANSITION", "task is not awaiting acceptance")
            db.execute("UPDATE handoffs SET status='ACCEPTED',job_id=?,receipt_json=? WHERE id=?",
                       (job_id, dumps(receipt), handoff_id))
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

    def _require_evidence(self, db, task_id: str, refs: list[str], required_kinds=()):
        require(isinstance(refs, list) and all(isinstance(v, str) for v in refs),
                "EVIDENCE_REQUIRED", "evidence must be a list of IDs")
        rows = []
        for ref in refs:
            r = db.execute("SELECT * FROM evidence WHERE id=? AND task_id=?", (ref, task_id)).fetchone()
            require(r is not None, "EVIDENCE_NOT_FOUND", f"unregistered evidence {ref}")
            # A database pointer is not proof that the underlying evidence still
            # exists or has not been changed. Every acceptance/promotion must
            # verify bytes against the content-addressed digest.
            blob = self.evidence_root / r["sha256"][:2] / r["sha256"]
            try:
                content = blob.read_bytes()
            except OSError as exc:
                raise ContractError("EVIDENCE_MISSING", f"unreadable evidence {ref}") from exc
            require(hashlib.sha256(content).hexdigest() == r["sha256"],
                    "EVIDENCE_CORRUPT", f"evidence {ref} hash mismatch")
            rows.append(r)
        for kind in required_kinds:
            require(any(r["kind"] == kind for r in rows), "ACCEPTANCE_FAILED", f"missing evidence kind: {kind}")
        return rows

    def record_evidence(self, task_id: str, worker_id: str, generation: int, kind: str, text: str,
                        attempt_id: str | None = None, metadata: dict | None = None) -> dict:
        """Immutable, content-addressed small Worker notes; big raw logs remain on Node Broker."""
        require(kind in KINDS and isinstance(text, str) and 0 < len(text.encode()) <= 32768,
                "INVALID_EVIDENCE", "evidence must be approved type, nonempty and <=32KiB")
        require(metadata is None or isinstance(metadata, dict), "INVALID_EVIDENCE", "metadata must be object")
        if attempt_id is not None:
            validate_id(attempt_id, "attempt_id")
        content = text.encode("utf-8")
        sha = hashlib.sha256(content).hexdigest()
        dst = self.evidence_root / sha[:2] / sha
        dst.parent.mkdir(parents=True, exist_ok=True)
        # Immutable by hash, crash-safe publication. A file alone grants no authority.
        tmp = dst.with_name(sha + '.' + uuid.uuid4().hex + '.tmp')
        with tmp.open('xb') as handle:
            os.fchmod(handle.fileno(), 0o600)
            handle.write(content)
            handle.flush()
            os.fsync(handle.fileno())
        # Link publishes without replacing existing content-addressed evidence.
        # A pre-existing corrupted blob must be reported, not silently "repaired".
        try:
            os.link(tmp, dst)
        except FileExistsError:
            require(hashlib.sha256(dst.read_bytes()).hexdigest() == sha,
                    "EVIDENCE_CORRUPT", "existing blob disagrees with its content hash")
        finally:
            tmp.unlink(missing_ok=True)
        with connect(self.path) as db, write_tx(db):
            self._require_assignment(db, task_id, worker_id, generation)
            if attempt_id is not None:
                r = db.execute("SELECT task_id FROM handoffs WHERE attempt_id=?", (attempt_id,)).fetchone()
                require(r is not None and r["task_id"] == task_id, "INVALID_ATTEMPT", "attempt not in task")
            ref = "EV-" + uuid.uuid4().hex[:20]
            db.execute("INSERT INTO evidence VALUES(?,?,?,?,?,?,?,?,?)",
                       (ref, task_id, attempt_id, kind, sha, worker_id, generation, dumps(metadata or {}), utcnow()))
            self._audit(db, task_id, "EVIDENCE_RECORDED", {"evidence_id": ref, "kind": kind, "sha256": sha})
        return {"evidence_id": ref, "sha256": sha, "kind": kind}

    def read_evidence(self, evidence_id: str) -> dict:
        with connect(self.path) as db:
            row = db.execute("SELECT * FROM evidence WHERE id=?", (evidence_id,)).fetchone()
            require(row is not None, "EVIDENCE_NOT_FOUND", evidence_id)
            data = (self.evidence_root / row["sha256"][:2] / row["sha256"]).read_bytes()
            require(hashlib.sha256(data).hexdigest() == row["sha256"], "EVIDENCE_CORRUPT", evidence_id)
            return {**dict(row), "text": data.decode("utf-8"), "metadata": json.loads(row["metadata_json"])}

    def complete(self, task_id: str, worker_id: str, generation: int, evidence: list[str]):
        with connect(self.path) as db, write_tx(db):
            task = self._require_assignment(db, task_id, worker_id, generation)
            acceptance = validate_acceptance(json.loads(task["acceptance_json"]))
            self._require_evidence(db, task_id, evidence, acceptance.get("required_evidence_kinds", []))
            if acceptance["type"] == "job_exit":
                require(task["last_job_id"] and task["last_event_json"], "ACCEPTANCE_FAILED", "no authenticated Job result")
                event = json.loads(task["last_event_json"])
                verify_event(self.secret, event)
                result = event.get("payload", {}).get("result", {})
                require(event["job_id"] == task["last_job_id"] and event["type"] == "JOB_SUCCEEDED"
                        and result.get("state") == "SUCCEEDED"
                        and result.get("exit_code") == acceptance["exit_code"],
                        "ACCEPTANCE_FAILED", "authenticated job result does not meet objective check")
            else:
                require(False, "HUMAN_ACCEPTANCE_REQUIRED", "worker evidence alone cannot certify task completion")
            db.execute("UPDATE tasks SET status='DONE',worker_id=NULL,updated_at=? WHERE id=?", (utcnow(), task_id))
            self._audit(db, task_id, "TASK_COMPLETED", {"evidence": evidence})
        return self.task(task_id)

    def human_complete(self, task_id: str, approved_by: str, evidence: list[str]) -> dict:
        """Operator-only action; caller authentication is enforced by deployment boundary."""
        require(isinstance(approved_by, str) and approved_by.strip(), "OPERATOR_REQUIRED", "human identity required")
        with connect(self.path) as db, write_tx(db):
            task = db.execute("SELECT * FROM tasks WHERE id=?", (task_id,)).fetchone()
            require(task is not None and task["status"] in ("ASSIGNED", "RESULT_READY"),
                    "INVALID_TRANSITION", "task not available for manual acceptance")
            predicate = validate_acceptance(json.loads(task["acceptance_json"]))
            require(predicate["type"] == "evidence_only", "INVALID_ACCEPTANCE", "job result tasks use machine acceptance")
            require(bool(predicate["required_evidence_kinds"]), "EVIDENCE_REQUIRED", "manual acceptance needs criteria")
            self._require_evidence(db, task_id, evidence, predicate["required_evidence_kinds"])
            db.execute("UPDATE tasks SET status='DONE',worker_id=NULL,lease_expires_at=NULL,updated_at=? WHERE id=?",
                       (utcnow(), task_id))
            self._audit(db, task_id, "HUMAN_ACCEPTED", {"approved_by": approved_by, "evidence": evidence})
        return self.task(task_id)

    def escalate(self, task_id: str, worker_id: str, generation: int, question: str,
                 observations: list[str], evidence: list[str]):
        require(isinstance(question, str) and question.strip() and isinstance(observations, list) and
                isinstance(evidence, list) and bool(evidence), "EVIDENCE_REQUIRED", "question and evidence required")
        with connect(self.path) as db, write_tx(db):
            self._require_assignment(db, task_id, worker_id, generation)
            self._require_evidence(db, task_id, evidence)
            esc_id = "ESC-" + uuid.uuid4().hex[:20]
            db.execute("INSERT INTO escalations VALUES(?,?,?,?,?,'OPEN',NULL,?)",
                       (esc_id, task_id, question, dumps(observations), dumps(evidence), utcnow()))
            db.execute("UPDATE tasks SET status='BLOCKED',worker_id=NULL,updated_at=? WHERE id=?", (utcnow(), task_id))
            self._audit(db, task_id, "ESCALATED", {"escalation_id": esc_id, "question": question})
            return {"escalation_id": esc_id, "task_id": task_id, "status": "OPEN"}

    def resolve_escalation(self, escalation_id: str, decision_id: str, approved_by: str, rationale: str,
                           execution_policy: dict | None = None, acceptance: dict | None = None):
        """Requires explicit human command; scheduler never invokes this automatically."""
        validate_id(decision_id, "decision_id")
        require(approved_by.strip() and rationale.strip(), "INVALID_DECISION", "human approval required")
        with connect(self.path) as db, write_tx(db):
            esc = db.execute("SELECT * FROM escalations WHERE id=?", (escalation_id,)).fetchone()
            require(esc is not None and esc["status"] == "OPEN", "INVALID_ESCALATION", "not an open escalation")
            db.execute("INSERT INTO decisions VALUES(?,?,?,?)", (decision_id, approved_by, rationale, utcnow()))
            db.execute("UPDATE escalations SET status='RESOLVED',resolution_decision=? WHERE id=?", (decision_id, escalation_id))
            task = db.execute("SELECT * FROM tasks WHERE id=?", (esc["task_id"],)).fetchone()
            policy = validate_execution_policy(execution_policy) if execution_policy is not None else json.loads(task["policy_json"])
            predicate = validate_acceptance(acceptance) if acceptance is not None else json.loads(task["acceptance_json"])
            db.execute("UPDATE tasks SET status='READY',decision_id=?,policy_json=?,acceptance_json=?,updated_at=? WHERE id=?",
                       (decision_id, dumps(policy), dumps(predicate), utcnow(), esc["task_id"]))
            self._audit(db, esc["task_id"], "HUMAN_DECISION", {"decision_id": decision_id, "escalation_id": escalation_id})
        return {"task_id": esc["task_id"], "decision_id": decision_id, "status": "READY"}

    def events(self, task_id: str) -> list[dict]:
        with connect(self.path) as db:
            return [{**dict(r), "details": json.loads(r["details_json"])} for r in db.execute("SELECT * FROM task_events WHERE task_id=? ORDER BY id", (task_id,))]

    def reconcile_handoffs(self, broker) -> dict:
        """Only authenticated lookup of the actual Broker may resolve an uncertain submit.

        On transport failure this raises without changing PREPARED: no blind replay.
        Permit must have expired before a confirmed-absent handoff can be aborted.
        """
        recovered, aborted, pending = [], [], []
        with connect(self.path) as db:
            rows = db.execute("SELECT * FROM handoffs WHERE status='PREPARED' ORDER BY created_at").fetchall()
        for h in rows:
            result = broker.lookup_handoff(h["id"])  # May fail: leave undecided if unreachable
            if result is not None:
                self.acknowledge_handoff(h["id"], result["receipt"])
                recovered.append(h["id"])
                continue
            expires = json.loads(h["permitted_json"])["claims"]["expires_at"]
            # Allow clock skew between nodes before concluding no valid submission is possible.
            if time.time() <= expires + 120:
                pending.append(h["id"])
                continue
            with connect(self.path) as db, write_tx(db):
                row = db.execute("SELECT status,task_id FROM handoffs WHERE id=?", (h["id"],)).fetchone()
                if row and row["status"] == "PREPARED":
                    db.execute("UPDATE handoffs SET status='ABORTED' WHERE id=?", (h["id"],))
                    db.execute("UPDATE tasks SET status='READY',worker_id=NULL,lease_expires_at=NULL,updated_at=? WHERE id=? AND status='HANDOFF_PREPARED'",
                               (utcnow(), row["task_id"]))
                    self._audit(db, row["task_id"], "HANDOFF_ABORTED", {"handoff_id": h["id"], "reason": "expired and broker confirms absent"})
                    aborted.append(h["id"])
        return {"recovered": recovered, "aborted": aborted, "pending": pending}

    def propose_lesson(self, task_id: str, worker_id: str, generation: int, title: str, summary: str,
                       scope: dict, supporting_evidence: list[str]) -> dict:
        require(isinstance(title, str) and title.strip() and isinstance(summary, str) and summary.strip()
                and isinstance(scope, dict) and scope and all(isinstance(k, str) and isinstance(v, str) for k,v in scope.items()),
                "INVALID_LESSON", "title, summary and exact string scope required")
        with connect(self.path) as db, write_tx(db):
            self._require_assignment(db, task_id, worker_id, generation)
            require(bool(supporting_evidence), "EVIDENCE_REQUIRED", "lesson requires evidence")
            self._require_evidence(db, task_id, supporting_evidence)
            ident = "LES-" + uuid.uuid4().hex[:20]
            db.execute("INSERT INTO lessons(id,task_id,title,summary,scope_json,supporting_json,state,author_worker,created_at) VALUES(?,?,?,?,?,?,?,?,?)",
                       (ident, task_id, title, summary, dumps(scope), dumps(supporting_evidence), "CANDIDATE", worker_id, utcnow()))
            self._audit(db, task_id, "LESSON_PROPOSED", {"lesson_id": ident})
            return {"lesson_id": ident, "state": "CANDIDATE"}

    def verify_lesson(self, lesson_id: str, task_id: str, worker_id: str, generation: int,
                      verification_evidence: list[str]) -> dict:
        with connect(self.path) as db, write_tx(db):
            self._require_assignment(db, task_id, worker_id, generation)
            lesson = db.execute("SELECT * FROM lessons WHERE id=?", (lesson_id,)).fetchone()
            require(lesson is not None and lesson["state"] == "CANDIDATE", "INVALID_LESSON", "only candidates may be verified")
            require(worker_id != lesson["author_worker"] and task_id != lesson["task_id"],
                    "INDEPENDENT_REVIEW_REQUIRED", "independent verifier and separate task required")
            verifier_task = db.execute("SELECT * FROM tasks WHERE id=?", (task_id,)).fetchone()
            require(verifier_task["last_job_id"] and verifier_task["last_event_json"],
                    "INDEPENDENT_REVIEW_REQUIRED", "verification must include a signed Job result")
            job_event = json.loads(verifier_task["last_event_json"])
            verify_event(self.secret, job_event)
            require(job_event["job_id"] == verifier_task["last_job_id"] and
                    job_event["type"] == "JOB_SUCCEEDED" and
                    job_event["payload"].get("result",{}).get("exit_code") == 0,
                    "INDEPENDENT_REVIEW_REQUIRED", "verification Job did not pass")
            require(bool(verification_evidence), "EVIDENCE_REQUIRED", "verification evidence needed")
            refs = self._require_evidence(db, task_id, verification_evidence, ["verification"])
            require(any(r["producer"] == worker_id for r in refs), "INDEPENDENT_REVIEW_REQUIRED", "verification must be recorded by reviewer")
            db.execute("UPDATE lessons SET state='PEER_CHECKED',verified_by=?,verification_json=?,verified_at=? WHERE id=?",
                       (worker_id, dumps(verification_evidence), utcnow(), lesson_id))
            self._audit(db, task_id, "LESSON_PEER_CHECKED", {"lesson_id": lesson_id, "author_task_id": lesson["task_id"]})
            return {"lesson_id": lesson_id, "state": "PEER_CHECKED"}

    def approve_lesson(self, lesson_id: str, approved_by: str, rationale: str) -> dict:
        """Explicit human promotion, after an independent successful verification Job.

        Only the privileged operator control boundary may invoke this method.
        """
        require(isinstance(approved_by, str) and approved_by.strip() and
                isinstance(rationale, str) and rationale.strip(), "OPERATOR_REQUIRED", "approval and rationale required")
        with connect(self.path) as db, write_tx(db):
            lesson = db.execute("SELECT * FROM lessons WHERE id=?", (lesson_id,)).fetchone()
            require(lesson is not None and lesson["state"] == "PEER_CHECKED", "INVALID_LESSON", "independent evidence required")
            db.execute("UPDATE lessons SET state='VERIFIED',verified_by=?,verified_at=? WHERE id=?",
                       (approved_by, utcnow(), lesson_id))
            self._audit(db, lesson["task_id"], "LESSON_VERIFIED", {"lesson_id": lesson_id, "approved_by": approved_by,
                                                                  "rationale": rationale})
        return {"lesson_id": lesson_id, "state": "VERIFIED"}

    def knowledge_search(self, scope: dict, include_candidates: bool = False) -> list[dict]:
        """Exact-scoped, evidence-linked retrieval; deliberately no semantic ranking."""
        require(isinstance(scope, dict) and scope and all(isinstance(k, str) and isinstance(v, str) for k,v in scope.items()),
                "INVALID_SCOPE", "exact scope dictionary required")
        with connect(self.path) as db:
            rows = db.execute("SELECT * FROM lessons ORDER BY verified_at DESC,created_at DESC").fetchall()
        return [{**dict(r), "scope": json.loads(r["scope_json"]),
                 "supporting_evidence": json.loads(r["supporting_json"])} for r in rows
                if (r["state"] == "VERIFIED" or include_candidates)
                and all(json.loads(r["scope_json"]).get(k) == v for k,v in scope.items())]
