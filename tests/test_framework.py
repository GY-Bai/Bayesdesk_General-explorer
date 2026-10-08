from __future__ import annotations

import json
from pathlib import Path
import sys
import tempfile
import time
import unittest

from bayesdesk.broker import Broker
from bayesdesk.leader import Leader
from bayesdesk.errors import ContractError

ROOT = Path(__file__).resolve().parents[1]
SECRET = "fixture-secret-for-tests-12345678"
SHA = "a" * 40


class FrameworkTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        base = Path(self.temp.name)
        self.leader = Leader(base / "control.sqlite3", SECRET)
        self.broker = Broker(base / "node.sqlite3", {"node_id": "test-node",
                              "allocatable": {"cpu_units": 8, "memory_mib": 8192, "gpu_count": 1}},
                             {"smoke.v1": {"argv": [sys.executable, "-m", "bayesdesk.simjob"],
                                           "cwd": str(ROOT), "parameters": {
                                               "duration": {"type": "integer", "flag": "--duration", "min": 0, "max": 30},
                                               "result": {"type": "enum", "flag": "--result", "choices": ["pass", "fail"]}}}},
                             SECRET, base / "jobs", executor_name="local")
        self.leader.approve_decision("D1", "human", "approve baseline tests")
        self.leader.create_task("T1", "D1", "develop smoke", [], ["never change goal"], {"tests": ["smoke"]})

    def _prepare(self, task_id="T1", worker="worker-a", profile=None, inputs=None):
        assignment = self.leader.assign(worker)
        self.assertIsNotNone(assignment)
        self.assertEqual(assignment["task_id"], task_id)
        profile = profile or {"cpu_units": 2, "memory_mib": 1024, "gpu_count": 0}
        handoff = self.leader.prepare_handoff(task_id, worker, assignment["generation"],
                                             "smoke.v1", SHA, {"full": profile})
        job = {"schema_version": 1, "handoff_id": handoff["handoff_id"],
               "task_id": task_id, "attempt_id": handoff["attempt_id"], "decision_id": handoff["decision_id"],
               "generation": handoff["generation"], "source_commit": SHA,
               "recipe_id": "smoke.v1", "inputs": inputs or {"duration": 0, "result": "pass"},
               "profile_name": "full", "profile": profile, "timeout_seconds": 10,
               "permit": handoff["permit"]}
        return assignment, handoff, job

    def _wait_terminal(self, job_id, timeout=8):
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            self.broker.reconcile()
            state = self.broker.status(job_id)["state"]
            if state in ("SUCCEEDED", "FAILED"):
                return state
            time.sleep(.07)  # Test harness only, not any agent.
        self.fail(f"job did not finish: {self.broker.status(job_id)}")

    def test_worker_handoff_and_event_resume(self):
        assignment, handoff, job = self._prepare()
        submitted = self.broker.submit(job)
        self.assertEqual(self.broker.submit(job)["job_id"], submitted["job_id"])
        self.leader.acknowledge_handoff(handoff["handoff_id"], submitted["job_id"])
        self.assertEqual(self.leader.task("T1")["status"], "WAITING_JOB")
        self.assertEqual(len(self.broker.dispatch()), 1)
        self.assertEqual(self._wait_terminal(submitted["job_id"]), "SUCCEEDED")
        events = self.broker.outbox()
        self.assertEqual(len(events), 1)
        self.assertEqual(self.leader.ingest_event(events[0]), {"deduplicated": False, "pending_handoff": False})
        self.assertTrue(self.leader.ingest_event(events[0])["deduplicated"])
        self.broker.mark_delivered(events[0]["event_id"])
        self.assertEqual(self.broker.outbox(), [])
        self.assertEqual(self.leader.task("T1")["status"], "RESULT_READY")
        next_assignment = self.leader.assign("worker-b")
        self.assertGreater(next_assignment["generation"], assignment["generation"])
        self.leader.complete("T1", "worker-b", next_assignment["generation"], ["job manifest"]) 
        self.assertEqual(self.leader.task("T1")["status"], "DONE")

    def test_backfill_gpu_and_cpu_parallel(self):
        _, h1, j1 = self._prepare(profile={"cpu_units": 2, "memory_mib": 1024, "gpu_count": 1},
                                  inputs={"duration": 2, "result": "pass"})
        jid1 = self.broker.submit(j1)["job_id"]
        self.leader.acknowledge_handoff(h1["handoff_id"], jid1)
        self.leader.create_task("T2", "D1", "cpu build", [], [], {"test": "pass"})
        _, h2, j2 = self._prepare(task_id="T2", profile={"cpu_units": 2, "memory_mib": 1024, "gpu_count": 0},
                                  inputs={"duration": 0, "result": "pass"})
        jid2 = self.broker.submit(j2)["job_id"]
        self.leader.acknowledge_handoff(h2["handoff_id"], jid2)
        self.leader.create_task("T3", "D1", "second gpu experiment", [], [], {})
        _, h3, j3 = self._prepare(task_id="T3", profile={"cpu_units": 2, "memory_mib": 1024, "gpu_count": 1})
        jid3 = self.broker.submit(j3)["job_id"]
        self.leader.acknowledge_handoff(h3["handoff_id"], jid3)
        started = self.broker.dispatch()
        self.assertEqual({x["job_id"] for x in started}, {jid1, jid2})
        self.assertEqual(self.broker.status(jid3)["state"], "QUEUED")
        self.assertEqual(self.broker.inspect()["reserved"]["gpu_count"], 1)
        self.assertEqual(self._wait_terminal(jid2), "SUCCEEDED")
        self.assertEqual(self._wait_terminal(jid1), "SUCCEEDED")
        self.broker.dispatch()
        self.assertIn(self.broker.status(jid3)["state"], ("STARTING", "RUNNING"))
        self.assertEqual(self._wait_terminal(jid3), "SUCCEEDED")

    def test_two_dispatchers_do_not_overallocate_gpu(self):
        from concurrent.futures import ThreadPoolExecutor
        _, h1, j1 = self._prepare(profile={"cpu_units": 2, "memory_mib": 1024, "gpu_count": 1},
                                  inputs={"duration": 1, "result": "pass"})
        jid1 = self.broker.submit(j1)["job_id"]
        self.leader.acknowledge_handoff(h1["handoff_id"], jid1)
        self.leader.create_task("T2", "D1", "gpu study B", [], [], {})
        _, h2, j2 = self._prepare(task_id="T2", profile={"cpu_units": 2, "memory_mib": 1024, "gpu_count": 1})
        jid2 = self.broker.submit(j2)["job_id"]
        self.leader.acknowledge_handoff(h2["handoff_id"], jid2)
        with ThreadPoolExecutor(max_workers=2) as pool:
            results = list(pool.map(lambda _: self.broker.dispatch(limit=1), range(2)))
        self.assertEqual(sum(len(x) for x in results), 1)
        self.assertEqual(self.broker.inspect()["reserved"]["gpu_count"], 1)
        self.assertEqual(self.broker.status(jid2)["state"], "QUEUED")
        self.assertEqual(self._wait_terminal(jid1), "SUCCEEDED")
        self.broker.dispatch(limit=1)
        self.assertEqual(self._wait_terminal(jid2), "SUCCEEDED")

    def test_expired_assignment_requeued_and_fenced(self):
        from bayesdesk.storage import connect, write_tx
        first = self.leader.assign("worker-a")
        with connect(self.leader.path) as db, write_tx(db):
            db.execute("UPDATE tasks SET lease_expires_at=0 WHERE id='T1'")
        self.assertEqual(self.leader.requeue_expired()["requeued"], ["T1"])
        second = self.leader.assign("worker-b")
        self.assertGreater(second["generation"], first["generation"])
        with self.assertRaises(ContractError) as ctx:
            self.leader.complete("T1", "worker-a", first["generation"], ["old-evidence"])
        self.assertEqual(ctx.exception.code, "STALE_ASSIGNMENT")

    def test_quote_is_not_reservation(self):
        q = self.broker.quote({"cpu_units": 4, "memory_mib": 4096, "gpu_count": 1})
        self.assertTrue(q["admission_now"])
        self.assertFalse(q["binding"])

    def test_broker_rejects_forged_or_changed_profiles(self):
        _, _, job = self._prepare()
        job["profile"]["memory_mib"] = 2048
        with self.assertRaises(ContractError) as ctx:
            self.broker.submit(job)
        self.assertEqual(ctx.exception.code, "PROFILE_NOT_APPROVED")

    def test_broker_permit_binds_source_commit(self):
        _, _, job = self._prepare()
        job["source_commit"] = "b" * 40
        with self.assertRaises(ContractError) as ctx:
            self.broker.submit(job)
        self.assertEqual(ctx.exception.code, "INVALID_PERMIT")

    def test_concurrent_idempotent_submission(self):
        from concurrent.futures import ThreadPoolExecutor
        _, _, job = self._prepare()
        with ThreadPoolExecutor(max_workers=5) as pool:
            outputs = list(pool.map(lambda _: self.broker.submit(job), range(5)))
        self.assertEqual(len({x["job_id"] for x in outputs}), 1)
        self.assertEqual(self.broker.inspect()["queued"], 1)

    def test_broker_rejects_arbitrary_input(self):
        _, _, job = self._prepare()
        job["inputs"]["unsafe_shell"] = "rm -rf /"
        with self.assertRaises(ContractError) as ctx:
            self.broker.submit(job)
        self.assertEqual(ctx.exception.code, "INVALID_INPUT")

    def test_conflicting_idempotency_key_rejected(self):
        _, _, job = self._prepare()
        self.broker.submit(job)
        job["inputs"]["result"] = "fail"
        with self.assertRaises(ContractError) as ctx:
            self.broker.submit(job)
        self.assertEqual(ctx.exception.code, "IDEMPOTENCY_CONFLICT")

    def test_handoff_event_arrived_before_ack(self):
        _, handoff, job = self._prepare()
        accepted = self.broker.submit(job)
        self.broker.dispatch()
        self._wait_terminal(accepted["job_id"])
        event = self.broker.outbox()[0]
        response = self.leader.ingest_event(event)
        self.assertTrue(response["pending_handoff"])
        self.leader.acknowledge_handoff(handoff["handoff_id"], accepted["job_id"])
        self.assertEqual(self.leader.task("T1")["status"], "RESULT_READY")

    def test_failure_event_not_marked_success(self):
        _, handoff, job = self._prepare(inputs={"duration": 0, "result": "fail"})
        accepted = self.broker.submit(job)
        self.leader.acknowledge_handoff(handoff["handoff_id"], accepted["job_id"])
        self.broker.dispatch()
        self.assertEqual(self._wait_terminal(accepted["job_id"]), "FAILED")
        self.assertEqual(self.broker.outbox()[0]["type"], "JOB_FAILED")
        self.leader.ingest_event(self.broker.outbox()[0])
        self.assertEqual(self.leader.task("T1")["status"], "RESULT_READY")

    def test_escalation_requires_human_decision(self):
        assigned = self.leader.assign("worker-a")
        esc = self.leader.escalate("T1", "worker-a", assigned["generation"],
                                   "Is changing the model architecture permitted?", ["tested alternative A"], ["EV-11"])
        self.assertEqual(self.leader.task("T1")["status"], "BLOCKED")
        self.assertIsNone(self.leader.assign("worker-b"))
        self.leader.resolve_escalation(esc["escalation_id"], "D2", "human", "approve experiment change")
        self.assertEqual(self.leader.task("T1")["status"], "READY")
        self.assertIsNotNone(self.leader.assign("worker-b"))

    def test_dependency_blocks_task_until_accepted_completion(self):
        self.leader.create_task("T2", "D1", "dependent", ["T1"], [], {})
        first = self.leader.assign("worker-a")
        self.assertEqual(first["task_id"], "T1")
        self.leader.complete("T1", "worker-a", first["generation"], ["acceptance.json"])
        self.assertEqual(self.leader.assign("worker-b")["task_id"], "T2")

    def test_event_signature_rejects_tamper(self):
        _, handoff, job = self._prepare()
        accepted = self.broker.submit(job)
        self.leader.acknowledge_handoff(handoff["handoff_id"], accepted["job_id"])
        self.broker.dispatch()
        self._wait_terminal(accepted["job_id"])
        event = self.broker.outbox()[0]
        event["payload"]["result"]["exit_code"] = 999
        with self.assertRaises(ContractError) as ctx:
            self.leader.ingest_event(event)
        self.assertEqual(ctx.exception.code, "INVALID_EVENT_SIGNATURE")

    def test_worker_pool_emits_context_capsule(self):
        from bayesdesk.worker_pool import WorkerPool
        base = Path(self.temp.name)
        pool = WorkerPool(self.leader, {"max_concurrent": 1, "workers": [
            {"worker_id": "worker-cli", "argv": [sys.executable, "-c", "pass"], "cwd": str(ROOT)}]},
            base / "capsules")
        started = pool.tick()
        self.assertEqual(started[0]["task_id"], "T1")
        capsule = json.loads((base / "capsules" / "T1-g1.json").read_text())
        self.assertEqual(capsule["decision_id"], "D1")
        self.assertEqual(capsule["constraints"], ["never change goal"])
        self.assertEqual(pool.tick(), [])

    def test_stale_worker_fencing(self):
        first = self.leader.assign("worker-a")
        self.leader.complete("T1", "worker-a", first["generation"], ["check"])
        with self.assertRaises(ContractError) as ctx:
            self.leader.escalate("T1", "worker-a", first["generation"], "old", [], ["x"])
        self.assertEqual(ctx.exception.code, "STALE_ASSIGNMENT")


if __name__ == "__main__":
    unittest.main()
