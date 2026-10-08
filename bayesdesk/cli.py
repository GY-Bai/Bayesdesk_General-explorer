"""JSON-only CLI to integrate Claude Code, Codex, shell scripts, and external schedulers."""
from __future__ import annotations
import argparse
import json
import os
from pathlib import Path
import sys
import time

from .broker import Broker
from .leader import Leader
from .errors import ContractError


def parse_json(value: str):
    if value.startswith("@"):
        return json.loads(Path(value[1:]).read_text())
    return json.loads(value)


def secret():
    return os.environ.get("BAYESDESK_SHARED_SECRET", "")


def main(argv=None):
    p = argparse.ArgumentParser(prog="bayesdesk", description="Deterministic orchestration; all outputs JSON")
    p.add_argument("--leader-db", default="./state/leader.sqlite3")
    p.add_argument("--broker-db", default="./state/broker.sqlite3")
    p.add_argument("--node-config", default="./examples/node.json")
    p.add_argument("--recipes", default="./examples/recipes.json")
    p.add_argument("--jobs-root", default="./state/jobs")
    p.add_argument("--executor", choices=["local", "systemd-user"], default="local")
    actions = p.add_subparsers(dest="system", required=True)
    leader = actions.add_parser("leader")
    lead = leader.add_subparsers(dest="action", required=True)
    for name in ["decision", "task-create", "task", "tasks", "assign", "handoff-prepare", "handoff-ack",
                 "complete", "human-complete", "escalate", "escalation-resolve", "ingest-event", "events", "renew", "requeue-expired", "reconcile-handoffs", "evidence-add", "evidence-read", "lesson-propose", "lesson-verify", "lesson-approve", "knowledge-search"]:
        sp = lead.add_parser(name)
        if name not in ("tasks", "assign", "requeue-expired", "reconcile-handoffs"):
            sp.add_argument("data", help="JSON object or @filename (except task/events: use task ID string)")
        if name == "assign":
            sp.add_argument("worker_id")
    broker = actions.add_parser("broker")
    br = broker.add_subparsers(dest="action", required=True)
    for name in ["inspect", "quote", "submit", "status", "lookup-handoff", "tick", "outbox", "delivered", "release-unknown", "serve"]:
        sp = br.add_parser(name)
        if name in ("quote", "submit", "status", "lookup-handoff", "delivered", "release-unknown"):
            sp.add_argument("data", help="JSON/@filename for quote,submit,release-unknown; otherwise string ID")
        if name == "serve":
            sp.add_argument("--interval", type=float, default=5)
    pool = actions.add_parser("worker-pool", help="launch short-lived external coding agents")
    pool.add_argument("--config", required=True, help="JSON worker launch configuration")
    pool.add_argument("--capsules", default="./state/capsules")
    pool.add_argument("--once", action="store_true")
    pool.add_argument("--interval", type=float, default=5)
    coordination = actions.add_parser("coordinator", help="trusted LOCAL handoff/event reconciliation")
    coordination.add_argument("--once", action="store_true")
    coordination.add_argument("--interval", type=float, default=5)
    actions.add_parser("bridge-pump", help="development only: transfer events between two local DBs")
    args = p.parse_args(argv)
    try:
        if args.system == "leader":
            ctl = Leader(args.leader_db, secret())
            a = args.action
            if a == "tasks":
                out = ctl.list_tasks()
            elif a == "requeue-expired":
                out = ctl.requeue_expired()
            elif a == "assign":
                out = ctl.assign(args.worker_id)
            elif a == "reconcile-handoffs":
                node = parse_json("@" + args.node_config)
                recipes = parse_json("@" + args.recipes)
                broker = Broker(args.broker_db, node, recipes, secret(), args.jobs_root, args.executor)
                out = ctl.reconcile_handoffs(broker)  # local only; remote transport must authenticate
            elif a == "evidence-read":
                out = ctl.read_evidence(args.data)
            elif a in ("task", "events"):
                out = ctl.task(args.data) if a == "task" else ctl.events(args.data)
            else:
                data = parse_json(args.data)
                out = {
                    "decision": lambda: ctl.approve_decision(**data),
                    "task-create": lambda: ctl.create_task(**data),
                    "handoff-prepare": lambda: ctl.prepare_handoff(**data),
                    "handoff-ack": lambda: ctl.acknowledge_handoff(**data),
                    "complete": lambda: ctl.complete(**data),
                    "human-complete": lambda: ctl.human_complete(**data),
                    "evidence-add": lambda: ctl.record_evidence(**data),
                    "lesson-propose": lambda: ctl.propose_lesson(**data),
                    "lesson-verify": lambda: ctl.verify_lesson(**data),
                    "lesson-approve": lambda: ctl.approve_lesson(**data),
                    "knowledge-search": lambda: ctl.knowledge_search(**data),
                    "renew": lambda: ctl.renew(**data),
                    "escalate": lambda: ctl.escalate(**data),
                    "escalation-resolve": lambda: ctl.resolve_escalation(**data),
                    "ingest-event": lambda: ctl.ingest_event(data),
                }[a]()
        elif args.system == "broker":
            node = parse_json("@" + args.node_config)
            recipes = parse_json("@" + args.recipes)
            ctl = Broker(args.broker_db, node, recipes, secret(), args.jobs_root, args.executor)
            a = args.action
            if a == "inspect": out = ctl.inspect()
            elif a == "quote": out = ctl.quote(parse_json(args.data))
            elif a == "submit": out = ctl.submit(parse_json(args.data))
            elif a == "status": out = ctl.status(args.data)
            elif a == "lookup-handoff": out = ctl.lookup_handoff(args.data)
            elif a == "tick": out = ctl.tick()
            elif a == "outbox": out = ctl.outbox()
            elif a == "delivered":
                ctl.mark_delivered(args.data)
                out = {"event_id": args.data, "delivered": True}
            elif a == "release-unknown": out = ctl.release_unknown(**parse_json(args.data))
            else:
                if args.interval < 1:
                    raise ContractError("INVALID_INTERVAL", "interval must be >=1s")
                while True:
                    outcome = ctl.tick()
                    if outcome["finished"] or outcome["launched"]:
                        print(json.dumps(outcome), flush=True)
                    time.sleep(args.interval)  # plain daemon timer; no model sessions
        elif args.system == "coordinator":
            from .coordinator import LocalCoordinator
            node = parse_json("@" + args.node_config)
            recipes = parse_json("@" + args.recipes)
            service = LocalCoordinator(Leader(args.leader_db, secret()),
                                       Broker(args.broker_db, node, recipes, secret(), args.jobs_root, args.executor))
            if args.once:
                out = service.tick()
            else:
                if args.interval < 1:
                    raise ContractError("INVALID_INTERVAL", "interval must be >=1s")
                while True:
                    change = service.tick()
                    if change["events_delivered"] or change["handoffs"]["recovered"] or change["handoffs"]["aborted"] or change["expired_assignments"]:
                        print(json.dumps(change), flush=True)
                    time.sleep(args.interval)
        elif args.system == "worker-pool":
            from .worker_pool import WorkerPool
            ctl = WorkerPool(Leader(args.leader_db, secret()), parse_json("@" + args.config), args.capsules)
            if args.once:
                out = ctl.tick()
            else:
                if args.interval < 1:
                    raise ContractError("INVALID_INTERVAL", "interval must be >=1s")
                while True:
                    changed = ctl.tick()
                    if changed:
                        print(json.dumps({"started": changed}), flush=True)
                    time.sleep(args.interval)
        else:
            node = parse_json("@" + args.node_config)
            recipes = parse_json("@" + args.recipes)
            source = Broker(args.broker_db, node, recipes, secret(), args.jobs_root, args.executor)
            target = Leader(args.leader_db, secret())
            n = 0
            for event in source.outbox():
                target.ingest_event(event)
                source.mark_delivered(event["event_id"])
                n += 1
            out = {"events_forwarded": n}
        print(json.dumps({"ok": True, "data": out}, ensure_ascii=False, default=str))
        return 0
    except ContractError as err:
        print(json.dumps(err.as_dict(), ensure_ascii=False), file=sys.stderr)
        return 2
    except (ValueError, KeyError, TypeError, OSError) as err:
        print(json.dumps({"ok": False, "error": {"code": "OPERATION_FAILED", "message": str(err), "retryable": False}},
                         ensure_ascii=False), file=sys.stderr)
        return 3


if __name__ == "__main__":
    sys.exit(main())
