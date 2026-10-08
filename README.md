# Bayesdesk General Explorer

**Engineer Leader / Worker Pool / Resource Broker** — a small, deterministic orchestration prototype for AI-assisted engineering and long-running compute experiments.

[Architecture design](docs/ARCHITECTURE.md) · [Operations guide](docs/OPERATIONS.md)

## The contract

The human makes **research decisions**. The **Engineer Leader assigns, tracks and escalates** but does not debug or rearchitect. Short-lived **Workers write code, investigate failures and submit compute jobs**. The **Node Broker is ordinary code**: it validates strict JSON contracts, schedules CPU/RAM/exclusive-GPU resources, runs trusted recipes, writes evidence and emits durable terminal events. No LLM is kept alive waiting for a 12-hour GPU job.

The Broker does **not** parse natural language or permit arbitrary shell snippets from a Worker. It only accepts operator-registered recipe IDs and typed parameters with a signed worker handoff permit.

```text
  Human + ChatGPT (research approval)
                |
       Engineer Leader (Task DAG / assignment / escalation)
              /   \
     Worker Pool    Node Broker (SQLite / capacity / typed recipes)
          |            ^
          +-- Job ----->|----> systemd / compute task
          |                    |
          +<-- Event Inbox <-- Outbox + result manifest
```

## Quick start

```bash
python3 -m pip install -e .
python3 -m unittest discover -s tests -v
export BAYESDESK_SHARED_SECRET="$(python3 -c 'import secrets;print(secrets.token_hex(32))')"
python3 -m bayesdesk broker inspect
python3 -m bayesdesk broker quote '{"cpu_units":2,"memory_mib":1024,"gpu_count":0}'
```

All CLI commands print JSON envelopes (`ok/data` or `ok/error`); see [walkthrough](docs/OPERATIONS.md). The test recipes and capacity file are examples, **not** a measured configuration of any GPU node.

## Implemented in the first prototype

- SQLite-backed Leader Task DAG, worker assignment generation, expiry/renewal, handoff preparation/acknowledgment and explicit human escalation.
- Signed HMAC permits with immutable resource profile scope; versioned Job input with trusted Recipe parameters and exact field validation.
- Node Broker `inspect/quote/submit/status/tick/outbox`, idempotent submission, CPU+RAM+exclusive-GPU reservation and queue backfill with head protection.
- Job process wrapper writing atomic outcome manifest, stdout/stderr; detached dev backend and systemd user-service backend; `UNKNOWN` safety quarantine.
- Signed, idempotent event outbox/inbox; result events wake up a **new** Worker, not a sleeping model session.
- Stdlib unittest regression suite with actual detached subprocess tests.

## Explicit limitations

This is a **working prototype**, not a production-ready distributed service. The `bridge-pump` demo only works with locally available control and node databases; Japan OCI ↔ Shanxi networking, Worker LLM adapters, verified code checkouts, strong GPU-device isolation, automatic reconciliation of `HANDOFF_PREPARED`, content-addressed evidence, robust checkpoint recovery and verified organizational knowledge still need implementation. `--executor local` is for testing; it cannot guarantee quota isolation/recovery. See [architecture design](docs/ARCHITECTURE.md) before deploying on a shared machine.

## Why not a multi-agent chat room?

This repo deliberately favors **exception-based management** over autonomous agent discussion. Engineering details are handled by Workers, long jobs by Broker, and architectural ambiguity by a structured escalation back to the human decision owner. Tasks, evidence and jobs persist independently from all Agent sessions.

An opt-in `worker-pool` command launches short-lived external Worker CLI processes from configured fixed argv and per-assignment Context Capsules. There is no automatic/undocumented Claude Code API integration; see Operations for the adapter boundary.
