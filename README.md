# Bayesdesk General Explorer — V0.2

**Engineer Leader / Worker Pool / Resource Broker** — a small, deterministic orchestration prototype for AI-assisted engineering and long-running compute experiments.

[Architecture design](docs/ARCHITECTURE.md) · [Design rationale](docs/DESIGN_RATIONALE.md) · [Operations](docs/OPERATIONS.md) · [Next-agent handoff](docs/ADAPTER_HANDOFF.md) · [中文交接](docs/EXECUTIVE_HANDOFF_ZH.md) · [Changelog](CHANGELOG.md)

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

## Implemented in V0.2 (local prototype)

- SQLite-backed Leader Task DAG, worker assignment generation, expiry/renewal, signed Broker receipt and handoff-recovery reconciliation, and explicit human escalation.
- Task-approved execution policies with typed input constraints, per-recipe attempt budgets, immutable resource profiles; HMAC permits pin exact Job inputs, recipe and source commit.
- Node Broker `inspect/quote/submit/status/tick/outbox`, idempotent submission, CPU+RAM+exclusive-GPU reservation and queue backfill with head protection.
- Job process wrapper writing atomic outcome manifest, stdout/stderr; detached dev backend and systemd user-service backend; `UNKNOWN` safety quarantine.
- Signed, idempotent event outbox/inbox; local deterministic `coordinator --once` / `coordinator --interval 5` reconciles lost Handoff ACK and event delivery without waking an LLM. A result marks the Task `RESULT_READY` for a **new** Worker, not a sleeping model session.
- Content-addressed Worker evidence with SHA-256 revalidation, machine-result acceptance, distinct peer verification and human-promoted VERIFIED lessons scoped for reuse.
- Stdlib unittest regression suite with actual detached subprocess tests; CLI Worker environment variables default to a minimal allowlist.

## Explicit limitations

This is a **working prototype**, not a production-ready distributed service. The `bridge-pump` demo only works with locally available control and node databases; Japan OCI ↔ Shanxi networking, Worker LLM adapters, verified code checkouts, strong GPU-device isolation, remote transport for prepared-handoff reconciliation, immutable raw artifact replication, checkpoint recovery, strong operator authentication and artifact-linked scientific acceptance still need implementation. `--executor local` is for testing; it cannot guarantee quota isolation/recovery. See [architecture design](docs/ARCHITECTURE.md) before deploying on a shared machine.

## Why not a multi-agent chat room?

This repo deliberately favors **exception-based management** over autonomous agent discussion. Engineering details are handled by Workers, long jobs by Broker, and architectural ambiguity by a structured escalation back to the human decision owner. Tasks, evidence and jobs persist independently from all Agent sessions.

An opt-in `worker-pool` command launches short-lived external Worker CLI processes from configured fixed argv and per-assignment Context Capsules. There is no automatic/undocumented Claude Code API integration; see Operations for the adapter boundary.

## Handoff to deployment/adaptation agents

Review [ADAPTER_HANDOFF.md](docs/ADAPTER_HANDOFF.md) and [DESIGN_RATIONALE.md](docs/DESIGN_RATIONALE.md) before connecting any real host. **V0.2 has no production operator/Worker authentication boundary**; do not provide untrusted Workers with the signing secret or control database privileges. Machine Job success is not scientific acceptance.
