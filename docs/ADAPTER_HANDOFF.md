# Implementation handoff for the next engineering agent — V0.2

This document is an **implementation boundary and adaptation checklist**, not a license to change architecture. The repository intentionally does not control a real remote machine yet.

## Starting point

1. Read `README.md`, `docs/DESIGN_RATIONALE.md`, `docs/ARCHITECTURE.md`, and `docs/OPERATIONS.md`.
2. Run `python -m unittest discover -s tests -v` with Python >=3.11. Do not skip failed tests to force a green result.
3. Observe the interfaces and source implementation: `leader.py` / `worker_pool.py` / `broker.py` / `policies.py` / `contracts.py` / `recipes.py`.
4. Preserve the distinction: **human Decision**, **Leader routing**, **Worker engineering**, **Broker deterministic execution**.

## Existing machine discovery is mandatory, not a theoretical default

The existing Shanxi shell queue is **not present in this repository**. Before migrating it, collect the actual script and its schema, existing queued/running Jobs, service identities, resource accounting rules and crash-recovery behavior. Create a compatibility plan preserving existing jobs. Never assume `examples/node.json` is a measured description of the machine. Inventory `nproc`, cgroup version, GPU model, CUDA, RAM, disk, active consumers, Gitea runner, systemd user service availability and network connectivity.

## Safe adapter deliverables (in order)

**A. Trust boundary / local IPC.** Create authenticated Unix socket/RPC endpoints so workers can request task ownership, submit evidence, prepare/acknowledge handoffs and query scoped status *without inheriting the Leader/Broker HMAC signing key, operator credentials or direct SQLite write permissions*. Give the Coordinator and Node Broker distinct OS identities and protected data directories. Ensure operator decisions/promotion are only exposed on an authenticated operator interface. Do not confuse `approved_by` metadata with authentication.

**B. Resource Broker adapter.** Map the current shell queue to `inspect`, nonbinding `quote`, idempotent `submit`, `lookup_handoff`, `status`, `tick`, `outbox`. No conditional natural language. Use exact versioned Recipe/Job JSON and fixed argv; any new field must be validated at both ends. Choose `systemd-user` only after checking linger and cgroup behavior. Enforce GPU device exclusivity through OS permissions/container device ACL, not just a SQLite count. Use pinned Git commit worktrees (`source.repo_root`, `source.workdir`); pin Python deps/data/config versions separately. Avoid sending large checkpoints via event JSON.

**C. Reliable Japan OCI ↔ Shanxi bridge.** Implement authenticated/TLS transport, signed acknowledgements, retries and a persistent outbox on node / inbox on Coordinator. Handle event-before-handoff-ack. For ambiguous submission, query same `handoff_id` and verify Broker-signed receipt; never manufacture a new Job. Avoid a separate database commit pretending to be one atomic distributed transaction. Reconcile after disconnects and restarts.

**D. Worker adapter.** Select a supported CLI invocation for the installed Claude/Codex version; do not assume `claude --print -- {capsule}` works unmodified. Add a scoped skill/tool wrapper for structured APIs and Context Capsules; set finite turn/token budgets, no `sleep` loops. Assign via `Leader.assign`, renew leases during implementation, exit after durable acceptance. On terminal result, allocate **new** Worker session to inspect evidence. New Worker must receive task decision, acceptance policy, commit, Job/result references and relevant verified lessons.

**E. Experiment-specific acceptance.** `job_exit=0` verifies only process success. Introduce typed metric predicates for throughput, accuracy, loss, variance, control group/negative controls and scientific protocol without using arbitrary executable predicate text. Preserve source commit, dataset hash, config hash, random seeds, model/checkpoint hash, CUDA/driver/Python versions and training interruptions. Human retains research decisions; any scientific change requires new Decision and experiment Attempt.

**F. Evidence retention.** Control-plane evidence blobs are small (<=32KiB) and content addressed; node-local stdout/stderr and model artifacts are not yet globally replicated or independently authenticated. Add artifact manifest with SHA256 for every file, remote replication, retention/GC policies, redaction and provenance of each Worker hypothesis. Only peer-checked and human-promoted lessons are injected as verified in context; support correction/retraction.

## V0.2 protocol sequence (local trusted simulation)

1. Operator records `leader decision` and `leader task-create` with an approved `execution_policy`, nonempty `acceptance` and explicit `knowledge_scope`.
2. `leader assign <worker_id>` returns `(task_id, generation)`.
3. Worker requests `leader handoff-prepare` with **exact** input parameter values, selected approved Profile(s), Recipe and Git SHA; this generates signed Permit.
4. Worker builds exact version-1 Job JSON and submits to Broker. A Broker **receipt** (not an unsigned `job_id`) is returned.
5. `leader handoff-ack` verifies receipt, Task/Attempt/Decision/Commit and transitions Task to `WAITING_JOB`; Worker releases.
6. `broker serve` (ordinary OS daemon) runs Job independently and writes local result + outbox. `bridge-pump` is local demonstration only.
7. Control inbox verifies and deduplicates event; task moves to `RESULT_READY`; a new Worker takes assignment, writes technical interpretation and either finishes by authorized `job_exit` acceptance or escalates.
8. `coordinator --once` or `leader reconcile-handoffs` resolves prepared handoffs by a trusted local Broker lookup. The Coordinator also delivers signed terminal events using inbox-then-outbox acknowledgement. All existing commands are **local only**, not distributed network services.

## Security caveats / failure modes to exercise

| Fault | Required behavior |
| --- | --- |
| GPU busy but CPU free | CPU-only Job may backfill without violating RAM/CPU reservation |
| Broker accepts Job then ACK disappears | Reconcile by `handoff_id`; no duplicate launch |
| Broker unavailable during lookup | Keep `HANDOFF_PREPARED` unchanged |
| Permit expired and Broker reliably reports absent | Requeue after skew allowance and a fresh decision if needed |
| Node process state ambiguous after restart | `UNKNOWN` reservation; no automatic retry |
| Worker expired or replaying old generation | Reject stale actions |
| Evidence blob missing/tampered | Block acceptance and knowledge verification |
| Resource Quote becomes stale | Commit-time atomic dispatch re-check; no over-allocation |
| Job exits 0 but study fails hypothesis | Do not equate process success with science completion |
| Worker can see HMAC/operator key or write control DB | **Deployment blocked** until OS/API trust boundaries are corrected |

## Interface stability and deliberate omissions

- Accepted JSON/contract version **1**: evolving schemas requires explicit compatibility handling, not natural-language interpretation.
- HMAC shared secret is a *prototype credential*, not production identity.
- External Worker launcher is opt-in and does not automatically implement Claude/Codex tools or permissions.
- No live integration to a connected node is claimed; the reference Broker default `local` executor is smoke-test-only.
- `source_commit` pins Git content only when a Recipe explicitly specifies source and executor enforces pinning. Environment, dataset and containers remain to be pinned.

## Review criteria before production merge/deployment

Demonstrate identity/authorization and file permission separation; real systemd Job survives worker disconnection and restart; GPU cannot be overcommitted or bypassed; exact-scoped input and acceptance constraints pass adversarial tests; Event Outbox survives offline periods; artifacts validate by content hash; prepared handoff recovers after all crash windows; no model wake-ups occur while idle compute waits. Produce a machine-measured Node inventory and deployment roll-back plan.
