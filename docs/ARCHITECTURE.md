# Bayesdesk General Explorer — Architecture Decision Record v0.2

Date: 2026-10-08. Status: **hardened prototype with tested local contract paths**, not production-certified.

## 1. Why this project exists

The user currently makes *research and architecture decisions* with ChatGPT and delegates implementation, debugging and long-running experiments to Claude Code. The latter can burn tokens waiting on slow GPU jobs, or repeatedly ask a human to resolve an architectural ambiguity. A single GTX 1060 GPU is scarce, while CPU/RAM may still be available; a single machine-wide mutex needlessly serializes all work. A pre-existing shell-based host queue motivated a more principled, persistent resource interface.

Requirements in the user's own conceptual order:

1. Research/strategy remains **human-controlled**, supported by ChatGPT/Deep Research. No autonomous model decides to replace the research goal.
2. Engineer Leader **dispatches only**. It does not write code, debug, rearchitect or execute experiments. Deterministic scheduling code owns ordinary state transitions; the optional Leader LLM is only an adapter for engineering task decomposition and high-quality escalation packaging.
3. Workers implement, test, debug, examine evidence and ask for resource quotes. They choose **an approved** compact/full resource profile or wait unchanged; they do not own long-running processes.
4. A Node Broker is **normal code**, not an LLM. It accepts typed recipes and versioned machine contracts, never natural-language conditions or arbitrary shell strings.
5. Worker exits after **durable job acceptance**. Broker supervises a long-running job and persists outcomes/events independently of all AI sessions.
6. At-least-once evidence delivery, immutable attempts, source references and independently verifiable lessons prevent redundant exploratory work. A record of failure is not automatically a validated root cause.
7. GPU exclusivity can coexist with CPU-only backfill, subject to RAM and resource budgets; avoid global host locks.
8. The system fits an M2 Mac development terminal, Japan OCI control node, Shanxi Ubuntu Ryzen 7 / GTX 1060 compute node and existing Gitea CI. Do not force 12-hour studies into CI Runner jobs.

## 2. Decision and authority boundary

```text
 Human / ChatGPT [research, human approval]
                | approved Decision / escalation
                v
  Engineer Leader [task assignment, DAG, quotas, report escalation]
       |                                     |
       | assign task                         | read capacity, job status
       v                                     v
  Worker Pool -----------------------> Node Resource Broker
  implement/test/debug     signed job   SQLite queue / CPU-RAM-GPU admission
  prepare handoff          contract     trusted recipe -> systemd unit
       |                                     |
       +------ Task/Attempt/Evidence <-------+ durable result + event outbox
                        |
                   Event inbox -> Scheduler -> New Worker when needed
```

The control path does **not** proxy every worker-to-broker request through an LLM. A worker may work in an isolated worktree. It may read source code and edit in its approved sandbox without using the compute queue; **shared/heavy computation** must go through the broker with OS permissions enforced.

### Authority matrix

| Action | Human | Leader | Worker | Broker |
| --- | --- | --- | --- | --- |
| Approve research direction, redefine metric/target | owner | report only | suggest only | none |
| Select eligible engineering task / worker assignment | authorize policy | execute | claim | none |
| Edit code or develop fix | authorize scope | forbidden | yes | forbidden |
| Choose one approved job resource profile | set limits | route limits | yes | validate only |
| Reserve GPU/CPU/RAM and execute | set limits | forbidden | forbidden | sole owner |
| Classify technical root cause | research when escalated | forbidden | yes | raw error codes only |
| Submit durable escalation with evidence | decide | forward | prepare | emit raw failure |
| Verify success against fixed machine checks | define criteria | view status | analyze results | run checks, record outcomes |

**Important:** Approval and completion are different. `exit_code=0` / `JOB_SUCCEEDED` means a process ran successfully; it never proves a scientific hypothesis or satisfies Task acceptance automatically.

## 3. Durable entities

- **Decision**: explicit human approval with identity, rationale and version. An LLM answer alone is not a signed-off decision.
- **Task**: engineering objective, immutable acceptance constraints, DAG dependencies, current state, Worker fencing generation.
- **Worker Assignment**: short lease for implementation, distinct from the physical compute reservation.
- **Handoff**: stable ID, Attempt ID, source commit, allowed profiles and a signed limited permit. `PREPARED -> ACCEPTED` is the handoff protocol.
- **Job**: immutable recipe, typed inputs, selected resource profile, walltime and exclusive node ownership. A Task may produce many Jobs/Attempts.
- **Event**: persisted outbox/inbox delivery of job termination; signature and event ID allow verification and deduplication.
- **Evidence**: raw logs, metrics, result manifest and configuration; immutable under a job-specific directory. The project currently stores file references. Future revision needs content-addressed hashes, remote artifact replication and verified science-level lessons.
- **Escalation**: scope boundary crossed; Worker submits observations and evidence, Leader forwards. Only the human resolves research-level ambiguity.

### Task status

`READY -> ASSIGNED -> HANDOFF_PREPARED -> WAITING_JOB -> RESULT_READY -> ASSIGNED -> DONE`.

Instead of handing off, an assigned Worker may escalate: `ASSIGNED -> BLOCKED`; an explicit approved Decision resolves `BLOCKED -> READY`.

Some tasks complete via direct acceptance evidence (`ASSIGNED -> DONE`) without a Broker Job. The Leader does not determine scientific validity: Worker/acceptance policy generates evidence, operator controls research criteria.

### Worker state

An LLM process has an ephemeral lifecycle independent of Task/Job. A new Worker resumes by loading a bounded Context Capsule from Task, Evidence and Decision. Worker assignment generation fences off old Workers. A 15-minute default lease requires renewal for longer implementation work; expiry is separately requeued. **Prepared handoffs are not automatically requeued** because the remote Broker might already have accepted the job.

### Job status

`QUEUED -> STARTING -> RUNNING -> SUCCEEDED|FAILED`. If process state or result is uncertain: `UNKNOWN` quarantines reservation and forbids automatic replay. An operator who has confirmed the original process and its descendants are gone can transition `UNKNOWN -> LOST`, emitting a signed error event.

Job results are stored atomically in `result.json`. `STARTING` is persisted **before** invoking the OS; a crash in the launch acknowledgment gap must be reconciled, never silently retried. A finished command with missing manifest is **not** assumed safe to rerun.

## 4. Broker contract (machine code only)

### Node capabilities

`node.json` describes `allocatable`, which excludes OS reserve:

```json
{"node_id":"shanxi","allocatable":{"cpu_units":12,"memory_mib":12288,"gpu_count":1}}
```

All integer budgets are accounting quantities, **not observed CPU%**. `cpu_units=1` means up to one logical CPU worth of CFS CPUQuota (100%) in `systemd-user` mode. It does not grant an exclusive core or bound memory bandwidth. Memory is MiB. `gpu_count=1` means whole-GPU exclusive reservation in V1; true hardware access enforcement additionally requires OS device/permission isolation. Do **not** subtract VRAM estimates as though GTX1060 supports safe independent GPU partitions.

`inspect` returns allocatable, reserved (jobs in STARTING/RUNNING/UNKNOWN), available and queued count. `quote(profile)` is a **nonbinding snapshot**, may become stale immediately. `submit(job)` is idempotent by handoff_id, validates an HMAC permit, trusted recipe + typed args + selected profile + node capacity; it **queues** the job rather than promising immediate resources. `dispatch` uses an `IMMEDIATE` SQLite write transaction for reservation. All other Writers use the same local SQLite DB; never share that WAL file across host NFS.

### Typed recipe

Node administrator registers an immutable `recipe_id` with a fixed `argv` prefix, a trusted `cwd`, and typed input parameters. Only `integer`, `enum` and rooted `path` are supported. No shell interpolation or free-form argv accepted from Workers. A recipe upgrade must produce a new version (`training.v2`, etc). V0.2 can materialize an exact source commit into a per-Job Git worktree when Recipe `source` is configured. The `systemd-user` executor requires this pinning; the local smoke-test executor may omit it. Pinned source alone does **not** pin Python/CUDA dependencies or datasets.

### Scheduling policy

Jobs are ordered by priority (V1 submissions default to equal priority) and insertion order, not by randomized IDs. The broker reserves CPU, RAM and GPU atomically. If queue head is blocked, later jobs can backfill **only while preserving enough CPU and RAM for the blocked head**; this protects a pending GPU training job against indefinite CPU-side starvation. No automatic GPU preemption or unapproved resource shrinking. A Worker can choose a compact profile before submit or submit a full profile to wait; the Broker never invents another scientific configuration.

### Job completion

The independent `job_entry` process writes stdout/stderr and then an atomic manifest. The Broker's `reconcile()` reads the manifest, writes the terminal Job state and event to the SQLite outbox **within one transaction**. An external forwarder can deliver events to the Coordinator with at-least-once retries and acknowledgement. The simple `bridge-pump` CLI works **only when both databases are local**; it is a test utility, not a network solution.

The Coordinator's event inbox verifies an HMAC event signature and deduplicates `event_id`. It supports **event-before-handoff-ack** ordering by parking the inbox message until the handoff is acknowledged. A terminal event only changes a Task into `RESULT_READY`, so a new Worker can evaluate scientific acceptance or debug failures; the Leader doesn't perform technical reasoning.

## 5. Three typed contracts

### Task Contract

Approved objective, immutable constraints, fixed acceptance predicates, dependency IDs, priority, lease generation, evidence requirement. Only the human issues a new research Decision; scheduling code cannot silently modify success criteria.

### Job Contract v1

Exact keys:

`schema_version`, `handoff_id`, `task_id`, `attempt_id`, `decision_id`, `generation`, `source_commit`, `recipe_id`, `inputs`, `profile_name`, `profile`, `timeout_seconds`, `permit`.

A permit is an HMAC-SHA256 over immutable claims: handoff, task, attempt, Decision, Worker generation, recipe, **source commit, exact input fingerprint**, approved profiles, max walltime and expiry. `execution_policy` supplies operator-approved recipe/input/profile/budget constraints. Broker validates its signature and selected resources. Worker cannot submit arbitrary NLP text as executable instructions.

### Escalation Contract

`task_id`, `worker_id`, `generation`, question, observations[], evidence_refs[]. Mandatory evidence prevents unexplained escalation. `resolve_escalation` requires explicit human approval metadata and creates a new Decision. Initial CLI identification of `approved_by` is **not an identity-provider authentication mechanism**, so deployment must restrict access using Unix service users and operator-only API credentials.

## 6. Durable Worker handoff and wake-up

1. Leader assigns one task (generation incremented).
2. Worker edits approved files in isolated worktree and writes Attempt/Evidence; inspect/quote only to learn resource options.
3. Worker chooses approved profile and calls `handoff-prepare`. Coordinator persists handoff and returns a signed permit.
4. Worker posts job to **node** Broker; Broker persists it and returns Job ID. Exact duplicates return the same ID; modified duplicates are rejected.
5. Worker calls `handoff-ack`. Coordinator transitions to `WAITING_JOB`; Worker **exits**. If acknowledgement was lost, use `broker lookup-handoff` and `leader reconcile-handoffs` (local trusted CLI only), verifying the signed Broker receipt. Never blindly create a new Job. Remote reconciler transport is a deployment adapter milestone.
6. Node supervisor runs independently. `systemd-run --user` is the deployment backend; a `local` detached process backend exists solely for tests/dev and cannot strongly enforce quotas or reliable reattachment.
7. Job writes atomic result. Broker emits signed terminal event into outbox. Local forwarder or future network bridge delivers event.
8. Coordinator receives terminal event and changes Task to `RESULT_READY`. Next short-lived Worker instance is dispatched to evaluate, debug or finish. **No LLM remains active during Job execution.**
9. If a Worker hits design conflict, it submits Escalation; Leader forwards without attempting a technical solution; human authorizes subsequent Decision.

Broker supervisor polling uses `time.sleep` in a normal daemon, not inside an LLM. A scheduled `systemd` timer can call `broker tick`; the Agent should never use a `sleep/check` conversation loop.

## 7. Knowledge model

Separate (A) machine observations (`exit_code`, stderr, metrics, versioned configuration, logs), (B) Worker hypotheses/attempt narratives and (C) validated lessons with explicit scope and contradictory evidence. V0.2 records content-addressed small Worker evidence, scopes Candidate lessons, requires independent verification before `PEER_CHECKED`, and requires operator promotion before `VERIFIED`. It **does not yet** implement semantic similarity, automatic retraction, signed remote raw artifact manifests or ARTEX-style graph compaction. Next iterations can model `Attempt --produced--> Evidence --supports/refutes--> Hypothesis --validated-as--> Lesson` and preserve temporal ordering as in Cairn.

Prior-art references: [ARTEX](https://github.com/Autumn-27/ARTEX), [Cairn](https://github.com/oritera/Cairn), [OpenAI Symphony](https://github.com/openai/symphony), [Herdr](https://github.com/herdrdev/herdr). These inspired boundaries; no source code is copied. Distinguish implemented components from ideas.

## 8. Threat model and constraints

- Shared signing secret (`BAYESDESK_SHARED_SECRET`) is for prototype permits/events, not full host-to-host mutual authentication. Use secret manager, TLS/mTLS and rotation in production; protect SQLite and broker socket from untrusted writers.
- Worker filesystem access should be restricted to an isolated worktree. A Worker with unrestricted shell/root access can bypass any broker; do not rely on prompt obedience. Execution of Worker-authored code should run under a restricted OS identity/container.
- `systemd-user` backend requires active user systemd manager and lingering for offline jobs; GPU device access/quotas must be separately enforced. `MemoryMax`/`CPUQuota` are not GPU VRAM enforcement.
- For source integrity, V0.2 optionally materializes a pinned Git worktree and requires it with the systemd executor; environment, dataset/config hashes, artifact verification, container isolation and production identity are still missing. **Do not deploy untrusted code until these are enforced.**
- A claimed Worker can time out during implementation. `requeue_expired` fences the old generation. The claim is not a distributed lock on an external git branch: independent worktrees and protected integration are also necessary.
- A job in `UNKNOWN` reserves resources until explicit recovery. This sacrifices some utilization to prevent accidental double execution; operator intervention is required.
- Remote artifact replication, authenticated outbox network transport, scientific metrics-based acceptance, full causal/temporal knowledge graph, enforced GPU device isolation, production Claude/Codex adapter and Postgres control plane are **future work, not V0.2 capabilities**.

## 9. Implementation/verification milestones

**M0 (delivered with V0.2 security and knowledge extensions):** executable Python stdlib protocol skeleton; SQLite leader; permit-bound job handoff; typed recipes; broker quote/submit; atomic multi-resource admission/backfill; detached/systemd launch adapters; result manifests; signed events; inbox dedup; basic opt-in short-lived Worker launcher and Context Capsule; escalation; tests.

**M1 (next):** read actual Shanxi queue shell script, preserve compatibility; onboard host and create trusted training recipes; correct `systemd` service user setup; simulate power loss and startup crash windows; remote prepared-handoff reconciliation transport; production Worker CLI skills/permission adapter, token budgets and Context Capsule retrieval; remote artifact replication.

**M2:** Japan OCI endpoint/mTLS and persistent event forwarding; PostgreSQL control plane; Gitea PR/CI adapter; pinned environment/dataset manifests; structured diagnostics, signed raw artifacts and full negative-knowledge applicability/retraction.

**M3:** empirically justified quotas, aging/priority experiments, checkpoint resume, per-run experiment verification and cold/semantic graph compaction.

## 10. Explicit non-goals

No autonomous deep research, no Agent Teams conversations, no NLP inside Broker, no automatic architecture changes, no blind resubmission of UNKNOWN jobs, no claim that CI success proves research success, no GPU overcommit or automatic trial parameter shrinkage, no exact-once distributed transaction guarantee.

## 11. V0.2 hardening and verified scope

- **Machine policy:** `execution_policy` is captured at human-approved Task creation, with allowed Recipes, resource Profiles, exact/enum/bounded integer inputs, max walltime, max Attempts and optional source SHA allowlist. A signed Handoff Permit commits to an exact input fingerprint.
- **Handoff:** Broker returns a signed receipt; Leader rejects fabricated Job IDs. A trusted local reconciler queries `handoff_id`, verifies receipt and recovers lost ACKs without replay. Remote authentication remains an adaptation task.
- **Acceptance:** `job_exit` validates the signed terminal Job result against the Task predicate. `evidence_only` requires operator-controlled `human_complete` plus registered Evidence. Exit success is not science proof.
- **Evidence:** small Worker notes are SHA-256-addressed and checked on read and acceptance; large GPU artifacts still need remote hash manifests and replication.
- **Knowledge:** `CANDIDATE -> PEER_CHECKED -> VERIFIED`; independent Worker+Task+Job result and human promotion required. Retrieval uses exact field/value knowledge scopes, not semantic search.
- **Security:** default Worker subprocess env is an allowlist; privileges require separate Unix users/authenticated service wrappers because in-process Python and the CLI do not themselves provide a production identity boundary.

See `docs/DESIGN_RATIONALE.md` for derivation, `docs/ADAPTER_HANDOFF.md` for implementation boundaries.
