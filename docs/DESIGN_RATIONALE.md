# Design rationale — from conversation to enforceable boundaries (V0.2)

> **Status:** decision record for downstream implementers. This explains *why* each boundary exists; it is not a claim that every capability is production-ready. Human research authority takes precedence over all automated scheduling.

## 1. Origin: the actual problem, not another autonomous agent team

The project grew out of a concrete workflow: the human researches hypotheses and architectures with ChatGPT; a coding agent implements and operates experiments. On scarce shared compute (Shanxi Ryzen 7 / 16 GiB RAM / GTX 1060 6 GiB), one GPU study can run 10–15 hours, yet CPU cores may remain usable. Agents habitually poll/sleep, spend tokens waiting, repeat failed attempts, and ask the human poorly contextualized architectural questions. Multiple coding agents independently querying `nvidia-smi` create a time-of-check/time-of-use race; a single host mutex, conversely, forces wasteful serialization.

**Deliberate operating model:** Human decides research direction; Engineer Leader routes, tracks and escalates only; Workers implement, test and diagnose; deterministic Node Brokers own compute, admission and lifecycle. No autonomous agent debate, no LLM inside Broker, no Agent required during a long Job. The original host shell queue is the bootstrap precedent and should be inspected before replacing it.

## 2. Derivation / alternatives rejected

| Original observation | Design invariant | Rejected shortcut | Executable anchor |
| --- | --- | --- | --- |
| GPU saturated does not imply CPU exhausted | CPU/RAM/GPU are a resource vector | One machine-wide busy bit | `broker.py::dispatch/inspect/quote` |
| Two agents may see identical free VRAM | Reservation happens atomically in Broker | Worker reads `nvidia-smi`, then starts `python train.py` | SQLite `BEGIN IMMEDIATE`, Broker reservation |
| Worker must choose faster compact or delayed full execution | Broker validates **approved** profiles; it never invents new experiment parameters | Broker rewriting batch/precision automatically | `policies.py`, signed `contracts.py`, Broker `submit` |
| Model sleeps through multi-hour training | Persist handoff and terminate Worker session | Model tool `sleep` loop / CI job as sole keeper | `leader.py::acknowledge_handoff`, `worker_pool.py`, `job_entry.py` |
| Process crash may coincide with launch | Uncertain process state quarantines reservation | Immediately resubmitting `RUNNING/STARTING` Job | Broker `UNKNOWN`, operator release |
| Worker may disappear after Broker accepted Job but before ACK | Durable signed Broker receipt and reconciliation | Unauthenticated `job_id` provided by Worker | `lookup_handoff`, `reconcile_handoffs` |
| Manager becomes a debugging bottleneck | Leader only assigns, tracks and escalates; debugging remains Worker role | Leader as code-writing superagent | `leader.py`, privilege matrix |
| Architecture questions repeatedly interrupt human | Escalation references specific evidence and needs explicit human Decision | “Ask human what to do” chat messages | `escalate`, `resolve_escalation` |
| Code and failed investigations are equally expensive | Preserve observations, Attempt and signed result references | Ephemeral session summary only | SQLite Events + content-addressed `evidence` |
| Previous LLM asserted a plausible but wrong root cause | Hypothesis ≠ verified knowledge; independent check then human promotion | Adding all model summaries to permanent instruction corpus | `propose_lesson`, `verify_lesson`, `approve_lesson` |
| Broker is code, cannot understand prose | `recipe_id`, typed inputs, signed permit, strict machine fields | Free-form conditional-natural-language job submission | `contracts.py`, `recipes.py` |

## 3. Role boundaries: *who decides what*

- **Human + ChatGPT:** research direction, scientific hypotheses, acceptance definition, changes of model family/objective, promotions to verified organizational knowledge, resource budget approval. ChatGPT supplies research, not executable authorization.
- **Engineer Leader (deterministic core):** Task DAG, assignment with fencing generation, prepared/accepted handoff transitions, escalation, requeue policy, event routing. Not entitled to fix bugs, interpret research validity, or alter scientific acceptance. An optional LLM adapter may draft plans or escalation prose but does not bypass the policy gate.
- **Worker:** code changes and technical diagnostics in a scoped worktree, explicit resource choice from approved profiles, evidence annotations and handoff. No direct unconstrained heavyweight OS processes, no private signing key, no operator promotion privileges. Its lifetime is **not** the lifetime of a Task or Job.
- **Node Broker:** local CPU/RAM/GPU accounting and admission, immutable allowlisted recipe execution, program monitoring, manifests, event outbox. It MUST NOT evaluate language, modify experimental goals, or infer what a failed result means scientifically.
- **Coordinator/OS:** timers, event inbox, dedupe, restart recovery, systemd resource enforcement. These are ordinary code rather than an always-awake LLM.

**Security caveat:** V0.2 exposes trusted in-process Python and local CLI interfaces, not a fully authenticated service boundary. Separation requires distinct OS identities, filesystem ACLs, and authenticated requests. Sanitizing child process environment is defense in depth, not a sandbox.

## 4. Contract and state ownership

1. **Decision**: requires operator authentication in the eventual deployment; records author, scope, rationale. In V0.2 `approved_by` is *metadata*, not cryptographic identity verification.
2. **Task Contract**: objective, human-approved execution policy, acceptance predicate, dependency graph and generation-fenced Worker lease. Immutable unless a new human Decision explicitly authorizes scope updates.
3. **Job Contract**: versioned immutable recipe identifier, pinned source commit, exact typed parameter values, selected approved resource vector, timeout, HMAC permit. `quote` is informational; `submit` queues; `dispatch` atomically reserves resources. Resource capacity is **allocatable** (system reserve already subtracted), not observed utilization.
4. **Handoff**: `PREPARED -> ACCEPTED` only with a Broker-signed receipt for the correct task/attempt/commit. If submit acknowledgement disappears, consult `lookup_handoff` rather than resubmit a novel Job. A prepared handoff may only be aborted after permit expiry + skew allowance and a reachable Broker affirmatively lacking the Job.
5. **Job**: `QUEUED -> STARTING -> RUNNING -> SUCCEEDED/FAILED` or `UNKNOWN`; code cannot assume a lost timer means the compute process was killed. `UNKNOWN` holds reservation, never auto replays.
6. **Result**: an authenticated Job termination event moves a Task to `RESULT_READY` (not `DONE`). Worker / deterministic acceptance checks may complete `job_exit` tasks; evidence-only tasks need explicit operator acceptance. Exit code is **not** a research result.
7. **Evidence & Lessons**: evidence stores sha256-validated small content blobs and provenance; raw Job artifacts remain node-local. Lesson states `CANDIDATE -> PEER_CHECKED -> VERIFIED`, requiring different Worker, independent successful Job, verification evidence, then human promotion. Verified remains scoped and revisable, not a law of nature.

## 5. Scheduling: allocate without holding sessions

Backfill CPU-only work while GPU is reserved. Head-of-queue protection preserves CPU/RAM for the waiting head when necessary; future schedulers may refine this with timestamps and empirical runtime estimates. Worker sees `inspect` and non-binding `quote` and chooses an existing approved resource profile. It exits after durable acceptance. The node supervisor (`broker serve`) and trusted local reconciliation driver (`coordinator`), not an LLM, periodically reconcile process state, handoff acknowledgements and events. Existing GPU run is not tied to any Worker lease.

A Job that finishes with exit 0 is **process success**, not scientific validity. The independent Experiment Worker must still analyze the authorized benchmarks, ablations, negative controls, training environment and checkpoint provenance.

## 6. Prior-art: what is and isn't included

- **ARTEX**: useful inspiration for structured traces, evidence linkage, cross-task inheritance and hot/cold graph digest. V0.2 has linked records and content-addressed small notes; it does **not** implement full ARTEX graph, offline digest or implicit cross-task inheritance.
- **Cairn**: Fact / Intent / Hint and chronological exploration. V0.2 has durable timestamps and Event records; it does **not** implement historical semantic truth maintenance or time-decay beliefs.
- **Symphony**: deterministic scheduling and state-driven dispatch over perpetual conversational collaboration.
- **Herdr**: observable CLI agent work. V0.2 has a finite external Worker launcher, not a Herdr terminal UI.
- **Actionable Error design**: errors should have stable `code` and `retryable` semantics. Not every arbitrary stderr is an operator instruction. Broker errors are data, not permission to attempt a workaround beyond policy.

References: [ARTEX](https://github.com/Autumn-27/ARTEX), [Cairn](https://github.com/oritera/Cairn), [OpenAI Symphony](https://github.com/openai/symphony), [Herdr](https://github.com/herdrdev/herdr).

## 7. Anti-goals / boundaries for downstream agents

Do **not** add a multi-agent debating layer, a free-form shell execution facility in Node Broker, automatic scientific goal changes, unrestricted Worker access to control signing secrets, speculative retry of `UNKNOWN`, global host locking for all jobs, or model-driven polling of long-running compute.

Do **not** claim that HMAC solves authorization when untrusted agents share the same OS identity or can mutate the control database. Do **not** claim that a verified source commit makes the runtime dependencies immutable: pinned interpreter, CUDA toolkit, dependency/environment digest and dataset hashes are also necessary.

## 8. V0.2 acceptance audit

Local offline suite tests: independent Worker claim, no GPU over-reservation under contention, CPU backfill, HMAC permit/input/profile checks, signed Broker receipt, prepared handoff recovery, provenance-aware machine acceptance, SHA-256 small evidence, independent verification, event signature/dedup and stale-Worker fencing.

**Explicitly incomplete:** live Shanxi host integration and measurements; Java/Go/Claude/Codex worker adapters; production mTLS/authenticated RPC across Japan OCI and Shanxi; operator identity provider; verified artifact replication and checkpoint resumption; GPU device ACL isolation and empirical CPU/RAM quotas; graph compaction; complete multi-asset science evaluation. See `ADAPTER_HANDOFF.md`.
