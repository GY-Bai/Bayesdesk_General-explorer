# Operations guide — V0.2 (trusted local prototype)

Python >=3.11, standard-library runtime. **Not a production remote-agent authentication service**. Start with `docs/DESIGN_RATIONALE.md` and `docs/ADAPTER_HANDOFF.md`. Paths and capacities in `examples/` are illustrative only, NOT measured Shanxi host resources.

## 1. Installation and offline verification

```bash
python3 -m pip install -e .
python3 -m unittest discover -s tests -v
export BAYESDESK_SHARED_SECRET="$(python3 -c 'import secrets;print(secrets.token_hex(32))')"
python3 -m bayesdesk broker inspect
python3 -m bayesdesk broker quote '{"cpu_units":2,"memory_mib":1024,"gpu_count":0}'
```

The CLI accepts JSON literal or `@/path/to/input.json`, emits a JSON envelope (`ok/data` or `ok/error`), and uses local SQLite by default under `./state/`. Ensure **only trusted coordinator/broker users** can access their database files and keys. Exporting the secret in a shared terminal is for local development only.

## 2. Trust and process ownership

- **Human/operator:** create Decisions, approve science-level changes, manually accept evidence-only tasks, promote peer-checked lessons, release UNKNOWN only after inspecting OS state. These commands currently trust the invoking process and `approved_by` string — **operator identity is NOT verified by the CLI**. An authenticated operator-only service must be added before exposing them to any Worker.
- **Leader/coordinator:** owns task DB and signing key; validates Task execution policy; issues signed limited handoff permit; verifies Broker-signed Job receipt.
- **Worker:** must not inherit signing key, operator credentials, or direct DB write privilege. The optional WorkerPool uses an explicit environment allowlist and drops signing keys. A Worker needs a **separately implemented, authenticated worker-scoped RPC adapter** to request handoffs in deployment.
- **Node Broker:** owns Job SQLite, resource reservation and launch identity; only typed Recipe data. In production set `--executor systemd-user` and require pinned Git recipe source; systemd CPUQuota/MemoryMax is **not** GPU device isolation.

## 3. Exact local handshake (developer exercise)

A Task must have explicit `execution_policy` and machine-verifiable acceptance. Legacy V0.1 free-form Task acceptance no longer passes contract validation. For this smoke task, the process result is the only acceptance criterion; it does not constitute scientific evidence.

```bash
python3 -m bayesdesk leader decision '{"decision_id":"D1","approved_by":"local-human","rationale":"approve smoke demo"}'
python3 -m bayesdesk leader task-create '{
  "task_id":"T1", "decision_id":"D1", "objective":"smoke test",
  "dependencies":[], "constraints":["do not change target"],
  "acceptance":{"type":"job_exit","exit_code":0},
  "execution_policy":{"recipes":{"smoke.v1":{
    "profiles":{"compact":{"cpu_units":2,"memory_mib":1024,"gpu_count":0}},
    "inputs":{"duration":{"type":"integer","min":0,"max":30},
              "result":{"type":"enum","choices":["pass","fail"]}},
    "max_timeout_seconds":30,"max_attempts":3
  }}},
  "knowledge_scope":{"node":"smoke"}
}'
python3 -m bayesdesk leader assign worker-a
```

Prepare handoff using `leader handoff-prepare` with the returned `generation` and an actual 40-character Git commit SHA. In this smoke-only example, `examples/recipes.json` runs the fixed local demo without requiring a Git worktree. This is **not** suitable for untrusted production Worker code.

```json
{
  "task_id":"T1", "worker_id":"worker-a", "generation":1,
  "recipe_id":"smoke.v1", "source_commit":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
  "allowed_profiles":{"compact":{"cpu_units":2,"memory_mib":1024,"gpu_count":0}},
  "inputs":{"duration":0,"result":"pass"},
  "max_timeout_seconds":30
}
```

Save that JSON to `handoff-request.json` and call `leader handoff-prepare @handoff-request.json`. Save the returned `permit`, `handoff_id`, `attempt_id`, `decision_id` and build `job.json` with EXACT Job contract keys:

```json
{
  "schema_version":1,"handoff_id":"HOF-RETURNED","task_id":"T1",
  "attempt_id":"ATT-RETURNED","decision_id":"D1","generation":1,
  "source_commit":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
  "recipe_id":"smoke.v1","inputs":{"duration":0,"result":"pass"},
  "profile_name":"compact",
  "profile":{"cpu_units":2,"memory_mib":1024,"gpu_count":0},
  "timeout_seconds":30,"permit":{"claims":{},"signature":"REPLACE-WITH-PREPARE-PERMIT"}
}
```

The `permit` placeholder above is not executable; replace it with the **complete signed permit** from handoff prepare. Then:

```bash
python3 -m bayesdesk broker submit @job.json
# Save returned `receipt` JSON as @receipt.json. Construct:
# {"handoff_id":"HOF-RETURNED","receipt":<BROKER_RECEIPT_OBJECT>}
python3 -m bayesdesk leader handoff-ack @ack.json
python3 -m bayesdesk broker tick
python3 -m bayesdesk coordinator --once  # LOCAL DBs ONLY; also repairs lost ACKs
python3 -m bayesdesk leader tasks
python3 -m bayesdesk leader assign evaluator-a
python3 -m bayesdesk leader complete '{"task_id":"T1","worker_id":"evaluator-a","generation":2,"evidence":[]}'
```

A real supervisor should run `broker serve --interval 5` and `coordinator --interval 5` as **separate ordinary OS services**, never inside a model `sleep` loop. The latter is local/trusted demo only; deployment must replace its in-process Broker with an authenticated remote client. `broker tick` is for integration testing or a scheduled timer. Check Task `RESULT_READY` before assigning the evaluator. Detached `local` launcher is development-only: it does not reliably enforce resource caps or cgroup recovery.

## 4. Repair/recovery / persistent queries

```bash
python3 -m bayesdesk broker status JOB-ID
python3 -m bayesdesk broker lookup-handoff HOF-ID
python3 -m bayesdesk leader reconcile-handoffs  # trust-local example only
python3 -m bayesdesk coordinator --once        # local event pump + ACK recovery
python3 -m bayesdesk broker outbox
python3 -m bayesdesk leader events T1
python3 -m bayesdesk leader requeue-expired
```

If a Broker call fails, leave the Task in `HANDOFF_PREPARED` and retry reconciliation; do not create a new Job ID. If a Job enters `UNKNOWN`, preserve its reservation until OS/systemd/descendant status is inspected. An operator must specifically authorize `broker release-unknown`, not an LLM retry policy.

## 5. Evidence and organizational memory

- Use `leader evidence-add` to persist a short Worker note (max 32KiB), type `diagnostic`, `test_report`, `verification`, etc.; it receives `EV-*` ID and content SHA256. Blob hash is verified again on read and acceptance.
- Use `leader lesson-propose` to register a scoped `CANDIDATE` with evidence references.
- Independent Worker **in a distinct task** with a successful signed Job and verification Evidence may call `lesson-verify` → `PEER_CHECKED`.
- Only an authenticated operator-facing adapter may call `lesson-approve` → `VERIFIED`. The local CLI itself does not enforce identity. `knowledge-search` retrieves exact scoped lessons, which may be loaded into new Context Capsules.
- Evidence only (`acceptance.type=evidence_only`) requires operator `human-complete`. `job_exit` verifies signed termination, not financial/economic/scientific metrics. Custom science validators need their own *typed* schema and tests.

## 6. Optional external WorkerPool

A local operator may configure a trusted, finite external CLI process:

```json
{
  "max_concurrent":1,
  "env_allowlist":["ANTHROPIC_API_KEY"],
  "workers":[{"worker_id":"coding-1","argv":["claude","--print","--","{capsule}"],
              "cwd":"/path/to/approved/worktree"}]
}
```

**The command line is illustrative and must be verified against the actual installed agent CLI.** The extra environment allowlist is operator-controlled and may intentionally expose model-provider credentials, but cannot include `BAYESDESK_SHARED_SECRET` or `BAYESDESK_OPERATOR_TOKEN`. Do not run an untrusted Worker under a user that can read private `./state` data or bypass the GPU Broker. The Worker CLI needs an explicit scoped RPC/skill adapter; the generic subprocess launcher does not automatically provide these abilities.

```bash
python3 -m bayesdesk worker-pool --config /secure/workers.json --once
```

## 7. Shanxi deployment and forward compatibility

Before connecting a production node, inspect its existing shell queue, active and queued work, host allocations, startup environment, CUDA and service identities. Define `allocatable` as total capacity minus operating-system reserve. GTX 1060 v1 GPU policy is **single-card exclusive**, not per-VRAM slicing. Preserve existing experiments while migrating queue bookkeeping. CI remains for build/tests and need not own 12-hour trainings. The Japan OCI↔Shanxi network bridge, verified dataset/config/SDK hashes, checkpoint recovery, signed remote artifacts, operator identity and real GPU ACL enforcement are next-agent deliverables. See `ADAPTER_HANDOFF.md`.
