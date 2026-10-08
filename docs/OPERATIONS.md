# Operations and safety guide (developer preview)

## Install / smoke test

Requirements: Python >=3.11; stdlib-only runtime. On a Linux production node use systemd user manager; on the local developer machine use `--executor local` **for smoke tests only**.

```bash
python3 -m pip install -e .
python3 -m unittest discover -s tests -v
export BAYESDESK_SHARED_SECRET="$(python3 -c 'import secrets;print(secrets.token_hex(32))')"
# Keep secret consistent for control-plane and node Broker; do not commit it.
python3 -m bayesdesk broker inspect
python3 -m bayesdesk broker quote '{"cpu_units":2,"memory_mib":1024,"gpu_count":0}'
```

> The provided `examples/node.json` contains **illustrative** capacity, not measured Shanxi host quotas. Do not blindly deploy it. `examples/recipes.json` runs `python3 -m bayesdesk.simjob` and requires package install / accessible Python path.

## End-to-end protocol

```bash
# 1. Human records research approval; never auto-approve via LLM.
python3 -m bayesdesk leader decision '{"decision_id":"D1","approved_by":"human","rationale":"smoke tests only"}'
# 2. Create a Task with objective, constraints and acceptance specification.
python3 -m bayesdesk leader task-create '{"task_id":"T1","decision_id":"D1","objective":"smoke","dependencies":[],"constraints":[],"acceptance":{"exit_code":0}}'
# 3. Assign ephemeral Worker: get generation.
python3 -m bayesdesk leader assign coding-worker-1
# 4. Worker prepares a signed handoff (save JSON output to a file).
python3 -m bayesdesk leader handoff-prepare '{"task_id":"T1","worker_id":"coding-worker-1","generation":1,"recipe_id":"smoke.v1","source_commit":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","allowed_profiles":{"compact":{"cpu_units":2,"memory_mib":1024,"gpu_count":0}},"max_timeout_seconds":30}'
# 5. Construct @job.json using exact Job Contract fields, and the returned signed permit.
python3 -m bayesdesk broker submit @job.json
# 6. Worker persists acceptance using the returned job_id, then EXITS.
python3 -m bayesdesk leader handoff-ack '{"handoff_id":"HOF-...","job_id":"JOB-..."}'
# 7. Execute the queue (real node: systemd user service/timer, not an Agent).
python3 -m bayesdesk broker tick
# 8. Transfer signed event into the leader DB. This bridge is LOCAL-ONLY developer demo.
python3 -m bayesdesk bridge-pump
# 9. Restart a short-lived Worker to evaluate the result and acceptance evidence.
python3 -m bayesdesk leader assign evaluator-1
python3 -m bayesdesk leader complete '{"task_id":"T1","worker_id":"evaluator-1","generation":2,"evidence":["state/jobs/JOB-.../result.json"]}'
```

## Actual Shanxi host onboarding checklist

Do not assume 12 CPU units or 12 GiB RAM; measure `nproc`, `/proc/meminfo`, `nvidia-smi`, cgroup version, disk reservations and baseline existing services. Choose `allocatable` after system reserve. Protect the single GTX1060 as one exclusive GPU slot. Inventory existing CUDA and training tools; create a pinned `training.v1` trusted recipe, not a free-form shell command. The Broker is a **single host-side authority**, and should run in an account with controlled privileges. The Worker must not be able to bypass the Broker with direct unrestricted heavy computation.

For `systemd-user` verify `systemctl --user` is functional and enable lingering for the service identity when authorized. Launch the supervisor with `python3 -m bayesdesk --executor systemd-user broker serve --interval 5`. Manage the supervisor itself with systemd, not a terminal. The daemon sleep is a cheap OS timer, not an LLM loop. Systemd properties enforce RAM/CPU; GPU device isolation remains an additional deployment task.

## Recovery and troubleshooting

- `broker status JOB-ID`: read exact state; no `sleep` loop should be executed by an Agent.
- `broker outbox`: signed events awaiting delivery. Upstream must acknowledge before calling `broker delivered EVT-ID`.
- `leader events TASK-ID`: inspect Task audit for Worker handoffs and event receipt.
- `leader requeue-expired`: requeue expired `ASSIGNED` tasks; never automatically requeue a prepared handoff without verifying the node Broker.
- `broker release-unknown @inspection.json`: **operator only**. Example payload: `{"job_id":"JOB-...","inspected_by":"operator","reason":"verified no systemd unit or descendants remain"}`. Unknown jobs hold resources until confirmed.
- `leader escalation-resolve @decision.json`: **operator only**; cannot be carried out by autonomous Leader LLM.
- Keep local Broker SQLite on local disk, not NFS. Snapshot/backup the DB and job Artifacts together.

## Acceptance tests

- CPU-only job runs while GPU-exclusive job holds GPU.
- A second GPU-exclusive job stays queued.
- Idempotent submit returns original Job ID; changed replay fails.
- Worker exit does not kill computation.
- Event-before-ack still resumes Task after acknowledgment.
- Signed result events reject tampering; duplicate events are ignored.
- Failure leads to `RESULT_READY` for a Debug Worker, not an automatic research-decision change.
- Expired worker generations cannot complete a task.
- Scientific Task acceptance remains separate from process exit status.
- Unknown execution states never auto-relaunch.

## Optional ephemeral Worker Pool

Configure a trusted, finite-lived CLI Agent command in a local JSON file, e.g.:

```json
{"max_concurrent":1,"workers":[{"worker_id":"coding-1","argv":["claude","--print","--", "{capsule}"],"cwd":"/path/to/approved/worktree"}]}
```

**This is an illustrative adapter configuration, not a guaranteed Claude CLI invocation syntax.** Verify your installed CLI's supported arguments and enable only after inspecting permissions, tool access and billing. Worker CLI must know how to consume the Context Capsule and invoke `leader renew`, `handoff-prepare`/`handoff-ack`, `complete` or `escalate` endpoints. A generic CLI process is not automatically able to do these tasks unless you supply a dedicated agent skill/system prompt and tool integration.

```bash
python3 -m bayesdesk worker-pool --config /secure/workers.json --once
# Or run the dispatcher as a systemd service, with finite Worker sessions:
python3 -m bayesdesk worker-pool --config /secure/workers.json --interval 10
```

This adapter is opt-in. `worker-pool` itself never calls an LLM; the configured external command might. An expired assignment is requeued; a prepared handoff is not silently requeued. Production deployments need per-Worker isolated checkout, credential guardrails, and explicit token budgets.
