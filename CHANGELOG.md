# Changelog

## V0.2 — contract hardening, recovery and evidence

- Enforce Task-authorized Recipe, typed input constraints, exact resource profiles, attempt budget and timeouts before signing a Handoff Permit.
- Sign and verify Broker Job receipts; reconcile lost ACK by stable Handoff ID without speculative duplicate launches.
- Verify job result authenticity for machine `job_exit` acceptance; evidence-only completion requires explicit operator action.
- Add content-addressed small Evidence blobs; verify SHA-256 during read and every acceptance/lesson reference check.
- Add scoped candidate Lesson registry, independent peer check, explicit operator promotion to VERIFIED and scoped search for short-lived Worker context.
- Materialize exact Git commit worktrees when Recipe source is provided; systemd execution requires pinned Recipe source. Environment, dataset and checkpoint integrity remain integration work.
- Prevent Recipe drift after enqueue by persisting Recipe fingerprint; uncertain launches remain quarantined as UNKNOWN.
- Prevent early result manifest from releasing resource reservation while the OS process still lives.
- Default Worker subprocess environment to a restricted allowlist. No Broker or operator signing keys are inherited by Agent subprocesses.
- Add a deterministic **local** Coordinator that recovers lost handoff ACKs and bridges durable terminal events without starting any model.
- Add `DESIGN_RATIONALE.md`, `ADAPTER_HANDOFF.md`, `EXECUTIVE_HANDOFF_ZH.md`, updated operations documentation and expanded tests.

### Changes from V0.1

- Task creation now requires an explicit typed `acceptance` object (`job_exit` or `evidence_only`). To prepare a Job, Task must declare `execution_policy` granting Recipe, inputs, profiles and limits. Legacy free-form task creation is deliberately rejected for new Tasks.
- `leader handoff-prepare` requires *exact* `inputs` and a Task-approved Recipe/Profile.
- `leader handoff-ack` requires a signed `receipt` dict rather than arbitrary `job_id`.
- New APIs: `broker lookup-handoff`, `leader reconcile-handoffs`, evidence/lesson/human-complete commands.
- Broker SQLite `jobs` table gains `recipe_hash`; legacy queued Jobs lack recipe provenance and are conservatively quarantined rather than silently launched under changed Recipe code.
- Local Worker launcher can specify `env_allowlist`, but cannot receive sensitive Bayesdesk signing keys.

### Deployment / production limitations

The control-plane↔Node bridge is local-only and no real Shanxi hardware has been inspected. Operator identity and worker-scoped RPC are missing; the same Unix user can bypass HMAC protections by reading private control state. GPU cgroup/device isolation, checkpoint restart, remote signed artifact manifests and network transport remain for a subsequent Agent. **Do not equate 33 local tests with proof of production safety.**
