# Temporal × Agent Substrate: Integration Ideas

Temporal holds the agent's *logical* state: event history, timers, signals and retries. Substrate holds its *physical* state: sandboxed RAM and filesystem snapshots, multiplexed onto a small worker pool. In short, Temporal is the durable brain and Substrate is the cheap, isolated body.

---

## 1. Sandbox as an Activity

A Temporal workflow runs the agent loop: LLM calls, planning and human approvals. Untrusted tool and code execution is handed off to a Substrate actor through an Activity that calls the actor via `atenet-router`. The actor ID is derived from the workflow ID, so each run owns one persistent, isolated sandbox. The sandbox suspends between steps and resumes on the next call.

**Benefits**
- Works with both projects as they are today. Neither needs changes.
- Untrusted code runs in a gVisor or microVM sandbox, while orchestration code stays trusted.
- Cost: the sandbox only uses compute while a step is actually running, and is snapshotted while the agent waits on LLMs or humans.
- Temporal retries and timeouts work well with Substrate's request parking (requests wait instead of getting a 503).

**Challenges**
- Retries are not idempotent: a retried Activity reaches a sandbox whose state has already changed.
- Large tool outputs can bloat workflow history. Store outputs externally and keep only references in the history.
- Actor lifecycle (create, reap, orphan cleanup) has to be handled explicitly.
- The Activity worker needs a SPIFFE identity that the router accepts.

**Next steps**
- Build a prototype: a Python or Go Temporal workflow plus a `SandboxExec` Activity against the Substrate sandbox demo on kind.
- Define an idempotency-key convention for sandbox calls.
- Measure end-to-end latency with resume included (target under 400ms).

---

## 2. Snapshot-Linked Workflow History

Each successful workflow step records a reference to a Substrate snapshot in workflow state. If a step fails, the sandbox is restored to the last known-good snapshot before the retry. A step's side effects on the sandbox therefore either complete or are undone, much like a transaction. Child workflows can also *fork* from one snapshot and explore branches in parallel: "try three fixes, keep the one that passes the tests."

**Benefits**
- Clean retries: this solves the idempotency problem from idea 1 at the environment level.
- Undoing a step (saga compensation) becomes a snapshot restore instead of hand-written undo logic.
- Parallel branching suits RL rollouts, evals and best-of-N agent strategies.
- Debugging: any failed step can be replayed in the exact sandbox state it ran in.

**Challenges**
- Unclear whether Substrate exposes addressable snapshot IDs and fork or restore-to-snapshot, or only suspend/resume of the latest state.
- Snapshot storage cost and garbage collection (retention tied to the workflow's lifetime).
- External side effects (API calls, git pushes) can't be undone by restoring a snapshot.
- Consistency between snapshot writes and workflow events.

**Next steps**
- Read the Substrate API and architecture docs for snapshot semantics. Ask in `#substrate-dev`.
- If the primitives are missing, draft an upstream proposal for snapshot IDs, fork and restore.
- Prototype a fork-from-snapshot child-workflow pattern on the counter demo.

---

## 3. Substrate as a Serverless Worker Target

Temporal's Serverless Workers (AWS Lambda, Cloud Run) start, scale and shut down Workers based on task-queue backlog. Substrate could be another target: Workers run as suspended actors that resume in under 400ms when work arrives. This is most valuable for multi-tenant platforms where customers supply their own workflow or activity code, since each tenant gets a VM-grade boundary.

**Benefits**
- Scale-to-zero density at 10–30x oversubscription, on your own Kubernetes cluster instead of Lambda.
- Safe execution of customer-supplied Worker code.
- Credentials stay out of the sandbox: Temporal API keys and mTLS are injected by the egress gateway.
- Fits with Temporal Worker Versioning: an ActorTemplate maps to a Worker Deployment Version.

**Challenges**
- Normal Workers long-poll task queues, so they never go idle and Substrate can't suspend them. This requires a push/invoke model.
- Depends on Temporal's serverless invocation interface being open to third-party compute targets.
- Cold-start budget: the Temporal SDK has to initialize quickly after a restore.
- The scaling signal (backlog latency) has to be mapped onto actor activation.

**Next steps**
- Study how Temporal's Lambda and Cloud Run serverless integrations invoke Workers, and whether that interface is pluggable.
- Prototype a bridge controller: watch task-queue backlog, then resume actors that poll for a single task and drain.
- Talk to Temporal's AI Partner Ecosystem team about a Substrate target.
