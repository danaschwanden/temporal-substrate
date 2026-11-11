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
- Partly answered by the `ateapi` source: `RevertActor` restores an actor to its last *durable* snapshot, and `CreateActor` can seed a new actor from a snapshot **Tag** (`source_tag`), which is a basis for forking. Still open: whether arbitrary earlier snapshot generations can be tagged and restored.
- Snapshot storage cost and garbage collection (retention tied to the workflow's lifetime).
- External side effects (API calls, git pushes) can't be undone by restoring a snapshot.
- Consistency between snapshot writes and workflow events.

**Next steps**
- Read the Substrate API and architecture docs for snapshot semantics. Ask in `#substrate-dev`.
- If the primitives are missing, draft an upstream proposal for snapshot IDs, fork and restore.
- Prototype a fork-from-snapshot child-workflow pattern on the counter demo.

---

## 3. Substrate as a Serverless Worker Target

Temporal's Serverless Workers start, scale and stop Workers based on task-queue load. The open-source scaler behind them, the **Worker Controller Instance (WCI)** in [`temporal-auto-scaled-workers`](https://github.com/temporalio/temporal-auto-scaled-workers) (MIT), has a pluggable compute-provider interface. So Substrate becomes a target by writing a **compute provider**, not a new WCI. Workers run as Substrate actors that resume from a pre-warmed snapshot. This is most valuable for multi-tenant platforms where customers supply their own Worker code, since each tenant gets a VM-grade boundary.

**How the WCI works**
- The WCI is a system workflow, one per Worker Deployment Version (deployment name + build ID). The Matching Service signals it when a task finds no free Worker. It also polls backlog and dispatch metrics, runs a scaling algorithm, and calls a compute provider.
- A provider implements four methods: `LaunchStrategy`, `ValidateConfig`, `InvokeWorker` and `UpdateWorkerSetSize`. It registers under a type string (`aws-lambda`, `aws-ecs`, `aws-agentcore`, `gcp-cloud-run`, `k8s`, `subprocess`).
- There are two launch strategies. **Invoke** starts one short-lived Worker per scaling event (Lambda). **Worker-set** resizes a managed pool (ECS, Cloud Run, k8s). The `rate-based` algorithm drives worker-sets and can scale down to zero.

**Design: a worker-set of actors**
- One `ActorTemplate` per build ID. Substrate builds a **golden snapshot** of each template after the Worker boots, so a new Worker is a resume instead of a cold start.
- One pool of actors per build ID, named `<prefix>-0 … <prefix>-(pool-1)`. `UpdateWorkerSetSize(N)` keeps actors `[0, N)` running (create, revert if crashed, resume) and suspends `[N, pool)`. It keeps no state, so WCI retries are safe.
- Worker-set was chosen over invoke because **Substrate doesn't suspend idle actors by itself**. The caller has to call `SuspendActor`. Under invoke, nothing would ever scale down. Under worker-set, the WCI's own scale-down decisions free the compute.

**Benefits**
- Scale-to-zero at 10–30x oversubscription, on your own Kubernetes cluster instead of Lambda.
- Fast cold starts: the golden snapshot already holds an initialized Worker.
- Safe execution of customer-supplied Worker code in gVisor or microVM sandboxes.
- Credentials stay out of the sandbox: the Worker's Temporal API key can be injected by Substrate's egress gateway.
- Maps directly onto Worker Versioning, which Serverless Workers require anyway: one Worker Deployment Version ↔ one `ActorTemplate` ↔ one pool.

**Challenges**
- **Self-hosted only.** The WCI is wired into the Temporal server (a task hook in the Matching Service), so a custom provider means a custom server build. Temporal Cloud only gets this if it's upstreamed.
- **Closed enum.** Provider types are validated against a fixed list, so adding `substrate` needs a small patch to `iface/spec.go`.
- **Pre-release.** The provider interface already changed between March and November 2026 (it gained a `RequestContext` argument). Both projects are pre-1.0.
- **Connections after resume.** A restored Worker's gRPC connections and in-flight long-polls to Temporal are dead. The SDK must reconnect cleanly. This is the biggest unknown.
- **Scale-down mid-task.** Suspending a busy Worker snapshots it mid-Activity. Temporal retries the task elsewhere, but a graceful drain before suspend is better.
- **Dependency weight.** Substrate's generated API lives in its main Go module (Go 1.27, Kubernetes dependencies). A separate API module upstream would help.
- **Trust in both directions.** The WCI's SPIFFE identity must be allowed by `ate-api-server`. The provider's endpoint and credentials must be operator-side, never in the tenant's spec.

**Next steps**
1. Integrate the skeleton in `substrate-provider/` into a checkout of `temporal-auto-scaled-workers` and run its tests against the real dependencies.
2. Spike the riskiest assumption: suspend a running Go Worker on Substrate, resume it, and confirm it reconnects and picks up new tasks.
3. Run end to end on kind: Temporal dev server with WCI enabled + Substrate + an `ActorTemplate` wrapping a sample Worker.
4. Add graceful drain, and move operator settings to dynamic config.
5. Share the result with Temporal (upstreaming the provider) and in Substrate's `#substrate-dev` (a separate API module).

**Provider skeleton:** `substrate-provider/` contains `substrate.go`, `substrate_dial.go`, tests, the enum patch and an example WCI spec. See its README for integration steps and what has been verified.
