# actor: the Temporal Worker as a Substrate actor (Substrate side)

The Temporal Go "Run Hello World" quickstart, with its Worker run as an Agent Substrate actor. It also holds the Substrate resources that host it.

| Path | What it is |
|---|---|
| `workflow.go`, `activity.go` | `SayHelloWorkflow` and `Greet` from the quickstart, unchanged (Go module `my-org/quickstart`) |
| `lifecycle/` | Makes any Temporal Worker actor-ready: serves [`workerprotocol`](../workerprotocol), connects on `/start`, disconnects on `/drain` |
| `cmd/worker/main.go` | The app: task queue plus `Register` of the workflow and activity, handed to `lifecycle.Run` |
| `Dockerfile` | The actor image. It pins the Temporal SDK from `versions.env`, runs the lifecycle tests, and is built from the repo root because it includes `workerprotocol/`. |
| `deploy/worker-pool.yaml` | Kubernetes namespace `temporal-workers` and WorkerPool `hello-worker`: the gVisor worker pods that host actors |
| `deploy/hello-worker-template.yaml` | ActorTemplate `hello-worker` in atespace `hello-v1`: image, wakeup probe, size. No Temporal settings. |
| `deploy/access-policy.yaml` | Lets the Temporal server's identity manage atespace `hello-v1` |

## The lifecycle module

```go
err := lifecycle.Run(ctx, lifecycle.Options{
	TaskQueue: "my-task-queue",
	Register: func(r lifecycle.Registry) {
		r.RegisterWorkflow(quickstart.SayHelloWorkflow)
		r.RegisterActivity(quickstart.Greet)
	},
})
```

That's the whole interface. Behind it:

- It serves `/readyz`, `/start` and `/drain` on port 80.
- It connects to the frontend named in `/start`, with that request's namespace, version (Worker Versioning) and identity. A repeat `/start` doesn't reconnect, and a different one gets `409`.
- On `/drain` it stops the Worker and closes the client, so the next snapshot holds no connection.
- With `tls: true` (e.g. Temporal Cloud), it trusts `Options.CAFile` (default `$SSL_CERT_FILE`, the egress gateway's CA) and sends a placeholder API key for the gateway to replace.

`lifecycle.go` holds the state machine and is free of Temporal SDK code. `temporal.go` is the SDK adapter (dial and `worker.New`). The tests drive the module through `workerprotocol.Client` with a fake connection to Temporal.

Do expensive, connection-free initialization before `lifecycle.Run`. The golden snapshot is taken as soon as `/readyz` answers, so every actor inherits it. Nothing created there may need to differ per actor.

## One atespace per version

Atespace `hello-v1` belongs to Worker Deployment Version `hello/v1`. The provider owns the actors in it, named `worker-0` … `worker-2`. A new release gets a new atespace, template and access policy (for example `hello-v2`), and its own version in Temporal.
