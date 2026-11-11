# client: demo drivers (run in the cluster)

Two small programs, packaged as image `hello-client` (`Dockerfile`). Both run as one-off pods in namespace `temporal` (`kubectl run`), where they reach the frontend at `$TEMPORAL_ADDRESS` (`temporal-frontend.temporal.svc.cluster.local:7233`). getting-started.md runs `setup` in step 5 and `start` in step 6. They form their own Go module and don't import the actor's code: the workflow is started by its registered name.

| Program | What it does |
|---|---|
| `setup/` | Registers namespace `default`, Worker Deployment `hello`, and version `v1` with a compute config. The provider is `substrate`: atespace `hello-v1`, template `hello-worker`, 3 actors, and frontend `temporal-frontend.temporal.svc.cluster.local:7233` in cleartext. The scaler is `rate-based`: 0–3 Workers, starting at 1. Idempotent. Flags: `/setup -h`. |
| `start/` | The quickstart starter: runs `SayHelloWorkflow("Temporal")` on `my-task-queue`, pinned to version `hello/v1`, and prints the result. |

The compute config doesn't repeat the version: Temporal passes it to the provider, which sends it to each Worker. Creating the version makes the WCI validate the config with the provider, so `setup` is also the first real call from Temporal to Substrate.
