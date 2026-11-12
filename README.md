# Temporal Serverless Workers on Agent Substrate

Runs [Temporal](https://docs.temporal.io/) [Workers](https://docs.temporal.io/serverless-workers) as [Agent Substrate](https://github.com/agent-substrate/substrate) [Actors](https://ate.dev/docs/concepts/architecture/). Temporal's [Worker Controller Instance (WCI)](https://docs.temporal.io/serverless-workers#worker-controller-instance) decides how many Workers a Worker Deployment Version needs. A [compute provider](https://docs.temporal.io/serverless-workers#compute-providers)  then resumes or suspends Substrate Actors to match. Each Worker runs in a [gVisor](https://gvisor.dev/) sandbox and starts by resuming a pre-warmed snapshot.

**To run the demo**, follow [getting-started.md](getting-started.md). Docker is the only requirement.

## Layout

The repo has one directory per component, plus the protocol the two sides share.

```
temporal-substrate-demo/
├── toolbox.sh           opens a shell in the toolbox container (Go, kubectl, Docker CLI)
├── versions.env         pinned commits and versions for every build
├── tools/               the toolbox image
├── workerprotocol/      SHARED: the provider↔Worker contract (stdlib only, imported by both sides)
├── provider/            TEMPORAL SIDE: the "substrate" compute provider
│   ├── substrate*.go      copied into temporal-auto-scaled-workers (the WCI)
│   └── patches/           registers the "substrate" provider type with the WCI
├── temporal-server/     TEMPORAL SIDE: packaging and deployment of the server
│   ├── Dockerfile         fetches Temporal + WCI + Substrate, adds the provider, tests, builds
│   └── deploy/            Kubernetes manifests: server, dynamic config, Web UI
├── actor/               SUBSTRATE SIDE: the Temporal Worker as a Substrate actor
│   ├── lifecycle/         makes any Temporal Worker actor-ready (implements workerprotocol)
│   ├── *.go, cmd/worker   hello-world workflow and activity; main only registers them
│   ├── Dockerfile         the actor's container image (built from the repo root)
│   └── deploy/            Substrate resources: WorkerPool, ActorTemplate, access policy
└── client/              DEMO DRIVERS: run as one-off pods in the cluster
    ├── setup/             registers namespace, deployment and version (+ compute config)
    └── start/             starts the hello-world workflow
```

| Directory | Runs in | Built into | Talks to |
|---|---|---|---|
| `workerprotocol/` | both sides | the server binary and the actor image | — |
| `provider/` | the `temporal-server` process | the server binary (build tag `computeprovider_substrate`) | `ate-api-server`; Workers via `atenet-router` |
| `temporal-server/` | pod in namespace `temporal` | image `temporal-server-substrate` | — |
| `actor/` | actors in atespace `hello-v1` | image `hello-worker` | the Temporal frontend, via Substrate's egress gateway |
| `client/` | one-off pods in namespace `temporal` | image `hello-client` | the Temporal frontend |

## How the two sides meet

There are exactly two interfaces between Temporal and Substrate:

1. **Provider → Substrate API.** The provider creates, resumes and suspends the actors in the version's atespace, and creates each actor's egress policy.
2. **Provider → Worker, [`workerprotocol`](workerprotocol).** Through `atenet-router`, the provider sends `POST /start` after a resume and `POST /drain` before a suspend. `/start` carries everything the Worker needs: the frontend address, namespace, deployment version and identity. The Worker connects only between the two calls, so a snapshot never holds a live connection.

Configuration has one owner. The version comes from the WCI, and the frontend from the provider's compute config. The ActorTemplate holds only the image, so the address the Worker dials and the address its egress policy allows can't diverge.

Each directory has its own README with the details.
