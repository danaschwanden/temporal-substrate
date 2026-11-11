# temporal-server: the Temporal server image and deployment (Temporal side)

Builds and deploys a `temporal-server` with the Worker Controller and the [`../provider`](../provider) compiled in.

| File | What it is |
|---|---|
| `Dockerfile` | Multi-stage build from the repo root. It fetches Temporal, the WCI and Substrate at the commits in `versions.env`, applies the provider patch, runs the provider's tests, and builds the server. The result is pushed as `localhost:5001/temporal-server-substrate:dev`. |
| `deploy/temporal.yaml` | Namespace `temporal`, ServiceAccount `temporal-server`, server config and dynamic config, server Deployment, `temporal-frontend` Service, Web UI |

## What the manifest sets up for the provider

- **Identity.** The ServiceAccount's pod certificate carries `spiffe://cluster.local/ns/temporal/sa/temporal-server`. That ID is the principal in the access policy of atespace `hello-v1` ([`../actor/deploy/access-policy.yaml`](../actor/deploy/access-policy.yaml)), and it must be allowed by `atenet-router`.
- **Credentials.** A projected volume at `/var/run/ate` holds the Substrate CA bundle (`ca.crt`) and the pod certificate (`credential-bundle.pem`).
- **Network access.** The namespace label `ate.dev/api-access=true` lets Substrate's NetworkPolicy admit the pod to `ate-api-server`.
- **Dynamic config.** It sets `workercontroller.enabled` and allows the `substrate` provider.

Persistence is in-memory SQLite in a single pod, so a restart loses all state. Re-run `client/setup` after a restart.
