# provider: the `substrate` compute provider (Temporal side)

A `ComputeProvider` for [temporal-auto-scaled-workers](https://github.com/temporalio/temporal-auto-scaled-workers), the Worker Controller (WCI) behind Temporal Serverless Workers. It isn't a program of its own: [`../temporal-server/Dockerfile`](../temporal-server/Dockerfile) copies the files into the WCI's `wci/workflow/compute_provider` package, runs their tests, and compiles them into `temporal-server` with `-tags computeprovider_substrate`. It imports [`../workerprotocol`](../workerprotocol).

## How it works

- **Launch strategy `worker-set`**, driven by the `rate-based` scaler, which can scale to zero. Substrate never suspends idle actors by itself, so the WCI's scale-down decisions are what free the compute.
- **One atespace per Worker Deployment Version.** The atespace is dedicated to the version's pool. Actors are named `worker-0` … `worker-(pool_size-1)`, and only those names are ever acted on. `UpdateWorkerSetSize(N)` converges the pool and keeps no state, so WCI retries are safe:
  - Actors `[0, N)`: create if missing, create the egress policy (before the first resume), revert if crashed, resume, then `POST /start`.
  - Actors `[N, pool)`: `POST /drain`, then suspend.
- **Single source of the Worker's settings.** The `/start` request is built here:
  - from the WCI's `RequestContext`: namespace, deployment name and build ID;
  - from the config: the frontend `temporal_host:temporal_port` and TLS;
  - plus the actor name as identity.

  The same host and port go into the actor's egress policy. If the config changes, a polling Worker answers `409`, and the provider drains it and starts it again.
- **Operator-side connections.** `ate-api-server` (gRPC) and `atenet-router` (CONNECT over mTLS) use the server pod's Substrate pod certificate. They are created once per process, because the WCI calls provider constructors on every activity.

## Files

| File | What it is |
|---|---|
| `substrate.go` | Provider: config, pool reconciliation, egress policy, `/start` request, error classification |
| `substrate_router.go` | `workerLifecycle` adapter: `workerprotocol.Client` over a tunnel through `atenet-router` |
| `substrate_dial.go` | Connections and TLS (pod certificate, servicedns CA, rotation) |
| `substrate_test.go` | Tests against an in-memory fake of `ate-api-server` and the Workers |
| `substrate_router_test.go` | Router adapter over real mTLS + CONNECT, with the real `workerprotocol` server behind it |
| `patches/0001-add-substrate-compute-provider-type.patch` | Adds `"substrate"` to the WCI's closed provider-type enum |

## Config (provider details of a scaling group)

Unknown keys are rejected, so a typo can't silently fall back to a default.

| Key | Required | Meaning |
|---|---|---|
| `atespace` | yes | Atespace dedicated to this version's pool |
| `actor_template` | yes | ActorTemplate for this build ID (in that atespace) |
| `temporal_host` | yes | Frontend host: sent to Workers in `/start`, allowed in their egress policy |
| `temporal_port` | no | Default `7233` |
| `temporal_tls` | no | Default `true` (https rule); `false` = cleartext h2c rule |
| `temporal_credential_uri` | no | Injects `Authorization: Bearer <credential>` via the egress gateway (TLS only) |
| `pool_size` | no | Default `30`; must be ≥ the scaler's `max_count` |

Operator environment on the server (set in [`../temporal-server/deploy/temporal.yaml`](../temporal-server/deploy/temporal.yaml)): `SUBSTRATE_CA_FILE`, `SUBSTRATE_CREDENTIAL_BUNDLE`, and optionally `SUBSTRATE_API_ADDRESS` and `SUBSTRATE_ROUTER_ADDRESS`.

## Verification status

- Run so far: `go vet` and 14 tests with the race detector, against stubs of the Substrate and Temporal types and the real `workerprotocol` module. The seven Substrate API calls were diffed against the real generated `ateapi.ControlClient`.
- Not yet run here: compiling against the real Temporal and Substrate modules. The server image build (getting-started.md, step 2) runs the tests against them and fails if they fail.

## Open TODOs

- Update the egress policy when the config changes (today an existing policy is kept).
- Check for a golden snapshot in `ValidateConfig`.
- Move operator settings to dynamic config, as the AWS and GCP providers do.
