# Substrate compute provider for Temporal Serverless Workers (skeleton)

A `ComputeProvider` for [temporal-auto-scaled-workers](https://github.com/temporalio/temporal-auto-scaled-workers), the Worker Controller Instance (WCI) behind Temporal Serverless Workers. It runs Temporal Workers as [Agent Substrate](https://github.com/agent-substrate/substrate) actors: sandboxed, snapshotted and resumed from a pre-warmed golden snapshot.

Written against `temporal-auto-scaled-workers@51edc88` (2026-11-03) and `substrate@94e0a26a` (2026-11-05). Both are pre-1.0, so expect breakage.

## Files

| File | What it is |
|---|---|
| `substrate.go` | The provider: config parsing, pool reconciliation, error classification |
| `substrate_dial.go` | gRPC connection to `ate-api-server` over SPIFFE mTLS (operator-side settings) |
| `substrate_test.go` | Tests against an in-memory fake of `ate-api-server` |
| `0001-add-substrate-compute-provider-type.patch` | Adds `"substrate"` to the WCI's closed provider-type enum |
| `example-wci-spec.json` | A WCI spec using this provider with the `rate-based` algorithm |

## How it works

- **Launch strategy: `worker-set`**, driven by the `rate-based` algorithm, which can scale to zero. Substrate never suspends idle actors by itself, so the WCI's scale-down decisions are what free the compute.
- **One pool per Worker Deployment Version.** Actors are named `<prefix>-<i>` for `i` in `[0, pool_size)`. The prefix is a hash of namespace, deployment and build ID unless `name_prefix` is set.
- **`UpdateWorkerSetSize(N)`** lists the pool and converges it. Actors `[0, N)` are created if missing, reverted if crashed, and resumed. Actors `[N, pool)` are suspended. It keeps no state, so WCI activity retries are safe.
- **Cold start = resume.** Each `ActorTemplate` gets a golden snapshot by default, taken after the Worker has booted. New actors start from that snapshot instead of from scratch.

## Integrate

```bash
cd temporal-auto-scaled-workers
git apply ../substrate-provider/0001-add-substrate-compute-provider-type.patch
cp ../substrate-provider/substrate*.go wci/workflow/compute_provider/
go get go.ate.dev/substrate@94e0a26a   # bumps the go directive to 1.27
go test -tags computeprovider_substrate ./wci/workflow/compute_provider/
```

Operator environment on the server that runs the WCI: `SUBSTRATE_API_ADDRESS`, `SUBSTRATE_TLS_CERT_FILE`, `SUBSTRATE_TLS_KEY_FILE`, `SUBSTRATE_TLS_CA_FILE`. The client SPIFFE ID must be allowed by `ate-api-server`'s access policy for the target atespace. Then enable it with dynamic config: `WorkerControllerEnabled: true` and add `substrate` to `WorkerControllerEnabledComputeProviders`.

## Verification status

The module proxy was unreachable when this was written, so the code has **not** been compiled against the real dependencies. Instead:
- `actorControl`'s six method signatures were diffed against the real generated `ateapi.ControlClient` and match.
- `errors.go` and the `registry.go` types are the real files.
- `ateapi`, gRPC and `dynamicconfig` were stubbed with the same names and signatures. `go vet` and `go test -race` pass against those stubs.

The first real step is the `go test` command above.

## Open TODOs (marked in code)

- **Drain before suspend.** Scale-down snapshots a Worker as-is, possibly mid-Activity. Temporal retries the task elsewhere, but a graceful drain is better.
- **Verify the server's SPIFFE ID** instead of relying on hostname verification.
- **Move operator settings to dynamic config**, as the AWS and GCP providers do.
- **Check for a golden snapshot in `ValidateConfig`.**
