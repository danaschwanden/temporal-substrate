# workerprotocol: the provider↔Worker contract (shared)

The only code both sides import: [`../provider`](../provider) (client side, inside `temporal-server`) and [`../actor/lifecycle`](../actor/lifecycle) (server side, inside each actor). It uses only the standard library, so it adds nothing to the Temporal server build. Go module `temporal-substrate-demo/workerprotocol`; both modules use it through a `replace` directive.

## The contract

All three endpoints are served on `Port` (80), which the ActorTemplate's wakeup probe also uses.

| Endpoint | Called by | Meaning |
|---|---|---|
| `GET /readyz` | Substrate wakeup probe | 200 once initialized. The golden snapshot is taken right after the first 200. |
| `POST /start` + `StartRequest` | provider, after a resume | Connect to Temporal and poll. Idempotent; `409` if already started with a different request. `502` if Temporal is unreachable. |
| `POST /drain` | provider, before a suspend | Stop polling, finish in-flight tasks within `WorkerStopTimeout`, disconnect. Idempotent. |

`StartRequest` has these fields, all validated on both sides:

- `identity`: the actor name.
- `address`: the frontend, as `host:port`.
- `tls`
- `namespace`
- `deploymentName`, `buildId`

Timing budget (constants). The invariants hold by construction and a test checks them:

| Constant | Value | Bounds |
|---|---|---|
| `DialTimeout` | 10 s | Worker's connect in `/start` |
| `StartTimeout` | `DialTimeout` + 35 s | provider's `/start` call (router may resume the actor first) |
| `WorkerStopTimeout` | 25 s | Worker's wait for in-flight tasks in `/drain` |
| `DrainTimeout` | `WorkerStopTimeout` + 15 s | provider's `/drain` call |

## Interface

- **Server side:** `NewHandler(Worker) http.Handler`, where `Worker` is `Ready()`, `Start(ctx, StartRequest)` and `Drain(ctx)`.
- **Client side:** `Client{Dial, Host}` with `.Start(ctx, req)` and `.Drain(ctx)`. `Dial` supplies the connection: for the provider, a tunnel through `atenet-router`.

`protocol_test.go` runs the real client against the real handler. On top of that, each side tests its own adapter against the other side's real code: the actor's lifecycle is driven by `Client`, and the provider's router adapter talks to `NewHandler`.
