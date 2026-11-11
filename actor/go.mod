module my-org/quickstart

go 1.24

require temporal-substrate-demo/workerprotocol v0.0.0

replace temporal-substrate-demo/workerprotocol => ../workerprotocol

// actor/Dockerfile adds the Temporal SDK requirement (TEMPORAL_SDK_VERSION in
// versions.env) and go.sum at build time.
