# The hello-world Worker, run as a Substrate actor. Build from the repo root
# (the image includes workerprotocol/):
#   docker build -f actor/Dockerfile --build-arg TEMPORAL_SDK_VERSION=... .
ARG GO_IMAGE=golang:1.27

FROM ${GO_IMAGE} AS build
ARG TEMPORAL_SDK_VERSION
ENV GOTOOLCHAIN=auto CGO_ENABLED=0
WORKDIR /src
COPY workerprotocol/ workerprotocol/
COPY actor/ actor/
WORKDIR /src/actor
RUN --mount=type=cache,target=/root/go/pkg/mod --mount=type=cache,target=/root/.cache/go-build \
    set -e; \
    go get "go.temporal.io/sdk@${TEMPORAL_SDK_VERSION}"; \
    go mod tidy; \
    go test ./lifecycle/; \
    go build -trimpath -o /worker ./cmd/worker

# Runs as root inside the actor's gVisor sandbox, so it can listen on port 80.
FROM gcr.io/distroless/static-debian12
COPY --from=build /worker /worker
ENTRYPOINT ["/worker"]
