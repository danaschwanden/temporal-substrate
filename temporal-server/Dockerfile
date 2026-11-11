# Temporal server with the Worker Controller (WCI) and the substrate compute
# provider compiled in. Build from the repo root:
#   docker build -f temporal-server/Dockerfile --build-arg ... .
# getting-started.md (step 2) passes the pins from versions.env.
ARG GO_IMAGE=golang:1.27

FROM ${GO_IMAGE} AS build
ARG TEMPORAL_REF
ARG WCI_REF
ARG SUBSTRATE_REF
ENV GOTOOLCHAIN=auto
WORKDIR /src

# 1. Sources at the pinned commits.
RUN set -e; \
    fetch() { git init -q "$1" && git -C "$1" fetch -q --depth 1 "$2" "$3" && git -C "$1" checkout -q FETCH_HEAD; }; \
    fetch temporal                     https://github.com/temporalio/temporal.git                     "${TEMPORAL_REF}"; \
    fetch temporal-auto-scaled-workers https://github.com/temporalio/temporal-auto-scaled-workers.git "${WCI_REF}"; \
    fetch substrate                    https://github.com/agent-substrate/substrate.git               "${SUBSTRATE_REF}"

# 2. Add the provider to the WCI and run its tests against the real dependencies.
COPY workerprotocol/ demo/workerprotocol/
COPY provider/ demo/provider/
RUN --mount=type=cache,target=/root/go/pkg/mod --mount=type=cache,target=/root/.cache/go-build \
    set -e; cd temporal-auto-scaled-workers; \
    git apply ../demo/provider/patches/*.patch; \
    cp ../demo/provider/substrate*.go wci/workflow/compute_provider/; \
    go mod edit -replace go.ate.dev/substrate=../substrate \
                -replace temporal-substrate-demo/workerprotocol=../demo/workerprotocol; \
    go mod tidy; \
    go test -tags computeprovider_substrate ./wci/workflow/compute_provider/

# 3. Build the server against the patched WCI. Go ignores replace directives in
#    dependencies, so they are repeated on the server module.
RUN --mount=type=cache,target=/root/go/pkg/mod --mount=type=cache,target=/root/.cache/go-build \
    set -e; cd temporal; \
    go mod edit -replace go.temporal.io/auto-scaled-workers=../temporal-auto-scaled-workers \
                -replace go.ate.dev/substrate=../substrate \
                -replace temporal-substrate-demo/workerprotocol=../demo/workerprotocol; \
    go mod tidy; \
    make temporal-server BUILD_TAG=computeprovider_substrate; \
    go tool nm temporal-server | grep -q substrateComputeProvider

FROM alpine:3.22
RUN apk add --no-cache ca-certificates tzdata && \
    addgroup -g 1000 temporal && \
    adduser -u 1000 -G temporal -D temporal
COPY --from=build /src/temporal/temporal-server /usr/local/bin/temporal-server
WORKDIR /etc/temporal
USER temporal
ENTRYPOINT ["/usr/local/bin/temporal-server"]
