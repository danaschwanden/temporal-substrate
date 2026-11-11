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

# 1b. The provider needs only Substrate's generated API package, which imports
#     nothing but gRPC and protobuf. Replacing go.ate.dev/substrate with the
#     whole repo would put all of Substrate's requirements into the server's
#     module graph (it raised go.opentelemetry.io/otel/log to a version the
#     server doesn't compile with). So: a module with just that package,
#     requiring gRPC and protobuf at the server's own versions, which leaves
#     every server dependency where it was.
RUN set -e; \
    mkdir -p substrate-api/api/ateapi; \
    cp -r substrate/api/ateapi/v1 substrate-api/api/ateapi/v1; \
    v() { awk -v m="$1" '$1 == m { print $2; exit }' temporal/go.mod; }; \
    printf 'module go.ate.dev/substrate\n\ngo 1.24\n\nrequire (\n\tgoogle.golang.org/grpc %s\n\tgoogle.golang.org/protobuf %s\n)\n' \
      "$(v google.golang.org/grpc)" "$(v google.golang.org/protobuf)" > substrate-api/go.mod; \
    cat substrate-api/go.mod; \
    grep -q 'grpc v1' substrate-api/go.mod && grep -q 'protobuf v1' substrate-api/go.mod

# 2. Add the provider to the WCI and run its tests against the real dependencies.
COPY workerprotocol/ demo/workerprotocol/
COPY provider/ demo/provider/
RUN --mount=type=cache,target=/root/go/pkg/mod --mount=type=cache,target=/root/.cache/go-build \
    set -e; cd temporal-auto-scaled-workers; \
    git apply ../demo/provider/patches/*.patch; \
    cp ../demo/provider/substrate*.go wci/workflow/compute_provider/; \
    go mod edit -replace go.ate.dev/substrate=../substrate-api \
                -replace temporal-substrate-demo/workerprotocol=../demo/workerprotocol; \
    go mod tidy; \
    go test -tags computeprovider_substrate ./wci/workflow/compute_provider/

# 3. Build the server against the patched WCI. Go ignores replace directives in
#    dependencies, so they are repeated on the server module.
RUN --mount=type=cache,target=/root/go/pkg/mod --mount=type=cache,target=/root/.cache/go-build \
    set -e; cd temporal; \
    go mod edit -replace go.temporal.io/auto-scaled-workers=../temporal-auto-scaled-workers \
                -replace go.ate.dev/substrate=../substrate-api \
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
