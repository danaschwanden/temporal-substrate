# The demo client (setup and start), run as one-off pods in the cluster.
# Build from the repo root:
#   docker build -f client/Dockerfile --build-arg TEMPORAL_SDK_VERSION=... --build-arg TEMPORAL_API_VERSION=... .
ARG GO_IMAGE=golang:1.27

FROM ${GO_IMAGE} AS build
ARG TEMPORAL_SDK_VERSION
ARG TEMPORAL_API_VERSION
ENV GOTOOLCHAIN=auto CGO_ENABLED=0
WORKDIR /src/client
COPY client/ ./
RUN --mount=type=cache,target=/root/go/pkg/mod --mount=type=cache,target=/root/.cache/go-build \
    set -e; \
    go get "go.temporal.io/sdk@${TEMPORAL_SDK_VERSION}" "go.temporal.io/api@${TEMPORAL_API_VERSION}" github.com/google/uuid@v1.6.0; \
    go mod tidy; \
    go build -trimpath -o /out/setup ./setup; \
    go build -trimpath -o /out/start ./start

FROM gcr.io/distroless/static-debian12
COPY --from=build /out/ /
# The frontend, as seen from inside the cluster.
ENV TEMPORAL_ADDRESS=temporal-frontend.temporal.svc.cluster.local:7233
ENTRYPOINT ["/start"]
