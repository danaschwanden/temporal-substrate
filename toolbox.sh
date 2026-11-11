#!/usr/bin/env bash
# ./toolbox.sh       open a shell in the toolbox (builds and starts it if needed)
# ./toolbox.sh rm    remove the toolbox and its caches
#
# The toolbox shares the host's Docker (socket) and network, so kind's
# kubeconfig, the localhost:5001 registry and kubectl port-forward work as on
# a workstation. This repo is mounted read-only at /repo; /work and /root
# (clones, Go caches, kubeconfig) persist between shells.
set -euo pipefail
cd "$(dirname "$0")"
NAME=tsd-toolbox

if [ "${1:-}" = "rm" ]; then
  docker rm -f "$NAME" >/dev/null 2>&1 || true
  docker volume rm tsd-home tsd-work >/dev/null 2>&1 || true
  echo "Toolbox removed."
  exit 0
fi

if [ "$(docker inspect -f '{{.State.Running}}' "$NAME" 2>/dev/null)" != "true" ]; then
  docker rm -f "$NAME" >/dev/null 2>&1 || true
  # shellcheck disable=SC1091
  . ./versions.env
  echo "Building the toolbox..."
  docker build -q -t tsd-toolbox \
    --build-arg GO_IMAGE="$GO_IMAGE" --build-arg KUBECTL_VERSION="$KUBECTL_VERSION" tools >/dev/null
  docker run -d --name "$NAME" --network host \
    -v /var/run/docker.sock:/var/run/docker.sock \
    -v "$PWD":/repo:ro -v tsd-home:/root -v tsd-work:/work \
    tsd-toolbox sleep infinity >/dev/null
fi
exec docker exec -it "$NAME" bash
