# The toolbox: Go, the Docker CLI (driving the host's Docker through the
# mounted socket), kubectl, git and bash, so nothing gets installed or built
# on the host. ./toolbox.sh builds and enters it.
ARG GO_IMAGE=golang:1.27
FROM docker:28-cli AS docker

FROM ${GO_IMAGE}
COPY --from=docker /usr/local/bin/docker /usr/local/bin/docker
COPY --from=docker /usr/local/libexec/docker/cli-plugins/docker-buildx /usr/local/libexec/docker/cli-plugins/docker-buildx

ARG KUBECTL_VERSION=v1.34.2
RUN curl -fsSLo /usr/local/bin/kubectl \
      "https://dl.k8s.io/release/${KUBECTL_VERSION}/bin/linux/$(dpkg --print-architecture)/kubectl" \
    && chmod +x /usr/local/bin/kubectl \
    && apt-get update -q && apt-get install -yq --no-install-recommends procps && rm -rf /var/lib/apt/lists/*

# Substrate pins a newer Go toolchain than the image may have: download it.
ENV GOTOOLCHAIN=auto \
    PATH=/root/go/bin:${PATH} \
    REPO=/repo

# Every shell: the pinned versions as environment variables (docker build
# --build-arg NAME picks them up), a recognizable prompt, and /work as home base.
RUN printf '%s\n' \
      'set -a; . /repo/versions.env; set +a' \
      'PS1="\[\e[1;36m\]toolbox\[\e[0m\]:\w\$ "' \
      'cd /work 2>/dev/null || true' >> /etc/bash.bashrc
WORKDIR /work
