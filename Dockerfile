# syntax=docker/dockerfile:1.28.0@sha256:bb22d9815c728170f72750f4e5b0d672e06176142e1d602c7e66c050100b7e5b
#
# Multi-stage Dockerfile for arcade-bridge. Produces a single static binary
# at /usr/local/bin/arcade-bridge on a distroless nonroot base.
#
# No ENV defaults are baked in: the bridge is configured entirely by flags, so
# pass them as the container command / Helm `args`. See docs/configuration.md.

FROM golang:1.27-alpine@sha256:b8c9b82affbbb82a020bbcf998dad2ba9516944ac7e883524642a481a9505e9c AS builder
RUN apk add --no-cache git ca-certificates
WORKDIR /src

COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod \
    go mod download

COPY . .
ARG VERSION=dev
ARG TARGETOS=linux
ARG TARGETARCH=amd64
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    set -eux; \
    mkdir -p /out; \
    CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} \
      go build -trimpath -buildvcs=false \
        -ldflags "-s -w -X main.Version=${VERSION}" \
        -o /out/arcade-bridge ./cmd/arcade-bridge

FROM gcr.io/distroless/static:nonroot@sha256:d62067879de890a1def9da0e7766b7d20810a5e5555cef8506b752437d7a3e88
USER nonroot:nonroot
COPY --from=builder /out/ /usr/local/bin/

# The licences travel with the image, not only with the source tree. The binary
# above is statically linked, so it contains the code of every module in
# LICENSE-THIRD-PARTY and none of their licence files. Apache-2.0 section 4,
# MIT, BSD-3 and Open BSV all require their text to reach the recipient of a
# binary, so an image carrying the code and not the notices does not discharge
# any of them.
COPY LICENSE NOTICE LICENSE-THIRD-PARTY /usr/share/doc/arcade-bridge/
# subtree / block delivery lanes, the retrieval plane, the facade, then the
# metrics and health listener (-metrics-addr, which also serves /healthz, /readyz).
EXPOSE 9143 9144 9165 9166 9167
ENTRYPOINT ["/usr/local/bin/arcade-bridge"]
