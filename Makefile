BIN      := arcade-bridge
PKG      := ./...
VERSION  ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
IMAGE    ?= ghcr.io/lightwebinc/$(BIN)
LDFLAGS  := -s -w -X main.Version=$(VERSION)

.PHONY: all build test lint tidy fmt image clean help

all: build

build:                 ## build arcade-bridge on the host
	CGO_ENABLED=0 go build -trimpath -ldflags '$(LDFLAGS)' -o $(BIN) ./cmd/arcade-bridge

test:                  ## go test -race ./...
	go test -race -count=1 $(PKG)

lint:                  ## golangci-lint run
	golangci-lint run

tidy:
	go mod tidy

fmt:                   ## gofmt -w
	gofmt -w .

image:                 ## build the container image locally
	docker build --build-arg VERSION=$(VERSION) -t $(IMAGE):$(VERSION) .

clean:
	rm -f $(BIN)

help:                  ## list targets
	@awk 'BEGIN {FS = ":.*?## "} /^[a-zA-Z_-]+:.*?## / {printf "  \033[36m%-14s\033[0m %s\n", $$1, $$2}' $(MAKEFILE_LIST) | sort
