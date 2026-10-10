# arcade-bridge

[![CI](https://github.com/lightwebinc/arcade-bridge/actions/workflows/ci.yml/badge.svg)](https://github.com/lightwebinc/arcade-bridge/actions/workflows/ci.yml)
[![CodeQL](https://github.com/lightwebinc/arcade-bridge/actions/workflows/codeql.yml/badge.svg)](https://github.com/lightwebinc/arcade-bridge/actions/workflows/codeql.yml)
[![Release](https://img.shields.io/github/v/release/lightwebinc/arcade-bridge)](https://github.com/lightwebinc/arcade-bridge/releases)
[![Go Reference](https://pkg.go.dev/badge/github.com/lightwebinc/arcade-bridge.svg)](https://pkg.go.dev/github.com/lightwebinc/arcade-bridge)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)

> Part of the [**BSV Layered Multicast**](https://github.com/lightwebinc/bsv-multicast) open-source project. See the main repository for the full architecture, design docs, and BRC specifications.

A landing-tier bridge that runs an **unmodified
[Arcade v2](https://github.com/bsv-blockchain/arcade) +
[merkle-service](https://github.com/bsv-blockchain/merkle-service) stack with a
multicast delivery fabric as its transport.**

It is the application-tier sibling of
[teranode-bridge](https://github.com/lightwebinc/teranode-bridge). Where that
bridge fronts a mining Teranode cluster, this one fronts the broadcaster tier:
an Arcade operator is a delivery consumer, not a miner. Subtrees and blocks
arrive down the tunnel, transactions go up on the open class, and nothing here
carries miner entitlements. The shared machinery (lane termination, the
object cache, the retrieval plane, hash-order discipline, block wire
conversion) is imported from teranode-bridge's public packages, never forked.

## How it works

```text
  fabric ══push══▶ arcade-bridge
      lanes ─▶ cache ─┬─▶ announce ──▶ merkle-service   (hash, dataHubUrl)
                      └─▶ retrieval ◀─ fetch ── merkle-service
      Arcade ─▶ POST /txs ─▶ facade ─▶ up-tunnel ─▶ fabric ─▶ every miner
```

- **Proof ingest.** Pushed subtrees and blocks are cached and announced to
  merkle-service with its own Kafka messages, `dataHubUrl` pointing at the
  bridge's retrieval plane, so the fetch never leaves the box and Arcade's
  lifecycle runs unchanged.
- **Broadcast.** The propagation facade serves the Teranode-shaped
  `POST /txs` Arcade already speaks and sends one copy up the tunnel instead
  of O(N) unicast POSTs; the fabric fans it out to every miner. Standard-format
  submissions are hydrated to extended format, and Teranode's failure-list
  grammar is preserved.

[Architecture](docs/architecture.md) covers both contracts in full.

## Documentation

- [Architecture](docs/architecture.md): components, the announce and fetch contract, the facade contract, hydration, byte order, failure modes
- [Configuration](docs/configuration.md): every flag, defaults, deployment examples, reading the stats
- [Metrics reference](docs/references/prometheusMetrics.md): every series and the chart's alert expressions
- [BRC-143 Subtree Data](https://github.com/lightwebinc/bsv-multicast/blob/main/docs/brc-143-subtree-data.md) and [BRC-144 Block Frame](https://github.com/lightwebinc/bsv-multicast/blob/main/docs/brc-144-block-frame.md): what the delivery lanes carry
- [BRC-30 Transaction Extended Format](https://github.com/bsv-blockchain/BRCs/blob/master/transactions/0030.md): what the up-tunnel stream carries
- [Deliver once](https://github.com/lightwebinc/bsv-multicast/blob/main/docs/deliver-once.md): one crossing per landing site, and how a co-located Arcade + Teranode site fans locally

## Requirements

- Go 1.26 or later
- A reachable merkle-service Kafka and an Arcade v2 stack (or `-mode sink`,
  which needs neither)
- Network reachability from the stack's containers back to the bridge's
  retrieval plane (containers cannot dial loopback; advertise an address they
  can reach)

## Build

```bash
go build ./cmd/arcade-bridge
go test -race ./...
```

## Run

```bash
# Sink: receive, parse, count and serve, with no announce targets.
# Use for lane burn-in before the Arcade stack attaches.
./arcade-bridge -mode sink -stats-every 10s

# Feed plus facade: also serve Arcade's broadcast path into the fabric.
./arcade-bridge \
  -advertise     'http://192.0.2.10:9165' \
  -kafka         'localhost:19092' \
  -edge-ingress  '2001:db8:59::a,2001:db8:59::b' \
  -hydrate-asset 'http://192.0.2.20:20090/api/v1'
```

See [docs/configuration.md](docs/configuration.md) for the full flag
reference.

## Default ports

| Port | Direction | Carries |
| --- | --- | --- |
| `9143` | in | subtree lane (bare BRC-143 push frames) |
| `9144` | in | block lane (bare BRC-144 push frames) |
| `9165` | in | retrieval plane, merkle-service's fetches (`/api/v1`) |
| `9166` | in | propagation facade (`POST /txs`, `POST /tx`, `GET /health`) |
| `9167` | in | `/metrics`, `/healthz`, `/readyz` |
| `8725` | out | bare BRC-30 EF transaction stream to the fabric ingress |

The delivery lanes use the canonical `9143`/`9144` shared with teranode-bridge
([lane numbering](https://github.com/lightwebinc/bsv-multicast/blob/main/docs/lane-numbering.md)); a site running both stacks uses one slot and a
local tee ([deliver once](https://github.com/lightwebinc/bsv-multicast/blob/main/docs/deliver-once.md)).

## Observability

Prometheus series are `arcade_bridge_*` on `-metrics-addr` (default
`[::]:9167`), covering lane, cache, announce, facade, and up-tunnel counters.
The same numbers appear as structured `log/slog` stats blocks every
`-stats-every` (default 60s). `/readyz` reports ready once the lanes are bound
and the retrieval plane is listening. See the
[metrics reference](docs/references/prometheusMetrics.md).

## Helm chart

A Kubernetes Helm chart is published from a dedicated chart repository:

- Repository: [`charts/arcade-bridge`](https://github.com/lightwebinc/charts/tree/main/charts/arcade-bridge)
- OCI: `helm install bridge oci://ghcr.io/lightwebinc/charts/arcade-bridge`

`config.advertise` and `config.kafka` are effectively required unless
`config.mode=sink`; setting `config.edgeIngress` turns the facade on. See the
chart README for the three-service shape and scaling notes.

## Layout

```
.
├── cmd/arcade-bridge/   # entrypoint: flags, wiring, per-class lane handlers, collector
├── msannounce/          # merkle-service announcement messages + Kafka producer
├── facade/              # Teranode-shaped POST /txs surface + EF hydration
├── uptunnel/            # bare EF stream to the fabric ingress, with failover
├── docs/                # architecture, configuration, metrics reference
├── Dockerfile
├── Makefile
└── .github/workflows/{ci,codeql,image-publish,release,vuln}.yml
```

## Dependencies

- [`github.com/lightwebinc/teranode-bridge`](https://github.com/lightwebinc/teranode-bridge): the shared landing-tier packages (`lanes`, `cache`, `registry`, `retrieval`, `hashid`, `tnwire`)
- [`github.com/lightwebinc/shard-common`](https://github.com/lightwebinc/shard-common): `objfmt`, the push object-frame codecs
- [`github.com/bsv-blockchain/go-sdk`](https://github.com/bsv-blockchain/go-sdk): transaction parsing and extended-format serialization
- [`github.com/twmb/franz-go`](https://github.com/twmb/franz-go): Kafka producer

Arcade's and merkle-service's modules are deliberately not linked; their
announcement wire shape is reproduced and pinned by contract-fixture tests.

## Releases

Releases and notes live on [GitHub Releases](https://github.com/lightwebinc/arcade-bridge/releases); there is no CHANGELOG.

## License

Apache 2.0. See [LICENSE](LICENSE).
