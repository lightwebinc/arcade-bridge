# arcade-bridge

[![CI](https://github.com/lightwebinc/arcade-bridge/actions/workflows/ci.yml/badge.svg)](https://github.com/lightwebinc/arcade-bridge/actions/workflows/ci.yml)
[![Go Reference](https://pkg.go.dev/badge/github.com/lightwebinc/arcade-bridge.svg)](https://pkg.go.dev/github.com/lightwebinc/arcade-bridge)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)

> Part of the [**BSV Layered Multicast**](https://github.com/lightwebinc/bsv-multicast) open-source project. See the main repository for the full architecture, design docs, and BRC specifications.

A landing-tier shim that runs an **unmodified
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

## Proof ingest: the delivery lanes feed merkle-service

merkle-service exists to pool the subtree and block firehose and turn it into
per-transaction proofs. Stock, it hears announcements over libp2p and fetches
the bytes from the announcing datahub across the network. Behind this bridge,
the same objects are already pushed to the landing machine, so the fetch never
leaves the box:

```text
  fabric ══push══▶ arcade-bridge
      lanes ─▶ cache ─┬─▶ announce ──▶ merkle-service   (hash, dataHubUrl)
                      └─▶ retrieval ◀─ fetch ── merkle-service
      merkle-service ─▶ STUMP / BLOCK_PROCESSED ─▶ Arcade
      Arcade ─▶ MINED + merkle path ─▶ clients
```

The announcements are merkle-service's own Kafka messages with `dataHubUrl`
pointing at the bridge's retrieval plane. merkle-service keeps its ordinary
announce, fetch, STUMP pipeline unchanged, and Arcade's transaction lifecycle
(`SEEN_ON_NETWORK`, `MINED`, compound BUMP construction) runs exactly as it
would against a real datahub.

## Broadcast: one submit reaches every miner

Arcade's stock propagation broadcasts by POSTing each batch to every datahub
in parallel: O(N) unicast. The **propagation facade** serves the same
Teranode-shaped `POST /txs` surface Arcade already speaks, and forwards one
copy up the consumer tunnel into the fabric's open transaction ingress. The
fabric fans it out to every miner-tier consumer:

```text
  Arcade propagation
      │ POST /txs                       ▲ per-tx verdicts
      ▼                                 │ (Teranode failure-list)
  arcade-bridge facade ────────────────┘
      parse ─▶ ensure EF ─▶ hydrate (recent cache / asset fallback)
      up-tunnel: one bare EF stream
      │
      ▼
  fabric ingress ─▶ miner A, miner B, ... miner N   (one copy, fanned)
```

The response contract is Teranode's own failure-list grammar, so Arcade's
per-transaction classifier, retry reaper, and wallet-visible rows behave
identically. The fabric is extended-format native at ingress: EF submissions
pass through byte for byte, standard-serialized ones are hydrated from the
facade's recent-submission cache (children usually chain onto parents the
facade just carried) with an optional asset-API fallback, and a parent nowhere
to be found fails that one transaction as `TX_MISSING_PARENT`, the verdict
Arcade already knows how to retry.

## Documentation

- [Architecture](docs/architecture.md): components, the announce and fetch contract, the facade contract, hydration, byte order, failure modes
- [Configuration](docs/configuration.md): every flag, defaults, deployment examples, reading the stats
- [BRC-143 Subtree Data](https://github.com/lightwebinc/bsv-multicast/blob/main/docs/brc-143-subtree-data.md) and [BRC-144 Block Frame](https://github.com/lightwebinc/bsv-multicast/blob/main/docs/brc-144-block-frame.md): what the delivery lanes carry
- [BRC-30 Transaction Extended Format](https://github.com/bsv-blockchain/BRCs/blob/master/transactions/0030.md): what the up-tunnel stream carries
- [Deliver once](docs/deliver-once.md): one crossing per landing site, and how a co-located Arcade + Teranode site fans locally

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

# Feed: announce received subtrees and blocks to merkle-service.
./arcade-bridge \
  -advertise 'http://192.0.2.10:9165' \
  -kafka     'localhost:19092'

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

The delivery lanes use the canonical `9143`/`9144` shared with teranode-bridge:
a landing site receives each object class once, on the same port, whichever
bridge terminates it. Two bridges cannot bind these on one host, which is
deliberate - it forecloses the wasteful two-slot / double-delivery topology. A
site that runs both stacks delivers over ONE slot into a small local tee; see
[docs/deliver-once.md](docs/deliver-once.md). `8725` outbound matches the
object plane's transaction class number.

## Observability

Prometheus series are `arcade_bridge_*` on `-metrics-addr` (default
`[::]:9167`), covering lane, cache, announce, facade, and up-tunnel counters.
The same numbers appear as structured `log/slog` stats blocks every
`-stats-every` (default 60s). `/readyz` reports ready once the lanes are bound
and the retrieval plane is listening.

## Layout

```
.
├── cmd/arcade-bridge/   # entrypoint: flags, wiring, per-class lane handlers
├── msannounce/          # merkle-service announcement messages + Kafka producer
├── facade/              # Teranode-shaped POST /txs surface + EF hydration
├── uptunnel/            # bare EF stream to the fabric ingress, with failover
└── .github/workflows/   # CI
```

## Dependencies

- [`github.com/lightwebinc/teranode-bridge`](https://github.com/lightwebinc/teranode-bridge): the shared landing-tier packages (`lanes`, `cache`, `retrieval`, `hashid`, `tnwire`)
- [`github.com/lightwebinc/shard-common`](https://github.com/lightwebinc/shard-common): `objfmt`, the push object-frame codecs
- [`github.com/bsv-blockchain/go-sdk`](https://github.com/bsv-blockchain/go-sdk): transaction parsing and extended-format serialization
- [`github.com/twmb/franz-go`](https://github.com/twmb/franz-go): Kafka producer

The bridge deliberately does not link Arcade's or merkle-service's modules.
The one contract it needs, two small JSON announcement messages, is reproduced
from their wire shape and pinned by contract-fixture tests, so a small shim
does not pull in another service's dependency tree.

## License

Apache 2.0. See [LICENSE](LICENSE).
