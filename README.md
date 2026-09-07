# arcade-bridge

A landing-tier shim that terminates consumer delivery lanes in front of an
**unmodified** [Arcade v2](https://github.com/bsv-blockchain/arcade) +
[merkle-service](https://github.com/bsv-blockchain/merkle-service) stack, so
BSVA's broadcaster tier runs with a multicast delivery fabric as its
transport.

It is the app-tier sibling of
[teranode-bridge](https://github.com/lightwebinc/teranode-bridge) — and
deliberately not that binary. teranode-bridge is the **miner-tier** landing
shim: it holds the submitter role, subscribes to blockchain notifications,
gates origin by mine-tag, and publishes the cluster's subtrees and blocks up a
miner-entitled tunnel. An arcade operator is **not a miner**: subtrees and
blocks arrive *down* the tunnel, transactions go *up* on the open class, and
nothing here carries miner entitlements. The shared machinery — lane
termination, the copy-on-put object cache, the retrieval plane, hash-order
discipline, BRC-144 wire conversion — is imported from teranode-bridge's
top-level packages, never forked.

## Forward path (built)

```
subtree lane (BRC-143) ─┐
                        ├─ cache ──┬─ merkle-service Kafka announce (JSON)
block lane   (BRC-144) ─┘          └─ retrieval plane  /subtree/{h}  /block/{h}
```

merkle-service learns of each object as `{hash, dataHubUrl}` on its own Kafka
topics and fetches the bytes from this bridge's retrieval plane — already
pushed, so the "fetch" never leaves the box. Its ordinary
announce → fetch → STUMP → `BLOCK_PROCESSED` pipeline then drives arcade's
`SEEN_ON_NETWORK`/`MINED` lifecycle unchanged. Block announcements carry
height, header and coinbase inline, extracted through `tnwire`'s
tested-lossless round trip.

`-mode sink` receives, counts and serves without announcing — lane burn-in
before the stack attaches.

## Broadcast path (built)

The **propagation facade** (`-facade-listen`, enabled by `-edge-ingress`)
serves the Teranode-shaped submission surface arcade's stock propagation
already speaks — `POST /txs` with concatenated raw transactions, `POST /tx`,
`GET /health` — and streams accepted transactions up-tunnel to the fabric's
open tx ingress as one bare extended-format stream, collapsing arcade's
per-datahub broadcast into a single submit that reaches every miner-tier
consumer. The response contract is Teranode's own failure-list grammar, so
arcade's per-tx classifier, reaper and wallet-visible rows behave exactly as
against a real datahub.

The fabric is EF-native at ingress: extended-format submissions pass through
byte-for-byte; standard-serialized ones are hydrated (source data from the
facade's recent-submission cache — children chaining onto parents the facade
just carried — then an optional `-hydrate-asset` fallback), and a parent
nowhere to be found fails that one transaction as `TX_MISSING_PARENT`, the
verdict arcade already knows how to retry.

Metrics prefix `arcade_bridge_`; `/metrics`, `/healthz`, `/readyz` on
`-metrics-addr`.

## Defaults

| Flag | Default | |
| --- | --- | --- |
| `-subtree-listen` | `[::]:9163` | BRC-143 delivery lane |
| `-block-listen` | `[::]:9164` | BRC-144 delivery lane |
| `-retrieval-listen` | `[::]:9165` | with `-api-prefix /api/v1` |
| `-advertise` | — | base URL merkle-service fetches from (containers cannot dial loopback) |
| `-kafka` | — | merkle-service's brokers; mind the advertised-listener rule |
| `-subtree-topic` / `-block-topic` | `subtree` / `block` | merkle-service's topic names |
| `-metrics-addr` | `[::]:9167` | |

Licensed Apache-2.0.
