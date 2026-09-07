# Architecture

`arcade-bridge` sits on the landing machine between a multicast delivery
fabric and an unmodified Arcade v2 + merkle-service deployment. It has two
independent halves:

- **Feed** (fabric to stack): terminate the subtree and block delivery lanes,
  cache the objects, announce them to merkle-service, and serve the resulting
  fetches locally.
- **Facade** (stack to fabric): serve the datahub submission surface Arcade's
  propagation already speaks, and forward accepted transactions up the tunnel
  as one extended-format stream.

Both halves reuse teranode-bridge's public packages. Nothing in either half
requires a change to Arcade, merkle-service, or the fabric.

## Why the same announce-shim trick works here

merkle-service is built around announce plus fetch: its p2p client hears a
subtree or block announcement, publishes `{hash, dataHubUrl}` onto its own
Kafka topics, and its processors fetch the bytes from that URL
(`GET {dataHubUrl}/subtree/{hash}` for the raw transaction-id list,
`GET {dataHubUrl}/block/{hash}` for the block).

The bridge already holds those bytes: they were pushed to it over the
delivery lanes. So it publishes the same Kafka messages itself, with
`dataHubUrl` naming its own retrieval plane, and the wide-area transfer
becomes a push while merkle-service keeps its ordinary pipeline:

```
 fabric ══ BRC-143 / BRC-144 push ══▶ lanes ─▶ cache
                                                │ 1. store first
                                                ▼
                                     msannounce ─▶ Kafka topics subtree / block
                                                        │ 2. {hash, dataHubUrl}
                                                        ▼
                                                 merkle-service
                                                        │ 3. GET /subtree/{hash}
                                                        ▼    GET /block/{hash}
                                     retrieval plane (serves from the cache)
                                                        │
                                                        ▼
                4. STUMPs and BLOCK_PROCESSED callbacks to Arcade, as always
```

Ordering is load-bearing: the object is cached **before** the announcement is
produced, because merkle-service fetches the moment the announcement lands
and a fetch that races the store would 404 and charge the bridge's peer
health.

The announcement messages mirror merkle-service's decoder exactly and are
pinned by contract-fixture tests in `msannounce`. A block announcement
carries height, header, and coinbase inline; those fields are extracted
through `tnwire`'s round trip (BRC-144 to Teranode wire and back), which the
teranode-bridge test suite asserts is lossless, rather than through a third
parser of the block layout.

### Peer identity and peer health

Announcements carry a stable synthetic `peerId` and `clientName`.
merkle-service keys its per-peer fetch-health breaker on the peer id; it
never dials it as a libp2p identity. Keep the id stable across restarts: a
changing id resets breaker history. Because every announcement names the same
retrieval URL, the breaker sees the bridge as one peer, so the cache TTL
(`-cache-ttl`, default 30 minutes) should comfortably exceed merkle-service's
worst-case Kafka consumer lag. A fetch after expiry is an honest 404 that its
stale-announcement grace then has to excuse.

### Duplicates

The fabric deduplicates identical objects, but an edge redial can redeliver
one. The cache is the dedup gate: an object already held is stored again
(refreshing its TTL) but not re-announced. Downstream, merkle-service's own
processing is idempotent per hash, so a duplicate announcement is harmless
either way.

## The facade

Arcade's propagation broadcasts a batch by concatenating raw transactions and
POSTing them to `{endpoint}/txs` at every configured or discovered datahub.
The facade serves that exact surface:

- `POST /txs`: concatenated transactions, split by walking each one's own
  structure (the stream is self-delimiting).
- `POST /tx`: the same grammar with one element.
- `GET /health`: any HTTP response restores a tripped Arcade endpoint
  breaker.

### Response contract

The contract is Teranode's, not an invention of this bridge, because Arcade
parses it per transaction:

- **HTTP 200**: the whole batch is accepted. The body carries nothing.
- **Any other status with a body starting `Failed to process transactions:`**
  followed by one `<NAME> (<num>): [ProcessTransaction][<txid>] <message>`
  line per failure: a per-transaction verdict list. A transaction absent from
  the list was accepted. The facade emits `TX_MISSING_PARENT (34)` for an
  unresolvable parent and `TX_INVALID (31)` for a malformed stream.
- **A non-200 without that body shape** is pure infrastructure: no
  per-transaction vote, Arcade requeues. The facade answers a bodyless 503
  when the up-tunnel itself is down, because that is an endpoint condition,
  not a verdict about any transaction.

Honouring the grammar exactly is what lets Arcade's classifier, its durable
retry reaper, and its wallet-visible rows behave identically against the
facade and against a real datahub.

### Extended-format hydration

The fabric is extended-format native at ingress (BRC-30: each input carries
its previous output's satoshis and locking script), so the facade guarantees
EF on the way up:

```
 incoming tx ─▶ parse (go-sdk reads EF and standard alike)
                  │
                  ├─ every input has source data ──────────▶ emit EF, forward
                  │
                  └─ an input lacks source data:
                       1. recent-submission cache (parents this facade
                          just carried; children usually chain onto them)
                       2. -hydrate-asset fallback (GET /tx/{parent},
                          with bounded retry on 429)
                       3. neither: TX_MISSING_PARENT verdict for that
                          transaction only
```

Every accepted transaction's EF bytes are stored in the recent-submission
cache keyed by txid, which is what makes step 1 the fast path for chained
workloads.

### Up-tunnel stream

Accepted transactions are written to the fabric's open transaction ingress as
one bare EF stream over a long-lived TCP connection (`uptunnel`). The stream
is self-delimiting, so a partial write desynchronises the reader; any write
error closes the connection and the next submit redials. Several ingress
addresses may be configured (a dual-homed tunnel's per-side inners): a failed
submit advances to the next address before retrying once, so a side flip does
not strand submissions.

## Byte order

Hashes exist in two orders: wire order inside frames and lane objects,
display order everywhere a human or an API sees them (announcement hashes,
URL path segments, verdict lines). Every conversion goes through
teranode-bridge's `hashid` package. A subtree is identified by the root hash
carried in its frame header; a block by the double-SHA256 of its 80-byte
header, exactly as the chain identifies it.

## Failure modes

| Failure | Behaviour |
| --- | --- |
| Kafka unreachable at startup | warn and continue; produces surface per announcement and are retried by the lane redial path |
| Kafka down mid-run | the announcement fails, the lane handler returns the error, the edge redials and redelivers |
| retrieval fetch for an expired object | honest 404, never an empty 200; merkle-service classifies by announcement age |
| up-tunnel down | facade answers a bodyless 503 (infrastructure, not verdict); Arcade requeues |
| malformed transaction in a batch | one `TX_INVALID` line, remaining stream abandoned (self-delimiting streams cannot resync); Arcade narrows the chunk and resubmits |
| unresolvable parent | `TX_MISSING_PARENT` verdict for that transaction only; the rest of the batch proceeds |

## Package layout

```
cmd/arcade-bridge/   wiring: flags, lane handlers, collector, stats loop
msannounce/          SubtreeMessage / BlockMessage + synchronous Kafka producer
facade/              HTTP surface, verdict grammar, EF hydration, ParentSource
uptunnel/            long-lived bare EF stream with address failover
```

Imported from teranode-bridge: `lanes` (per-class TCP listeners over bare
object streams), `cache` (hash-keyed, TTL'd, copy-on-put), `retrieval` (the
asset-style pull surface), `hashid`, `tnwire`, `encode`. Those packages are
the documented extension seams of that repository; this bridge is a consumer
of them, not a fork.
