# Header feeder: making the chain-tip path fabric-native

Arcade tracks the chain tip through an embedded
[go-chaintracks](https://github.com/bsv-blockchain/go-chaintracks) instance,
fed over libp2p / a block-headers source. That is the one part of the Arcade
stack that does not ride the multicast fabric. The fabric already carries a
**BRC-135 header lane** (bare 80-byte headers; the `overlay-headers` consumer
and txmint's `headersink` already receive and verify against it), so a feeder
that pushed those headers into chaintracks would close the last non-fabric
dependency and make the tip (and the nLockTime finality gate that depends on
it) fabric-native.

This document records what is and isn't possible against go-chaintracks
`v1.2.11` (the version Arcade pins), and the buildable design.

## Findings (go-chaintracks v1.2.11, read from source)

1. **The `Chaintracks` interface is read-only for headers.** It exposes
   `GetHeight`, `GetTip`, `GetHeaderByHeight`, `GetHeaderByHash`, `GetHeaders`,
   `GetNetwork`, `Subscribe`, `SubscribeReorg`, with no header-ingest method
   (`chaintracks/interface.go`).

2. **The `ChainManager` implementation does have ingest, as library calls.**
   `ChainManager.SetChainTip(ctx, branchHeaders)` is the real tip-advancer:
   it updates `byHeight`/`byHash`, sets the tip to the last header of the
   branch, prunes orphans, publishes the tip event to subscribers, and writes
   header files. It **trusts the branch**: no PoW or linkage validation
   inside `SetChainTip` (`chainmanager/loader.go`).
   `ChainManager.AddHeader(header)` is index-only (stores by hash, does not
   advance the tip) and is not sufficient on its own
   (`chainmanager/manager.go`).

3. **The chaintracks-server HTTP API is read-only.** Its routes are all GET
   (`/v2/tip`, `/v2/height/{h}`, `/v2/hash/{h}`, `/v2/headers`, and the legacy
   `findChainTip*`). There is **no remote header-ingest endpoint**; headers
   enter a running server only through the ChainManager's own P2P subscription
   and `SyncFromRemoteTip`.

**Consequence:** a feeder cannot push headers into a stock chaintracks-server
over HTTP. Ingest is a library-level call on an in-process ChainManager.

## Design (recommended: embed the ChainManager, no upstream change)

Build a small `chaintracks-feeder` service:

```
BRC-135 header lane ─▶ feeder
                        ├─ parse 80-byte header, double-SHA256 → hash
                        ├─ link prevHash → known chain → height = parent.Height+1
                        ├─ ChainManager.SetChainTip([{header, height, hash}])
                        └─ serve the read-only chaintracks HTTP API
                                        │
                        Arcade  chaintracks.mode=remote ─▶ feeder
```

- The feeder runs go-chaintracks' `ChainManager` **as a library** (not the
  stock server binary), reads the header lane, and calls `SetChainTip` to
  advance the tip. It serves the ordinary read-only chaintracks HTTP surface
  (the ChainManager already has the route handlers), so Arcade points
  `chaintracks.mode=remote` at the feeder and is otherwise unchanged.
- **Height derivation is the one gap.** BRC-135 carries only the 80-byte
  header, not the height. The feeder derives height by linking `prevHash` to
  the ChainManager's known chain (`GetHeaderByHash(prevHash).Height + 1`), and
  bootstraps the first header from a checkpoint or a one-time
  `SyncFromRemoteTip` before switching to the fabric feed. txmint's
  `headersink` already solves the same problem (it re-anchors the header chain
  through the node after a lane gap) and is the reference implementation for
  the lane reader + re-anchor logic.
- **Delivery side.** Either the feeder joins the header lane directly (it is a
  consumer SDA like any other), or arcade-bridge gains a `-header-listen` lane
  that receives BRC-135 headers and hands them to the feeder. The lane itself
  is already proven on the fabric (the `overlay-headers` consumer is live and
  headersink verifies BUMPs against it).

## Alternative (cleaner long-term, needs upstream)

Ask BSVA to add a **header-ingest endpoint** to chaintracks-server (a POST
that feeds `SetChainTip`) or an explicit **external-feed mode** that disables
the P2P source. Then no embedded ChainManager is needed and the feeder becomes
a thin lane-to-HTTP shim. This is the tidier boundary but carries an upstream
dependency; the embedded-library path above ships without it.

## Scope caveat (why this is a testnet/mainnet item)

On the **regtest** lab, Arcade **force-disables** its embedded chaintracks and
the nLockTime/BIP113 finality gate (both are nil under regtest by design).
So the feeder's value (a fabric-native tip and a working finality gate) is a
**testnet/mainnet** concern; the regtest lab cannot validate Arcade *consuming*
the feeder end to end. The feeder mechanism itself (a ChainManager tip
advancing from fabric-delivered headers) is verifiable standalone, and the
header-lane delivery is already proven on the lab. Build this alongside the
first testnet Arcade, not against regtest.

## Effort

~1 to 1.5 days for the embedded-library path: the lane reader and re-anchor
logic already exist in `headersink`; the new work is the `SetChainTip` glue,
the height-linkage bootstrap, and serving the read-only HTTP surface.
