# Configuration

Every setting is a flag. There is no config file and no environment
variables, so a unit file or container command line is the complete record of
how a bridge runs.

## Modes

| Mode | What runs |
| --- | --- |
| `-mode all` (default) | lanes, cache, retrieval plane, merkle-service announce; plus the facade when `-edge-ingress` is set |
| `-mode sink` | lanes, cache, retrieval plane only: receive, parse, count, serve. No Kafka, no facade. Use for lane burn-in before the Arcade stack attaches |

`-mode all` requires `-advertise` and `-kafka` and exits without them.

## Delivery lanes

| Flag | Default | |
| --- | --- | --- |
| `-subtree-listen` | `[::]:9163` | subtree lane, bare BRC-143 push frames |
| `-block-listen` | `[::]:9164` | block lane, bare BRC-144 push frames |
| `-max-object` | `0` | per-object size ceiling; `0` means the codec default |

Each lane is a dedicated TCP listener carrying exactly one object class. The
stream is bare: no length prefix, no type tag. Objects are delimited by
walking their own structure, so a malformed object is unrecoverable for that
connection and the only correct response is to drop it and let the edge
redial.

## Cache and retrieval plane

| Flag | Default | |
| --- | --- | --- |
| `-cache-bytes` | `1073741824` (1 GiB) | object cache ceiling |
| `-cache-ttl` | `30m` | how long a pushed object stays fetchable |
| `-retrieval-listen` | `[::]:9165` | retrieval plane listen address |
| `-api-prefix` | `/api/v1` | path prefix; must match what is announced |

Size `-cache-ttl` to comfortably exceed merkle-service's worst-case Kafka
consumer lag: a fetch after expiry is an honest 404 that its
stale-announcement grace then has to excuse.

## Announcing to merkle-service

| Flag | Default | |
| --- | --- | --- |
| `-advertise` | (required in `-mode all`) | base URL merkle-service fetches from, for example `http://192.0.2.10:9165`. The bridge appends `-api-prefix`. Containers cannot dial loopback or a v6-only address; advertise an address they can reach |
| `-kafka` | (required in `-mode all`) | merkle-service's Kafka bootstrap, comma-separated `host:port` |
| `-subtree-topic` | `subtree` | merkle-service's subtree topic |
| `-block-topic` | `block` | merkle-service's block topic |
| `-peer-id` | `arcade-bridge` | peer label on announcements; merkle-service keys its fetch-health breaker on it, so keep it stable |
| `-client-name` | `arcade-bridge` | clientName on announcements |

Kafka reminder: the broker must advertise an address this process can dial
back. A Redpanda whose external listener advertises `localhost` is reachable
only from the same machine.

merkle-service must allow private fetch addresses when the retrieval plane is
on RFC1918/ULA space (`DATAHUB_ALLOW_PRIVATE_IPS=true` in its environment);
its SSRF guard refuses them by default.

## Facade and up-tunnel

| Flag | Default | |
| --- | --- | --- |
| `-facade-listen` | `[::]:9166` | facade listen address; the facade starts only when `-edge-ingress` is set, and never in `-mode sink` |
| `-edge-ingress` | (off) | up-tunnel submit host(s), comma-separated failover list. With a dual-homed tunnel these are the side-A and side-B slot inners |
| `-edge-tx-port` | `8725` | fabric ingress port for the bare EF stream |
| `-hydrate-asset` | (off) | asset API base including its prefix, for example `http://192.0.2.20:20090/api/v1`, used to fetch parents the recent-submission cache misses |

Point Arcade at the facade by listing it in `datahub_urls`:

```yaml
datahub_urls:
  - http://192.0.2.10:9166
```

Arcade probes `GET /health` to restore a tripped endpoint breaker; any HTTP
response from the facade satisfies it.

## Observability

| Flag | Default | |
| --- | --- | --- |
| `-metrics-addr` | `[::]:9167` | `/metrics`, `/healthz`, `/readyz`; empty disables |
| `-stats-every` | `1m` | interval between structured stats log blocks; `0` disables |

`/readyz` reports ready once every lane is bound and the retrieval plane is
listening. Prometheus series:

| Series | Labels | Meaning |
| --- | --- | --- |
| `arcade_bridge_lane_objects_total` | `lane` | objects received per delivery lane |
| `arcade_bridge_lane_bytes_total` | `lane` | bytes received per delivery lane |
| `arcade_bridge_lane_errors_total` | `lane` | stream errors (connection dropped, malformed object) |
| `arcade_bridge_lane_objects_rejected_total` | `lane` | objects rejected by a lane handler |
| `arcade_bridge_cache_entries` / `_cache_bytes` | | current cache occupancy |
| `arcade_bridge_announce_total` | `class` | announcements published to merkle-service |
| `arcade_bridge_announce_failures_total` | | failed announcement publishes |
| `arcade_bridge_facade_batches_total` | | facade submit batches received |
| `arcade_bridge_facade_txs_total` | `result` | per-transaction outcomes: `accepted`, `hydrated`, `missing_parent`, `malformed` |
| `arcade_bridge_uptunnel_sent_total` / `_bytes_total` | | transactions and bytes streamed up-tunnel |
| `arcade_bridge_uptunnel_failures_total` | | up-tunnel dial and write failures |

The signals worth watching: `announce_failures_total` climbing means
merkle-service is not hearing about objects the bridge holds;
`facade_txs_total{result="missing_parent"}` climbing under a chained workload
means the recent cache is undersized or `-hydrate-asset` is unset;
`uptunnel_failures_total` climbing with `sent_total` flat means the fabric
ingress is unreachable and Arcade is seeing 503s.

## Deployment examples

Lane burn-in, before the stack attaches:

```bash
arcade-bridge -mode sink -stats-every 10s
```

Feed only (proof ingest through the fabric, broadcast still direct):

```bash
arcade-bridge \
  -advertise 'http://192.0.2.10:9165' \
  -kafka     'localhost:19092'
```

Full posture, feed plus facade:

```bash
arcade-bridge \
  -advertise     'http://192.0.2.10:9165' \
  -kafka         'localhost:19092' \
  -edge-ingress  '2001:db8:59::a,2001:db8:59::b' \
  -hydrate-asset 'http://192.0.2.20:20090/api/v1'
```

## Lane numbers

The lane defaults (`9163`/`9164`) deliberately sit clear of teranode-bridge's
(`9143`/`9144`) so both shims can share a host. The outbound port (`8725`)
matches the object plane's transaction class number: the facade submits on
the open class, which is the whole point. A bridge for a non-mining consumer
has no business on the miner-gated object-submit ports, and none is
configured here.
