# arcade-bridge Prometheus Metrics Reference

Every series is `arcade_bridge_*`, served on `-metrics-addr` (default
`[::]:9167`). All values are read from the bridge's internal counters at scrape
time, so they reset to zero on restart. The format mirrors
[teranode-bridge's metrics reference](https://github.com/lightwebinc/teranode-bridge/blob/main/docs/references/prometheusMetrics.md).

## Metric types

| Type | Description |
|------|-------------|
| Counter | Monotonic; only increases or resets to zero on restart. Always suffixed `_total`. |
| Gauge | A value that can go up and down. |

## Endpoints

| Path | Purpose |
|------|---------|
| `GET /metrics` | Prometheus exposition |
| `GET /healthz` | Bare liveness |
| `GET /readyz` | Ready once every delivery lane is bound and the retrieval plane is listening |

## Delivery lanes

| Metric Name | Type | Description |
|-------------|------|-------------|
| `arcade_bridge_lane_objects_total` | Counter | Objects received, by `lane` (`subtree`, `block`). |
| `arcade_bridge_lane_bytes_total` | Counter | Bytes received, by `lane`. |
| `arcade_bridge_lane_errors_total` | Counter | Objects whose handler failed, by `lane`: the announcement did not produce, or the frame could not be parsed for its inline block fields. The object stays cached and the connection is kept; the next redelivery announces it again. Framing faults are not counted here. |
| `arcade_bridge_lane_connections_dropped_total` | Counter | Connections dropped on a framing fault (a malformed object, or one over `-max-object`), by `lane`. A bare stream has no resync point, so the connection must go. |
| `arcade_bridge_lane_objects_rejected_total` | Counter | Well-framed objects refused on lane format policy, by `lane`. No lane enforces one today, so non-zero is unexpected; exported for parity with teranode-bridge. |

## Cache

| Metric Name | Type | Description |
|-------------|------|-------------|
| `arcade_bridge_cache_entries` | Gauge | Objects currently cached. |
| `arcade_bridge_cache_bytes` | Gauge | Bytes currently cached. |

## Announcements

Absent in `-mode sink`. The `announce_queue_*` series appear only when `-announce-queue` is above zero.

| Metric Name | Type | Description |
|-------------|------|-------------|
| `arcade_bridge_announce_total` | Counter | Announcements published to merkle-service, by `class` (`subtree`, `block`). |
| `arcade_bridge_announce_failures_total` | Counter | Failed announcement publishes. |
| `arcade_bridge_announce_queue_depth` | Gauge | Announcements waiting to be produced. Climbing means merkle-service's Kafka is slower than the lanes. |
| `arcade_bridge_announce_queue_capacity` | Gauge | Announce queue depth ceiling (`-announce-queue`). |
| `arcade_bridge_announce_queue_done_total` | Counter | Announcements produced from the queue. |
| `arcade_bridge_announce_queue_failed_total` | Counter | Announcements that errored; the object stays unannounced and a redelivery retries. |
| `arcade_bridge_announce_queue_dropped_total` | Counter | Announcements dropped because the queue was full. The object is cached and retrievable but **not advertised**; nothing else shows this. |

## Propagation facade

Present only when the facade is enabled (`-edge-ingress` set).

| Metric Name | Type | Description |
|-------------|------|-------------|
| `arcade_bridge_facade_txs_total` | Counter | Transaction outcomes, by `result` (`accepted`, `hydrated`, `missing_parent`, `malformed`). |
| `arcade_bridge_facade_batches_total` | Counter | Submit batches received. |

## Up-tunnel

Present only when the facade is enabled.

| Metric Name | Type | Description |
|-------------|------|-------------|
| `arcade_bridge_uptunnel_sent_total` | Counter | Transactions streamed up-tunnel. |
| `arcade_bridge_uptunnel_bytes_total` | Counter | Bytes streamed up-tunnel. |
| `arcade_bridge_uptunnel_failures_total` | Counter | Up-tunnel write or dial failures. |

## Alerting

These ship as a `PrometheusRule` in the
[Helm chart](https://github.com/lightwebinc/charts/tree/main/charts/arcade-bridge).

| Alert | Expression | Why |
|-------|-----------|-----|
| `ArcadeBridgeAnnounceFailures` | `increase(arcade_bridge_announce_failures_total[10m]) > 0` for 10m | merkle-service is not being told about delivered objects. |
| `ArcadeBridgeLaneErrors` | `increase(arcade_bridge_lane_errors_total[10m]) > 0` for 30m | Objects are cached but not announced. |
| `ArcadeBridgeLaneDropped` | `increase(arcade_bridge_lane_connections_dropped_total[15m]) > 0` for 15m | The delivery stream is malformed or oversized. |
| `ArcadeBridgeUptunnelStalled` | `rate(arcade_bridge_uptunnel_failures_total[5m]) > 0 and rate(arcade_bridge_uptunnel_sent_total[5m]) == 0` for 5m | Broadcasts are not reaching the fabric. |
| `ArcadeBridgeMissingParents` | `rate(arcade_bridge_facade_txs_total{result="missing_parent"}[15m]) > 0` for 15m | Standard-format submissions cannot be hydrated. |
