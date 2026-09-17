package main

import (
	"github.com/lightwebinc/teranode-bridge/cache"
	"github.com/lightwebinc/teranode-bridge/lanes"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/lightwebinc/arcade-bridge/facade"
	"github.com/lightwebinc/arcade-bridge/msannounce"
	"github.com/lightwebinc/arcade-bridge/uptunnel"
)

// collector exposes the lane, cache and announce snapshots as Prometheus
// series under the arcade_bridge_ prefix. Snapshot-based: the sources keep
// their own atomics and the collector reads them at scrape time.
type collector struct {
	laneSet  []*lanes.Lane
	objects  *cache.Cache
	producer *msannounce.Producer
	fac      *facade.Server
	up       *uptunnel.Client
	annQ     *announceQueue

	laneObjects, laneBytes, laneErrors            *prometheus.Desc
	laneDropped, laneRejected                     *prometheus.Desc
	cacheEntries, cacheBytes                      *prometheus.Desc
	announceTotal, announceFailures               *prometheus.Desc
	annQDepth, annQCapacity                       *prometheus.Desc
	annQDone, annQFailed, annQDropped             *prometheus.Desc
	facadeTxs, facadeBatches                      *prometheus.Desc
	uptunnelSent, uptunnelBytes, uptunnelFailures *prometheus.Desc
}

func newCollector(laneSet []*lanes.Lane, objects *cache.Cache, producer *msannounce.Producer, fac *facade.Server, up *uptunnel.Client, annQ *announceQueue) *collector {
	lane := []string{"lane"}
	class := []string{"class"}
	result := []string{"result"}
	return &collector{
		laneSet: laneSet, objects: objects, producer: producer, fac: fac, up: up, annQ: annQ,
		// The announce queue is the seam between "we accepted the object" and
		// "merkle-service was told". Depth climbing means the downstream is
		// slower than the lane; dropped>0 means objects are cached and
		// retrievable but UNADVERTISED, which nothing else here would show.
		annQDepth:        prometheus.NewDesc("arcade_bridge_announce_queue_depth", "Announcements waiting to be produced.", nil, nil),
		annQCapacity:     prometheus.NewDesc("arcade_bridge_announce_queue_capacity", "Announce queue depth ceiling.", nil, nil),
		annQDone:         prometheus.NewDesc("arcade_bridge_announce_queue_done_total", "Announcements produced from the queue.", nil, nil),
		annQFailed:       prometheus.NewDesc("arcade_bridge_announce_queue_failed_total", "Announcements that errored; the object stays unannounced and a redelivery retries.", nil, nil),
		annQDropped:      prometheus.NewDesc("arcade_bridge_announce_queue_dropped_total", "Announcements dropped because the queue was full; object cached but NOT advertised.", nil, nil),
		facadeTxs:        prometheus.NewDesc("arcade_bridge_facade_txs_total", "Facade transaction outcomes.", result, nil),
		facadeBatches:    prometheus.NewDesc("arcade_bridge_facade_batches_total", "Facade submit batches received.", nil, nil),
		uptunnelSent:     prometheus.NewDesc("arcade_bridge_uptunnel_sent_total", "Transactions streamed up-tunnel.", nil, nil),
		uptunnelBytes:    prometheus.NewDesc("arcade_bridge_uptunnel_bytes_total", "Bytes streamed up-tunnel.", nil, nil),
		uptunnelFailures: prometheus.NewDesc("arcade_bridge_uptunnel_failures_total", "Up-tunnel write/dial failures.", nil, nil),
		laneObjects:      prometheus.NewDesc("arcade_bridge_lane_objects_total", "Objects received per delivery lane.", lane, nil),
		laneBytes:        prometheus.NewDesc("arcade_bridge_lane_bytes_total", "Bytes received per delivery lane.", lane, nil),
		laneErrors:       prometheus.NewDesc("arcade_bridge_lane_errors_total", "Objects whose lane handler failed: the announcement did not produce, or the frame could not be parsed for its inline block fields. The object stays cached and the connection is kept; it is announced again on the next redelivery. Framing faults are NOT counted here - see lane_connections_dropped_total.", lane, nil),
		laneDropped:      prometheus.NewDesc("arcade_bridge_lane_connections_dropped_total", "Connections dropped on a framing fault. Bare streams have no resync point, so every byte after a codec fault - or an object over the -max-object ceiling - is suspect and the connection must go.", lane, nil),
		laneRejected:     prometheus.NewDesc("arcade_bridge_lane_objects_rejected_total", "Well-framed objects refused on lane format policy. No arcade-bridge lane enforces one today, so a non-zero value is unexpected; it is exported for parity with teranode-bridge and so a future policy needs no dashboard change.", lane, nil),
		cacheEntries:     prometheus.NewDesc("arcade_bridge_cache_entries", "Objects currently cached.", nil, nil),
		cacheBytes:       prometheus.NewDesc("arcade_bridge_cache_bytes", "Bytes currently cached.", nil, nil),
		announceTotal:    prometheus.NewDesc("arcade_bridge_announce_total", "Announcements published to merkle-service.", class, nil),
		announceFailures: prometheus.NewDesc("arcade_bridge_announce_failures_total", "Failed announcement publishes.", nil, nil),
	}
}

func (c *collector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.annQDepth
	ch <- c.annQCapacity
	ch <- c.annQDone
	ch <- c.annQFailed
	ch <- c.annQDropped
	ch <- c.laneObjects
	ch <- c.laneBytes
	ch <- c.laneErrors
	ch <- c.laneDropped
	ch <- c.laneRejected
	ch <- c.cacheEntries
	ch <- c.cacheBytes
	ch <- c.announceTotal
	ch <- c.announceFailures
	ch <- c.facadeTxs
	ch <- c.facadeBatches
	ch <- c.uptunnelSent
	ch <- c.uptunnelBytes
	ch <- c.uptunnelFailures
}

func (c *collector) Collect(ch chan<- prometheus.Metric) {
	if c.annQ != nil {
		_, done, failed, dropped, depth, capacity := c.annQ.stats()
		ch <- prometheus.MustNewConstMetric(c.annQDepth, prometheus.GaugeValue, float64(depth))
		ch <- prometheus.MustNewConstMetric(c.annQCapacity, prometheus.GaugeValue, float64(capacity))
		ch <- prometheus.MustNewConstMetric(c.annQDone, prometheus.CounterValue, float64(done))
		ch <- prometheus.MustNewConstMetric(c.annQFailed, prometheus.CounterValue, float64(failed))
		ch <- prometheus.MustNewConstMetric(c.annQDropped, prometheus.CounterValue, float64(dropped))
	}
	for _, l := range c.laneSet {
		s := l.Stats()
		ch <- prometheus.MustNewConstMetric(c.laneObjects, prometheus.CounterValue, float64(s.Objects), s.Name)
		ch <- prometheus.MustNewConstMetric(c.laneBytes, prometheus.CounterValue, float64(s.Bytes), s.Name)
		ch <- prometheus.MustNewConstMetric(c.laneErrors, prometheus.CounterValue, float64(s.Errors), s.Name)
		ch <- prometheus.MustNewConstMetric(c.laneDropped, prometheus.CounterValue, float64(s.Dropped), s.Name)
		ch <- prometheus.MustNewConstMetric(c.laneRejected, prometheus.CounterValue, float64(s.Rejected), s.Name)
	}
	cs := c.objects.Stats()
	ch <- prometheus.MustNewConstMetric(c.cacheEntries, prometheus.GaugeValue, float64(cs.Entries))
	ch <- prometheus.MustNewConstMetric(c.cacheBytes, prometheus.GaugeValue, float64(cs.Bytes))
	if c.producer != nil {
		ps := c.producer.Stats()
		ch <- prometheus.MustNewConstMetric(c.announceTotal, prometheus.CounterValue, float64(ps.Subtrees), "subtree")
		ch <- prometheus.MustNewConstMetric(c.announceTotal, prometheus.CounterValue, float64(ps.Blocks), "block")
		ch <- prometheus.MustNewConstMetric(c.announceFailures, prometheus.CounterValue, float64(ps.Failures))
	}
	if c.fac != nil {
		fs := c.fac.Stats()
		ch <- prometheus.MustNewConstMetric(c.facadeTxs, prometheus.CounterValue, float64(fs.Accepted), "accepted")
		ch <- prometheus.MustNewConstMetric(c.facadeTxs, prometheus.CounterValue, float64(fs.Hydrated), "hydrated")
		ch <- prometheus.MustNewConstMetric(c.facadeTxs, prometheus.CounterValue, float64(fs.MissingParent), "missing_parent")
		ch <- prometheus.MustNewConstMetric(c.facadeTxs, prometheus.CounterValue, float64(fs.Malformed), "malformed")
		ch <- prometheus.MustNewConstMetric(c.facadeBatches, prometheus.CounterValue, float64(fs.Batches))
	}
	if c.up != nil {
		us := c.up.Stats()
		ch <- prometheus.MustNewConstMetric(c.uptunnelSent, prometheus.CounterValue, float64(us.Sent))
		ch <- prometheus.MustNewConstMetric(c.uptunnelBytes, prometheus.CounterValue, float64(us.Bytes))
		ch <- prometheus.MustNewConstMetric(c.uptunnelFailures, prometheus.CounterValue, float64(us.Failures))
	}
}
