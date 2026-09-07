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

	laneObjects, laneBytes, laneErrors, laneRejected *prometheus.Desc
	cacheEntries, cacheBytes                         *prometheus.Desc
	announceTotal, announceFailures                  *prometheus.Desc
	facadeTxs, facadeBatches                         *prometheus.Desc
	uptunnelSent, uptunnelBytes, uptunnelFailures    *prometheus.Desc
}

func newCollector(laneSet []*lanes.Lane, objects *cache.Cache, producer *msannounce.Producer, fac *facade.Server, up *uptunnel.Client) *collector {
	lane := []string{"lane"}
	class := []string{"class"}
	result := []string{"result"}
	return &collector{
		laneSet: laneSet, objects: objects, producer: producer, fac: fac, up: up,
		facadeTxs:        prometheus.NewDesc("arcade_bridge_facade_txs_total", "Facade transaction outcomes.", result, nil),
		facadeBatches:    prometheus.NewDesc("arcade_bridge_facade_batches_total", "Facade submit batches received.", nil, nil),
		uptunnelSent:     prometheus.NewDesc("arcade_bridge_uptunnel_sent_total", "Transactions streamed up-tunnel.", nil, nil),
		uptunnelBytes:    prometheus.NewDesc("arcade_bridge_uptunnel_bytes_total", "Bytes streamed up-tunnel.", nil, nil),
		uptunnelFailures: prometheus.NewDesc("arcade_bridge_uptunnel_failures_total", "Up-tunnel write/dial failures.", nil, nil),
		laneObjects:      prometheus.NewDesc("arcade_bridge_lane_objects_total", "Objects received per delivery lane.", lane, nil),
		laneBytes:        prometheus.NewDesc("arcade_bridge_lane_bytes_total", "Bytes received per delivery lane.", lane, nil),
		laneErrors:       prometheus.NewDesc("arcade_bridge_lane_errors_total", "Stream errors per delivery lane.", lane, nil),
		laneRejected:     prometheus.NewDesc("arcade_bridge_lane_objects_rejected_total", "Objects rejected per delivery lane.", lane, nil),
		cacheEntries:     prometheus.NewDesc("arcade_bridge_cache_entries", "Objects currently cached.", nil, nil),
		cacheBytes:       prometheus.NewDesc("arcade_bridge_cache_bytes", "Bytes currently cached.", nil, nil),
		announceTotal:    prometheus.NewDesc("arcade_bridge_announce_total", "Announcements published to merkle-service.", class, nil),
		announceFailures: prometheus.NewDesc("arcade_bridge_announce_failures_total", "Failed announcement publishes.", nil, nil),
	}
}

func (c *collector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.laneObjects
	ch <- c.laneBytes
	ch <- c.laneErrors
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
	for _, l := range c.laneSet {
		s := l.Stats()
		ch <- prometheus.MustNewConstMetric(c.laneObjects, prometheus.CounterValue, float64(s.Objects), s.Name)
		ch <- prometheus.MustNewConstMetric(c.laneBytes, prometheus.CounterValue, float64(s.Bytes), s.Name)
		ch <- prometheus.MustNewConstMetric(c.laneErrors, prometheus.CounterValue, float64(s.Errors), s.Name)
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
