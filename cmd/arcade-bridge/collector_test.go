package main

import (
	"context"
	"encoding/binary"
	"io"
	"log/slog"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/lightwebinc/shard-common/objfmt"
	"github.com/lightwebinc/teranode-bridge/cache"
	"github.com/lightwebinc/teranode-bridge/lanes"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
)

func gather(t *testing.T, laneSet []*lanes.Lane, objects *cache.Cache) map[string]*dto.MetricFamily {
	t.Helper()
	reg := prometheus.NewRegistry()
	reg.MustRegister(newCollector(laneSet, objects, nil, nil, nil, nil))
	mfs, err := reg.Gather()
	if err != nil {
		t.Fatalf("gather: %v", err)
	}
	out := make(map[string]*dto.MetricFamily, len(mfs))
	for _, mf := range mfs {
		out[mf.GetName()] = mf
	}
	return out
}

// TestLaneSeriesExported pins the lane series the collector exports. The
// framing-fault counter was missing entirely, so the only evidence of a
// malformed stream was a log line.
func TestLaneSeriesExported(t *testing.T) {
	laneSet := []*lanes.Lane{{Name: "subtree", Class: objfmt.ClassSubtree}}
	got := gather(t, laneSet, cache.New(cache.Options{MaxBytes: 1 << 20, TTL: time.Minute}))

	for _, name := range []string{
		"arcade_bridge_lane_objects_total",
		"arcade_bridge_lane_bytes_total",
		"arcade_bridge_lane_errors_total",
		"arcade_bridge_lane_connections_dropped_total",
		"arcade_bridge_lane_objects_rejected_total",
	} {
		if got[name] == nil {
			t.Errorf("series %s is not exported", name)
		}
	}

	// The errors counter is per-object HANDLER failures, not stream errors;
	// the help string said the latter and sent readers looking in the wrong
	// place for a framing fault.
	if h := got["arcade_bridge_lane_errors_total"].GetHelp(); strings.Contains(h, "Stream errors") {
		t.Errorf("lane_errors help still describes stream errors: %q", h)
	}
	// The rejected counter cannot move today; the help has to say so, or a
	// permanently flat series reads as a healthy one.
	if h := got["arcade_bridge_lane_objects_rejected_total"].GetHelp(); !strings.Contains(h, "No arcade-bridge lane enforces one today") {
		t.Errorf("lane_objects_rejected help does not say no lane enforces a policy: %q", h)
	}
}

// TestLaneDroppedCounterMoves drives a real framing fault through a real lane
// and reads it back off the collector. Exporting the descriptor is not the
// same as wiring it to the counter that moves.
func TestLaneDroppedCounterMoves(t *testing.T) {
	l := &lanes.Lane{
		Name: "subtree", Class: objfmt.ClassSubtree, Addr: "127.0.0.1:0",
		Log:    slog.New(slog.NewTextHandler(io.Discard, nil)),
		Handle: func(context.Context, []byte) error { return nil },
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- l.Serve(ctx) }()

	deadline := time.Now().Add(5 * time.Second)
	for !l.Bound() {
		if time.Now().After(deadline) {
			t.Fatal("lane never bound")
		}
		time.Sleep(time.Millisecond)
	}

	conn, err := net.Dial("tcp", l.ListenerAddr().String())
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	// A BRC-143 header whose node count cannot be addressed: malformed, and a
	// bare stream has no resync point, so the lane must drop the connection.
	hdr := make([]byte, objfmt.SubtreeHeaderSize)
	binary.BigEndian.PutUint64(hdr[32:40], ^uint64(0))
	if _, err := conn.Write(hdr); err != nil {
		t.Fatalf("write: %v", err)
	}
	// The lane closing on us is the signal that it processed the fault.
	_ = conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	if _, err := conn.Read(make([]byte, 1)); err == nil {
		t.Fatal("lane kept a connection open after a framing fault")
	}
	_ = conn.Close()

	got := gather(t, []*lanes.Lane{l}, cache.New(cache.Options{MaxBytes: 1 << 20, TTL: time.Minute}))
	mf := got["arcade_bridge_lane_connections_dropped_total"]
	if mf == nil || len(mf.GetMetric()) != 1 {
		t.Fatalf("dropped series missing: %v", mf)
	}
	if v := mf.GetMetric()[0].GetCounter().GetValue(); v != 1 {
		t.Fatalf("framing fault must count one dropped connection, got %v", v)
	}

	cancel()
	if err := <-done; err != nil {
		t.Fatalf("serve: %v", err)
	}
}
