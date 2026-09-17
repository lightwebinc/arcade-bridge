package main

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"testing"
	"time"
)

// slowAnnouncer blocks until released, modelling a Kafka produce that is slow
// rather than failing — the condition that actually hurt, because a failure
// returns instantly and only a SLOW call holds the lane's read loop.
type slowAnnouncer struct {
	release chan struct{}
	mu      sync.Mutex
	calls   int
	err     error
}

func (s *slowAnnouncer) note() {
	s.mu.Lock()
	s.calls++
	s.mu.Unlock()
}
func (s *slowAnnouncer) n() int { s.mu.Lock(); defer s.mu.Unlock(); return s.calls }

func (s *slowAnnouncer) Subtree(ctx context.Context, _ string) error {
	s.note()
	select {
	case <-s.release:
	case <-ctx.Done():
		return ctx.Err()
	}
	return s.err
}

func (s *slowAnnouncer) Block(ctx context.Context, _ string, _ uint64, _, _ []byte) error {
	return s.Subtree(ctx, "")
}

// THE regression this whole change exists for: handleSubtree must return while
// the announce is still in flight. lanes.Lane calls Handle inline in its
// per-connection read loop, so a handler that waits on Kafka is a socket that
// is not being read — and the EDGE, not this process, pays for it: measured on
// devnet 2026-09-17, 6,016 queue_full sheds against 4,145 objects delivered.
//
// The assertion is deliberately about TIME rather than about the queue's
// internals: "the read loop is not blocked" is the property, and any future
// refactor that reintroduces blocking fails here regardless of how it is built.
func TestHandleSubtreeDoesNotBlockOnSlowAnnounce(t *testing.T) {
	objects, announced, log := testFixtures(t)
	ann := &slowAnnouncer{release: make(chan struct{})}
	q := newAnnounceQueue(context.Background(), 16, 1, log)
	defer func() { close(ann.release); q.close() }()

	done := make(chan struct{})
	go func() {
		defer close(done)
		if err := handleSubtree(context.Background(), subtreeFrame(t, 0x21),
			objects, announced, ann, q, log); err != nil {
			t.Errorf("handleSubtree: %v", err)
		}
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("handleSubtree blocked on a slow announce — the lane read loop is stalled " +
			"and the edge will shed this consumer's lane")
	}

	// It really did defer the work rather than skip it.
	deadline := time.Now().Add(2 * time.Second)
	for ann.n() == 0 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if ann.n() != 1 {
		t.Fatalf("announce calls = %d, want 1 — the announce must be deferred, not dropped", ann.n())
	}
}

// The object must be servable the instant the handler returns, even though the
// announce has not happened: the retrieval plane is what merkle-service pulls
// from, and caching after announcing would let a fetch race the store.
func TestObjectIsCachedBeforeTheAnnounceRuns(t *testing.T) {
	objects, announced, log := testFixtures(t)
	ann := &slowAnnouncer{release: make(chan struct{})}
	q := newAnnounceQueue(context.Background(), 16, 1, log)
	defer func() { close(ann.release); q.close() }()

	obj := subtreeFrame(t, 0x22)
	if err := handleSubtree(context.Background(), obj, objects, announced, ann, q, log); err != nil {
		t.Fatalf("handleSubtree: %v", err)
	}
	var key [32]byte
	copy(key[:], obj[:32])
	if _, _, ok := objects.Get(key); !ok {
		t.Fatal("object not retrievable while its announce is still queued — " +
			"merkle-service would 404 on the fetch that the announcement triggers")
	}
}

// A full queue must DROP and count, never block. Blocking would reintroduce the
// exact backpressure this type removes, and an unbounded queue would trade it
// for unbounded memory — a worse failure than the one being fixed.
func TestFullQueueDropsRatherThanBlocking(t *testing.T) {
	log := slog.New(slog.NewTextHandler(discard{}, nil))
	q := newAnnounceQueue(context.Background(), 1, 0, log) // 0 workers: nothing drains
	defer q.close()

	blocked := make(chan struct{})
	go func() {
		defer close(blocked)
		for i := 0; i < 50; i++ {
			q.submit(announceJob{what: "x", run: func(context.Context) error { return nil }})
		}
	}()
	select {
	case <-blocked:
	case <-time.After(2 * time.Second):
		t.Fatal("submit blocked on a full queue — that is the backpressure this type exists to remove")
	}
	if _, _, _, dropped, _, _ := q.stats(); dropped == 0 {
		t.Fatal("a full queue must COUNT its drops; silent loss here is indistinguishable " +
			"from the edge-side shedding it replaced")
	}
}

// A failed announce must not mark the object announced, or the redelivery that
// is supposed to retry it is suppressed forever. Same invariant the inline path
// has always had — it has to survive the move onto the worker.
func TestFailedAsyncAnnounceLeavesObjectUnannounced(t *testing.T) {
	objects, announced, log := testFixtures(t)
	boom := errors.New("kafka down")
	ann := &slowAnnouncer{release: make(chan struct{}), err: boom}
	close(ann.release) // return immediately, with the error
	q := newAnnounceQueue(context.Background(), 16, 1, log)

	obj := subtreeFrame(t, 0x23)
	if err := handleSubtree(context.Background(), obj, objects, announced, ann, q, log); err != nil {
		t.Fatalf("handleSubtree: %v", err)
	}
	q.close() // drains

	if _, _, failed, _, _, _ := q.stats(); failed != 1 {
		t.Fatalf("failed announces = %d, want 1", failed)
	}
	// Not marked => a redelivery announces again.
	ann2 := &slowAnnouncer{release: make(chan struct{})}
	close(ann2.release)
	q2 := newAnnounceQueue(context.Background(), 16, 1, log)
	if err := handleSubtree(context.Background(), obj, objects, announced, ann2, q2, log); err != nil {
		t.Fatalf("redelivery: %v", err)
	}
	q2.close()
	if ann2.n() != 1 {
		t.Fatalf("redelivery after a failed announce must re-announce, got %d", ann2.n())
	}
}

type discard struct{}

func (discard) Write(p []byte) (int, error) { return len(p), nil }
