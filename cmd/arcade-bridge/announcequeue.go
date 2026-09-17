package main

import (
	"context"
	"log/slog"
	"sync"
	"sync/atomic"
)

// announceQueue decouples the merkle-service announce from the delivery lane's
// read loop.
//
// WHY THIS EXISTS. lanes.Lane calls Handle INLINE in its per-connection read
// loop (teranode-bridge lanes.go), so for as long as a handler runs, nothing is
// reading the socket. handleSubtree used to announce synchronously, and a
// Kafka produce that takes a second is a second of not reading — the receive
// window fills, the edge's write blocks, and at its deadline the edge counts an
// i/o timeout and eventually sheds the object.
//
// That is not a theoretical coupling. Measured on devnet 2026-09-17, consumer
// arcade-us: 6,016 queue_full sheds against 4,145 subtree objects delivered —
// the edge shedding the majority of a lane because THIS process was not
// draining it. Every diagnostic on the edge pointed at a healthy fabric and a
// slow subscriber, which was correct and useless: the cause was downstream
// latency arriving as TCP backpressure.
//
// The fix is to make the read loop's work bounded and local: cache the object
// (in-memory, microseconds) and hand the announce to a worker. The socket then
// drains at memory speed regardless of how slow Kafka is.
//
// BOUNDED ON PURPOSE. An unbounded queue would convert downstream stall into
// unbounded memory here, which is a worse failure than the one being fixed.
// When it fills the drop is explicit, counted and logged HERE — the honest
// place — instead of appearing as loss on the edge's socket where nothing can
// attribute it. This mirrors sdapool's split between a shed (the subscriber is
// behind) and a drop (there is nowhere to put it).
type announceQueue struct {
	ch   chan announceJob
	log  *slog.Logger
	wg   sync.WaitGroup
	once sync.Once

	submitted atomic.Uint64
	done      atomic.Uint64
	failed    atomic.Uint64
	dropped   atomic.Uint64
}

// announceJob is one announce plus whatever must happen only if it SUCCEEDS.
// The success half is carried with the job because the registry Mark is what
// records "merkle-service knows about this"; running it on a failed announce
// would suppress the re-announce that a redelivery is supposed to trigger.
type announceJob struct {
	what string
	run  func(context.Context) error
}

// newAnnounceQueue starts n workers draining a queue of the given depth.
func newAnnounceQueue(ctx context.Context, depth, workers int, log *slog.Logger) *announceQueue {
	if depth <= 0 {
		depth = 1024
	}
	if workers <= 0 {
		workers = 1
	}
	q := &announceQueue{ch: make(chan announceJob, depth), log: log}
	for i := 0; i < workers; i++ {
		q.wg.Add(1)
		go q.worker(ctx)
	}
	return q
}

func (q *announceQueue) worker(ctx context.Context) {
	defer q.wg.Done()
	for j := range q.ch {
		// The job's own context is the PROCESS lifetime, not the lane
		// connection's: an announce must not be cancelled because the edge
		// happened to redial between accepting the object and announcing it.
		if err := j.run(ctx); err != nil {
			q.failed.Add(1)
			q.log.Error("announce failed", "what", j.what, "err", err)
			continue
		}
		q.done.Add(1)
	}
}

// submit queues an announce, or counts a drop if the queue is full. It never
// blocks: blocking here would reintroduce exactly the backpressure this type
// exists to remove.
func (q *announceQueue) submit(j announceJob) {
	q.submitted.Add(1)
	select {
	case q.ch <- j:
	default:
		n := q.dropped.Add(1)
		// Rate-limited by powers of two: a stalled downstream would otherwise
		// produce one line per object and bury the first one, which is the
		// only one that says when the stall began.
		if n&(n-1) == 0 {
			q.log.Error("announce queue full, object cached but NOT announced",
				"what", j.what, "dropped_total", n, "depth", cap(q.ch),
				"hint", "merkle-service is not keeping up; the object is retrievable but unadvertised")
		}
	}
}

// close stops the workers after the queue drains.
func (q *announceQueue) close() {
	q.once.Do(func() { close(q.ch) })
	q.wg.Wait()
}

// stats reports counters for the metrics collector.
func (q *announceQueue) stats() (submitted, done, failed, dropped, depth, capacity uint64) {
	return q.submitted.Load(), q.done.Load(), q.failed.Load(), q.dropped.Load(),
		uint64(len(q.ch)), uint64(cap(q.ch))
}
