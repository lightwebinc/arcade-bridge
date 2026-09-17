// arcade-bridge terminates consumer delivery lanes in front of an unmodified
// Arcade v2 + merkle-service stack.
//
// It is the app-tier sibling of teranode-bridge, and deliberately NOT that
// binary: teranode-bridge is the miner-tier landing bridge (submitter role,
// blockchain-notification reverse path, miner-gated up-tunnel publish), while
// an arcade operator is a plain delivery consumer — subtrees and blocks come
// DOWN the tunnel, transactions go UP on the open class, and nothing here
// holds miner entitlements. The shared machinery (lanes, cache, retrieval
// plane, hash discipline, BRC-144 wire conversion) is imported from
// teranode-bridge's promoted packages, never forked.
//
// Forward path (this binary):
//
//	subtree lane (BRC-143) ─┐
//	                        ├─ cache ── merkle-service Kafka announce
//	block lane   (BRC-144) ─┘      └── retrieval plane (/subtree, /block)
//
// merkle-service fetches announced bytes from the retrieval plane — already
// pushed, so the "fetch" never leaves the box — builds STUMPs, and drives
// arcade's SEEN/MINED lifecycle exactly as it would against a datahub.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/lightwebinc/shard-common/objfmt"
	"github.com/lightwebinc/teranode-bridge/cache"
	"github.com/lightwebinc/teranode-bridge/hashid"
	"github.com/lightwebinc/teranode-bridge/lanes"
	"github.com/lightwebinc/teranode-bridge/registry"
	"github.com/lightwebinc/teranode-bridge/retrieval"
	"github.com/lightwebinc/teranode-bridge/tnwire"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/lightwebinc/arcade-bridge/facade"
	"github.com/lightwebinc/arcade-bridge/msannounce"
	"github.com/lightwebinc/arcade-bridge/uptunnel"
)

// Version is stamped at build time via -ldflags "-X main.Version=...".
var Version = "dev"

func main() {
	var (
		// Canonical tunnel-facing delivery-lane ports, shared with teranode-bridge:
		// a landing site receives each object class once, on the same port,
		// whichever bridge terminates it. Two bridges cannot bind these on one
		// host - that is deliberate: it stops an operator falling into the
		// wasteful two-slot / double-delivery topology. A co-located site runs
		// ONE delivery slot into a lanefan tee (see docs/deliver-once.md) and the
		// bridges move to distinct LOOPBACK ports behind it.
		subtreeListen = flag.String("subtree-listen", "[::]:9143", "delivery lane: subtrees (BRC-143)")
		blockListen   = flag.String("block-listen", "[::]:9144", "delivery lane: blocks (BRC-144)")

		retrievalListen = flag.String("retrieval-listen", "[::]:9165", "retrieval plane listen address")
		apiPrefix       = flag.String("api-prefix", "/api/v1", "path prefix for the retrieval plane; must match what is announced")
		advertise       = flag.String("advertise", "", "base URL merkle-service should fetch from, e.g. http://192.0.2.10:9165 (required unless -mode sink). merkle-service appends /subtree/{hash} and /block/{hash} to <advertise><api-prefix>. Containers cannot dial loopback or v6-only addresses — use the host's IPv4 the containers can reach")

		kafkaBrokers = flag.String("kafka", "", "merkle-service Kafka bootstrap (comma-separated host:port; required unless -mode sink). Remember the EXTERNAL advertised-listener rule: the broker must advertise an address this process can dial back")
		subtreeTopic = flag.String("subtree-topic", "subtree", "merkle-service subtree topic")
		blockTopic   = flag.String("block-topic", "block", "merkle-service block topic")
		peerID       = flag.String("peer-id", "arcade-bridge", "peer label stamped on announcements. merkle-service keys its per-peer fetch-health breaker on it; it is never dialed. Keep it STABLE — a changing id resets breaker history")
		clientName   = flag.String("client-name", "arcade-bridge", "clientName stamped on announcements")

		cacheBytes = flag.Int64("cache-bytes", 1<<30, "object cache ceiling in bytes")

		announceQueueDepth = flag.Int("announce-queue", 4096, "pending merkle-service announcements held off the lane read loop. The lane calls Handle inline, so announcing synchronously stops the socket draining and the EDGE sheds the lane (measured: 6,016 shed vs 4,145 delivered). 0 disables the queue and announces inline — diagnostic only")
		announceWorkers    = flag.Int("announce-workers", 4, "workers draining the announce queue. >1 because a single Kafka produce is latency-bound, not CPU-bound; merkle-service dedups by hash so ordering across objects is not load-bearing")
		cacheTTL           = flag.Duration("cache-ttl", 30*time.Minute, "how long a pushed object stays fetchable. Must comfortably exceed merkle-service's worst-case Kafka consumer lag: a fetch after expiry is an honest 404 that its stale-announcement grace then has to excuse")
		maxObject          = flag.Int("max-object", 0, "per-object size ceiling (0 = codec default)")

		facadeListen = flag.String("facade-listen", "[::]:9166", "propagation facade listen address (POST /txs, /tx, GET /health); started only when -edge-ingress is set")
		edgeIngress  = flag.String("edge-ingress", "", "up-tunnel tx submit host(s), reachable only through the tunnel; comma-separated failover list — with a dual-homed tunnel, the side-A and side-B slot inners")
		edgeTxPort   = flag.Int("edge-tx-port", 8725, "edge ingress port for the bare EF tx stream (open class)")
		hydrateAsset = flag.String("hydrate-asset", "", "optional asset API base incl. prefix (e.g. http://192.0.2.10:20090/api/v1) used to fetch parents the recent-submission cache misses")

		mode        = flag.String("mode", "all", "all = receive + cache + serve + announce; sink = receive, count and serve only (no Kafka) — lane burn-in before the stack attaches")
		metricsAddr = flag.String("metrics-addr", "[::]:9167", "HTTP listener for /metrics, /healthz, /readyz (empty = off)")
		statsEvery  = flag.Duration("stats-every", time.Minute, "interval between stats lines (0 = off)")
	)
	flag.Parse()

	log := slog.New(slog.NewTextHandler(os.Stderr, nil))
	sink := *mode == "sink"
	if *mode != "all" && *mode != "sink" {
		log.Error("unknown -mode", "mode", *mode)
		os.Exit(2)
	}

	objects := cache.New(cache.Options{MaxBytes: *cacheBytes, TTL: *cacheTTL})
	// Which objects merkle-service has actually been TOLD about. Deliberately
	// not the cache: the cache holds bytes so a pull can be served, this holds
	// the announce verdict, and only a successful announce writes to it. Aged
	// alongside the cache, which is what makes a failed announce self-healing —
	// the object is announced again on the next redelivery instead of sitting
	// cached and invisible until it falls out of both.
	announced := registry.New(*cacheTTL, 1<<20)
	ret := retrieval.New(retrieval.Config{Listen: *retrievalListen, APIPrefix: *apiPrefix}, objects, objects, noTxs{}, log)

	var producer *msannounce.Producer
	// ann is the same producer behind the handlers' seam. It is assigned only
	// when one is built: a nil *msannounce.Producer stored in an interface is
	// NOT a nil interface, and the handlers read nil as sink mode.
	var ann announcer
	baseURL := ""
	if !sink {
		if *advertise == "" || *kafkaBrokers == "" {
			log.Error("-mode all requires -advertise and -kafka (use -mode sink for lane burn-in)")
			os.Exit(2)
		}
		baseURL = strings.TrimRight(*advertise, "/") + *apiPrefix
		var err error
		producer, err = msannounce.New(msannounce.Config{
			Brokers:      strings.Split(*kafkaBrokers, ","),
			SubtreeTopic: *subtreeTopic,
			BlockTopic:   *blockTopic,
			DataHubURL:   baseURL,
			PeerID:       *peerID,
			ClientName:   *clientName,
		}, log)
		if err != nil {
			log.Error("announce producer", "err", err)
			os.Exit(1)
		}
		ann = producer
		defer producer.Close()
		pingCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		if err := producer.Ping(pingCtx); err != nil {
			// Not fatal: compose ordering may bring Kafka up after us, and
			// produce retries surface per announcement.
			log.Warn("kafka not reachable yet", "err", err)
		}
		cancel()
	}

	// Declared here because the lane handlers below close over it; ASSIGNED
	// once the process context exists, and always before any lane Serves, so
	// no object can be handled before there is a drain for it. Nil when
	// disabled (-announce-queue=0) or in sink mode: the handlers then keep
	// their inline path.
	var annQ *announceQueue

	laneSet := []*lanes.Lane{
		{
			Name: "subtree", Class: objfmt.ClassSubtree, Addr: *subtreeListen, Log: log, MaxObject: *maxObject,
			SizeHistogram: true,
			Handle: func(ctx context.Context, obj []byte) error {
				return handleSubtree(ctx, obj, objects, announced, ann, annQ, log)
			},
		},
		{
			Name: "block", Class: objfmt.ClassBlock, Addr: *blockListen, Log: log, MaxObject: *maxObject,
			SizeHistogram: true,
			Handle: func(ctx context.Context, obj []byte) error {
				return handleBlock(ctx, obj, objects, announced, ann, annQ, log)
			},
		},
	}

	// Propagation facade: arcade's POST /txs surface, forwarding one bare EF
	// stream up the tunnel. Enabled by -edge-ingress — but never in sink
	// mode, whose whole point is touching nothing.
	var fac *facade.Server
	var up *uptunnel.Client
	if !sink && *edgeIngress != "" {
		up = &uptunnel.Client{Addrs: ingressAddrs(*edgeIngress, *edgeTxPort), Log: log}
		defer up.Close()
		recent := cache.New(cache.Options{MaxBytes: 256 << 20, TTL: *cacheTTL})
		var fetch facade.ParentSource
		if *hydrateAsset != "" {
			fetch = facade.AssetParentSource(*hydrateAsset, nil)
		}
		fac = facade.New(facade.Config{Listen: *facadeListen}, up, recent, fetch, log)
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	if ann != nil && *announceQueueDepth > 0 {
		annQ = newAnnounceQueue(ctx, *announceQueueDepth, *announceWorkers, log)
		// Closed BEFORE the producer (defer order is LIFO and the producer's
		// Close was deferred earlier), so in-flight announcements still have a
		// live Kafka client to finish on rather than failing at shutdown.
		defer annQ.close()
		log.Info("announce queue armed", "depth", *announceQueueDepth, "workers", *announceWorkers)
	}

	prometheus.MustRegister(newCollector(laneSet, objects, producer, fac, up, annQ))

	var wg sync.WaitGroup
	for _, l := range laneSet {
		wg.Add(1)
		go func() { defer wg.Done(); logIfErr(log, "lane "+l.Name, l.Serve(ctx)) }()
	}
	wg.Add(1)
	go func() { defer wg.Done(); logIfErr(log, "retrieval", ret.Serve(ctx)) }()
	if fac != nil {
		wg.Add(1)
		go func() { defer wg.Done(); logIfErr(log, "facade", fac.Serve(ctx)) }()
	}

	if *metricsAddr != "" {
		mux := http.NewServeMux()
		mux.Handle("/metrics", promhttp.Handler())
		mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
		mux.HandleFunc("/readyz", func(w http.ResponseWriter, _ *http.Request) {
			for _, l := range laneSet {
				if !l.Bound() {
					http.Error(w, "lane "+l.Name+" not bound", http.StatusServiceUnavailable)
					return
				}
			}
			if !ret.Listening() {
				http.Error(w, "retrieval not listening", http.StatusServiceUnavailable)
				return
			}
			w.WriteHeader(http.StatusOK)
		})
		srv := &http.Server{Addr: *metricsAddr, Handler: mux}
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
				log.Error("metrics server", "err", err)
			}
		}()
		go func() { <-ctx.Done(); _ = srv.Close() }()
	}

	if *statsEvery > 0 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			t := time.NewTicker(*statsEvery)
			defer t.Stop()
			for {
				select {
				case <-ctx.Done():
					return
				case <-t.C:
					logStats(log, laneSet, objects, producer, fac, up)
				}
			}
		}()
	}

	log.Info("arcade-bridge up", "version", Version, "mode", *mode, "subtree", *subtreeListen, "block", *blockListen,
		"retrieval", *retrievalListen, "announce_base", baseURL, "facade", facadeState(fac, *facadeListen))
	<-ctx.Done()
	wg.Wait()
}

// announcer is the announce seam the lane handlers publish through:
// *msannounce.Producer in production, a stub in tests. A nil announcer is sink
// mode — receive, cache and serve, tell no one.
type announcer interface {
	Subtree(ctx context.Context, displayHash string) error
	Block(ctx context.Context, displayHash string, height uint64, header, coinbase []byte) error
}

// handleSubtree stores the frame and announces it. Store before announcing:
// merkle-service fetches the moment the announcement lands, and a fetch that
// races the store would 404 and charge our peer health.
//
// The cache is not the dedup gate; `announced` is. An object whose announce
// FAILED stays cached — the retrieval plane must still be able to serve a pull
// for it — but is not recorded as announced, so the next redelivery announces
// it again. Gating on the cache instead would read that redelivery as a
// duplicate and leave merkle-service permanently unaware of an object we hold.
func handleSubtree(ctx context.Context, obj []byte, objects *cache.Cache, announced *registry.Registry,
	producer announcer, q *announceQueue, log *slog.Logger) error {

	if len(obj) < objfmt.SubtreeHeaderSize {
		return fmt.Errorf("subtree frame too short: %d bytes", len(obj))
	}
	root, err := hashid.FromWire(obj[:32])
	if err != nil {
		return err
	}
	objects.Put(cache.Key(root), "subtree", obj)
	if _, done := announced.Lookup(registry.Key(root)); done {
		// Lookup, not Mark, because Mark would INSERT an unannounced object.
		// Mark on the way out only refreshes the entry, so the suppression
		// window tracks the cache's TTL exactly as it did when the cache was
		// the gate: an object the edge keeps redelivering is never re-announced.
		announced.Mark(registry.Key(root), registry.Delivered)
		log.Debug("subtree already announced, not re-announced", "root", root.Display())
		return nil
	}
	log.Info("subtree received", "root", root.Display(), "bytes", len(obj))
	if producer == nil {
		announced.Mark(registry.Key(root), registry.Delivered)
		return nil
	}
	// The object is CACHED above and therefore already retrievable; only the
	// announce is deferred, so merkle-service can never be told about an object
	// we cannot serve. Returning here is what lets the lane read the next
	// object instead of waiting on Kafka — see announceQueue.
	if q != nil {
		q.submit(announceJob{
			what: "subtree " + root.Display(),
			run: func(c context.Context) error {
				if err := producer.Subtree(c, root.Display()); err != nil {
					return err
				}
				announced.Mark(registry.Key(root), registry.Delivered)
				return nil
			},
		})
		return nil
	}
	if err := producer.Subtree(ctx, root.Display()); err != nil {
		return err
	}
	announced.Mark(registry.Key(root), registry.Delivered)
	return nil
}

// handleBlock stores the frame and announces it with the inline fields
// merkle-service's BlockMessage carries. Height, header and coinbase are
// extracted through the tested-lossless tnwire round trip rather than a
// third parser of the BRC-144 layout.
//
// Announce-failure handling is handleSubtree's: cached either way, recorded as
// announced only on success.
func handleBlock(ctx context.Context, obj []byte, objects *cache.Cache, announced *registry.Registry,
	producer announcer, q *announceQueue, log *slog.Logger) error {

	if len(obj) < objfmt.BlockPrefixSize {
		return fmt.Errorf("block frame too short: %d bytes", len(obj))
	}
	id := hashid.DoubleSHA256(obj[:80])
	objects.Put(cache.Key(id), "block", obj)
	if _, done := announced.Lookup(registry.Key(id)); done {
		announced.Mark(registry.Key(id), registry.Delivered) // refresh; see handleSubtree
		log.Debug("block already announced, not re-announced", "hash", id.Display())
		return nil
	}
	if producer == nil {
		log.Info("block received", "hash", id.Display(), "bytes", len(obj))
		announced.Mark(registry.Key(id), registry.Delivered)
		return nil
	}
	tn, err := tnwire.ToTeranode(obj)
	if err != nil {
		return fmt.Errorf("block %s: %w", id.Display(), err)
	}
	blk, err := tnwire.FromTeranode(tn)
	if err != nil {
		return fmt.Errorf("block %s: %w", id.Display(), err)
	}
	log.Info("block received", "hash", id.Display(), "height", blk.Height, "bytes", len(obj))
	if q != nil {
		q.submit(announceJob{
			what: "block " + id.Display(),
			run: func(c context.Context) error {
				if err := producer.Block(c, id.Display(), blk.Height, blk.Header, blk.Coinbase); err != nil {
					return err
				}
				announced.Mark(registry.Key(id), registry.Delivered)
				return nil
			},
		})
		return nil
	}
	if err := producer.Block(ctx, id.Display(), blk.Height, blk.Header, blk.Coinbase); err != nil {
		return err
	}
	announced.Mark(registry.Key(id), registry.Delivered)
	return nil
}

// noTxs satisfies retrieval.TxStore for a bridge with no tx lane: every
// member lookup is an honest miss (404, never 200-empty).
type noTxs struct{}

func (noTxs) Get(cache.Key) ([]byte, string, bool) { return nil, "", false }

func logIfErr(log *slog.Logger, what string, err error) {
	if err != nil && !errors.Is(err, context.Canceled) {
		log.Error(what, "err", err)
	}
}

func facadeState(fac *facade.Server, listen string) string {
	if fac == nil {
		return "off"
	}
	return listen
}

// ingressAddrs joins each comma-separated host with the port.
func ingressAddrs(hosts string, port int) []string {
	var out []string
	for _, h := range strings.Split(hosts, ",") {
		h = strings.TrimSpace(h)
		if h == "" {
			continue
		}
		out = append(out, net.JoinHostPort(h, strconv.Itoa(port)))
	}
	return out
}

func logStats(log *slog.Logger, laneSet []*lanes.Lane, objects *cache.Cache, producer *msannounce.Producer, fac *facade.Server, up *uptunnel.Client) {
	for _, l := range laneSet {
		s := l.Stats()
		log.Info("lane stats", "lane", s.Name, "conns", s.Conns, "active", s.Active,
			"objects", s.Objects, "bytes", s.Bytes, "errors", s.Errors, "dropped", s.Dropped,
			"rejected", s.Rejected)
	}
	cs := objects.Stats()
	log.Info("cache stats", "entries", cs.Entries, "bytes", cs.Bytes, "hits", cs.Hits,
		"misses", cs.Misses, "evicted", cs.Evicted, "expired", cs.Expired)
	if producer != nil {
		ps := producer.Stats()
		log.Info("announce stats", "subtrees", ps.Subtrees, "blocks", ps.Blocks, "failures", ps.Failures)
	}
	if fac != nil {
		fs := fac.Stats()
		log.Info("facade stats", "batches", fs.Batches, "accepted", fs.Accepted, "hydrated", fs.Hydrated,
			"missing_parent", fs.MissingParent, "malformed", fs.Malformed, "submit_failures", fs.SubmitFailures)
	}
	if up != nil {
		us := up.Stats()
		log.Info("up-tunnel stats", "sent", us.Sent, "bytes", us.Bytes, "failures", us.Failures, "redials", us.Redials)
	}
}
