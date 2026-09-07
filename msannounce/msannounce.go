// Package msannounce publishes merkle-service-shaped announcements for
// objects this bridge holds, pointing them at its own retrieval plane.
//
// This is the announce-shim trick aimed at a different consumer: where
// teranode-bridge/announce injects Teranode's protobuf Kafka messages so an
// unmodified cluster pulls pushed bytes from the landing tier, this package
// injects merkle-service's JSON messages so an unmodified merkle-service
// (the ingest half of BSVA's Arcade v2 broadcaster stack) does the same. The
// wide-area transfer becomes a push; merkle-service keeps its ordinary
// announce → fetch → STUMP path unchanged.
//
// Message shapes mirror merkle-service internal/kafka/messages.go: an
// announcement carries the object's display-order hash plus a DataHubURL to
// fetch content from, and a block announcement additionally carries height,
// header and coinbase inline. Reprocess-only fields (Override*,
// ReprocessNonce, AttemptCount) are never set by a live announcer and are
// omitted here.
package msannounce

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log/slog"
	"math"
	"sync/atomic"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

// SubtreeMessage is merkle-service's subtree announcement.
type SubtreeMessage struct {
	Hash       string `json:"hash"`
	DataHubURL string `json:"dataHubUrl"`
	PeerID     string `json:"peerId"`
	ClientName string `json:"clientName"`

	// AnnouncedAtUnixMs lets merkle-service classify a datahub 404 on a
	// message that sat in Kafka as its own consumer lag rather than a lying
	// peer. Stamp it at publish time; the bridge's cache TTL should exceed
	// any realistic lag or fresh announcements will 404 honestly.
	AnnouncedAtUnixMs int64 `json:"announcedAtUnixMs,omitempty"`
}

// BlockMessage is merkle-service's block announcement.
type BlockMessage struct {
	Hash       string `json:"hash"`
	Height     uint32 `json:"height"`
	Header     string `json:"header"`   // 80 bytes, hex
	Coinbase   string `json:"coinbase"` // full coinbase transaction, hex
	DataHubURL string `json:"dataHubUrl"`
	PeerID     string `json:"peerId"`
	ClientName string `json:"clientName"`
}

// Config describes the producer.
type Config struct {
	Brokers      []string
	SubtreeTopic string // merkle-service kafka.subtreeTopic (its default: "subtree")
	BlockTopic   string // merkle-service kafka.blockTopic (its default: "block")

	// DataHubURL is the full base merkle-service should fetch from — this
	// bridge's retrieval plane including any API prefix. merkle-service
	// appends /subtree/{hash} and /block/{hash} verbatim.
	DataHubURL string

	// PeerID is a stable synthetic label. merkle-service keys peer health on
	// it; it is never dialed as a libp2p identity. ClientName is the
	// human-readable counterpart carried beside it.
	PeerID     string
	ClientName string
}

// Stats is a point-in-time snapshot of producer counters.
type Stats struct {
	Subtrees, Blocks, Failures uint64
}

// Producer publishes announcements. Publishes are synchronous: an
// announcement that cannot be produced is an error the lane handler sees,
// because merkle-service cannot re-observe a one-shot announcement.
type Producer struct {
	cl  *kgo.Client
	cfg Config
	log *slog.Logger

	subtrees, blocks, failures atomic.Uint64

	// now is a seam for tests.
	now func() time.Time
}

// New builds the producer and verifies the brokers are reachable.
func New(cfg Config, log *slog.Logger) (*Producer, error) {
	if cfg.DataHubURL == "" {
		return nil, fmt.Errorf("msannounce: DataHubURL is required")
	}
	if cfg.SubtreeTopic == "" || cfg.BlockTopic == "" {
		return nil, fmt.Errorf("msannounce: subtree and block topics are required")
	}
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(cfg.Brokers...),
		kgo.ProducerBatchCompression(kgo.SnappyCompression(), kgo.NoCompression()),
	)
	if err != nil {
		return nil, fmt.Errorf("msannounce: %w", err)
	}
	return &Producer{cl: cl, cfg: cfg, log: log, now: time.Now}, nil
}

// Ping verifies broker reachability.
func (p *Producer) Ping(ctx context.Context) error { return p.cl.Ping(ctx) }

// Close flushes and releases the client.
func (p *Producer) Close() {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = p.cl.Flush(ctx)
	p.cl.Close()
}

// Stats returns a snapshot of the counters.
func (p *Producer) Stats() Stats {
	return Stats{Subtrees: p.subtrees.Load(), Blocks: p.blocks.Load(), Failures: p.failures.Load()}
}

// Subtree announces a subtree by its display-order root hash.
func (p *Producer) Subtree(ctx context.Context, displayHash string) error {
	msg := SubtreeMessage{
		Hash:              displayHash,
		DataHubURL:        p.cfg.DataHubURL,
		PeerID:            p.cfg.PeerID,
		ClientName:        p.cfg.ClientName,
		AnnouncedAtUnixMs: p.now().UnixMilli(),
	}
	if err := p.produce(ctx, p.cfg.SubtreeTopic, displayHash, msg); err != nil {
		return err
	}
	p.subtrees.Add(1)
	return nil
}

// Block announces a block by its display-order hash. Header and coinbase are
// raw bytes as carried in the BRC-144 frame; height is validated into the
// message's uint32.
func (p *Producer) Block(ctx context.Context, displayHash string, height uint64, header, coinbase []byte) error {
	if height > math.MaxUint32 {
		return fmt.Errorf("msannounce: height %d exceeds uint32", height)
	}
	msg := BlockMessage{
		Hash:       displayHash,
		Height:     uint32(height),
		Header:     hex.EncodeToString(header),
		Coinbase:   hex.EncodeToString(coinbase),
		DataHubURL: p.cfg.DataHubURL,
		PeerID:     p.cfg.PeerID,
		ClientName: p.cfg.ClientName,
	}
	if err := p.produce(ctx, p.cfg.BlockTopic, displayHash, msg); err != nil {
		return err
	}
	p.blocks.Add(1)
	return nil
}

func (p *Producer) produce(ctx context.Context, topic, key string, msg any) error {
	body, err := json.Marshal(msg)
	if err != nil {
		p.failures.Add(1)
		return err
	}
	rec := &kgo.Record{Topic: topic, Key: []byte(key), Value: body}
	if err := p.cl.ProduceSync(ctx, rec).FirstErr(); err != nil {
		p.failures.Add(1)
		return fmt.Errorf("msannounce: produce %s: %w", topic, err)
	}
	return nil
}
