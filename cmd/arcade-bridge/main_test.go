package main

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/lightwebinc/shard-common/objfmt"
	"github.com/lightwebinc/teranode-bridge/cache"
	"github.com/lightwebinc/teranode-bridge/registry"
)

// stubAnnouncer stands in for *msannounce.Producer. err is what the next
// publish returns, so a test can fail an announce and then let it succeed.
type stubAnnouncer struct {
	err      error
	subtrees []string
	blocks   []string
}

func (s *stubAnnouncer) Subtree(_ context.Context, hash string) error {
	s.subtrees = append(s.subtrees, hash)
	return s.err
}

func (s *stubAnnouncer) Block(_ context.Context, hash string, _ uint64, _, _ []byte) error {
	s.blocks = append(s.blocks, hash)
	return s.err
}

func testFixtures(t *testing.T) (*cache.Cache, *registry.Registry, *slog.Logger) {
	t.Helper()
	return cache.New(cache.Options{MaxBytes: 1 << 20, TTL: time.Minute}),
		registry.New(time.Minute, 1024),
		slog.New(slog.NewTextHandler(io.Discard, nil))
}

// subtreeFrame is a minimal BRC-143 push frame: a 32-byte merkle root and a
// zero node count. handleSubtree reads the root and nothing else.
func subtreeFrame(t *testing.T, fill byte) []byte {
	t.Helper()
	obj := append(bytes.Repeat([]byte{fill}, 32), make([]byte, 8)...)
	if n, err := objfmt.SubtreeSize(obj); err != nil || n != len(obj) {
		t.Fatalf("fixture is not a valid BRC-143 frame: size=%d err=%v len=%d", n, err, len(obj))
	}
	return obj
}

// blockFrame is a minimal but self-consistent BRC-144 push frame. handleBlock
// transcodes it through tnwire for the inline announcement fields, so unlike
// the subtree fixture this one has to survive a real parse.
func blockFrame(t *testing.T, fill byte) []byte {
	t.Helper()
	var cb []byte
	cb = binary.LittleEndian.AppendUint32(cb, 1) // version
	cb = append(cb, 0x01)                        // input count
	cb = append(cb, make([]byte, 32)...)         // prev txid
	cb = binary.LittleEndian.AppendUint32(cb, 0xffffffff)
	cb = append(cb, 0x02, 0x51, 0x51) // script len + OP_1 OP_1
	cb = binary.LittleEndian.AppendUint32(cb, 0xffffffff)
	cb = append(cb, 0x01)                                 // output count
	cb = binary.LittleEndian.AppendUint64(cb, 5000000000) // satoshis
	cb = append(cb, 0x01, 0x51)                           // script len + OP_1
	cb = binary.LittleEndian.AppendUint32(cb, 0)          // locktime

	root := bytes.Repeat([]byte{0x77}, 32)
	obj := make([]byte, 0, 256)
	obj = append(obj, bytes.Repeat([]byte{fill}, 80)...) // header: fill varies the block hash
	obj = binary.BigEndian.AppendUint64(obj, 1)          // transaction count
	obj = binary.BigEndian.AppendUint64(obj, uint64(len(cb)))
	obj = binary.BigEndian.AppendUint64(obj, 1) // subtree count
	obj = append(obj, root...)
	obj = append(obj, cb...)
	obj = binary.BigEndian.AppendUint64(obj, 800000) // height
	obj = binary.BigEndian.AppendUint64(obj, 0)      // coinbase BUMP length
	if n, err := objfmt.BlockSize(obj); err != nil || n != len(obj) {
		t.Fatalf("fixture is not a valid BRC-144 frame: size=%d err=%v len=%d", n, err, len(obj))
	}
	return obj
}

// TestHandleSubtreeReannouncesAfterFailedAnnounce is the announce-failure
// invariant: a failed announce must not poison the object's future. The cache
// keeps the bytes either way — the retrieval plane still has to serve a pull —
// but only a SUCCESSFUL announce suppresses the next redelivery. Gating on the
// cache instead left merkle-service permanently unaware of an object we held.
func TestHandleSubtreeReannouncesAfterFailedAnnounce(t *testing.T) {
	objects, announced, log := testFixtures(t)
	obj := subtreeFrame(t, 0x11)
	boom := errors.New("kafka down")
	ann := &stubAnnouncer{err: boom}

	// 1. First delivery: the announce fails and the handler says so.
	if err := handleSubtree(context.Background(), obj, objects, announced, ann, log); !errors.Is(err, boom) {
		t.Fatalf("failed announce must surface to the lane: %v", err)
	}
	if len(ann.subtrees) != 1 {
		t.Fatalf("first delivery must attempt one announce, got %d", len(ann.subtrees))
	}
	// The bytes stay servable even though nobody was told about them.
	var key cache.Key
	copy(key[:], obj[:32])
	if _, _, ok := objects.Get(key); !ok {
		t.Fatal("an object whose announce failed must stay cached and servable")
	}

	// 2. Redelivery: announced again, because it was never recorded.
	ann.err = nil
	if err := handleSubtree(context.Background(), obj, objects, announced, ann, log); err != nil {
		t.Fatalf("redelivery after a failed announce must succeed: %v", err)
	}
	if len(ann.subtrees) != 2 {
		t.Fatalf("redelivery must re-announce, got %d announces", len(ann.subtrees))
	}

	// 3. Redelivery after SUCCESS is still suppressed, exactly as before.
	if err := handleSubtree(context.Background(), obj, objects, announced, ann, log); err != nil {
		t.Fatalf("duplicate redelivery must not error: %v", err)
	}
	if len(ann.subtrees) != 2 {
		t.Fatalf("a successfully announced object must not be re-announced, got %d", len(ann.subtrees))
	}
}

// TestHandleBlockReannouncesAfterFailedAnnounce is the same invariant on the
// block lane, which additionally transcodes the frame for its inline fields.
func TestHandleBlockReannouncesAfterFailedAnnounce(t *testing.T) {
	objects, announced, log := testFixtures(t)
	obj := blockFrame(t, 0xAB)
	boom := errors.New("kafka down")
	ann := &stubAnnouncer{err: boom}

	if err := handleBlock(context.Background(), obj, objects, announced, ann, log); !errors.Is(err, boom) {
		t.Fatalf("failed announce must surface to the lane: %v", err)
	}
	if len(ann.blocks) != 1 {
		t.Fatalf("first delivery must attempt one announce, got %d", len(ann.blocks))
	}

	ann.err = nil
	if err := handleBlock(context.Background(), obj, objects, announced, ann, log); err != nil {
		t.Fatalf("redelivery after a failed announce must succeed: %v", err)
	}
	if len(ann.blocks) != 2 {
		t.Fatalf("redelivery must re-announce, got %d announces", len(ann.blocks))
	}

	if err := handleBlock(context.Background(), obj, objects, announced, ann, log); err != nil {
		t.Fatalf("duplicate redelivery must not error: %v", err)
	}
	if len(ann.blocks) != 2 {
		t.Fatalf("a successfully announced block must not be re-announced, got %d", len(ann.blocks))
	}
}

// TestHandleDistinctObjectsEachAnnounce guards the obvious way to "fix" the
// above wrongly: suppressing nothing, or suppressing everything.
func TestHandleDistinctObjectsEachAnnounce(t *testing.T) {
	objects, announced, log := testFixtures(t)
	ann := &stubAnnouncer{}
	for _, fill := range []byte{0x01, 0x02, 0x03} {
		if err := handleSubtree(context.Background(), subtreeFrame(t, fill), objects, announced, ann, log); err != nil {
			t.Fatalf("fill %#x: %v", fill, err)
		}
	}
	if len(ann.subtrees) != 3 {
		t.Fatalf("three distinct subtrees must produce three announces, got %d", len(ann.subtrees))
	}
}

// TestHandleSubtreeSinkMode pins sink behaviour: no announcer, so there is
// nothing to fail and the object counts as fully handled on first delivery.
func TestHandleSubtreeSinkMode(t *testing.T) {
	objects, announced, log := testFixtures(t)
	obj := subtreeFrame(t, 0x22)
	for i := 0; i < 2; i++ {
		if err := handleSubtree(context.Background(), obj, objects, announced, nil, log); err != nil {
			t.Fatalf("sink mode must not error: %v", err)
		}
	}
	if n := announced.Stats().Entries; n != 1 {
		t.Fatalf("sink mode must record the object once, got %d entries", n)
	}
}
