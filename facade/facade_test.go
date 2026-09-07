package facade

// Contract fixtures: the response-body assertions below use arcade's OWN
// parsing rules (teranode/client.go at the pinned review SHA) re-declared as
// fixtures — header line, then one "<NAME> (<num>): …" line per failure,
// txid extracted via the [ProcessTransaction][<txid>] wrapper. A body this
// facade emits must settle under those rules exactly as Teranode's does.

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http/httptest"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-sdk/script"
	sdkTx "github.com/bsv-blockchain/go-sdk/transaction"
	"github.com/lightwebinc/teranode-bridge/cache"
)

func discardLogger() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

// arcade's parse fixtures.
const fixtureHeader = "Failed to process transactions:"

var fixtureWrappedTxid = regexp.MustCompile(`\[ProcessTransaction\]\[([0-9a-fA-F]{64})\]`)

type recordingSubmitter struct {
	mu   sync.Mutex
	got  [][]byte
	fail error
}

func (r *recordingSubmitter) Submit(_ context.Context, ef []byte) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.fail != nil {
		return r.fail
	}
	cp := append([]byte(nil), ef...)
	r.got = append(r.got, cp)
	return nil
}

func (r *recordingSubmitter) count() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.got)
}

func p2pkh(t *testing.T) *script.Script {
	t.Helper()
	s, err := script.NewFromHex("76a914" + strings.Repeat("00", 20) + "88ac")
	if err != nil {
		t.Fatal(err)
	}
	return s
}

// makeChain builds a parent with one output and a child spending it.
func makeChain(t *testing.T) (parent, child *sdkTx.Transaction) {
	return makeChainSats(t, 1000)
}

// makeChainSats varies the parent value so distinct chains get distinct
// txids — two identical calls would otherwise build byte-identical
// transactions.
func makeChainSats(t *testing.T, sats uint64) (parent, child *sdkTx.Transaction) {
	t.Helper()
	ls := p2pkh(t)
	parent = sdkTx.NewTransaction()
	parent.AddOutput(&sdkTx.TransactionOutput{Satoshis: sats, LockingScript: ls})
	child = sdkTx.NewTransaction()
	child.AddInput(&sdkTx.TransactionInput{
		SourceTXID: parent.TxID(), SourceTxOutIndex: 0,
		UnlockingScript: &script.Script{}, SequenceNumber: 0xffffffff,
	})
	child.AddOutput(&sdkTx.TransactionOutput{Satoshis: sats - 100, LockingScript: ls})
	return parent, child
}

func newServer(t *testing.T, sub Submitter, fetch ParentSource) (*Server, *cache.Cache) {
	t.Helper()
	recent := cache.New(cache.Options{MaxBytes: 1 << 20, TTL: time.Minute})
	log := discardLogger()
	return New(Config{}, sub, recent, fetch, log), recent
}

func post(t *testing.T, s *Server, body []byte) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest("POST", "/txs", strings.NewReader(string(body)))
	w := httptest.NewRecorder()
	s.Handler().ServeHTTP(w, req)
	return w
}

func TestEFPassthrough(t *testing.T) {
	parent, child := makeChain(t)
	child.Inputs[0].SetSourceTxOutput(parent.Outputs[0])
	ef, err := child.EF()
	if err != nil {
		t.Fatal(err)
	}
	sub := &recordingSubmitter{}
	s, _ := newServer(t, sub, nil)
	w := post(t, s, ef)
	if w.Code != 200 {
		t.Fatalf("want 200, got %d: %s", w.Code, w.Body)
	}
	if sub.count() != 1 || string(sub.got[0]) != string(ef) {
		t.Fatalf("submitted bytes differ from the EF input")
	}
}

func TestHydrationFromRecentCache(t *testing.T) {
	parent, child := makeChain(t)
	raw := child.Bytes() // standard serialization: no source data
	sub := &recordingSubmitter{}
	s, recent := newServer(t, sub, nil)
	recent.Put(cache.Key(*parent.TxID()), "tx", parent.Bytes())

	w := post(t, s, raw)
	if w.Code != 200 {
		t.Fatalf("want 200, got %d: %s", w.Code, w.Body)
	}
	// The forwarded bytes must be extended format with the parent's data.
	got, _, err := sdkTx.NewTransactionFromStream(sub.got[0])
	if err != nil {
		t.Fatal(err)
	}
	sats := got.Inputs[0].SourceTxSatoshis()
	if sats == nil || *sats != 1000 {
		t.Fatalf("hydration lost source satoshis: %v", sats)
	}
	if s.Stats().Hydrated != 1 {
		t.Fatalf("hydrated counter = %d", s.Stats().Hydrated)
	}
}

func TestMissingParentVerdict(t *testing.T) {
	_, child := makeChain(t)
	sub := &recordingSubmitter{}
	s, _ := newServer(t, sub, nil)
	w := post(t, s, child.Bytes())
	if w.Code != 422 {
		t.Fatalf("want 422, got %d", w.Code)
	}
	lines := strings.Split(strings.TrimRight(w.Body.String(), "\n"), "\n")
	if lines[0] != fixtureHeader {
		t.Fatalf("header line %q", lines[0])
	}
	if len(lines) != 2 || !strings.Contains(lines[1], "TX_MISSING_PARENT (34)") {
		t.Fatalf("verdict lines: %v", lines)
	}
	m := fixtureWrappedTxid.FindStringSubmatch(lines[1])
	if m == nil || m[1] != child.TxID().String() {
		t.Fatalf("wrapped txid not extractable by arcade's rule: %q", lines[1])
	}
	if sub.count() != 0 {
		t.Fatal("a failed tx must not be forwarded")
	}
}

func TestBatchMixedVerdicts(t *testing.T) {
	parentA, childA := makeChain(t)
	childA.Inputs[0].SetSourceTxOutput(parentA.Outputs[0])
	efA, err := childA.EF()
	if err != nil {
		t.Fatal(err)
	}
	_, childB := makeChainSats(t, 777) // parent never provided
	body := append(append([]byte(nil), efA...), childB.Bytes()...)

	sub := &recordingSubmitter{}
	s, _ := newServer(t, sub, nil)
	w := post(t, s, body)
	if w.Code != 422 {
		t.Fatalf("want 422, got %d: %s", w.Code, w.Body)
	}
	if sub.count() != 1 {
		t.Fatalf("accepted tx must still be forwarded, got %d", sub.count())
	}
	// Absent-from-list means accepted: exactly one keyed line, naming childB.
	body422 := w.Body.String()
	if strings.Contains(body422, childA.TxID().String()) {
		t.Fatalf("accepted tx appears in the failure list:\n%s", body422)
	}
	if !strings.Contains(body422, childB.TxID().String()) {
		t.Fatalf("failed tx missing from the failure list:\n%s", body422)
	}
}

func TestMalformedStream(t *testing.T) {
	sub := &recordingSubmitter{}
	s, _ := newServer(t, sub, nil)
	w := post(t, s, []byte{0xde, 0xad, 0xbe, 0xef})
	if w.Code != 400 {
		t.Fatalf("want 400, got %d", w.Code)
	}
	if !strings.HasPrefix(w.Body.String(), fixtureHeader+"\n") {
		t.Fatalf("malformed verdict must still carry the failure-list shape:\n%s", w.Body)
	}
}

func TestParentSourceFallback(t *testing.T) {
	parent, child := makeChain(t)
	fetch := func(_ context.Context, txid string) ([]byte, error) {
		if txid != parent.TxID().String() {
			return nil, fmt.Errorf("unexpected txid %s", txid)
		}
		return parent.Bytes(), nil
	}
	sub := &recordingSubmitter{}
	s, _ := newServer(t, sub, fetch)
	w := post(t, s, child.Bytes())
	if w.Code != 200 {
		t.Fatalf("want 200 via fallback, got %d: %s", w.Code, w.Body)
	}
}

func TestChainedHydrationThroughRecent(t *testing.T) {
	// child spends parent; grandchild spends child. Submitting child (EF)
	// then grandchild (raw) must hydrate the grandchild from the recent
	// cache filled by the child's own submission.
	parent, child := makeChain(t)
	child.Inputs[0].SetSourceTxOutput(parent.Outputs[0])
	efChild, err := child.EF()
	if err != nil {
		t.Fatal(err)
	}
	grandchild := sdkTx.NewTransaction()
	grandchild.AddInput(&sdkTx.TransactionInput{
		SourceTXID: child.TxID(), SourceTxOutIndex: 0,
		UnlockingScript: &script.Script{}, SequenceNumber: 0xffffffff,
	})
	grandchild.AddOutput(&sdkTx.TransactionOutput{Satoshis: 800, LockingScript: p2pkh(t)})

	sub := &recordingSubmitter{}
	s, _ := newServer(t, sub, nil)
	if w := post(t, s, efChild); w.Code != 200 {
		t.Fatalf("child submit: %d", w.Code)
	}
	if w := post(t, s, grandchild.Bytes()); w.Code != 200 {
		t.Fatalf("grandchild submit: %d %s", w.Code, post(t, s, grandchild.Bytes()).Body)
	}
	if sub.count() != 2 {
		t.Fatalf("forwarded %d txs", sub.count())
	}
}

func TestUpTunnelDownIsInfraNotVerdict(t *testing.T) {
	parent, child := makeChain(t)
	child.Inputs[0].SetSourceTxOutput(parent.Outputs[0])
	ef, _ := child.EF()
	sub := &recordingSubmitter{fail: fmt.Errorf("dial tcp: connection refused")}
	s, _ := newServer(t, sub, nil)
	w := post(t, s, ef)
	if w.Code != 503 {
		t.Fatalf("want 503, got %d", w.Code)
	}
	if strings.HasPrefix(w.Body.String(), fixtureHeader) {
		t.Fatal("an infra failure must not carry a per-tx failure list")
	}
}

func TestHealth(t *testing.T) {
	s, _ := newServer(t, &recordingSubmitter{}, nil)
	req := httptest.NewRequest("GET", "/health", nil)
	w := httptest.NewRecorder()
	s.Handler().ServeHTTP(w, req)
	if w.Code != 200 {
		t.Fatalf("health: %d", w.Code)
	}
}
