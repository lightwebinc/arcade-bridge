// Package facade serves a Teranode-shaped transaction-submission surface for
// an unmodified arcade's stock propagation, and forwards accepted
// transactions up the consumer tunnel as one bare extended-format stream.
//
// Arcade broadcasts by POSTing concatenated raw transactions to every
// datahub's /txs in parallel — O(N) unicast. Pointed at this facade instead,
// one submit enters the fabric's open tx ingress and the fabric fans it to
// every miner-tier consumer. The response contract is Teranode's own: HTTP
// 200 accepts the whole batch; any other status may carry the structured
// failure list ("Failed to process transactions:" + one
// "<NAME> (<num>): [ProcessTransaction][<txid>] <message>" line per failed
// tx), which arcade parses per-tx — absent from the list means accepted.
// Honouring that shape is what lets arcade's classifier, reaper and
// wallet-visible rows behave exactly as they do against a real datahub.
//
// The fabric is extended-format-native at ingress, so a standard-serialized
// transaction is hydrated to EF before it is forwarded: source data comes
// from the facade's recent-submission cache (children usually chain onto
// parents this facade just forwarded), then an optional asset-API fallback.
// A parent nowhere to be found fails that tx as TX_MISSING_PARENT — the same
// verdict, with the same retry semantics, the real datahub would return.
package facade

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"sync/atomic"
	"time"

	sdkTx "github.com/bsv-blockchain/go-sdk/transaction"
	"github.com/lightwebinc/teranode-bridge/cache"
)

// Submitter forwards one extended-format transaction into the fabric. It is
// the seam tests substitute; production wires the up-tunnel client.
type Submitter interface {
	Submit(ctx context.Context, ef []byte) error
}

// ParentSource fetches a parent transaction's raw bytes by display-order
// txid when the recent-submission cache misses. nil disables the fallback.
type ParentSource func(ctx context.Context, txid string) ([]byte, error)

// Failure-list grammar constants — Teranode's /txs contract, which arcade
// parses (teranode/client.go parseTxsFailures at the pinned review SHA).
const (
	failureHeader = "Failed to process transactions:"

	codeTxInvalid       = "TX_INVALID (31)"
	codeTxMissingParent = "TX_MISSING_PARENT (34)"
)

// maxBody caps a /txs request. Teranode's own server cap is 32 MiB; match it.
const maxBody = 32 << 20

// Config describes the facade listener.
type Config struct {
	Listen string
}

// Stats is a point-in-time snapshot.
type Stats struct {
	Batches, Accepted, Hydrated, MissingParent, Malformed, SubmitFailures uint64
}

// Server is the facade.
type Server struct {
	cfg    Config
	sub    Submitter
	recent *cache.Cache
	fetch  ParentSource
	log    *slog.Logger

	batches, accepted, hydrated, missingParent, malformed, submitFailures atomic.Uint64
}

// New builds the facade. recent is the forwarded-transaction cache used for
// parent hydration (and refilled by every accepted submission); fetch may be
// nil.
func New(cfg Config, sub Submitter, recent *cache.Cache, fetch ParentSource, log *slog.Logger) *Server {
	return &Server{cfg: cfg, sub: sub, recent: recent, fetch: fetch, log: log}
}

// Stats returns a counter snapshot.
func (s *Server) Stats() Stats {
	return Stats{
		Batches: s.batches.Load(), Accepted: s.accepted.Load(), Hydrated: s.hydrated.Load(),
		MissingParent: s.missingParent.Load(), Malformed: s.malformed.Load(),
		SubmitFailures: s.submitFailures.Load(),
	}
}

// Handler returns the HTTP surface: POST /txs, POST /tx, GET /health.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	// Any HTTP response restores a tripped arcade endpoint breaker; /health
	// exists so the probe has a stable target.
	mux.HandleFunc("GET /health", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	mux.HandleFunc("POST /txs", s.handleTxs)
	mux.HandleFunc("POST /tx", s.handleTxs) // same grammar, one-element body
	return mux
}

// Serve runs the listener until ctx ends.
func (s *Server) Serve(ctx context.Context) error {
	srv := &http.Server{Addr: s.cfg.Listen, Handler: s.Handler(), ReadHeaderTimeout: 10 * time.Second}
	done := make(chan struct{})
	go func() {
		<-ctx.Done()
		shCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(shCtx)
		close(done)
	}()
	err := srv.ListenAndServe()
	<-done
	if errors.Is(err, http.ErrServerClosed) {
		return nil
	}
	return err
}

func (s *Server) handleTxs(w http.ResponseWriter, r *http.Request) {
	s.batches.Add(1)
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maxBody))
	if err != nil {
		http.Error(w, "reading body: "+err.Error(), http.StatusRequestEntityTooLarge)
		return
	}
	if len(body) == 0 {
		http.Error(w, "empty body", http.StatusBadRequest)
		return
	}

	var lines []string
	sawMalformed := false
	off := 0
	for off < len(body) {
		tx, n, err := sdkTx.NewTransactionFromStream(body[off:])
		if err != nil || n <= 0 {
			// The stream is self-delimiting; a malformed transaction
			// desynchronises everything behind it. One unkeyed line tells
			// arcade the chunk cannot be settled whole — it narrows and
			// resubmits, exactly as it does against Teranode.
			s.malformed.Add(1)
			sawMalformed = true
			lines = append(lines, fmt.Sprintf("%s: malformed transaction stream at offset %d", codeTxInvalid, off))
			break
		}
		off += n
		txid := tx.TxID().String()

		ef, hydrated, ferr := s.ensureEF(r.Context(), tx)
		if ferr != nil {
			var mp *missingParentError
			if errors.As(ferr, &mp) {
				s.missingParent.Add(1)
				lines = append(lines, fmt.Sprintf("%s: [ProcessTransaction][%s] error getting parent transaction %s", codeTxMissingParent, txid, mp.parent))
			} else {
				s.malformed.Add(1)
				lines = append(lines, fmt.Sprintf("%s: [ProcessTransaction][%s] %v", codeTxInvalid, txid, ferr))
			}
			continue
		}
		if err := s.sub.Submit(r.Context(), ef); err != nil {
			// The up-tunnel is down: that is an endpoint condition, not a
			// verdict about any transaction. A plain 503 with no failure
			// list means "pure infra" to arcade — no per-tx vote, requeue.
			s.submitFailures.Add(1)
			s.log.Warn("up-tunnel submit failed", "txid", txid, "err", err)
			http.Error(w, "up-tunnel unavailable", http.StatusServiceUnavailable)
			return
		}
		// Remember what we forwarded: children usually spend outputs of a
		// transaction this facade just carried, so this cache is the
		// hydration fast path. EF bytes parse identically for that purpose.
		s.recent.Put(cache.Key(txidKey(tx)), "tx", ef)
		s.accepted.Add(1)
		if hydrated {
			s.hydrated.Add(1)
		}
	}

	if len(lines) == 0 {
		w.WriteHeader(http.StatusOK)
		return
	}
	status := http.StatusUnprocessableEntity // TX_MISSING_PARENT family
	if sawMalformed {
		status = http.StatusBadRequest
	}
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.WriteHeader(status)
	fmt.Fprintln(w, failureHeader)
	for _, l := range lines {
		fmt.Fprintln(w, l)
	}
}

// missingParentError names the parent that could not be resolved.
type missingParentError struct{ parent string }

func (e *missingParentError) Error() string { return "missing parent " + e.parent }

// ensureEF returns the transaction's extended-format bytes, hydrating inputs
// that lack source data from the recent cache and then the ParentSource.
func (s *Server) ensureEF(ctx context.Context, tx *sdkTx.Transaction) (ef []byte, hydrated bool, err error) {
	for _, in := range tx.Inputs {
		if in.SourceTxSatoshis() != nil {
			continue
		}
		parentDisplay := in.SourceTXID.String()
		raw, _, ok := s.recent.Get(cache.Key(*in.SourceTXID))
		if !ok && s.fetch != nil {
			if fetched, ferr := s.fetch(ctx, parentDisplay); ferr == nil {
				raw, ok = fetched, true
			}
		}
		if !ok {
			return nil, false, &missingParentError{parent: parentDisplay}
		}
		parent, perr := sdkTx.NewTransactionFromBytes(raw)
		if perr != nil {
			return nil, false, fmt.Errorf("parent %s unparseable: %w", parentDisplay, perr)
		}
		idx := int(in.SourceTxOutIndex)
		if idx >= len(parent.Outputs) {
			return nil, false, fmt.Errorf("parent %s has no output %d", parentDisplay, idx)
		}
		in.SetSourceTxOutput(parent.Outputs[idx])
		hydrated = true
	}
	ef, err = tx.EF()
	if err != nil {
		return nil, false, err
	}
	return ef, hydrated, nil
}

// txidKey returns the transaction id in wire order as the cache key — the
// same bytes a child's SourceTXID carries, so lookups need no reversal.
func txidKey(tx *sdkTx.Transaction) [32]byte {
	return [32]byte(*tx.TxID())
}

// AssetParentSource returns a ParentSource that fetches raw transactions
// from a Teranode asset API base (including its /api/v1 prefix), with
// bounded retry on 429 — the asset's heavy-rate limiter trips on bursts.
func AssetParentSource(base string, client *http.Client) ParentSource {
	if client == nil {
		client = &http.Client{Timeout: 10 * time.Second}
	}
	base = strings.TrimRight(base, "/")
	return func(ctx context.Context, txid string) ([]byte, error) {
		var lastErr error
		backoff := 500 * time.Millisecond
		for attempt := 0; attempt < 3; attempt++ {
			req, err := http.NewRequestWithContext(ctx, http.MethodGet, base+"/tx/"+txid, nil)
			if err != nil {
				return nil, err
			}
			resp, err := client.Do(req)
			if err != nil {
				return nil, err
			}
			body, rerr := io.ReadAll(io.LimitReader(resp.Body, 32<<20))
			resp.Body.Close()
			switch {
			case resp.StatusCode == http.StatusOK && rerr == nil:
				return body, nil
			case resp.StatusCode == http.StatusTooManyRequests:
				lastErr = fmt.Errorf("asset 429")
				select {
				case <-ctx.Done():
					return nil, ctx.Err()
				case <-time.After(backoff):
				}
				backoff *= 2
				continue
			default:
				return nil, fmt.Errorf("asset %s: %d", txid, resp.StatusCode)
			}
		}
		return nil, lastErr
	}
}
