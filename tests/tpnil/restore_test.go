//go:build tpnil

package tpnil

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"
)

// TestTPNIL_StandingRestored is MTS-09 of the ISI-1 transport-substitution
// proposal: once the fabric-only profile is removed, both stock paths are back.
// It reads them as behavior through public interfaces, not as configuration:
// Broadcaster's health lists a discovered datahub again, healthy, and Merkle
// Service's own libp2p source announces the objects of a newly mined block.
//
//	go test -tags tpnil -run '^TestTPNIL_StandingRestored$' -count=1 -v ./tests/tpnil
func TestTPNIL_StandingRestored(t *testing.T) {
	need := func(name string) string {
		v := strings.TrimSpace(os.Getenv(name))
		if v == "" {
			t.Fatalf("%s is required", name)
		}
		return v
	}
	e := env{
		arcade:  strings.TrimRight(need("TPNIL_ARCADE_URL"), "/"),
		rpc:     need("TPNIL_TERANODE_RPC_URL"),
		rpcUser: need("TPNIL_TERANODE_RPC_USER"),
		rpcPass: need("TPNIL_TERANODE_RPC_PASSWORD"),
		mineTo:  need("TPNIL_MINE_ADDRESS"),
		merkle:  need("TPNIL_MERKLE_METRICS"),
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	// Broadcaster's stock path: datahub discovery finds a datahub on its own.
	var discovered, seen string
	waitForSeen(t, ctx, 120*time.Second, "a discovered, healthy datahub in Broadcaster's health", func() bool {
		b, code := get(ctx, e.arcade+"/health")
		if code != http.StatusOK {
			seen = fmt.Sprintf("GET /health: HTTP %d", code)
			return false
		}
		var h struct {
			Datahubs []struct {
				URL     string `json:"url"`
				Source  string `json:"source"`
				Healthy bool   `json:"healthy"`
			} `json:"datahub_urls"`
		}
		if err := json.Unmarshal(b, &h); err != nil {
			seen = fmt.Sprintf("GET /health: decode: %v", err)
			return false
		}
		sources := make([]string, 0, len(h.Datahubs))
		for _, d := range h.Datahubs {
			sources = append(sources, d.Source+" "+d.URL)
			if d.Source == "discovered" && d.Healthy {
				discovered = d.URL
				return true
			}
		}
		seen = "datahubs " + strings.Join(sources, ", ")
		return false
	}, func() string { return seen })

	// Merkle Service's stock path: its libp2p source announces again. One block
	// is mined so that there is something to announce.
	before := scrape(t, ctx, e.merkle).sum("merkle_p2p_announcements_total", "", "")
	var blocks []string
	if err := rpc(ctx, e, "generatetoaddress", []any{1, e.mineTo}, &blocks); err != nil || len(blocks) != 1 {
		t.Fatalf("mine one block: %v (%d blocks)", err, len(blocks))
	}
	var after float64
	waitForSeen(t, ctx, 120*time.Second, "Merkle Service libp2p announcements after block "+blocks[0], func() bool {
		after = scrape(t, ctx, e.merkle).sum("merkle_p2p_announcements_total", "", "")
		return after > before
	}, func() string {
		return fmt.Sprintf("merkle_p2p_announcements_total %.0f, %.0f before the block", after, before)
	})

	t.Logf("MTS-09 the standing configuration is restored: Broadcaster's datahub discovery lists %s again, healthy, and Merkle Service's libp2p source announced %.0f objects after block %s", discovered, after-before, blocks[0])
}
