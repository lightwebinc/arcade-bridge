//go:build tpnil

// Package tpnil holds the joined qualification of the multicast delivery fabric
// between an unmodified Broadcaster (Arcade), Teranode and Merkle Service, the
// MTS-01 to MTS-07 requirements of the TPNIL ISI-1 transport-substitution
// proposal. It drives only public interfaces: Arcade's API, Teranode's JSON-RPC,
// and the metrics endpoints of the two bridges and Merkle Service.
//
//	go test -tags tpnil -run '^TestTPNIL_FabricJoinedPath$' -count=1 -v ./tests/tpnil
//
// Each requirement prints one line beginning with its identifier, which a TPNIL
// component qualification plan names as required output.
package tpnil

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-sdk/transaction"
)

const (
	fixtureSchema = "tpnil-arcade-teranode-fixture@1.0.0"
	profileSchema = "lightweb-fabric-only-profile@1.0.0"
)

type fixture struct {
	Schema string `json:"schema"`
	TxID   string `json:"txid"`
	EFHex  string `json:"efHex"`
	RawHex string `json:"rawHex"`
}

// profile is the manifest of the fabric-only configuration, written from the
// running containers before the qualification and bound to it as a plan input.
type profile struct {
	Schema string `json:"schema"`
	Arcade struct {
		DatahubDiscovery bool     `json:"datahubDiscovery"`
		DatahubURLs      []string `json:"datahubURLs"`
		ConfigSHA256     string   `json:"configSha256"`
		P2PClientRunning bool     `json:"p2pClientRunning"`
	} `json:"arcade"`
	MerkleService struct {
		BootstrapPeers string `json:"bootstrapPeers"`
		DHTMode        string `json:"dhtMode"`
		MDNS           bool   `json:"mdns"`
	} `json:"merkleService"`
}

type env struct {
	fixture, profile                     string
	arcade, facade                       string
	rpc, rpcUser, rpcPass, mineTo        string
	arcadeBridge, teranodeBridge, merkle string
	headerLane                           string
}

func readEnv(t *testing.T) env {
	t.Helper()
	get := func(name string, required bool) string {
		v := strings.TrimSpace(os.Getenv(name))
		if v == "" && required {
			t.Fatalf("%s is required", name)
		}
		return v
	}
	return env{
		fixture:        get("TPNIL_FIXTURE", true),
		profile:        get("TPNIL_PROFILE", true),
		arcade:         strings.TrimRight(get("TPNIL_ARCADE_URL", true), "/"),
		facade:         strings.TrimRight(get("TPNIL_FACADE_URL", true), "/"),
		rpc:            get("TPNIL_TERANODE_RPC_URL", true),
		rpcUser:        get("TPNIL_TERANODE_RPC_USER", true),
		rpcPass:        get("TPNIL_TERANODE_RPC_PASSWORD", true),
		mineTo:         get("TPNIL_MINE_ADDRESS", true),
		arcadeBridge:   get("TPNIL_ARCADE_BRIDGE_METRICS", true),
		teranodeBridge: get("TPNIL_TERANODE_BRIDGE_METRICS", true),
		merkle:         get("TPNIL_MERKLE_METRICS", true),
		headerLane:     strings.TrimRight(get("TPNIL_HEADER_LANE_URL", false), "/"),
	}
}

// counters is one reading of every series the qualification compares.
type counters struct {
	facadeAccepted, uptunnelSent           float64
	arcadeSubtrees, arcadeBlocks           float64
	announcedSubtrees, announcedBlocks     float64
	txLane, reverseSubtrees, reverseBlocks float64
	echoVerified, echoMismatch             float64
	merkleP2P                              float64
}

func read(t *testing.T, ctx context.Context, e env) counters {
	t.Helper()
	ab := scrape(t, ctx, e.arcadeBridge)
	tb := scrape(t, ctx, e.teranodeBridge)
	ms := scrape(t, ctx, e.merkle)
	return counters{
		facadeAccepted:    ab.sum("arcade_bridge_facade_txs_total", "result", "accepted") + ab.sum("arcade_bridge_facade_txs_total", "result", "hydrated"),
		uptunnelSent:      ab.sum("arcade_bridge_uptunnel_sent_total", "", ""),
		arcadeSubtrees:    ab.sum("arcade_bridge_lane_objects_total", "lane", "subtree"),
		arcadeBlocks:      ab.sum("arcade_bridge_lane_objects_total", "lane", "block"),
		announcedSubtrees: ab.sum("arcade_bridge_announce_total", "class", "subtree"),
		announcedBlocks:   ab.sum("arcade_bridge_announce_total", "class", "block"),
		txLane:            tb.sum("teranode_bridge_lane_objects_total", "lane", "tx"),
		reverseSubtrees:   tb.sum("teranode_bridge_reverse_published_total", "class", "subtree"),
		reverseBlocks:     tb.sum("teranode_bridge_reverse_published_total", "class", "block"),
		echoVerified:      tb.sum("teranode_bridge_echo_verified_total", "", ""),
		echoMismatch:      tb.sum("teranode_bridge_echo_mismatch_total", "", ""),
		merkleP2P:         ms.sum("merkle_p2p_announcements_total", "", ""),
	}
}

func TestTPNIL_FabricJoinedPath(t *testing.T) {
	e := readEnv(t)
	ctx, cancel := context.WithTimeout(context.Background(), 12*time.Minute)
	defer cancel()

	// The fixture: its Extended Format bytes are parsed independently and must
	// yield the fixture's txid and standard serialization.
	var fx fixture
	readJSON(t, e.fixture, &fx)
	if fx.Schema != fixtureSchema {
		t.Fatalf("fixture schema %q, want %q", fx.Schema, fixtureSchema)
	}
	ef, err := hex.DecodeString(fx.EFHex)
	if err != nil {
		t.Fatalf("fixture EF: %v", err)
	}
	tx, err := transaction.NewTransactionFromBytes(ef)
	if err != nil {
		t.Fatalf("parse fixture EF: %v", err)
	}
	if tx.TxID().String() != fx.TxID || hex.EncodeToString(tx.Bytes()) != fx.RawHex {
		t.Fatalf("fixture EF does not parse to its own txid and raw bytes")
	}
	t.Logf("fixture: EF bytes parse to txid %s and to the fixture's raw bytes", fx.TxID)

	// MTS-01: the fabric-only configuration, as recorded from the running stack.
	var p profile
	readJSON(t, e.profile, &p)
	if p.Schema != profileSchema {
		t.Fatalf("profile schema %q, want %q", p.Schema, profileSchema)
	}
	if p.Arcade.DatahubDiscovery || p.Arcade.P2PClientRunning || len(p.Arcade.DatahubURLs) != 1 ||
		strings.TrimRight(p.Arcade.DatahubURLs[0], "/") != e.facade {
		t.Fatalf("Broadcaster is not fabric-only: %+v", p.Arcade)
	}
	if p.MerkleService.DHTMode != "off" || p.MerkleService.MDNS {
		t.Fatalf("Merkle Service libp2p is not isolated: %+v", p.MerkleService)
	}
	before := read(t, ctx, e)
	t.Logf("MTS-01 fabric-only configuration: Broadcaster's sole datahub is the facade, discovery and its p2p client are off; Merkle Service libp2p has DHT and mDNS off and an unreachable bootstrap peer")

	// MTS-02: submit through Broadcaster; its propagation reaches the facade and
	// the transaction lane delivers into Teranode.
	status := post(t, ctx, e.arcade+"/tx", "application/octet-stream", ef)
	if status/100 != 2 {
		t.Fatalf("Broadcaster POST /tx: HTTP %d", status)
	}
	var after counters
	waitFor(t, ctx, 90*time.Second, "Broadcaster's propagation through the facade to the transaction lane", func() bool {
		after = read(t, ctx, e)
		return after.facadeAccepted > before.facadeAccepted && after.uptunnelSent > before.uptunnelSent && after.txLane > before.txLane
	})
	t.Logf("MTS-02 Broadcaster's propagation reached the facade (%s accepted, %s sent up-tunnel) and the transaction lane delivered %s objects into Teranode",
		delta(before.facadeAccepted, after.facadeAccepted), delta(before.uptunnelSent, after.uptunnelSent), delta(before.txLane, after.txLane))
	waitFor(t, ctx, 90*time.Second, "Broadcaster status ACCEPTED_BY_NETWORK or later", func() bool {
		s := arcadeStatus(t, ctx, e.arcade, fx.TxID).TxStatus
		return s == "ACCEPTED_BY_NETWORK" || s == "SEEN_ON_NETWORK" || s == "SEEN_MULTIPLE_NODES" || s == "MINED"
	})

	// MTS-03: the mempool holds the transaction; mine one block.
	waitFor(t, ctx, 60*time.Second, "the transaction in Teranode's mempool", func() bool {
		var ids []string
		return rpc(ctx, e, "getrawmempool", nil, &ids) == nil && contains(ids, fx.TxID)
	})
	var blocks []string
	if err := rpc(ctx, e, "generatetoaddress", []any{1, e.mineTo}, &blocks); err != nil || len(blocks) != 1 {
		t.Fatalf("mine one block: %v (%d blocks)", err, len(blocks))
	}
	block := blocks[0]
	var mined struct {
		TxID      string `json:"txid"`
		Hex       string `json:"hex"`
		BlockHash string `json:"blockhash"`
	}
	waitFor(t, ctx, 60*time.Second, "the transaction recorded in the mined block", func() bool {
		return rpc(ctx, e, "getrawtransaction", []any{fx.TxID, 1}, &mined) == nil && mined.BlockHash == block
	})
	t.Logf("MTS-03 Teranode accepted transaction %s into its mempool and mined it in block %s", fx.TxID, block)

	// MTS-04: byte identity of the mined transaction.
	if mined.TxID != fx.TxID || mined.Hex != fx.RawHex {
		t.Fatalf("mined transaction diverged from the fixture")
	}
	t.Logf("MTS-04 the mined transaction's raw bytes equal the fixture's raw bytes")

	// MTS-05: the block's objects travel the reverse path and the lanes; the
	// bridge's echo check confirms byte identity; Merkle Service hears nothing
	// over libp2p.
	waitFor(t, ctx, 180*time.Second, "the subtree and block objects on both bridges' lanes", func() bool {
		after = read(t, ctx, e)
		return after.reverseSubtrees > before.reverseSubtrees && after.reverseBlocks > before.reverseBlocks &&
			after.arcadeSubtrees > before.arcadeSubtrees && after.arcadeBlocks > before.arcadeBlocks &&
			after.announcedSubtrees > before.announcedSubtrees && after.announcedBlocks > before.announcedBlocks &&
			after.echoVerified > before.echoVerified
	})
	if after.echoMismatch != before.echoMismatch {
		t.Fatalf("echo mismatch: an object returned with different bytes (%s)", delta(before.echoMismatch, after.echoMismatch))
	}
	if after.merkleP2P != before.merkleP2P {
		t.Fatalf("Merkle Service received %s libp2p announcements; the run is not fabric-only", delta(before.merkleP2P, after.merkleP2P))
	}
	t.Logf("MTS-05 the reverse path published %s subtrees and %s blocks; they arrived on the Merkle Service bridge's lanes (%s subtrees, %s blocks) and were announced (%s, %s); %s echoes verified byte-identical with 0 mismatches; Merkle Service received 0 libp2p announcements",
		delta(before.reverseSubtrees, after.reverseSubtrees), delta(before.reverseBlocks, after.reverseBlocks),
		delta(before.arcadeSubtrees, after.arcadeSubtrees), delta(before.arcadeBlocks, after.arcadeBlocks),
		delta(before.announcedSubtrees, after.announcedSubtrees), delta(before.announcedBlocks, after.announcedBlocks),
		delta(before.echoVerified, after.echoVerified))

	// MTS-06: Broadcaster's proof reproduces the merkle root of the header read
	// from Teranode.
	var proof arcadeTx
	waitFor(t, ctx, 240*time.Second, "Broadcaster status MINED with a merkle path", func() bool {
		proof = arcadeStatus(t, ctx, e.arcade, fx.TxID)
		return proof.TxStatus == "MINED" && proof.MerklePath != ""
	})
	if proof.BlockHash != block {
		t.Fatalf("Broadcaster's proof names block %s, mined %s", proof.BlockHash, block)
	}
	mp, err := transaction.NewMerklePathFromHex(proof.MerklePath)
	if err != nil {
		t.Fatalf("Broadcaster's merkle path: %v", err)
	}
	txid := fx.TxID
	root, err := mp.ComputeRootHex(&txid)
	if err != nil {
		t.Fatalf("compute root: %v", err)
	}
	var headerHex string
	if err := rpc(ctx, e, "getblockheader", []any{block, false}, &headerHex); err != nil {
		t.Fatalf("getblockheader: %v", err)
	}
	header, err := hex.DecodeString(headerHex)
	if err != nil || len(header) != 80 {
		t.Fatalf("block header: %v (%d bytes)", err, len(header))
	}
	if headerRoot := displayHex(header[36:68]); headerRoot != root {
		t.Fatalf("proof root %s, header root %s", root, headerRoot)
	}
	t.Logf("MTS-06 Broadcaster returned a mined proof whose computed root %s equals the merkle root of block %s's header", root, block)

	// MTS-07: the header lane, read from a header store it feeds, when one is
	// configured. The store indexes each lane header by the hash of its bytes.
	if e.headerLane == "" {
		t.Logf("MTS-07 not executed: no header lane consumer configured")
		return
	}
	var lane struct {
		Hash       string `json:"hash"`
		Height     uint32 `json:"height"`
		MerkleRoot string `json:"merkleRoot"`
	}
	waitFor(t, ctx, 120*time.Second, "the block header in the header lane's store", func() bool {
		b, code := get(ctx, e.headerLane+"/v1/header/"+block)
		return code == http.StatusOK && json.Unmarshal(b, &lane) == nil && lane.Hash == block
	})
	if lane.MerkleRoot != displayHex(header[36:68]) {
		t.Fatalf("header lane merkle root %s, Teranode header root %s", lane.MerkleRoot, displayHex(header[36:68]))
	}
	t.Logf("MTS-07 the header lane's store holds block %s at height %d with merkle root %s, equal to Teranode's header", block, lane.Height, lane.MerkleRoot)
}

type arcadeTx struct {
	TxStatus   string `json:"txStatus"`
	BlockHash  string `json:"blockHash"`
	MerklePath string `json:"merklePath"`
}

func arcadeStatus(t *testing.T, ctx context.Context, base, txid string) arcadeTx {
	t.Helper()
	var out arcadeTx
	b, code := get(ctx, base+"/tx/"+txid)
	if code == http.StatusOK {
		_ = json.Unmarshal(b, &out)
	}
	return out
}

func rpc(ctx context.Context, e env, method string, params []any, target any) error {
	if params == nil {
		params = []any{}
	}
	body, _ := json.Marshal(map[string]any{"jsonrpc": "1.0", "id": "tpnil", "method": method, "params": params})
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, e.rpc, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	req.SetBasicAuth(e.rpcUser, e.rpcPass)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	var envelope struct {
		Result json.RawMessage `json:"result"`
		Error  *struct {
			Code    int    `json:"code"`
			Message string `json:"message"`
		} `json:"error"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&envelope); err != nil {
		return err
	}
	if envelope.Error != nil {
		return fmt.Errorf("%s: %d %s", method, envelope.Error.Code, envelope.Error.Message)
	}
	return json.Unmarshal(envelope.Result, target)
}

func post(t *testing.T, ctx context.Context, url, contentType string, body []byte) int {
	t.Helper()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", contentType)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("POST %s: %v", url, err)
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, resp.Body)
	return resp.StatusCode
}

func get(ctx context.Context, url string) ([]byte, int) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, 0
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, 0
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(io.LimitReader(resp.Body, 8<<20))
	return b, resp.StatusCode
}

// samples is one scrape of a Prometheus text endpoint.
type samples []struct {
	name   string
	labels string
	value  float64
}

func scrape(t *testing.T, ctx context.Context, url string) samples {
	t.Helper()
	b, code := get(ctx, url)
	if code != http.StatusOK {
		t.Fatalf("scrape %s: HTTP %d", url, code)
	}
	var out samples
	for _, line := range strings.Split(string(b), "\n") {
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		name, labels, rest := line, "", ""
		if i := strings.IndexByte(line, '{'); i >= 0 {
			j := strings.LastIndexByte(line, '}')
			if j < i {
				continue
			}
			name, labels, rest = line[:i], line[i+1:j], strings.TrimSpace(line[j+1:])
		} else if f := strings.Fields(line); len(f) >= 2 {
			name, rest = f[0], f[1]
		} else {
			continue
		}
		if f := strings.Fields(rest); len(f) > 0 {
			if v, err := strconv.ParseFloat(f[0], 64); err == nil {
				out = append(out, struct {
					name   string
					labels string
					value  float64
				}{name, labels, v})
			}
		}
	}
	return out
}

// sum adds every sample of a family, or only those whose label key has value.
func (s samples) sum(name, key, value string) float64 {
	var total float64
	for _, x := range s {
		if x.name != name {
			continue
		}
		if key != "" && !strings.Contains(","+x.labels+",", ","+key+`="`+value+`",`) {
			continue
		}
		total += x.value
	}
	return total
}

func waitFor(t *testing.T, ctx context.Context, limit time.Duration, what string, ok func() bool) {
	t.Helper()
	deadline := time.Now().Add(limit)
	for time.Now().Before(deadline) {
		if ok() {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatalf("waiting for %s: %v", what, ctx.Err())
		case <-time.After(time.Second):
		}
	}
	t.Fatalf("timed out after %s waiting for %s", limit, what)
}

func readJSON(t *testing.T, path string, v any) {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	dec := json.NewDecoder(bytes.NewReader(b))
	dec.DisallowUnknownFields()
	if err := dec.Decode(v); err != nil {
		t.Fatalf("decode %s: %v", path, err)
	}
}

func contains(list []string, s string) bool {
	for _, x := range list {
		if x == s {
			return true
		}
	}
	return false
}

func delta(before, after float64) string {
	return strconv.FormatFloat(after-before, 'f', -1, 64)
}

// displayHex renders a hash in display (reversed) byte order.
func displayHex(b []byte) string {
	r := make([]byte, len(b))
	for i := range b {
		r[i] = b[len(b)-1-i]
	}
	return hex.EncodeToString(r)
}
