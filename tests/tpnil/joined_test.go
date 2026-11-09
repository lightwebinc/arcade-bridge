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
	"sort"
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
	// ProofPath is "fabric" when Merkle Service's only announcement source is
	// the bridge's lanes, or "stock" when no lane is provisioned to the bridge
	// and Merkle Service keeps its own libp2p source.
	ProofPath  string            `json:"proofPath"`
	Components map[string]string `json:"components"`
	Arcade     struct {
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
	rpc, rpcUser, rpcPass, mineTo, asset string
	arcadeBridge, teranodeBridge, merkle string
	headerLane                           string
	// Lane byte identity, optional: the host interface of the fabric network
	// and the transaction lane's port (MTS-02), and the proof bridge's
	// retrieval plane, the datahub Merkle Service fetches from (MTS-05).
	txLaneIface string
	txLanePort  int
	retrieval   string
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
		asset:          strings.TrimRight(get("TPNIL_TERANODE_ASSET_URL", true), "/"),
		arcadeBridge:   get("TPNIL_ARCADE_BRIDGE_METRICS", true),
		teranodeBridge: get("TPNIL_TERANODE_BRIDGE_METRICS", true),
		merkle:         get("TPNIL_MERKLE_METRICS", true),
		headerLane:     strings.TrimRight(get("TPNIL_HEADER_LANE_URL", false), "/"),
		txLaneIface:    get("TPNIL_TX_LANE_IFACE", false),
		txLanePort:     port(t, get("TPNIL_TX_LANE_PORT", false)),
		retrieval:      strings.TrimRight(get("TPNIL_PROOF_BRIDGE_RETRIEVAL_URL", false), "/"),
	}
}

func port(t *testing.T, v string) int {
	t.Helper()
	if v == "" {
		return 0
	}
	n, err := strconv.Atoi(v)
	if err != nil || n < 1 || n > 65535 {
		t.Fatalf("TPNIL_TX_LANE_PORT %q is not a port", v)
	}
	return n
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
	fabricProof := p.ProofPath == "fabric"
	if !fabricProof && p.ProofPath != "stock" {
		t.Fatalf("profile proofPath %q, want fabric or stock", p.ProofPath)
	}
	if fabricProof && (p.MerkleService.DHTMode != "off" || p.MerkleService.MDNS || !strings.HasPrefix(p.MerkleService.BootstrapPeers, "/ip4/127.0.0.1/")) {
		t.Fatalf("Merkle Service libp2p is not isolated: %+v", p.MerkleService)
	}
	for _, k := range sortedKeys(p.Components) {
		t.Logf("environment: %s %s", k, p.Components[k])
	}
	before := read(t, ctx, e)
	if fabricProof {
		t.Logf("MTS-01 fabric-only configuration: Broadcaster's sole datahub is the facade, discovery and its p2p client are off; Merkle Service libp2p has DHT and mDNS off and an unreachable bootstrap peer")
	} else {
		t.Logf("MTS-01 fabric-only broadcast: Broadcaster's sole datahub is the facade, discovery and its p2p client are off; Merkle Service keeps its stock libp2p source because no lane is provisioned to its bridge")
	}

	// MTS-02: submit through Broadcaster; its propagation reaches the facade and
	// the transaction lane delivers into Teranode.
	var capture *laneCapture
	if e.txLaneIface != "" && e.txLanePort != 0 {
		capture = startLaneCapture(t, e.txLaneIface, e.txLanePort)
	}
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
	if capture != nil {
		mts02LaneBytes(t, capture.stop(t), ef)
	} else {
		t.Logf("MTS-02 lane bytes not observed: no transaction lane capture configured")
	}
	var accepted string
	waitForSeen(t, ctx, 90*time.Second, "Broadcaster status ACCEPTED_BY_NETWORK or later", func() bool {
		st, err := arcadeStatus(t, ctx, e.arcade, fx.TxID)
		if err != nil {
			accepted = err.Error()
			return false
		}
		accepted = "status " + st.TxStatus
		s := st.TxStatus
		return s == "ACCEPTED_BY_NETWORK" || s == "SEEN_ON_NETWORK" || s == "SEEN_MULTIPLE_NODES" || s == "MINED"
	}, func() string { return accepted })

	// MTS-03: the mempool holds the transaction; mine one block.
	waitFor(t, ctx, 60*time.Second, "the transaction in Teranode's mempool", func() bool {
		var ids []string
		return rpc(ctx, e, "getrawmempool", nil, &ids) == nil && contains(ids, fx.TxID)
	})
	// Teranode's block reply lists subtrees, not transactions, so inclusion is
	// read from the asset interface's subtree listing; a transaction that
	// reached the mempool after the candidate was cut goes in the next block.
	block, subtree := "", ""
	for attempt := 0; attempt < 3 && block == ""; attempt++ {
		var blocks []string
		if err := rpc(ctx, e, "generatetoaddress", []any{1, e.mineTo}, &blocks); err != nil || len(blocks) != 1 {
			t.Fatalf("mine one block: %v (%d blocks)", err, len(blocks))
		}
		// The asset interface may not have indexed a block the instant it is
		// mined, so a non-200 or undecodable answer is retried, not fatal.
		var st, seen string
		waitForSeen(t, ctx, 30*time.Second, "block "+blocks[0]+" in the asset interface", func() bool {
			got, err := subtreeHolding(ctx, e.asset, blocks[0], fx.TxID)
			if err != nil {
				seen = err.Error()
				return false
			}
			st = got
			return true
		}, func() string { return seen })
		if st != "" {
			block, subtree = blocks[0], st
		}
	}
	if block == "" {
		t.Fatalf("transaction %s is in none of the three blocks mined", fx.TxID)
	}
	var mined struct {
		TxID string `json:"txid"`
		Hex  string `json:"hex"`
	}
	if err := rpc(ctx, e, "getrawtransaction", []any{fx.TxID, 1}, &mined); err != nil {
		t.Fatalf("getrawtransaction: %v", err)
	}
	t.Logf("MTS-03 Teranode accepted transaction %s into its mempool and mined it in block %s (subtree %s)", fx.TxID, block, subtree)

	// MTS-04: byte identity of the mined transaction.
	if mined.TxID != fx.TxID || mined.Hex != fx.RawHex {
		t.Fatalf("mined transaction diverged from the fixture")
	}
	t.Logf("MTS-04 the mined transaction's raw bytes equal the fixture's raw bytes")

	// MTS-05: the block's objects travel the reverse path and the lanes; the
	// bridge's echo check confirms byte identity where a lane returns them;
	// Merkle Service hears nothing over libp2p. In the stock profile no lane
	// reaches the Merkle Service bridge, so the requirement is not executed and
	// the run records the stock source it used instead.
	if fabricProof {
		mts05Fabric(t, ctx, e, before)
		if e.retrieval != "" {
			mts05LaneBytes(t, ctx, e, block, subtree)
		} else {
			t.Logf("MTS-05 lane bytes not observed: no proof bridge retrieval plane configured")
		}
	}
	// MTS-06: Broadcaster's proof reproduces the merkle root of the header read
	// from Teranode.
	var proof arcadeTx
	var minedSeen string
	waitForSeen(t, ctx, 240*time.Second, "Broadcaster status MINED with a merkle path", func() bool {
		st, err := arcadeStatus(t, ctx, e.arcade, fx.TxID)
		if err != nil {
			minedSeen = err.Error()
			return false
		}
		proof = st
		minedSeen = fmt.Sprintf("status %s, merkle path %d characters", st.TxStatus, len(st.MerklePath))
		return st.TxStatus == "MINED" && st.MerklePath != ""
	}, func() string { return minedSeen })
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
	if !fabricProof {
		now := read(t, ctx, e)
		if now.merkleP2P <= before.merkleP2P {
			t.Fatalf("stock profile: Merkle Service recorded no libp2p announcement, so the proof source is unknown")
		}
		t.Logf("MTS-05 not executed: no subtree or block lane is provisioned to the Merkle Service bridge; Merkle Service took the block from its stock libp2p source (%s announcements during the run); the reverse path published %s subtrees and %s blocks to the fabric",
			delta(before.merkleP2P, now.merkleP2P), delta(before.reverseSubtrees, now.reverseSubtrees), delta(before.reverseBlocks, now.reverseBlocks))
	}

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

// subtreeHolding returns the subtree of block that lists txid, or "".
// subtreeHolding returns the subtree of the block that lists txid, or "" when
// the block lists it in none. An answer the asset interface could not give yet
// (non-200, or a body that does not decode) is the error, for the caller to retry.
func subtreeHolding(ctx context.Context, asset, block, txid string) (string, error) {
	b, code := get(ctx, asset+"/block/"+block+"/json")
	if code != http.StatusOK {
		return "", fmt.Errorf("asset block %s: HTTP %d", block, code)
	}
	var blk struct {
		Subtrees []string `json:"subtrees"`
	}
	if err := json.Unmarshal(b, &blk); err != nil {
		return "", fmt.Errorf("asset block %s: %v", block, err)
	}
	for _, st := range blk.Subtrees {
		b, code := get(ctx, asset+"/subtree/"+st+"/json")
		if code != http.StatusOK {
			return "", fmt.Errorf("asset subtree %s: HTTP %d", st, code)
		}
		var sub struct {
			Data struct {
				Nodes []struct {
					TxID string `json:"txid"`
				} `json:"Nodes"`
			} `json:"data"`
		}
		if err := json.Unmarshal(b, &sub); err != nil {
			return "", fmt.Errorf("asset subtree %s: %v", st, err)
		}
		for _, n := range sub.Data.Nodes {
			if n.TxID == txid {
				return st, nil
			}
		}
	}
	return "", nil
}

type arcadeTx struct {
	TxStatus   string `json:"txStatus"`
	BlockHash  string `json:"blockHash"`
	MerklePath string `json:"merklePath"`
}

// arcadeStatus reads Broadcaster's GET /tx answer. A non-200 answer or a body
// that does not decode is returned as the error, so a poll that times out can
// say what Broadcaster last answered instead of only that it never matched.
func arcadeStatus(t *testing.T, ctx context.Context, base, txid string) (arcadeTx, error) {
	t.Helper()
	var out arcadeTx
	b, code := get(ctx, base+"/tx/"+txid)
	if code != http.StatusOK {
		return out, fmt.Errorf("GET /tx/%s: HTTP %d", txid, code)
	}
	if err := json.Unmarshal(b, &out); err != nil {
		return out, fmt.Errorf("GET /tx/%s: decode: %v", txid, err)
	}
	return out, nil
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
	waitForSeen(t, ctx, limit, what, ok, nil)
}

// waitForSeen is waitFor that, on failure, also reports the last observation
// seen() returns.
func waitForSeen(t *testing.T, ctx context.Context, limit time.Duration, what string, ok func() bool, seen func() string) {
	t.Helper()
	last := func() string {
		if seen == nil {
			return ""
		}
		return "; last seen: " + seen()
	}
	deadline := time.Now().Add(limit)
	for time.Now().Before(deadline) {
		if ok() {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatalf("waiting for %s: %v%s", what, ctx.Err(), last())
		case <-time.After(time.Second):
		}
	}
	t.Fatalf("timed out after %s waiting for %s%s", limit, what, last())
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

// mts05Fabric is MTS-05 in the fabric profile.
func mts05Fabric(t *testing.T, ctx context.Context, e env, before counters) {
	t.Helper()
	var after counters
	waitFor(t, ctx, 180*time.Second, "the subtree and block objects on the Merkle Service bridge's lanes", func() bool {
		after = read(t, ctx, e)
		return after.reverseSubtrees > before.reverseSubtrees && after.reverseBlocks > before.reverseBlocks &&
			after.arcadeSubtrees > before.arcadeSubtrees && after.arcadeBlocks > before.arcadeBlocks &&
			after.announcedSubtrees > before.announcedSubtrees && after.announcedBlocks > before.announcedBlocks
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
}

func sortedKeys(m map[string]string) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

// mts02LaneBytes requires the transaction lane's captured stream to be the
// fixture's Extended Format bytes: one copy, or whole identical copies when
// Broadcaster propagated the transaction more than once.
func mts02LaneBytes(t *testing.T, stream, ef []byte) {
	t.Helper()
	if len(stream) == 0 || len(stream)%len(ef) != 0 {
		t.Fatalf("the transaction lane carried %d bytes (sha256 %s...); the fixture's Extended Format is %d bytes (sha256 %s...)",
			len(stream), digest(stream), len(ef), digest(ef))
	}
	copies := len(stream) / len(ef)
	for i := range copies {
		if !bytes.Equal(stream[i*len(ef):(i+1)*len(ef)], ef) {
			t.Fatalf("copy %d of %d on the transaction lane differs from the fixture's Extended Format bytes", i+1, copies)
		}
	}
	t.Logf("MTS-02 the transaction lane carried the fixture's Extended Format bytes exactly: %d bytes, sha256 %s..., %d copies, captured on the fabric network between the delivery edge and the processor bridge",
		len(ef), digest(ef), copies)
}

// mts05LaneBytes requires the objects the proof bridge serves Merkle Service,
// which it holds only from its subtree and block lanes, to be byte-identical
// to the processor's own: the same route on the bridge's retrieval plane and on
// Teranode's asset service.
func mts05LaneBytes(t *testing.T, ctx context.Context, e env, block, subtree string) {
	t.Helper()
	var parts []string
	for _, o := range []struct{ class, hash string }{{"subtree", subtree}, {"block", block}} {
		var fromBridge []byte
		var seen string
		waitForSeen(t, ctx, 60*time.Second, "the "+o.class+" on the proof bridge's retrieval plane", func() bool {
			b, err := fetchObject(ctx, e.retrieval+"/"+o.class+"/"+o.hash)
			if err != nil {
				seen = err.Error()
				return false
			}
			fromBridge = b
			return true
		}, func() string { return seen })
		fromProcessor, err := fetchObject(ctx, e.asset+"/"+o.class+"/"+o.hash)
		if err != nil {
			t.Fatalf("the processor's %s: %v", o.class, err)
		}
		if !bytes.Equal(fromBridge, fromProcessor) {
			t.Fatalf("%s %s: the proof bridge serves %d bytes (sha256 %s...), the processor %d bytes (sha256 %s...)",
				o.class, o.hash, len(fromBridge), digest(fromBridge), len(fromProcessor), digest(fromProcessor))
		}
		parts = append(parts, fmt.Sprintf("%s %s (%d bytes, sha256 %s...)", o.class, o.hash, len(fromBridge), digest(fromBridge)))
	}
	t.Logf("MTS-05 the subtree and block lanes delivered the processor's bytes: the proof bridge, which holds them only from its lanes, serves %s, byte-identical to the processor's asset service", strings.Join(parts, " and "))
}
