package msannounce

// The tests here are contract fixtures against merkle-service's decoder
// (internal/kafka/messages.go at the pinned review SHA): the JSON field names
// and value encodings below are what DecodeSubtreeMessage/DecodeBlockMessage
// read. Upstream is deliberately NOT a Go dependency — the mirror structs are
// the vendored fixture, and a field-name drift upstream must be caught by
// re-reviewing the pin, not by a silently updated import.

import (
	"encoding/json"
	"testing"
	"time"
)

// fixture mirrors of merkle-service's decode side.
type msSubtreeFixture struct {
	Hash              string `json:"hash"`
	DataHubURL        string `json:"dataHubUrl"`
	PeerID            string `json:"peerId"`
	ClientName        string `json:"clientName"`
	AttemptCount      int    `json:"attemptCount,omitempty"`
	AnnouncedAtUnixMs int64  `json:"announcedAtUnixMs,omitempty"`
}

type msBlockFixture struct {
	Hash       string `json:"hash"`
	Height     uint32 `json:"height"`
	Header     string `json:"header"`
	Coinbase   string `json:"coinbase"`
	DataHubURL string `json:"dataHubUrl"`
	PeerID     string `json:"peerId"`
	ClientName string `json:"clientName"`
}

func TestSubtreeMessageDecodesAsFixture(t *testing.T) {
	msg := SubtreeMessage{
		Hash:              "aa11",
		DataHubURL:        "http://192.0.2.1:9165/api/v1",
		PeerID:            "12D3KooWtest",
		ClientName:        "arcade-bridge",
		AnnouncedAtUnixMs: time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC).UnixMilli(),
	}
	b, err := json.Marshal(msg)
	if err != nil {
		t.Fatal(err)
	}
	var got msSubtreeFixture
	if err := json.Unmarshal(b, &got); err != nil {
		t.Fatal(err)
	}
	if got.Hash != msg.Hash || got.DataHubURL != msg.DataHubURL ||
		got.PeerID != msg.PeerID || got.ClientName != msg.ClientName ||
		got.AnnouncedAtUnixMs != msg.AnnouncedAtUnixMs {
		t.Fatalf("fixture mismatch: %+v vs %+v", got, msg)
	}
	if got.AttemptCount != 0 {
		t.Fatalf("live announcements must not carry AttemptCount, got %d", got.AttemptCount)
	}
}

// TestSubtreeMessageFieldNames pins the exact wire keys — a renamed Go field
// with a preserved struct shape would still pass the round-trip test above,
// so the raw JSON is asserted too.
func TestSubtreeMessageFieldNames(t *testing.T) {
	b, _ := json.Marshal(SubtreeMessage{Hash: "h", DataHubURL: "u", PeerID: "p", ClientName: "c", AnnouncedAtUnixMs: 7})
	want := `{"hash":"h","dataHubUrl":"u","peerId":"p","clientName":"c","announcedAtUnixMs":7}`
	if string(b) != want {
		t.Fatalf("wire drift:\n got %s\nwant %s", b, want)
	}
}

func TestBlockMessageFieldNames(t *testing.T) {
	b, _ := json.Marshal(BlockMessage{Hash: "h", Height: 9, Header: "aa", Coinbase: "bb", DataHubURL: "u", PeerID: "p", ClientName: "c"})
	want := `{"hash":"h","height":9,"header":"aa","coinbase":"bb","dataHubUrl":"u","peerId":"p","clientName":"c"}`
	if string(b) != want {
		t.Fatalf("wire drift:\n got %s\nwant %s", b, want)
	}
}

func TestBlockMessageDecodesAsFixture(t *testing.T) {
	msg := BlockMessage{Hash: "cc", Height: 11729, Header: "00ff", Coinbase: "01ff",
		DataHubURL: "http://192.0.2.1:9165/api/v1", PeerID: "p", ClientName: "c"}
	b, err := json.Marshal(msg)
	if err != nil {
		t.Fatal(err)
	}
	var got msBlockFixture
	if err := json.Unmarshal(b, &got); err != nil {
		t.Fatal(err)
	}
	if got != msBlockFixture(msg) {
		t.Fatalf("fixture mismatch: %+v vs %+v", got, msg)
	}
}

func TestBlockHeightBounds(t *testing.T) {
	p := &Producer{now: time.Now}
	err := p.Block(t.Context(), "h", uint64(1)<<40, nil, nil)
	if err == nil {
		t.Fatal("height above uint32 must be refused before any produce")
	}
}
