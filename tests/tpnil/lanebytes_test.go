//go:build tpnil

package tpnil

import (
	"bufio"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"
)

// laneCapture records one direction of a lane's TCP stream from outside both
// ends: tcpdump on the host side of the fabric network, filtered to segments
// sent to the lane's port. A lane carries whole objects back to back with no
// framing of its own, so the reassembled stream is exactly the bytes the lane
// delivered while the capture ran.
type laneCapture struct {
	cmd  *exec.Cmd
	file string
}

// startLaneCapture starts tcpdump on iface for TCP segments to port. It needs
// passwordless sudo, as the joined configuration already does.
func startLaneCapture(t *testing.T, iface string, port int) *laneCapture {
	t.Helper()
	dir := t.TempDir()
	file := filepath.Join(dir, "lane.pcap")
	cmd := exec.Command("sudo", "-n", "tcpdump", "-i", iface, "-U", "-n", "-s", "0", "-Z", "root",
		"-w", file, "tcp", "dst", "port", strconv.Itoa(port))
	errOut, err := cmd.StderrPipe()
	if err != nil {
		t.Fatalf("lane capture: %v", err)
	}
	if err := cmd.Start(); err != nil {
		t.Fatalf("lane capture: %v", err)
	}
	// tcpdump reports "listening on" once the capture is armed.
	armed, said := make(chan struct{}), make(chan string, 1)
	go func() {
		var all []string
		ready := false
		sc := bufio.NewScanner(errOut)
		for sc.Scan() {
			all = append(all, sc.Text())
			if !ready && strings.Contains(sc.Text(), "listening on") {
				ready = true
				close(armed)
			}
		}
		said <- strings.Join(all, "; ")
	}()
	select {
	case <-armed:
	case <-time.After(10 * time.Second):
		_ = cmd.Process.Kill()
		t.Fatalf("lane capture on %s did not start: %s", iface, <-said)
	}
	t.Cleanup(func() {
		if cmd.ProcessState == nil {
			_ = cmd.Process.Signal(os.Interrupt)
			_ = cmd.Wait()
		}
	})
	return &laneCapture{cmd: cmd, file: file}
}

// stop ends the capture and returns the reassembled stream.
func (c *laneCapture) stop(t *testing.T) []byte {
	t.Helper()
	time.Sleep(time.Second) // segments already on the wire reach the file
	_ = c.cmd.Process.Signal(os.Interrupt)
	_ = c.cmd.Wait()
	raw, err := os.ReadFile(c.file)
	if err != nil {
		t.Fatalf("lane capture: %v", err)
	}
	stream, err := tcpStream(raw)
	if err != nil {
		t.Fatalf("lane capture: %v", err)
	}
	return stream
}

type segment struct {
	seq     uint32
	payload []byte
}

// tcpStream reassembles the TCP payload of a classic pcap file holding one
// direction of one connection on Ethernet (IPv4 or IPv6). Retransmitted
// segments are deduplicated by sequence number; a hole is an error.
func tcpStream(raw []byte) ([]byte, error) {
	if len(raw) < 24 {
		return nil, errors.New("pcap: short file")
	}
	var bo binary.ByteOrder
	switch binary.LittleEndian.Uint32(raw) {
	case 0xa1b2c3d4, 0xa1b23c4d:
		bo = binary.LittleEndian
	case 0xd4c3b2a1, 0x4d3cb2a1:
		bo = binary.BigEndian
	default:
		return nil, errors.New("pcap: not a classic pcap file")
	}
	if lt := bo.Uint32(raw[20:]); lt != 1 {
		return nil, fmt.Errorf("pcap: link type %d, want Ethernet", lt)
	}
	var segs []segment
	for off := 24; off+16 <= len(raw); {
		n := int(bo.Uint32(raw[off+8:]))
		off += 16
		if off+n > len(raw) {
			return nil, errors.New("pcap: truncated record")
		}
		if s, ok := tcpSegment(raw[off : off+n]); ok && len(s.payload) > 0 {
			segs = append(segs, s)
		}
		off += n
	}
	if len(segs) == 0 {
		return nil, nil
	}
	base := segs[0].seq
	sort.SliceStable(segs, func(i, j int) bool { return int32(segs[i].seq-base) < int32(segs[j].seq-base) })
	start := segs[0].seq
	var out []byte
	for _, s := range segs {
		at := int(int32(s.seq - start))
		end := at + len(s.payload)
		switch {
		case at > len(out):
			return nil, fmt.Errorf("tcp: %d bytes missing at stream offset %d", at-len(out), len(out))
		case end <= len(out):
			continue // a retransmission of bytes already held
		default:
			out = append(out, s.payload[len(out)-at:]...)
		}
	}
	return out, nil
}

func tcpSegment(f []byte) (segment, bool) {
	if len(f) < 14 {
		return segment{}, false
	}
	et, p := binary.BigEndian.Uint16(f[12:]), f[14:]
	if et == 0x8100 && len(f) >= 18 {
		et, p = binary.BigEndian.Uint16(f[16:]), f[18:]
	}
	var tcp []byte
	switch et {
	case 0x0800:
		if len(p) < 20 || p[9] != 6 {
			return segment{}, false
		}
		ihl, total := int(p[0]&0x0f)*4, int(binary.BigEndian.Uint16(p[2:]))
		if total > len(p) || ihl > total {
			return segment{}, false
		}
		tcp = p[ihl:total]
	case 0x86dd:
		if len(p) < 40 || p[6] != 6 {
			return segment{}, false
		}
		plen := int(binary.BigEndian.Uint16(p[4:]))
		if 40+plen > len(p) {
			return segment{}, false
		}
		tcp = p[40 : 40+plen]
	default:
		return segment{}, false
	}
	if len(tcp) < 20 {
		return segment{}, false
	}
	doff := int(tcp[12]>>4) * 4
	if doff < 20 || doff > len(tcp) {
		return segment{}, false
	}
	return segment{seq: binary.BigEndian.Uint32(tcp[4:]), payload: tcp[doff:]}, true
}

// fetchObject reads one object over HTTP; a non-200 answer is an error.
func fetchObject(ctx context.Context, url string) ([]byte, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("GET %s: HTTP %d", url, resp.StatusCode)
	}
	return b, nil
}

func digest(b []byte) string {
	h := sha256.Sum256(b)
	return hex.EncodeToString(h[:8])
}
