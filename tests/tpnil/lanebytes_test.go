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
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"
)

// laneCapture records one direction of a lane's TCP stream from outside both
// ends: tcpdump on the host side of the fabric network, filtered to segments
// sent to the lane's port. A lane carries whole objects back to back with no
// framing of its own, so each connection's reassembled stream is exactly the
// bytes the lane delivered on it while the capture ran.
//
// tcpdump runs as root under sudo, so it is signaled directly, matched by its
// own capture file (a signal to sudo is not always relayed, and SIGKILL never
// is), and every wait for it is bounded.
type laneCapture struct {
	file   string
	done   chan struct{} // closed when tcpdump and sudo have exited
	stderr string        // tcpdump's messages, set before done closes
	ended  bool
}

// startLaneCapture starts tcpdump on iface for TCP segments to port at any of
// hosts (every host when hosts is empty). It needs passwordless sudo, as the
// joined configuration already does.
func startLaneCapture(t *testing.T, iface string, port int, hosts []string) *laneCapture {
	t.Helper()
	c := &laneCapture{file: filepath.Join(t.TempDir(), "lane.pcap"), done: make(chan struct{})}
	filter := "tcp dst port " + strconv.Itoa(port)
	if len(hosts) > 0 {
		filter += " and (dst host " + strings.Join(hosts, " or dst host ") + ")"
	}
	cmd := exec.Command("sudo", "-n", "tcpdump", "-i", iface, "-U", "-n", "-s", "0", "-Z", "root",
		"-w", c.file, filter)
	errOut, err := cmd.StderrPipe()
	if err != nil {
		t.Fatalf("lane capture: %v", err)
	}
	if err := cmd.Start(); err != nil {
		t.Fatalf("lane capture: %v", err)
	}
	// tcpdump reports "listening on" once the capture is armed. The reader
	// reaches EOF only when tcpdump has exited, then reaps sudo.
	armed := make(chan struct{})
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
		_ = cmd.Wait()
		c.stderr = strings.Join(all, "; ")
		close(c.done)
	}()
	t.Cleanup(func() { c.end(t) })
	select {
	case <-armed:
	case <-c.done:
		t.Fatalf("lane capture on %s exited before it started: %s", iface, c.stderr)
	case <-time.After(10 * time.Second):
		c.end(t)
		said := "tcpdump did not exit"
		select {
		case <-c.done:
			said = c.stderr
		default:
		}
		t.Fatalf("lane capture on %s did not start in 10 s: %s", iface, said)
	}
	return c
}

// signal sends sig to this capture's tcpdump. The pattern's bracket keeps it
// from matching the pkill and sudo command lines that carry it.
func (c *laneCapture) signal(sig string) {
	_ = exec.Command("sudo", "-n", "pkill", "-"+sig, "-f", "[t]cpdump .*-w "+regexp.QuoteMeta(c.file)).Run()
}

// end stops tcpdump once: an interrupt, so it flushes its file, then a kill if
// it has not exited in 10 s. It fails the test if tcpdump is still running.
func (c *laneCapture) end(t *testing.T) {
	t.Helper()
	if c.ended {
		return
	}
	c.ended = true
	for _, step := range []struct {
		sig  string
		wait time.Duration
	}{{"INT", 10 * time.Second}, {"KILL", 5 * time.Second}} {
		c.signal(step.sig)
		select {
		case <-c.done:
			return
		case <-time.After(step.wait):
		}
	}
	t.Errorf("lane capture: tcpdump writing %s did not exit", c.file)
}

// stop ends the capture and returns the reassembled stream of each TCP
// connection it saw, in order of first appearance.
func (c *laneCapture) stop(t *testing.T) []flowStream {
	t.Helper()
	time.Sleep(time.Second) // segments already on the wire reach the file
	c.end(t)
	raw, err := os.ReadFile(c.file)
	if err != nil {
		t.Fatalf("lane capture: %v", err)
	}
	flows, err := tcpStreams(raw)
	if err != nil {
		t.Fatalf("lane capture: %v", err)
	}
	return flows
}

// flowStream is one TCP connection's reassembled payload.
type flowStream struct {
	flow  string // source address and port
	bytes []byte
}

type segment struct {
	flow    string
	seq     uint32
	payload []byte
}

// tcpStreams reassembles, per connection, the TCP payload of a classic pcap
// file holding one direction of traffic on Ethernet (IPv4 or IPv6).
// Retransmitted segments are deduplicated by sequence number; a hole is an
// error.
func tcpStreams(raw []byte) ([]flowStream, error) {
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
	var order []string
	byFlow := map[string][]segment{}
	for off := 24; off+16 <= len(raw); {
		n := int(bo.Uint32(raw[off+8:]))
		off += 16
		if off+n > len(raw) {
			return nil, errors.New("pcap: truncated record")
		}
		if s, ok := tcpSegment(raw[off : off+n]); ok && len(s.payload) > 0 {
			if _, seen := byFlow[s.flow]; !seen {
				order = append(order, s.flow)
			}
			byFlow[s.flow] = append(byFlow[s.flow], s)
		}
		off += n
	}
	out := make([]flowStream, 0, len(order))
	for _, f := range order {
		b, err := reassemble(byFlow[f])
		if err != nil {
			return nil, fmt.Errorf("connection from %s: %w", f, err)
		}
		out = append(out, flowStream{flow: f, bytes: b})
	}
	return out, nil
}

// reassemble orders one connection's segments and joins their payloads.
func reassemble(segs []segment) ([]byte, error) {
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
	var src string
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
		src = net.IP(p[12:16]).String()
	case 0x86dd:
		if len(p) < 40 || p[6] != 6 {
			return segment{}, false
		}
		plen := int(binary.BigEndian.Uint16(p[4:]))
		if 40+plen > len(p) {
			return segment{}, false
		}
		tcp = p[40 : 40+plen]
		src = net.IP(p[8:24]).String()
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
	flow := net.JoinHostPort(src, strconv.Itoa(int(binary.BigEndian.Uint16(tcp[0:]))))
	return segment{flow: flow, seq: binary.BigEndian.Uint32(tcp[4:]), payload: tcp[doff:]}, true
}

// containerAddrs returns a container's addresses on every network it joins.
func containerAddrs(t *testing.T, name string) []string {
	t.Helper()
	out, err := exec.Command("sudo", "-n", "docker", "inspect", "-f",
		"{{range .NetworkSettings.Networks}}{{.IPAddress}} {{.GlobalIPv6Address}} {{end}}", name).Output()
	if err != nil {
		t.Fatalf("docker inspect %s: %v", name, err)
	}
	var addrs []string
	for _, a := range strings.Fields(string(out)) {
		if net.ParseIP(a) != nil {
			addrs = append(addrs, a)
		}
	}
	if len(addrs) == 0 {
		t.Fatalf("container %s has no address", name)
	}
	return addrs
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
