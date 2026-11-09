//go:build tpnil

package tpnil

import (
	"bytes"
	"crypto/rand"
	"io"
	"net"
	"os/exec"
	"testing"
)

// TestLaneCaptureLoopback checks the capture and reassembly on real traffic: a
// payload written over loopback TCP comes back byte for byte. It needs
// passwordless sudo and tcpdump, as the joined configuration does.
func TestLaneCaptureLoopback(t *testing.T) {
	if exec.Command("sudo", "-n", "tcpdump", "--version").Run() != nil {
		t.Skip("passwordless sudo tcpdump is not available")
	}
	ln, err := net.Listen("tcp", "[::1]:0")
	if err != nil {
		t.Skipf("IPv6 loopback: %v", err)
	}
	defer func() { _ = ln.Close() }()
	got := make(chan []byte, 1)
	go func() {
		c, err := ln.Accept()
		if err != nil {
			got <- nil
			return
		}
		b, _ := io.ReadAll(c)
		_ = c.Close()
		got <- b
	}()
	payload := make([]byte, 200_000) // many segments
	_, _ = rand.Read(payload)

	capture := startLaneCapture(t, "lo", ln.Addr().(*net.TCPAddr).Port)
	c, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.Write(payload); err != nil {
		t.Fatal(err)
	}
	_ = c.Close()
	if b := <-got; !bytes.Equal(b, payload) {
		t.Fatalf("the receiver read %d bytes, want %d", len(b), len(payload))
	}
	stream := capture.stop(t)
	if !bytes.Equal(stream, payload) {
		t.Fatalf("captured %d bytes (sha256 %s...), sent %d (sha256 %s...)", len(stream), digest(stream), len(payload), digest(payload))
	}
}
