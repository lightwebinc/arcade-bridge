package uptunnel

import (
	"context"
	"net"
	"testing"
	"time"
)

// collect accepts one connection and returns everything read from it.
func collect(t *testing.T, ln net.Listener, out chan<- []byte) {
	t.Helper()
	go func() {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		buf := make([]byte, 0, 1024)
		tmp := make([]byte, 256)
		for {
			_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
			n, err := conn.Read(tmp)
			buf = append(buf, tmp[:n]...)
			if err != nil {
				break
			}
		}
		out <- buf
	}()
}

func TestSubmitStreams(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	got := make(chan []byte, 1)
	collect(t, ln, got)

	c := &Client{Addrs: []string{ln.Addr().String()}}
	defer c.Close()
	if err := c.Submit(context.Background(), []byte("aaaa")); err != nil {
		t.Fatal(err)
	}
	if err := c.Submit(context.Background(), []byte("bbbb")); err != nil {
		t.Fatal(err)
	}
	c.Close()
	if s := string(<-got); s != "aaaabbbb" {
		t.Fatalf("stream carried %q", s)
	}
	if st := c.Stats(); st.Sent != 2 || st.Bytes != 8 {
		t.Fatalf("stats %+v", st)
	}
}

func TestFailoverToSecondAddr(t *testing.T) {
	// First address refuses (closed listener); second accepts.
	dead, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	deadAddr := dead.Addr().String()
	dead.Close()

	live, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer live.Close()
	got := make(chan []byte, 1)
	collect(t, live, got)

	c := &Client{Addrs: []string{deadAddr, live.Addr().String()}}
	defer c.Close()
	if err := c.Submit(context.Background(), []byte("xx")); err != nil {
		t.Fatalf("failover submit: %v", err)
	}
	c.Close()
	if s := string(<-got); s != "xx" {
		t.Fatalf("stream carried %q", s)
	}
	if st := c.Stats(); st.Failures == 0 {
		t.Fatal("first-address failure must be counted")
	}
}

func TestNoAddrs(t *testing.T) {
	c := &Client{}
	if err := c.Submit(context.Background(), []byte("x")); err == nil {
		t.Fatal("want error with no addresses")
	}
}
