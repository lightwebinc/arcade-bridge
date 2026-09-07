// Package uptunnel streams bare extended-format transactions to the fabric's
// open tx ingress over a long-lived TCP connection through the consumer
// tunnel.
//
// The stream is self-delimiting — no length prefix, no framing — so a
// partial write desynchronises the reader; any write error therefore closes
// the connection and the next submit redials. Several ingress addresses may
// be given (a dual-homed tunnel's per-side inners): a failed submit advances
// to the next address before retrying once, so a side flip does not strand
// submissions on the dead side.
package uptunnel

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

// Client is safe for concurrent use; writes serialize on one connection.
type Client struct {
	Addrs   []string      // host:port, tried in order
	Timeout time.Duration // per-write ceiling when the ctx has no deadline (default 10s)
	Log     *slog.Logger

	mu   sync.Mutex
	conn net.Conn
	idx  int

	sent, bytes, failures, redials atomic.Uint64
}

// Stats is a point-in-time snapshot.
type Stats struct{ Sent, Bytes, Failures, Redials uint64 }

// Stats returns a counter snapshot.
func (c *Client) Stats() Stats {
	return Stats{Sent: c.sent.Load(), Bytes: c.bytes.Load(), Failures: c.failures.Load(), Redials: c.redials.Load()}
}

// Submit writes one transaction's bytes. On a write error it fails over to
// the next address and retries once; the second failure is returned.
func (c *Client) Submit(ctx context.Context, b []byte) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	var lastErr error
	for attempt := 0; attempt < 2; attempt++ {
		if err := c.ensureConn(ctx); err != nil {
			lastErr = err
			c.failures.Add(1)
			c.advance()
			continue
		}
		dl, ok := ctx.Deadline()
		if !ok {
			t := c.Timeout
			if t == 0 {
				t = 10 * time.Second
			}
			dl = time.Now().Add(t)
		}
		_ = c.conn.SetWriteDeadline(dl)
		if _, err := c.conn.Write(b); err != nil {
			// A partial write poisons the stream: drop the connection.
			_ = c.conn.Close()
			c.conn = nil
			c.failures.Add(1)
			c.advance()
			lastErr = err
			continue
		}
		c.sent.Add(1)
		c.bytes.Add(uint64(len(b)))
		return nil
	}
	return fmt.Errorf("uptunnel: %w", lastErr)
}

// Close releases the connection.
func (c *Client) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.conn != nil {
		err := c.conn.Close()
		c.conn = nil
		return err
	}
	return nil
}

func (c *Client) ensureConn(ctx context.Context) error {
	if c.conn != nil {
		return nil
	}
	if len(c.Addrs) == 0 {
		return fmt.Errorf("no ingress addresses")
	}
	d := net.Dialer{Timeout: 5 * time.Second}
	conn, err := d.DialContext(ctx, "tcp", c.Addrs[c.idx%len(c.Addrs)])
	if err != nil {
		return err
	}
	c.conn = conn
	c.redials.Add(1)
	if c.Log != nil {
		c.Log.Info("up-tunnel connected", "addr", conn.RemoteAddr().String())
	}
	return nil
}

// advance moves to the next ingress address (no-op with a single address).
func (c *Client) advance() {
	if len(c.Addrs) > 1 {
		c.idx++
	}
}
