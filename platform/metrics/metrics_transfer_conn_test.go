// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package metrics

import (
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	dtomodel "github.com/prometheus/client_model/go"
)

func histCount(t *testing.T, p Path) uint64 {
	t.Helper()
	m := &dtomodel.Metric{}
	if err := transferFirstByteSeconds.WithLabelValues(p.String()).(prometheus.Histogram).Write(m); err != nil {
		t.Fatal(err)
	}
	return m.GetHistogram().GetSampleCount()
}

// TestCountConnCountsBothDirectionsAndOneFirstByte: a counted visitor
// conn's reads go to the agent, its writes came from it, the first write
// is the first byte; on the agent's side the directions swap.
func TestCountConnCountsBothDirectionsAndOneFirstByte(t *testing.T) {
	for _, agentSide := range []bool{false, true} {
		p := PathSSH // no production caller: the test owns its counters
		before := scrape(t, p)
		firstBefore := histCount(t, p)
		a, b := net.Pipe()
		c := CountConn(a, p, nil, time.Now(), agentSide, true)
		go func() {
			_, _ = b.Write([]byte("12345"))
			_, _ = io.ReadFull(b, make([]byte, 3))
			_, _ = io.ReadFull(b, make([]byte, 4))
			_ = b.Close()
		}()
		if _, err := io.ReadFull(c, make([]byte, 5)); err != nil {
			t.Fatal(err)
		}
		_, _ = c.Write([]byte("abc"))
		_, _ = c.Write([]byte("defg"))
		_ = c.Close()
		after := scrape(t, p)
		read, written := 5.0, 7.0
		to, from := read, written
		if agentSide {
			to, from = written, read
		}
		if got := after["to_agent"] - before["to_agent"]; got != to {
			t.Errorf("agentSide=%v: to_agent %v, want %v", agentSide, got, to)
		}
		if got := after["from_agent"] - before["from_agent"]; got != from {
			t.Errorf("agentSide=%v: from_agent %v, want %v", agentSide, got, from)
		}
		if got := histCount(t, p) - firstBefore; got != 1 {
			t.Errorf("agentSide=%v: %d first bytes observed, want 1", agentSide, got)
		}
		if _, ok := c.(interface{ Reset() error }); !ok {
			t.Error("a counted conn hides Reset: peerlink.Join could not cut it with an RST")
		}
	}
}

// TestCountHTTPCountsBodiesAndHeaderAsFirstByte: a request's body goes to
// the agent, the response's comes from it, its header is the first byte.
func TestCountHTTPCountsBodiesAndHeaderAsFirstByte(t *testing.T) {
	p := PathP2PRelay // no caller in this module: the test owns its counters
	before, firstBefore := scrape(t, p), histCount(t, p)
	r := httptest.NewRequest(http.MethodPost, "/", strings.NewReader("hello"))
	rec := httptest.NewRecorder()
	w := CountHTTP(rec, r, p, OpenMeter(p, "", ""), time.Now())
	_, _ = io.ReadAll(r.Body)
	w.WriteHeader(http.StatusCreated)
	_, _ = w.Write([]byte("response"))
	if err := http.NewResponseController(w).Flush(); err != nil {
		t.Fatalf("Flush through the counted writer: %v", err)
	}
	after := scrape(t, p)
	if got := after["to_agent"] - before["to_agent"]; got != 5 {
		t.Errorf("to_agent %v, want 5", got)
	}
	if got := after["from_agent"] - before["from_agent"]; got != 8 {
		t.Errorf("from_agent %v, want 8", got)
	}
	if got := histCount(t, p) - firstBefore; got != 1 {
		t.Errorf("%d first bytes, want 1", got)
	}
	if rec.Code != http.StatusCreated {
		t.Errorf("status %d", rec.Code)
	}
}

// The gRPC path's proxy asserts http.Flusher to send an SSE response's
// header before its body: CountHTTP's writer must be one, flushing w.
func TestCountHTTPWriterFlushes(t *testing.T) {
	rec := httptest.NewRecorder()
	f, ok := CountHTTP(rec, httptest.NewRequest(http.MethodGet, "/", nil), PathHTTPGRPC, OpenMeter(PathHTTPGRPC, "", ""), time.Now()).(http.Flusher)
	if !ok {
		t.Fatal("CountHTTP's writer is not an http.Flusher")
	}
	f.Flush()
	if !rec.Flushed || rec.Code != http.StatusOK {
		t.Fatalf("Flush did not reach w: flushed %v, code %d", rec.Flushed, rec.Code)
	}
}

// endConn is a conn with a link session's end handshake.
type endConn struct {
	net.Conn
	held, acked, synced bool
}

func (c *endConn) CloseWriteSync() error { c.synced = true; return nil }
func (c *endConn) HoldEndAck()           { c.held = true }
func (c *endConn) AckEnd()               { c.acked = true }

// TestCountConnPassesTheEndHandshake (lane RELAYCLOSE): a counted link
// session keeps its end handshake, so peerlink.Join holds its ENDACK and
// waits for the far end's through it (link FeatureEndAck); a counted
// plain conn gets none (Join closes its writes, as before).
func TestCountConnPassesTheEndHandshake(t *testing.T) {
	a, b := net.Pipe()
	defer a.Close()
	defer b.Close()
	e := &endConn{Conn: a}
	c := CountConn(e, PathTCPLink, nil, time.Now(), false, false)
	s, ok := c.(interface {
		CloseWriteSync() error
		HoldEndAck()
		AckEnd()
	})
	if !ok {
		t.Fatal("a counted link session lost its end handshake")
	}
	s.HoldEndAck()
	s.AckEnd()
	_ = s.CloseWriteSync()
	if !e.held || !e.acked || !e.synced {
		t.Fatalf("passed on: held %v acked %v synced %v", e.held, e.acked, e.synced)
	}
	if _, ok := CountConn(b, PathTCPLink, nil, time.Now(), false, false).(interface{ CloseWriteSync() error }); ok {
		t.Fatal("a counted plain conn claims an end handshake")
	}
}

// receiptConn is an endConn that reports the next hop's receipt.
type receiptConn struct {
	endConn
	received bool
}

func (c *receiptConn) CloseWriteSyncReceived(received func()) error {
	c.received = true
	received()
	return nil
}

// TestCountConnPassesTheReceipt (lane ENDACKTRUTH): a counted link session
// keeps CloseWriteSyncReceived, so a peerlink.Join onto it ends its hold
// at the next hop's receipt (link spec/HopHold.tla, mutant hold-until-ack);
// a counted session without one claims none.
func TestCountConnPassesTheReceipt(t *testing.T) {
	a, b := net.Pipe()
	defer a.Close()
	defer b.Close()
	e := &receiptConn{endConn: endConn{Conn: a}}
	r, ok := CountConn(e, PathTCPLink, nil, time.Now(), false, false).(interface {
		CloseWriteSyncReceived(func()) error
	})
	if !ok {
		t.Fatal("a counted link session lost CloseWriteSyncReceived")
	}
	ran := false
	if err := r.CloseWriteSyncReceived(func() { ran = true }); err != nil || !e.received || !ran {
		t.Fatalf("passed on: %v, received %v, ran %v", err, e.received, ran)
	}
	if _, ok := CountConn(&endConn{Conn: b}, PathTCPLink, nil, time.Now(), false, false).(interface {
		CloseWriteSyncReceived(func()) error
	}); ok {
		t.Fatal("a counted session without a receipt claims one")
	}
}
