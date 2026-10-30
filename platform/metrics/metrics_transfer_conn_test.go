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
		c := CountConn(a, p, time.Now(), agentSide, true)
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
	p := PathWG // no production caller: the test owns its counters
	before, firstBefore := scrape(t, p), histCount(t, p)
	r := httptest.NewRequest(http.MethodPost, "/", strings.NewReader("hello"))
	rec := httptest.NewRecorder()
	w := CountHTTP(rec, r, p, time.Now())
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
