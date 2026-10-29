// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package session

import (
	"io"
	"net"
	"testing"
	"time"
)

// TestTCPTunnelStreamLossCutsSessions: a session on an agent's frame
// streams whose stream ends under it (the relay ends the agent's primary:
// no heartbeat, a silent agent link, a superseded session) ends cut, never
// with a clean end: the agent never closed it, so its client must not
// take the bytes it got for the whole stream. Both entries: a TCP
// endpoint's visitor (an RST) and an in-process session (DialAgent:
// tcp-connect's data connections and the replica link join it, and a read
// error there aborts their session typed, link.CodeConnLost). Before the
// fix the stream's end closed its sessions as the agent's close does
// (io.EOF): the kind soak's held session that "ended cleanly with bytes
// not echoed" after its agent's heartbeat timed out.
func TestTCPTunnelStreamLossCutsSessions(t *testing.T) {
	for _, tc := range []struct {
		name string
		ag   harnessAgent
	}{
		{"data-conns", agentDataConns},
		{"aux-grpc", agentAuxOnly},
		{"one-grpc", agentOneConn},
	} {
		t.Run(tc.name, func(t *testing.T) {
			for _, e := range []struct {
				name string
				dial func(h *tunnelHarness, tgt string) (net.Conn, error)
			}{
				{"endpoint", func(h *tunnelHarness, _ string) (net.Conn, error) { return h.Dial(t), nil }},
				{"in-process", func(h *tunnelHarness, tgt string) (net.Conn, error) {
					return h.D.DialAgent(t.Context(), harnessAgentID, tgt)
				}},
			} {
				t.Run(e.name, func(t *testing.T) {
					tgt := startTarget(t) // a harness each: the rig's agent does not connect again
					h := startTunnelHarnessWith(t, tgt, nil, "", 0, relayCurrent, tc.ag)
					waitUntil(t, func() bool { return primaryStream(h) != nil }, func() string { return "no primary stream" })
					c, err := e.dial(h, tgt)
					if err != nil {
						t.Fatal(err)
					}
					defer c.Close() //nolint:errcheck
					if _, err := c.Write(targetHeader('d', 1<<40)); err != nil {
						t.Fatal(err)
					}
					buf := make([]byte, 64<<10)
					_ = c.SetReadDeadline(time.Now().Add(10 * time.Second))
					if _, err := io.ReadFull(c, buf[:1]); err != nil {
						t.Fatalf("no download before the stream ended: %v", err)
					}
					p := primaryStream(h)
					p.silentOnce.Do(func() { close(p.silent) }) // the relay ends the agent's primary
					for err == nil {                            // drain buffered bytes, then read the end
						_, err = c.Read(buf)
					}
					if err == io.EOF {
						t.Fatal("the client read a clean end (its 1<<40-byte download complete) after the agent's stream ended under the session")
					}
				})
			}
		})
	}
}

func primaryStream(h *tunnelHarness) *tcpAgentStream {
	h.D.Mu.RLock()
	defer h.D.Mu.RUnlock()
	return h.D.Streams[harnessAgentID]
}
