// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package session

import (
	"bytes"
	"fmt"
	"io"
	"testing"
	"time"
)

// BenchmarkTCPTunnelBusyAgentRTT moves 8 concurrent sessions through an
// agent connection with 40 ms RTT, on the agent link (current) or on
// auxiliary gRPC connections (round 4): per session the agent link
// moves at most a window per round trip, a gRPC stream at most its
// HTTP/2 window shared by its sessions.
func BenchmarkTCPTunnelBusyAgentRTT(b *testing.B) {
	const sessions, perSession = 8, 8 << 20
	for _, dir := range []string{"upload", "download"} {
		for name, ag := range map[string]harnessAgent{"agentlink": agentCurrent, "grpc": agentAuxOnly} {
			b.Run(fmt.Sprintf("%s/%s", dir, name), func(b *testing.B) {
				h := startTunnelHarnessWith(b, startTarget(b), nil, "", 40*time.Millisecond, relayCurrent, ag)
				move := upload
				if dir == "download" {
					move = download
				}
				// Busy first: the agent opens its further connections.
				if err := runConcurrent(sessions, func() error { return move(h, b, 1<<10) }); err != nil {
					b.Fatal(err)
				}
				if ag != agentCurrent { // the agent link opens no further streams
					h.WaitStreams(b, tcpAgentStreams)
				}
				b.SetBytes(sessions * perSession)
				for b.Loop() {
					if err := runConcurrent(sessions, func() error { return move(h, b, perSession) }); err != nil {
						b.Fatal(err)
					}
				}
			})
		}
	}
}

// BenchmarkTCPTunnelBusyAgentEcho is request/response latency: one session
// of a busy agent (its further connections open) echoing 1 KiB at a time.
func BenchmarkTCPTunnelBusyAgentEcho(b *testing.B) {
	for name, ag := range map[string]harnessAgent{"agentlink": agentCurrent, "grpc": agentAuxOnly} {
		b.Run(name, func(b *testing.B) {
			h := startTunnelHarnessWith(b, startTarget(b), nil, "", 0, relayCurrent, ag)
			if err := runConcurrent(8, func() error { return upload(h, b, 1<<10) }); err != nil {
				b.Fatal(err)
			}
			if ag != agentCurrent { // the agent link opens no further streams
				h.WaitStreams(b, tcpAgentStreams)
			}
			c := h.Dial(b)
			defer c.Close() //nolint:errcheck
			if _, err := c.Write(targetHeader('e', 0)); err != nil {
				b.Fatal(err)
			}
			msg := make([]byte, 1<<10)
			for b.Loop() {
				if _, err := c.Write(msg); err != nil {
					b.Fatal(err)
				}
				if _, err := io.ReadFull(c, msg); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// TestTCPTunnelAuxStreamsNegotiated: an agent's further streams are
// auxiliary gRPC connections, one per stream, whose sessions keep
// per-session credit where both sides know it: not with a relay from
// before link phase 1b or an agent from round 4. An agent whose agent link
// cannot connect (an HTTPS proxy) gets them too. Sessions arrive intact
// every way.
func TestTCPTunnelAuxStreamsNegotiated(t *testing.T) {
	for _, tc := range []struct {
		name   string
		relay  harnessRelay
		agent  harnessAgent
		credit bool // on the gRPC streams
	}{
		{"current", relayCurrent, agentNoLink, true},
		{"relay without credit", relayNoCredit, agentNoLink, false},
		{"older agent", relayCurrent, agentAuxOnly, false},
		{"agent link fails", relayCurrent, agentLinkFails, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := startTunnelHarnessWith(t, startTarget(t), nil, "", 0, tc.relay, tc.agent)
			payload := bytes.Repeat([]byte("0123456789abcdef"), 8<<10)
			if err := runConcurrent(8, func() error {
				if err := echo(h, t, payload); err != nil {
					return err
				}
				if err := upload(h, t, 1<<20); err != nil {
					return err
				}
				return download(h, t, 1<<20)
			}); err != nil {
				t.Fatal(err)
			}
			h.WaitStreams(t, tcpAgentStreams)
			h.WaitConns(t, tcpAgentStreams) // one connection per stream
			h.D.Mu.RLock()
			defer h.D.Mu.RUnlock()
			for _, as := range h.D.All[harnessAgentID] {
				if (as.cr != nil) != tc.credit {
					t.Errorf("credit on a gRPC stream: %v, want %v", as.cr != nil, tc.credit)
				}
			}
		})
	}
}
