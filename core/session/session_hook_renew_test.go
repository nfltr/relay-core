// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package session

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nfltr/relay/proto/pb"
	"google.golang.org/grpc/metadata"
)

// renewHook's session sends a data frame to the agent every millisecond
// (a WireGuard tunnel carrying traffic) and counts its renewals.
type renewHook struct{ renewed atomic.Int32 }

func (h *renewHook) Join(context.Context, string, string, metadata.MD) (HookedSession, error) {
	return h, nil
}
func (h *renewHook) Claims(string) bool { return false }
func (h *renewHook) Start(send func(*pb.HTTPProxyRequest) error, _ func()) error {
	go func() {
		for send(&pb.HTTPProxyRequest{Request: &pb.HTTPRequest{TransactionId: "busy", Method: "WG"}}) == nil {
			time.Sleep(time.Millisecond)
		}
	}()
	return nil
}
func (h *renewHook) Frame(string, []byte) {}
func (h *renewHook) Renew()               { h.renewed.Add(1) }
func (h *renewHook) Close()               {}

// A hooked session whose stream keeps carrying data still renews (relay-server
// spec/IPAM.tla "busy-skips-renew"): its heartbeats are skipped, its renewal
// is not, else a busy tunnel's address expires under it.
func TestBusyHookedSessionStillRenews(t *testing.T) {
	d, cancel := newTestDispatcher(t)
	defer cancel()
	d.Config.HeartbeatSendDuration = 300 * time.Millisecond
	h := &renewHook{}
	d.SessionHook = h
	client, ctx := dialDispatcher(t, serveDispatcherAs(t, d, "busy-renew"))
	stream, err := client.ConnectHTTP(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = stream.CloseSend() }()
	for deadline := time.Now().Add(2 * time.Second); h.renewed.Load() == 0 && time.Now().Before(deadline); {
		if _, err := stream.Recv(); err != nil {
			t.Fatal(err)
		}
	}
	if h.renewed.Load() == 0 {
		t.Fatal("a session sending data every millisecond was never renewed")
	}
}
