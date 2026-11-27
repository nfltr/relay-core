// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package connect

import (
	"bufio"
	"errors"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"github.com/nfltr/relay-core/core/identity"
	"github.com/nfltr/relay-core/core/session"
	"github.com/nfltr/relay/proto/wire/dataplane"
)

// TestSilentUpgradeIsClosed: a client that upgrades /connect to a data
// connection and then sends no hello is closed at DataPlaneSniffTimeout; it
// does not hold its connection and goroutine for as long as it likes (a
// relay bug class seen elsewhere: no handshake deadline after an upgrade).
func TestSilentUpgradeIsClosed(t *testing.T) {
	t.Parallel()
	f := &Frontend{Core: &session.Core{TCPDispatcher: session.NewTCPDispatcher(slog.New(slog.DiscardHandler), nil, nil)}}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		f.serveData(w, r, identity.Principal{Provider: "test"}, Config{E2EEPolicy: "allow"})
	}))
	defer srv.Close()
	nc, err := net.Dial("tcp", srv.Listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close() //nolint:errcheck
	req := "GET " + dataplane.UpgradePath + " HTTP/1.1\r\nHost: relay\r\nConnection: Upgrade\r\nUpgrade: " + dataplane.UpgradeToken + "\r\n\r\n"
	if _, err := nc.Write([]byte(req)); err != nil {
		t.Fatal(err)
	}
	br := bufio.NewReader(nc)
	resp, err := http.ReadResponse(br, nil)
	if err != nil || resp.StatusCode != http.StatusSwitchingProtocols {
		t.Fatalf("upgrade answered %v, %v; want 101", resp, err)
	}
	bound := session.DataPlaneSniffTimeout + 5*time.Second
	_ = nc.SetReadDeadline(time.Now().Add(bound))
	start := time.Now()
	if _, err := io.Copy(io.Discard, br); errors.Is(err, os.ErrDeadlineExceeded) {
		t.Fatalf("a silent upgraded connection still open after %v", bound)
	}
	t.Logf("closed after %v", time.Since(start).Round(time.Millisecond))
}
