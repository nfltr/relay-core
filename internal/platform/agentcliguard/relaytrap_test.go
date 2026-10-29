// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package agentcliguard

import (
	"net"
	"os"
	"strings"
	"testing"
)

// isolateRelay replaces whatever relay, key and saved config the host or a
// node agent set, counts every dial of its trap and restores the
// environment afterwards.
func TestIsolateRelayTrapsDialsAndRestoresEnv(t *testing.T) {
	t.Setenv(TrapEnv, "") // isolate again, as a fresh test binary would
	t.Setenv("NFLTR_SERVER", "relay.example.invalid:443")
	t.Setenv("NFLTR_API_KEY", "host-key")
	t.Setenv("NFLTR_PROXY_URL", "https://relay.example.invalid")
	home := os.Getenv("HOME")
	dials := isolateRelay()
	addr := os.Getenv(TrapEnv)
	if os.Getenv("NFLTR_PROXY_URL") != "" {
		t.Fatal("NFLTR_PROXY_URL not cleared")
	}
	if os.Getenv("NFLTR_SERVER") != addr || os.Getenv("GRPC_SERVER") != addr || os.Getenv("NFLTR_API_KEY") != "" {
		t.Fatalf("relay env not isolated: server=%q key set=%v", os.Getenv("NFLTR_SERVER"), os.Getenv("NFLTR_API_KEY") != "")
	}
	if h := os.Getenv("HOME"); h == home || !strings.HasPrefix(os.Getenv("NFLTR_CONFIG"), h) {
		t.Fatalf("HOME %q / NFLTR_CONFIG %q not moved to a temp dir", h, os.Getenv("NFLTR_CONFIG"))
	}
	c, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	_, _ = c.Read(make([]byte, 1)) // EOF: the trap counted the dial, then closed it
	_ = c.Close()
	if n := dials(); n != 1 {
		t.Fatalf("dials = %d, want 1", n)
	}
	if os.Getenv("NFLTR_SERVER") != "relay.example.invalid:443" || os.Getenv("NFLTR_API_KEY") != "host-key" || os.Getenv("HOME") != home {
		t.Fatal("environment not restored")
	}
}
