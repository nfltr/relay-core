// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package tlsconfig

import (
	"net"
	"testing"
)

func TestRefusePlaintextOffLoopback(t *testing.T) {
	for listen, refused := range map[string]bool{"127.0.0.1": false, "::1": false, "127.0.0.2": false, "0.0.0.0": true, "": true, "192.168.1.10": true, "localhost": true, "::": true} {
		if got := RefusePlaintext(listen) != nil; got != refused {
			t.Errorf("RefusePlaintext(%q) refused=%v, want %v", listen, got, refused)
		}
	}
}

func TestWrapListenerNilConfig(t *testing.T) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close() //nolint:errcheck
	if WrapListener(l, nil) != l || WrapGRPCListener(l, nil) != l {
		t.Fatal("a nil config wraps the listener")
	}
}
