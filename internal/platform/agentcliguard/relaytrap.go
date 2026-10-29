// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package agentcliguard

import (
	"fmt"
	"net"
	"os"
	"path/filepath"
	"sync/atomic"
)

// RelayEnv names what a test binary would read from the host's or a node
// agent's environment to find a relay: its address (gRPC and HTTP proxy,
// the host-local and inventory-origin overrides), credentials, the saved
// config and local relay state (NFLTR_CONFIG, NFLTR_STATE_DIR,
// XDG_STATE_HOME, HOME).
var RelayEnv = []string{
	"NFLTR_SERVER", "GRPC_SERVER", "NFLTR_PROXY_URL", "NFLTR_HOST_LOCAL_SERVER",
	"NFLTR_ORCH_INVENTORY_ORIGIN_URL", "NFLTR_API_KEY", "NFLTR_HOST_LOCAL_API_KEY",
	"NFLTR_LIVE_TEST_API_KEY", "NFLTR_BASIC_AUTH", "NFLTR_CONFIG", "NFLTR_STATE_DIR",
	"XDG_STATE_HOME", "HOME",
}

// goHomeEnv are the Go toolchain paths that default from HOME; they are
// pinned to the real ones so a `go build` inside a test keeps its caches.
var goHomeEnv = []string{"GOCACHE", "GOPATH", "GOENV"}

// TrapEnv carries the trap's address to re-exec'd copies of the test binary
// so they keep their parent's trap and HOME (dials are counted there).
const TrapEnv = "NFLTR_RELAY_GUARD_TRAP"

// isolateRelay keeps a test binary off every real relay (dogfood
// orchcli-split2 R4-G5: a test found a real claude on a node VM and dialed
// the live relay with a test key). Install runs it for every guarded
// package: it points NFLTR_SERVER and GRPC_SERVER at a loopback listener it
// owns, clears every other relay address, key and local relay state
// override, and moves HOME and the saved config (NFLTR_CONFIG) to an empty
// temp directory. A test that talks to a relay starts one itself and sets
// its own address. dials reports how many connections reached the listener
// (each is a test that tried to reach a relay from the ambient environment),
// restores the environment and removes the directory.
func isolateRelay() (dials func() int) {
	if os.Getenv(TrapEnv) != "" {
		return func() int { return 0 }
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		panic(fmt.Sprintf("agentcliguard: relay trap: %v", err))
	}
	dir, err := os.MkdirTemp("", "nfltr-relay-guard-home-*")
	if err != nil {
		panic(fmt.Sprintf("agentcliguard: %v", err))
	}
	var n atomic.Int64
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			n.Add(1)
			_ = c.Close()
			// At dial time, so `go test -v` shows it under the test that dialed.
			fmt.Fprintf(os.Stderr, "agentcliguard: relay trap %s dialed\n", ln.Addr())
		}
	}()
	addr := ln.Addr().String()
	saved := map[string]*string{}
	for _, k := range append(append([]string{TrapEnv}, RelayEnv...), goHomeEnv...) {
		if v, ok := os.LookupEnv(k); ok {
			saved[k] = &v
		} else {
			saved[k] = nil
		}
	}
	set := map[string]string{
		"NFLTR_SERVER": addr, "GRPC_SERVER": addr, TrapEnv: addr,
		"NFLTR_CONFIG": filepath.Join(dir, "nfltr.json"), "HOME": dir,
	}
	// Defaults go derives from HOME, fixed before HOME moves.
	if cache, err := os.UserCacheDir(); err == nil && os.Getenv("GOCACHE") == "" {
		set["GOCACHE"] = filepath.Join(cache, "go-build")
	}
	if home, err := os.UserHomeDir(); err == nil && os.Getenv("GOPATH") == "" {
		set["GOPATH"] = filepath.Join(home, "go")
	}
	if cfg, err := os.UserConfigDir(); err == nil && os.Getenv("GOENV") == "" {
		set["GOENV"] = filepath.Join(cfg, "go", "env")
	}
	for _, k := range RelayEnv {
		_ = os.Unsetenv(k)
	}
	for k, v := range set {
		_ = os.Setenv(k, v)
	}
	return func() int {
		_ = ln.Close()
		_ = os.RemoveAll(dir)
		for k, v := range saved {
			if v == nil {
				_ = os.Unsetenv(k)
			} else {
				_ = os.Setenv(k, *v)
			}
		}
		got := int(n.Load())
		if got > 0 {
			fmt.Fprintf(os.Stderr, "agentcliguard: %d connection(s) to the relay trap %s: a unit test must stay off the network; start a test relay and set its address\n", got, addr)
		}
		return got
	}
}
