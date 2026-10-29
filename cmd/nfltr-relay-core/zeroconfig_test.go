// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"bufio"
	"encoding/json"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/nfltr/relay/client/agent"
)

// TestZeroConfigServe: `serve` without --from-env needs no environment:
// it listens on loopback, writes a key file 0600 holding one generated key
// in its working directory, and admits an agent with that key; a second
// start reuses the file.
func TestZeroConfigServe(t *testing.T) {
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	start := func() (grpcAddr string) {
		cmd := exec.Command(exe, "serve")
		cmd.Dir = dir
		cmd.Env = []string{childEnv + "=1", "HOME=" + dir, "PROXY_PORT=0", "GRPC_PORT=0", "INTERNAL_PORT=0", "METRICS_PORT=0"}
		stderr, err := cmd.StderrPipe()
		if err != nil {
			t.Fatal(err)
		}
		if err := cmd.Start(); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = cmd.Process.Kill(); _ = cmd.Wait() })
		got := make(chan string, 1)
		go func() {
			sc := bufio.NewScanner(stderr)
			for sc.Scan() {
				var line struct{ Msg, Grpc, Proxy string }
				if json.Unmarshal(sc.Bytes(), &line) == nil && line.Msg == "relay core serving" {
					got <- line.Grpc + " " + line.Proxy
				}
			}
			close(got)
		}()
		select {
		case addrs, ok := <-got:
			if !ok {
				t.Fatal("zero-config serve exited before serving")
			}
			for _, a := range strings.Fields(addrs) {
				if !strings.HasPrefix(a, "127.0.0.1:") {
					t.Fatalf("zero-config serves on %s, want loopback", a)
				}
			}
			return strings.Fields(addrs)[0]
		case <-time.After(30 * time.Second):
			t.Fatal("zero-config serve did not serve within 30 s")
		}
		return ""
	}
	grpcAddr := start()
	path := filepath.Join(dir, zeroKeyFile)
	fi, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if fi.Mode().Perm() != 0o600 {
		t.Fatalf("key file mode %v, want 0600", fi.Mode().Perm())
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(strings.TrimSpace(string(data)), "\n")
	_, key, ok := strings.Cut(lines[len(lines)-1], ":")
	if !ok || !strings.HasPrefix(key, "rpc_") {
		t.Fatalf("key file holds no generated key")
	}
	a := agent.New(agent.Config{ServerAddr: grpcAddr, AgentID: "web", APIKey: key, Retries: 1,
		Routes: []agent.Route{{Backend: "http://127.0.0.1:1"}}, Logger: slog.New(slog.DiscardHandler)})
	if err := a.Connect(t.Context()); err != nil {
		t.Fatalf("an agent with the generated key: %v", err)
	}
	a.Close()

	start()
	if again, _ := os.ReadFile(path); string(again) != string(data) {
		t.Fatal("a second zero-config start rewrote the key file")
	}
}
