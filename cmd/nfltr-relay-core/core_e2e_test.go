// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"bufio"
	"bytes"
	"crypto/tls"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/nfltr/link"
	"github.com/nfltr/relay/client/agent"
	pb "github.com/nfltr/relay/proto/pb"
	"github.com/nfltr/relay/proto/wire/agentwire"
	"github.com/nfltr/relay/proto/wire/dataplane"
)

// childEnv makes this test binary run nfltr-relay-core (TestMain).
const childEnv = "NFLTR_RELAY_CORE_CHILD"

// coreProc is an nfltr-relay-core process this test started.
type coreProc struct {
	cmd                        *exec.Cmd
	proxy, grpc, internal, sni string
	done                       chan struct{} // closed once it exited (err)
	err                        error
}

// startCoreProcess runs nfltr-relay-core serve --from-env (this test binary
// as it) on loopback ports the kernel picks, the key file identity holding
// testKey, TCP on, and env on top; the addresses come from its "relay core
// serving" line.
func startCoreProcess(t *testing.T, env ...string) *coreProc {
	t.Helper()
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(exe, "serve", "--from-env")
	cmd.Env = coreEnv(t, env...)
	stderr, err := cmd.StderrPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	p := &coreProc{cmd: cmd, done: make(chan struct{})}
	ready := make(chan error, 1)
	go func() {
		sc := bufio.NewScanner(stderr)
		for sc.Scan() {
			if os.Getenv("CORE_LOG") != "" {
				_, _ = os.Stderr.Write(append(sc.Bytes(), '\n'))
			}
			var line struct{ Msg, Proxy, Grpc, Internal, Sni string }
			if json.Unmarshal(sc.Bytes(), &line) == nil && line.Msg == "relay core serving" {
				p.proxy, p.grpc, p.internal, p.sni = line.Proxy, line.Grpc, line.Internal, line.Sni
				ready <- nil
			}
		}
		ready <- errors.New("nfltr-relay-core exited before serving")
		p.err = cmd.Wait()
		close(p.done)
	}()
	t.Cleanup(func() {
		_ = cmd.Process.Kill()
		<-p.done
	})
	select {
	case err := <-ready:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("nfltr-relay-core did not serve within 30 s")
	}
	return p
}

// coreEnv is the environment of an nfltr-relay-core child (TestMain):
// loopback ports the kernel picks, the key file identity holding testKey,
// TCP on, and env on top.
func coreEnv(t *testing.T, env ...string) []string {
	return append(append(os.Environ(), childEnv+"=1", "NFLTR_API_KEY="+testKey, "TCP_ENABLED=true",
		"PROXY_PORT=0", "GRPC_PORT=0", "INTERNAL_PORT=0", "METRICS_PORT=0",
		"LISTEN_ADDRESS=127.0.0.1", "INTERNAL_ADDRESS=127.0.0.1", "TERMINATION_GRACE_PERIODSECONDS=20",
		"SECURITY_APIKEY_FILE="+t.TempDir()+"/none"), env...)
}

// serveAgent connects agent "web" with testKey's key (the relay SDK's
// agent; over TLS with relayTLS, when given): its HTTP to a backend that answers what it saw, its TCP sessions
// to an echo server.
func serveAgent(t *testing.T, grpcAddr string, relayTLS *tls.Config, opts ...agent.Option) {
	t.Helper()
	echo, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = echo.Close() })
	go func() {
		for {
			c, err := echo.Accept()
			if err != nil {
				return
			}
			go func() { _, _ = io.Copy(c, c); _ = c.Close() }()
		}
	}()
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, "web saw "+r.URL.Path+" key="+r.Header.Get("X-API-Key"))
	}))
	t.Cleanup(backend.Close)
	a := agent.New(agent.Config{ServerAddr: grpcAddr, AgentID: "web", APIKey: testKey[len("alice:"):], Retries: 1,
		Routes: []agent.Route{{Backend: backend.URL}}, TCPTunnel: true, TLSConfig: relayTLS, TCPForwardAddr: echo.Addr().String(),
		Logger: slog.New(slog.DiscardHandler)}, opts...)
	if err := a.Connect(t.Context()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(a.Close)
}

// dialData opens a data connection to the core's proxy port with key
// (over TLS with tlsCfg, when given).
func dialData(t *testing.T, proxy, key string, tlsCfg ...*tls.Config) (*dataplane.Mux, error) {
	t.Helper()
	var nc net.Conn
	var err error
	if len(tlsCfg) > 0 {
		nc, err = tls.Dial("tcp", proxy, tlsCfg[0])
	} else {
		nc, err = net.Dial("tcp", proxy)
	}
	if err != nil {
		return nil, err
	}
	m, _, err := dataplane.Upgrade(t.Context(), nc, "relay", key, 4<<20)
	if err != nil {
		_ = nc.Close()
		return nil, err
	}
	go func() { _ = m.Serve(nil) }()
	t.Cleanup(func() { _ = m.Close() })
	return m, nil
}

// echoed writes msg on c and reads it back.
func echoed(c io.ReadWriter, msg []byte) error {
	if _, err := c.Write(msg); err != nil {
		return err
	}
	got := make([]byte, len(msg))
	if _, err := io.ReadFull(c, got); err != nil {
		return err
	}
	if !bytes.Equal(got, msg) {
		return errors.New("echo mismatch")
	}
	return nil
}

// TestCoreProcessCarriesSessionsAndDrains: nfltr-relay-core alone (key
// file identity, no RPCServer, no database) carries an owner's sessions
// to its agent's port, as data connection sessions and as a hijacked
// /connect tunnel, and the key's own HTTP requests to the agent; refuses a
// stranger and an unknown agent; and drains on
// SIGTERM: no new client, GOAWAY on the open data connection, the open
// session runs on, and the process exits once it ended.
func TestCoreProcessCarriesSessionsAndDrains(t *testing.T) {
	p := startCoreProcess(t)
	serveAgent(t, p.grpc, nil)
	key := testKey[len("alice:"):]

	if _, err := dialData(t, p.proxy, "rpc_unknownkeyaaaaaaaaaaaaaaaaaaaaaaaaaaa"); func() bool {
		var nu *dataplane.NoUpgradeError
		return !errors.As(err, &nu) || nu.Status != http.StatusUnauthorized
	}() {
		t.Fatalf("an unknown key's upgrade: %v, want 401", err)
	}
	m, err := dialData(t, p.proxy, key)
	if err != nil {
		t.Fatal(err)
	}
	var s *dataplane.Session
	deadline := time.Now().Add(15 * time.Second)
	for { // the agent registers its TCP stream asynchronously
		if s, err = m.Open(&pb.TCPData{AgentId: "web", TargetAddr: "22"}); err == nil {
			if err = echoed(s, []byte("hello through the core")); err == nil {
				break
			}
			_ = s.Close()
		}
		if time.Now().After(deadline) {
			t.Fatalf("no session to the agent: %v", err)
		}
		time.Sleep(100 * time.Millisecond)
	}
	big := bytes.Repeat([]byte("0123456789abcdef"), 64<<10) // 1 MiB
	if err := echoed(s, big); err != nil {
		t.Fatalf("1 MiB echo: %v", err)
	}
	if ghost, err := m.Open(&pb.TCPData{AgentId: "ghost", TargetAddr: "22"}); err == nil {
		_, _ = ghost.Write([]byte("?"))
		if n, err := ghost.Read(make([]byte, 8)); n != 0 || !errors.Is(err, link.ErrOpenRefused) {
			t.Fatalf("an unknown agent's session read %d, %v; want the relay's refusal", n, err)
		}
		_ = ghost.Close()
	}

	// A key's own HTTP request to its agent (HTTP ingress through
	// frontend.CoreHost); the relay's key never reaches the agent.
	req, _ := http.NewRequest(http.MethodGet, "http://"+p.proxy+"/hello", nil)
	req.Header.Set("X-API-Key", key)
	req.Header.Set("X-Agent-Id", "web")
	if res, err := http.DefaultClient.Do(req); err != nil {
		t.Fatal(err)
	} else {
		body, _ := io.ReadAll(res.Body)
		_ = res.Body.Close()
		if res.StatusCode != http.StatusOK || string(body) != "web saw /hello key=" {
			t.Fatalf("own request: %d %q", res.StatusCode, body)
		}
	}
	if res, err := http.Get("http://" + p.proxy + "/hello"); err != nil || res.StatusCode != http.StatusUnauthorized {
		t.Fatalf("a request without a key: %v, %v; want 401", res, err)
	} else {
		_ = res.Body.Close()
	}

	// A released client's tunnel: a hijacked GET.
	nc, err := net.Dial("tcp", p.proxy)
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close() //nolint:errcheck
	_, _ = nc.Write([]byte("GET /connect/web/22 HTTP/1.1\r\nHost: relay\r\nX-API-Key: " + key + "\r\n" + agentwire.GenerationKey + ": " + strconv.Itoa(agentwire.Generation) + "\r\n\r\n"))
	br := bufio.NewReader(nc)
	status, err := br.ReadString('\n')
	if err != nil || !strings.Contains(status, " 200 ") {
		t.Fatalf("hijacked /connect: %q, %v", status, err)
	}
	if _, err := br.ReadString('\n'); err != nil { // the blank line
		t.Fatal(err)
	}
	if err := echoed(struct {
		io.Reader
		io.Writer
	}{br, nc}, []byte("hijacked")); err != nil {
		t.Fatalf("hijacked tunnel: %v", err)
	}
	_ = nc.Close()

	// The drain.
	if err := p.cmd.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "GOAWAY on the open data connection", func() bool {
		o, err := m.Open(&pb.TCPData{AgentId: "web", TargetAddr: "22"})
		if err == nil {
			_ = o.Close()
		}
		return errors.Is(err, link.ErrGoAway)
	})
	if c, err := net.DialTimeout("tcp", p.proxy, time.Second); err == nil {
		_ = c.Close()
		t.Fatal("the proxy port still takes clients during the drain")
	}
	if err := echoed(s, []byte("still open during the drain")); err != nil {
		t.Fatalf("the open session during the drain: %v", err)
	}
	select {
	case <-p.done:
		t.Fatalf("exited with a session open: %v", p.err)
	case <-time.After(300 * time.Millisecond):
	}
	_ = s.Close()
	start := time.Now()
	select {
	case <-p.done:
		if p.err != nil {
			t.Fatalf("exit: %v", p.err)
		}
		t.Logf("exited %v after its last session ended", time.Since(start).Round(time.Millisecond))
	case <-time.After(15 * time.Second):
		t.Fatal("still running 15 s after its last session ended (grace 20 s)")
	}
}

// waitFor polls cond up to 10 s.
func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	for deadline := time.Now().Add(10 * time.Second); !cond(); time.Sleep(50 * time.Millisecond) {
		if time.Now().After(deadline) {
			t.Fatal("timed out waiting for " + what)
		}
	}
}
