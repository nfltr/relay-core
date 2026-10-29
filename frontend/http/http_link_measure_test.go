// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package httpingress

// Link measurements: the HTTP tunnel to an agent today (Proxy -> in-memory broker
// -> session.Dispatcher -> ConnectHTTP gRPC stream -> agent processStream) against
// HTTP/1.1 on link sessions (a reverse proxy whose transport dials sessions
// on the agent's data connection; the agent side serves them with net/http).
// HTTP_LINK_MEASURE=1 runs it; it logs numbers, it asserts only that
// requests complete.

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"os"
	"runtime"
	"slices"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/nfltr/relay-core/broker"
	"github.com/nfltr/relay-core/core/directory"
	"github.com/nfltr/relay-core/core/session"
	"github.com/nfltr/relay/client/agent"
	"github.com/nfltr/relay/proto/pb"
	"github.com/nfltr/relay/proto/wire/dataplane"
	"github.com/spf13/viper"
)

// startGRPCHTTPPath is today's path: what dispatchRequestMiddleware hands a
// same-replica request to (Proxy.ServeHTTP), with the broker, dispatcher and
// a pkg/agent HTTP agent wired as server_run.go and relayserver wire them.
func startGRPCHTTPPath(t *testing.T, backend string) (string, func()) {
	h, _ := startGRPCHTTPHandler(t, backend)
	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)
	return srv.URL, func() {}
}

// startGRPCHTTPHandler is startGRPCHTTPPath's relay handler, and its agent.
func startGRPCHTTPHandler(t *testing.T, backend string) (http.Handler, agent.Agent) {
	for k, v := range map[string]any{
		"http.server.producers.cache_size": 10, "http.server.producers.cache_expiry": time.Minute,
		"default.request.timeout": 30 * time.Second, "http.server.fanout_workers": 1,
		"http.server.max_request_body_size": int64(2 << 30),
	} {
		viper.Set(k, v)
	}
	t.Cleanup(viper.Reset)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	b := broker.NewInMemoryMessageBroker()
	_ = b.SetTopicAutoCreation("ns", true)
	const podTopic = "measure-pod"
	_ = b.CreateTopic(podTopic, 1, true)
	sub, err := b.Subscribe(broker.ConsumerOptions{Topic: podTopic, SubscriptionName: "measure", Type: broker.ConsumerTypeExclusive})
	if err != nil {
		t.Fatal(err)
	}
	prod, err := b.CreateProducer(broker.ProducerOptions{Topic: podTopic})
	if err != nil {
		t.Fatal(err)
	}
	logger := slog.New(slog.DiscardHandler)
	d := session.NewDispatcher(ctx, logger, b, prod, directory.NewNoOpInventory(), directory.NewNoOpInventory(), 5*time.Second, session.DispatcherConfig{
		ConnectedAgentsThreshold: 100, HeartbeatRecvThreshold: 30 * time.Second, HeartbeatSendDuration: 10 * time.Second,
		SyncMapClearTimeout: time.Second, GRPCPort: 9090,
	})
	d.SingleTenant = true
	proxy := NewProxy(ctx, logger, b, testRelayCookie)
	go func() { _ = proxy.FanoutResponses(ctx, podTopic, sub) }()
	addr := serveDispatcherAs(t, d, measureAgentID)
	a := agent.New(agent.Config{ServerAddr: addr, AgentID: measureAgentID, Retries: 1, Logger: logger,
		Routes: []agent.Route{{Backend: backend}}, Concurrency: 512})
	if err := a.Connect(ctx); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(a.Close)
	waitAgentInfo(t, d.GetAllAgentInfo, measureAgentID)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		stripHopByHopHeaders(r.Header)
		r.Header.Set(session.AgentIDKey, measureAgentID)
		r.Header.Set(session.RequestAtProxyHeader, strconv.FormatInt(time.Now().UnixMilli(), 10))
		r.Header.Set(session.TransactionIDHeader, uuid.NewString())
		proxy.ServeHTTP(w, r)
	}), a
}

func TestHTTPPathMeasure(t *testing.T) {
	if os.Getenv("HTTP_LINK_MEASURE") == "" {
		t.Skip("measurement: HTTP_LINK_MEASURE=1")
	}
	paths := []struct {
		name  string
		start func(*testing.T, string) (string, func())
	}{{"grpc", startGRPCHTTPPath}, {"link", startLinkHTTPPath}}
	rounds, _ := strconv.Atoi(os.Getenv("HTTP_LINK_MEASURE"))
	for round := range max(rounds, 1) {
		for i := range paths {
			path := paths[(i+round)%2]
			if only := os.Getenv("HTTP_LINK_PATH"); only != "" && only != path.name {
				continue
			}
			t.Run(fmt.Sprint(path.name, round), func(t *testing.T) {
				backend := startMeasureBackend(t)
				g0, h0 := settle()
				base, warm := path.start(t, backend.URL)
				client := &http.Client{Transport: &http.Transport{MaxIdleConnsPerHost: 512, DisableCompression: true}}
				defer client.CloseIdleConnections()
				get := func(p string) error {
					resp, err := client.Get(base + p)
					if err != nil {
						return err
					}
					defer resp.Body.Close() //nolint:errcheck
					if _, err := io.Copy(io.Discard, resp.Body); err != nil {
						return err
					}
					if resp.StatusCode != 200 {
						return fmt.Errorf("status %d", resp.StatusCode)
					}
					return nil
				}
				if err := get("/1k"); err != nil {
					t.Fatal(err)
				}
				client.CloseIdleConnections()
				g1, h1 := settle()
				t.Logf("idle agent: %+d goroutines, %+d KiB heap (relay + agent, connected, one request served)", g1-g0, (int64(h1)-int64(h0))>>10)
				warm()

				// Small requests, keep-alive, one at a time.
				var lat []time.Duration
				for i := range 3200 {
					s := time.Now()
					if err := get("/1k"); err != nil {
						t.Fatal(err)
					}
					if i >= 200 {
						lat = append(lat, time.Since(s))
					}
				}
				slices.Sort(lat)
				t.Logf("1 KiB GET c1 keep-alive: p50 %v p99 %v", lat[len(lat)/2].Round(time.Microsecond), lat[len(lat)*99/100].Round(time.Microsecond))

				// 32 concurrent clients, 5 s.
				var n atomic.Int64
				stop := time.Now().Add(5 * time.Second)
				var wg sync.WaitGroup
				for range 32 {
					wg.Go(func() {
						for time.Now().Before(stop) {
							if get("/1k") == nil {
								n.Add(1)
							}
						}
					})
				}
				wg.Wait()
				t.Logf("1 KiB GET c32 keep-alive: %d req/s", n.Load()/5)

				// Large bodies.
				const big = 256 << 20
				for range 3 {
					s := time.Now()
					if err := get("/dl?n=" + strconv.Itoa(big)); err != nil {
						t.Fatal(err)
					}
					dl := time.Since(s)
					s = time.Now()
					req, _ := http.NewRequest(http.MethodPost, base+"/ul", io.LimitReader(&patternReader{}, big))
					req.ContentLength = big
					resp, err := client.Do(req)
					if err != nil {
						t.Fatal(err)
					}
					body, _ := io.ReadAll(resp.Body)
					_ = resp.Body.Close()
					if string(body) != strconv.Itoa(big) {
						t.Fatalf("upload: backend got %q", body)
					}
					t.Logf("256 MiB: download %.0f MiB/s, upload %.0f MiB/s", 256/dl.Seconds(), 256/time.Since(s).Seconds())
				}

				// Requests held at the backend.
				const held = 200
				client.CloseIdleConnections()
				g2, h2 := settle()
				var hw sync.WaitGroup
				for range held {
					hw.Go(func() { _ = get("/hold") })
				}
				waitUntil(t, func() bool { return backend.holding.Load() == held }, func() string { return fmt.Sprint(backend.holding.Load(), " held") })
				g3, h3 := settle()
				t.Logf("active request (held at the backend, %d): %.1f goroutines, %.1f KiB heap each (client, relay, agent, backend)", held,
					float64(g3-g2)/held, float64(int64(h3)-int64(h2))/1024/held)
				close(backend.release)
				hw.Wait()

				// A client that stops reading beside 8 that read.
				const each = 32 << 20
				stuck, err := client.Get(base + "/dl?n=" + strconv.Itoa(each))
				if err != nil {
					t.Fatal(err)
				}
				s := time.Now()
				var others sync.WaitGroup
				var failed atomic.Int32
				for range 8 {
					others.Go(func() {
						if get("/dl?n="+strconv.Itoa(each)) != nil {
							failed.Add(1)
						}
					})
				}
				done := make(chan struct{})
				go func() { others.Wait(); close(done) }()
				select {
				case <-done:
					t.Logf("stuck client beside 8 x 32 MiB: the 8 done in %v, %d failed", time.Since(s).Round(time.Millisecond), failed.Load())
				case <-time.After(60 * time.Second):
					t.Errorf("stuck client beside 8 x 32 MiB: the 8 not done in 60 s")
				}
				_ = stuck.Body.Close()
			})
		}
	}
}

const measureAgentID = "measure-agent"

// measureBackend serves /1k, /dl?n=, /ul (counts the body), /hold (waits
// for release).
type measureBackend struct {
	*httptest.Server
	holding atomic.Int32
	release chan struct{}
}

func startMeasureBackend(t *testing.T) *measureBackend {
	b := &measureBackend{release: make(chan struct{})}
	b.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/octet-stream")
		switch r.URL.Path {
		case "/1k":
			_, _ = w.Write(make([]byte, 1024))
		case "/dl":
			n, _ := strconv.Atoi(r.URL.Query().Get("n"))
			w.Header().Set("Content-Length", strconv.Itoa(n))
			_, _ = io.CopyN(w, &patternReader{}, int64(n))
		case "/ul":
			n, _ := io.Copy(io.Discard, r.Body)
			_, _ = fmt.Fprint(w, n)
		case "/hold":
			b.holding.Add(1)
			defer b.holding.Add(-1)
			select {
			case <-b.release:
			case <-r.Context().Done():
			}
			_, _ = w.Write([]byte("x"))
		}
	}))
	t.Cleanup(b.Close)
	return b
}

// startLinkHTTPPath is the phase-4 path: the relay's reverse proxy dials a
// session to the agent (DialAgent to dataplane.HTTPSessionTarget), on its link
// data connection once it has one; the agent serves it with net/http and a
// reverse proxy to the backend.
func startLinkHTTPPath(t *testing.T, backend string) (string, func()) {
	bu, _ := url.Parse(backend)
	agentSide := httputil.NewSingleHostReverseProxy(bu)
	agentSide.Transport = agent.NewBackendTransport(nil, 0)
	agentSide.BufferPool = measureBuffers
	h := startHarness(t, 0, relayCurrent, agentCurrent, func(ctx context.Context, client pb.DispatcherClient, dial agent.RelayDialer, logger *slog.Logger) {
		_ = agent.RunTunnel(ctx, client, dial, ctx, agent.Tunnel{HTTP: agentSide}, logger)
	}, func(context.Context, *session.TCPDispatcher, net.Conn) {})
	tr := &http.Transport{
		DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
			return h.d.DialAgent(context.WithoutCancel(ctx), harnessAgentID, dataplane.HTTPSessionTarget)
		},
		MaxIdleConnsPerHost: 1024, DisableCompression: true,
	}
	t.Cleanup(tr.CloseIdleConnections)
	rp := &httputil.ReverseProxy{
		Rewrite:   func(pr *httputil.ProxyRequest) { pr.SetURL(&url.URL{Scheme: "http", Host: "agent"}) },
		Transport: tr, FlushInterval: -1, BufferPool: measureBuffers,
	}
	srv := httptest.NewServer(rp)
	t.Cleanup(srv.Close)
	warm := func() { // two sessions at once make the agent bind its data connections, which new sessions take first
		var wg sync.WaitGroup
		for range 3 {
			wg.Go(func() {
				resp, err := http.Get(srv.URL + "/dl?n=" + strconv.Itoa(8<<20))
				if err == nil {
					_, _ = io.Copy(io.Discard, resp.Body)
					_ = resp.Body.Close()
				}
			})
		}
		wg.Wait()
		waitUntil(t, func() bool { return h.dataStreams() >= 1 }, func() string { return "no data connection" })
		tr.CloseIdleConnections()
		t.Logf("link: %d data connections", h.dataStreams())
	}
	return srv.URL, warm
}

// measureBuffers is the reverse proxies' copy buffer pool (as the agent's
// E2EE proxy has).
var measureBuffers = &poolBuffers{sync.Pool{New: func() any { b := make([]byte, 32<<10); return &b }}}

type poolBuffers struct{ p sync.Pool }

func (b *poolBuffers) Get() []byte { return *b.p.Get().(*[]byte) }

func (b *poolBuffers) Put(p []byte) { b.p.Put(&p) }

// settle returns the goroutine count and live heap once the runtime has
// settled.
func settle() (int, uint64) {
	var ms runtime.MemStats
	for range 3 {
		time.Sleep(100 * time.Millisecond)
		runtime.GC()
	}
	runtime.ReadMemStats(&ms)
	return runtime.NumGoroutine(), ms.HeapAlloc
}
