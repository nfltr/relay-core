// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package httpingress

// HTTP to an agent on link sessions (client generation 3): a request for an
// agent on this replica whose TCP streams serve HTTP sessions
// (dataplane.HTTPSessionsHeader) goes as HTTP/1.1 on a session to
// dataplane.HTTPSessionTarget, which the agent serves with net/http; other
// agents' requests go as ConnectHTTP messages (Proxy). Everything before
// the leaf (authentication, shares, policy, capture) is the same for both.

import (
	"context"
	"encoding/hex"
	"errors"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptrace"
	"net/http/httputil"
	"strings"
	"sync"
	"time"

	"github.com/nfltr/relay-core/core/frontend"
	"github.com/nfltr/relay-core/core/session"
	"github.com/nfltr/relay-core/platform/httpbody"
	"github.com/nfltr/relay/proto/wire/dataplane"
	"github.com/spf13/viper"
)

// agentHTTPHost is the transport's host for an agent: one keep-alive pool
// per agent, whatever characters its id has.
const agentHTTPHost = ".agent"

// agentHTTP is the leaf that serves a request to an agent on an HTTP
// session.
type agentHTTP struct {
	rp     *httputil.ReverseProxy
	logger *slog.Logger
	// maxBody is http.server.max_request_body_size, read once.
	maxBody int64
	// relayCookie names the relay's own cookies, never forwarded.
	relayCookie func(name string) bool
}

// agentHTTPState is a request's state between ServeHTTP and the proxy's
// hooks.
type agentHTTPState struct {
	agentID  string
	at       time.Time
	idle     *time.Timer
	timeout  time.Duration
	timedOut bool
	// responded: the response began or the request ended; the session's
	// grants no longer restart idle.
	responded bool
	mu        sync.Mutex
}

type agentHTTPStateKey struct{}

func newAgentHTTP(d *session.TCPDispatcher, logger *slog.Logger, relayCookie func(name string) bool) *agentHTTP {
	tr := &http.Transport{
		DialContext: func(ctx context.Context, _, addr string) (net.Conn, error) {
			host, _, _ := net.SplitHostPort(addr)
			id, err := hex.DecodeString(strings.TrimSuffix(host, agentHTTPHost))
			if err != nil {
				return nil, err
			}
			// The session outlives the dial: it is the pool's keep-alive
			// connection, closed by the transport.
			return d.DialAgent(context.WithoutCancel(ctx), string(id), dataplane.HTTPSessionTarget)
		},
		MaxIdleConnsPerHost: 64,
		// Below the agent's idle timeout (agent e2eeIdleTimeout, 120 s): the
		// relay closes an idle session before the agent does.
		IdleConnTimeout:    90 * time.Second,
		DisableCompression: true,
	}
	a := &agentHTTP{logger: logger, maxBody: viper.GetInt64("http.server.max_request_body_size"), relayCookie: relayCookie}
	a.rp = &httputil.ReverseProxy{
		Rewrite: func(pr *httputil.ProxyRequest) {
			st := pr.In.Context().Value(agentHTTPStateKey{}).(*agentHTTPState)
			out := pr.Out
			out.URL.Scheme = "http"
			out.URL.Host = hex.EncodeToString([]byte(st.agentID)) + agentHTTPHost
			// What the gRPC path never forwarded (prepareAgentRequest).
			out.Header = withoutRelayCookies(out.Header, a.relayCookie)
			for name, skip := range SkipHeaders {
				if skip(name) {
					out.Header.Del(name)
				}
			}
			// What the gRPC path forwarded and Rewrite drops: the forwarded
			// headers the relay's edge set (setForwardedHeaders).
			for _, name := range []string{"X-Forwarded-For", "X-Forwarded-Host", "X-Forwarded-Proto"} {
				if v := pr.In.Header.Values(name); len(v) > 0 {
					out.Header[name] = v
				}
			}
		},
		Transport:     tr,
		FlushInterval: -1,
		BufferPool:    agentHTTPBuffers,
		ModifyResponse: func(resp *http.Response) error {
			st := resp.Request.Context().Value(agentHTTPStateKey{}).(*agentHTTPState)
			st.respond()
			if resp.StatusCode == http.StatusSwitchingProtocols {
				// An upgraded connection (WebSocket) has no idle bound, and
				// its body is the connection ReverseProxy copies both ways.
				st.idle.Stop()
			} else {
				st.idle.Reset(st.timeout) // the response started; each read restarts the bound
				resp.Body = &idleBody{ReadCloser: resp.Body, st: st}
				holdLongResponse(resp)
			}
			if prefix := frontend.BrowsePrefix(resp.Request.Context()); prefix != "" {
				rewriteLocationHeader(resp.Header, prefix)
			}
			_ = copyMetadataHeaderToResponse(headerOnly(resp.Header), st.agentID, st.at.UnixMilli(), time.Now().UnixMilli(), nil)
			return nil
		},
		ErrorHandler: func(w http.ResponseWriter, r *http.Request, err error) {
			st := r.Context().Value(agentHTTPStateKey{}).(*agentHTTPState)
			var tooLarge *http.MaxBytesError
			switch {
			case errors.As(err, &tooLarge):
				http.Error(w, httpbody.TooLargeMessage(a.maxBody), http.StatusRequestEntityTooLarge)
			case st.didTimeOut():
				http.Error(w, "Request timed out", http.StatusGatewayTimeout)
			case r.Context().Err() != nil: // the visitor went away
			default:
				logger.Warn("agent http session failed", "agent_id", st.agentID, "error", err)
				http.Error(w, "Agent not connected", http.StatusBadGateway)
			}
		},
	}
	return a
}

// ServeHTTP serves r (AgentIDKey names the agent) on an HTTP session.
func (a *agentHTTP) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if a.maxBody > 0 && r.ContentLength > a.maxBody {
		http.Error(w, httpbody.TooLargeMessage(a.maxBody), http.StatusRequestEntityTooLarge)
		return
	}
	timeout := viper.GetDuration("default.request.timeout")
	ctx, cancel := context.WithCancel(r.Context())
	defer cancel()
	st := &agentHTTPState{agentID: r.Header.Get(session.AgentIDKey), at: time.Now(), timeout: timeout}
	// timeout bounds each wait for the agent (its response to start, or
	// its next body read), as on the gRPC path, not the whole response.
	st.idle = time.AfterFunc(timeout, func() {
		st.mu.Lock()
		st.timedOut = true
		st.mu.Unlock()
		cancel()
	})
	defer st.idle.Stop()
	defer st.respond()
	// After the body's last byte the agent may still feed what its window
	// holds to a slow backend: each grant of the session is progress
	// (TestUploadDrainToSlowBackendIsProgressLink).
	ctx = httptrace.WithClientTrace(ctx, &httptrace.ClientTrace{GotConn: func(info httptrace.GotConnInfo) {
		if g, ok := info.Conn.(interface{ OnGrant(func(int)) bool }); ok {
			g.OnGrant(st.granted)
		}
	}})
	r = r.WithContext(context.WithValue(ctx, agentHTTPStateKey{}, st))
	if a.maxBody > 0 && r.Body != nil {
		r.Body = http.MaxBytesReader(w, r.Body, a.maxBody)
	}
	if r.Body != nil && r.Body != http.NoBody {
		r.Body = &idleBody{ReadCloser: r.Body, st: st} // an upload's progress restarts the bound
	}
	a.rp.ServeHTTP(w, r)
}

// granted restarts the idle bound until the response begins: the agent
// took more of the request. A pooled session keeps it, as a no-op, until
// its next request's.
func (st *agentHTTPState) granted(int) {
	st.mu.Lock()
	if !st.responded {
		st.idle.Reset(st.timeout)
	}
	st.mu.Unlock()
}

// respond ends granted's restarts, before ModifyResponse sets the bound
// (or stops it for an upgrade).
func (st *agentHTTPState) respond() {
	st.mu.Lock()
	st.responded = true
	st.mu.Unlock()
}

func (st *agentHTTPState) didTimeOut() bool {
	st.mu.Lock()
	defer st.mu.Unlock()
	return st.timedOut
}

// idleBody restarts its request's idle bound at every read that moved
// bytes.
type idleBody struct {
	io.ReadCloser
	st *agentHTTPState
}

func (b *idleBody) Read(p []byte) (int, error) {
	n, err := b.ReadCloser.Read(p)
	if n > 0 {
		b.st.idle.Reset(b.st.timeout)
	}
	return n, err
}

// headerOnly lets copyMetadataHeaderToResponse set a response's header.
type headerOnly http.Header

func (h headerOnly) Header() http.Header       { return http.Header(h) }
func (headerOnly) Write(b []byte) (int, error) { return len(b), nil }
func (headerOnly) WriteHeader(int)             {}

// agentHTTPBuffers are the reverse proxy's copy buffers: a fresh 32 KiB
// buffer per response cost the link path 15-19 % at 32 concurrent
// requests (measured in the phase-4 design).
var agentHTTPBuffers = &copyBuffers{sync.Pool{New: func() any { b := make([]byte, 32<<10); return &b }}}

type copyBuffers struct{ p sync.Pool }

func (b *copyBuffers) Get() []byte  { return *b.p.Get().(*[]byte) }
func (b *copyBuffers) Put(p []byte) { b.p.Put(&p) }
