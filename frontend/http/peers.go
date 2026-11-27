// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package httpingress

import (
	"errors"
	"log/slog"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"

	"github.com/nfltr/link"
	"github.com/nfltr/relay-core/core/relaystatus"
	"github.com/nfltr/relay-core/core/session"
	"github.com/nfltr/relay-core/internal/platform/expiring"
	"github.com/nfltr/relay-core/platform/httpbody"
	relayv1 "github.com/nfltr/relay/proto/nfltr/relay/v1"
	"github.com/spf13/viper"
	"google.golang.org/grpc/codes"
)

// proxyCacheSize bounds the peer replicas the relay keeps a reverse proxy
// for (they share one transport; a replica set's churn only adds URLs).
const proxyCacheSize = 256

// proxyCache holds the reverse proxies to the peer replicas, by dispatcher
// URL.
type proxyCache struct {
	cache     *expiring.Cache[string, *httputil.ReverseProxy] // no TTL
	logger    *slog.Logger
	transport http.RoundTripper
}

func newProxyCache(logger *slog.Logger) *proxyCache {
	return &proxyCache{
		logger:    logger,
		transport: newProxyTransport(),
		cache:     expiring.New[string, *httputil.ReverseProxy](proxyCacheSize, 0, nil),
	}
}

// newProxyTransport creates a shared http.Transport with configurable timeouts
// for reverse-proxy connections to dispatcher pods.
func newProxyTransport() *http.Transport {
	return &http.Transport{
		DialContext: (&net.Dialer{
			Timeout:   viper.GetDuration("proxy.dial_timeout"),
			KeepAlive: viper.GetDuration("proxy.tcp_keepalive"),
		}).DialContext,
		ResponseHeaderTimeout: viper.GetDuration("proxy.response_header_timeout"),
		IdleConnTimeout:       viper.GetDuration("proxy.idle_conn_timeout"),
		MaxIdleConnsPerHost:   viper.GetInt("proxy.max_idle_conns_per_host"),
		ReadBufferSize:        32 * 1024,
		WriteBufferSize:       32 * 1024,
		ForceAttemptHTTP2:     true,
	}
}

// Get returns the reverse proxy to dispatcherURL. Two callers that miss at
// once each make one (it is a struct over the shared transport) and the
// later one is kept.
func (p *proxyCache) Get(dispatcherURL *url.URL) *httputil.ReverseProxy {
	key := dispatcherURL.String()
	if proxy, ok := p.cache.Get(key); ok {
		return proxy
	}
	proxy := &httputil.ReverseProxy{
		// The visitor's request as this replica's edge set it up: Host and
		// the forwarded headers (setForwardedHeaders) go on unchanged, not
		// with this replica's hop appended.
		Rewrite: func(pr *httputil.ProxyRequest) {
			pr.SetURL(dispatcherURL) // #nosec G704 -- dispatcherURL is a peer replica from the relay's own agent-to-pod resolution, never request input
			pr.Out.Host = pr.In.Host
			for _, name := range []string{"X-Forwarded-For", "X-Forwarded-Host", "X-Forwarded-Proto"} {
				if v := pr.In.Header.Values(name); len(v) > 0 {
					pr.Out.Header[name] = v
				}
			}
		},
	}
	proxy.Transport = p.transport
	proxy.ModifyResponse = func(resp *http.Response) error { holdLongResponse(resp); return nil }
	proxy.ErrorHandler = func(w http.ResponseWriter, r *http.Request, err error) {
		if p.logger != nil {
			p.logger.Error("reverse proxy error", "target", dispatcherURL.String(), "error", err)
		}
		if httpbody.IncompleteError(r) {
			http.Error(w, httpbody.Incomplete, http.StatusBadRequest)
			return
		}
		if replicaUnreachable(err) {
			writeReplicaUnreachable(w)
			return
		}
		http.Error(w, "bad gateway", http.StatusBadGateway)
	}
	p.cache.Add(key, proxy)
	return proxy
}

// replicaUnreachableMessage answers a request whose agent is connected to
// another replica of the relay that this one could not reach: not offline.
const replicaUnreachableMessage = "the agent is connected to another replica of the relay, which could not be reached; try again shortly"

// replicaUnreachable reports whether err is a request whose agent is
// connected to another replica this one could not reach: the replica
// link's no-route refusal (link.CodeNoRoute), or session.ErrReplicaUnreachable.
// Guards: TestPeerProxyUnreachableReplicaIs503, TestAgentHTTPUnreachableReplicaIs503.
func replicaUnreachable(err error) bool {
	var a *link.Abort
	return errors.As(err, &a) && a.Code == link.CodeNoRoute || errors.Is(err, session.ErrReplicaUnreachable)
}

// writeReplicaUnreachable answers 503 with Retry-After (replicaUnreachable).
func writeReplicaUnreachable(w http.ResponseWriter) {
	w.Header().Set("Retry-After", "5")
	relaystatus.WriteHTTP(w, http.StatusServiceUnavailable, codes.Unavailable, relayv1.ErrorReason_ERROR_REASON_UNSPECIFIED, replicaUnreachableMessage)
}
