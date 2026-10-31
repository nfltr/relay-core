// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

// Static header and URL rewriting (Gap 10).
//
// HeaderPolicy applies per-server static transformations to HTTP requests and
// responses before they are proxied to the agent.  Unlike ngrok's Traffic
// Policy (CEL), this implementation uses static string substitutions
// configured at server startup — no expression language required.
//
// Supported transforms:
//   - Add / overwrite request headers
//   - Remove request headers
//   - Add / overwrite response headers
//   - Remove response headers
//   - Rewrite URL path via a regex pattern + replacement string
//
// Wire via config (all optional, defaults to no-op):
//
//	http.request_headers.add:    map of header-name -> value
//	http.request_headers.remove: list of header names to strip
//	http.response_headers.add:   map of header-name -> value
//	http.response_headers.remove: list of header names to strip
//	http.url_rewrite.pattern:    Go regexp applied to r.URL.Path
//	http.url_rewrite.replacement: replacement string (supports $1 back-refs)

package httpingress

import (
	"bufio"
	"net"
	"net/http"
	"regexp"

	"log/slog"

	"github.com/spf13/viper"
)

// HeaderPolicy holds the static header and URL rewrite configuration.
type HeaderPolicy struct {
	addRequestHeaders     map[string]string
	removeRequestHeaders  []string
	addResponseHeaders    map[string]string
	removeResponseHeaders []string
	urlRewritePattern     *regexp.Regexp // nil = no URL rewrite
	urlRewriteReplacement string
}

// NewHeaderPolicyFromConfig reads the header/URL rewrite config from viper and
// returns a ready-to-use HeaderPolicy.  Returns nil if all transforms are
// disabled (zero-cost passthrough).
func NewHeaderPolicyFromConfig(logger *slog.Logger) *HeaderPolicy {
	p := &HeaderPolicy{
		addRequestHeaders:     viperStringMap("http.request_headers.add"),
		removeRequestHeaders:  viper.GetStringSlice("http.request_headers.remove"),
		addResponseHeaders:    viperStringMap("http.response_headers.add"),
		removeResponseHeaders: viper.GetStringSlice("http.response_headers.remove"),
	}

	if pattern := viper.GetString("http.url_rewrite.pattern"); pattern != "" {
		re, err := regexp.Compile(pattern)
		if err != nil {
			logger.Warn("header_policy: invalid url_rewrite.pattern — URL rewrite disabled",
				"pattern", pattern, "error", err)
		} else {
			p.urlRewritePattern = re
			p.urlRewriteReplacement = viper.GetString("http.url_rewrite.replacement")
		}
	}

	if p.isEmpty() {
		return nil
	}
	return p
}

func (p *HeaderPolicy) isEmpty() bool {
	return len(p.addRequestHeaders) == 0 &&
		len(p.removeRequestHeaders) == 0 &&
		len(p.addResponseHeaders) == 0 &&
		len(p.removeResponseHeaders) == 0 &&
		p.urlRewritePattern == nil
}

// ApplyRequest mutates the incoming request according to the policy.
// Safe to call concurrently — the policy is read-only after construction.
func (p *HeaderPolicy) ApplyRequest(r *http.Request) {
	for _, name := range p.removeRequestHeaders {
		r.Header.Del(name)
	}
	for name, value := range p.addRequestHeaders {
		r.Header.Set(name, value)
	}
	if p.urlRewritePattern != nil {
		r.URL.Path = p.urlRewritePattern.ReplaceAllString(r.URL.Path, p.urlRewriteReplacement)
		// Keep RequestURI in sync; httputil.ReverseProxy uses r.URL directly,
		// but defensive update prevents surprises with other middlewares.
		r.RequestURI = r.URL.RequestURI()
	}
}

// ApplyResponse mutates the outgoing response headers according to the policy.
func (p *HeaderPolicy) ApplyResponse(h http.Header) {
	for _, name := range p.removeResponseHeaders {
		h.Del(name)
	}
	for name, value := range p.addResponseHeaders {
		h.Set(name, value)
	}
}

// headerRewriteResponseWriter wraps http.ResponseWriter to intercept the
// response headers before they are flushed.
type headerRewriteResponseWriter struct {
	http.ResponseWriter
	policy      *HeaderPolicy
	headersSent bool
}

// WriteHeader applies the policy to every header block it sends: a 1xx's
// and the final response's.
func (rw *headerRewriteResponseWriter) WriteHeader(statusCode int) {
	if !rw.headersSent {
		// A 1xx other than 101 is not the final header block.
		rw.headersSent = statusCode >= 200 || statusCode == http.StatusSwitchingProtocols
		rw.policy.ApplyResponse(rw.Header())
	}
	rw.ResponseWriter.WriteHeader(statusCode)
}

func (rw *headerRewriteResponseWriter) Write(b []byte) (int, error) {
	if !rw.headersSent {
		rw.headersSent = true
		rw.policy.ApplyResponse(rw.Header())
	}
	return rw.ResponseWriter.Write(b)
}

// Hijack applies the policy to the headers of a protocol upgrade's 101,
// which its handler writes on the connection it takes over.
func (rw *headerRewriteResponseWriter) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	if !rw.headersSent {
		rw.headersSent = true
		rw.policy.ApplyResponse(rw.Header())
	}
	return http.NewResponseController(rw.ResponseWriter).Hijack()
}

// Flush sends the header at once (a streamed response's head before its
// body: the proxy asserts http.Flusher; TestProxyStreamedHeadReachesClientBeforeBody).
func (rw *headerRewriteResponseWriter) Flush() {
	if !rw.headersSent {
		rw.headersSent = true
		rw.policy.ApplyResponse(rw.Header())
	}
	_ = http.NewResponseController(rw.ResponseWriter).Flush()
}

// Unwrap lets http.ResponseController reach the server's writer.
func (rw *headerRewriteResponseWriter) Unwrap() http.ResponseWriter { return rw.ResponseWriter }

// HeaderRewriteMiddleware returns a middleware that applies p to every request
// and response.  If p is nil it returns next unchanged (zero cost).
func HeaderRewriteMiddleware(p *HeaderPolicy, next http.Handler) http.Handler {
	if p == nil {
		return next
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		p.ApplyRequest(r)
		next.ServeHTTP(&headerRewriteResponseWriter{ResponseWriter: w, policy: p}, r)
	})
}

// viperStringMap retrieves a map[string]string from viper.
// Viper stores YAML maps as map[string]interface{}; we normalise to strings.
func viperStringMap(key string) map[string]string {
	raw := viper.GetStringMapString(key)
	if len(raw) == 0 {
		return nil
	}
	return raw
}
