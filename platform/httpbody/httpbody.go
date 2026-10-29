// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

// Package httpbody is what the relay's request-body checks and the HTTP
// ingress's agent proxies share about a client's request body: a body that
// remembers how its reading ended, the mark the upload guard leaves on a
// request so a proxy answers a body cut short as the client's error, and the
// 413 text naming the limit.
package httpbody

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync/atomic"
)

// Incomplete answers a request whose body ended before its declared length.
const Incomplete = "incomplete request body: the client sent fewer bytes than its Content-Length"

// ErrTooLarge is a body past http.server.max_request_body_size.
var ErrTooLarge = errors.New("request body too large")

// Body records the first error reading the client's body, so a body that
// ended in error is never taken for a complete one.
type Body struct {
	io.ReadCloser
	failed atomic.Pointer[error]
}

// Unwrap is the body it reads (a tunnel's stream reads it directly).
func (b *Body) Unwrap() io.ReadCloser { return b.ReadCloser }

// Read reads the body, recording its first error other than io.EOF.
func (b *Body) Read(p []byte) (int, error) {
	n, err := b.ReadCloser.Read(p)
	if err != nil && err != io.EOF {
		b.failed.CompareAndSwap(nil, &err)
	}
	return n, err
}

// Failed is the first error reading the body other than its end; nil when
// none.
func (b *Body) Failed() error {
	if err := b.failed.Load(); err != nil {
		return *err
	}
	return nil
}

// short reports whether the body ended before its declared length.
func (b *Body) short() bool {
	err := b.Failed()
	return err != nil && isShort(err)
}

// isShort: HTTP/1 reports a body cut before its Content-Length as
// io.ErrUnexpectedEOF; HTTP/2 closes it with "declared a Content-Length".
func isShort(err error) bool {
	return errors.Is(err, io.ErrUnexpectedEOF) || strings.Contains(err.Error(), "declared a Content-Length")
}

type bodyKey struct{}

// Mark wraps r's body in a Body, in a copy of r whose context carries it
// (IncompleteError). The server inspects its own request's body after the
// handler (Expect: 100-continue, leftovers), so the original keeps it.
func Mark(r *http.Request) *http.Request {
	body := &Body{ReadCloser: r.Body}
	r = r.WithContext(context.WithValue(r.Context(), bodyKey{}, body))
	r.Body = body
	return r
}

// IncompleteError reports whether r's client body (Mark) ended short, so a
// failure forwarding it is the client's (400), not a bad gateway.
func IncompleteError(r *http.Request) bool {
	b, ok := r.Context().Value(bodyKey{}).(*Body)
	return ok && b.short()
}

// TooLargeMessage names the limit and the setting that raises it: one 413
// text for the declared and the streamed check.
func TooLargeMessage(limit int64) string {
	return fmt.Sprintf("request body too large: the relay accepts at most %s (http.server.max_request_body_size)", formatByteSize(limit))
}

func formatByteSize(n int64) string {
	switch {
	case n >= 1<<30 && n%(1<<30) == 0:
		return fmt.Sprintf("%d GiB", n>>30)
	case n >= 1<<20 && n%(1<<20) == 0:
		return fmt.Sprintf("%d MiB", n>>20)
	default:
		return fmt.Sprintf("%d bytes", n)
	}
}
