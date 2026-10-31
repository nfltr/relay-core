// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package util

import (
	"bufio"
	"net"
	"net/http"
)

// ResponseWriter wraps an http.ResponseWriter: it removes the given headers
// when the header is written (keeping them for GetRemovedHeaders) and
// records the status and the bytes written.
type ResponseWriter struct {
	headersToRemove []string
	removedHeaders  http.Header
	size            int
	statusCode      int
	w               http.ResponseWriter
	wroteHeader     bool
}

// NewResponseWriter wraps rw, removing headersToRemove from the response.
func NewResponseWriter(rw http.ResponseWriter, headersToRemove []string) *ResponseWriter {
	var removedHeaders http.Header
	if len(headersToRemove) > 0 {
		removedHeaders = make(http.Header, len(headersToRemove))
	}
	return &ResponseWriter{
		headersToRemove: headersToRemove,
		removedHeaders:  removedHeaders,
		w:               rw,
	}
}

// Header returns the wrapped writer's header.
func (rw *ResponseWriter) Header() http.Header {
	return rw.w.Header()
}

// Write writes buf, first the header with status 200 when none was written.
func (rw *ResponseWriter) Write(buf []byte) (int, error) {
	if !rw.wroteHeader {
		rw.WriteHeader(http.StatusOK)
	}

	n, err := rw.w.Write(buf)
	rw.size += n

	return n, err
}

func (rw *ResponseWriter) removeHeaders() {
	for _, header := range rw.headersToRemove {
		for _, v := range rw.w.Header().Values(header) {
			rw.removedHeaders.Add(header, v)
		}
		rw.w.Header().Del(header)
	}
}

// WriteHeader removes the headers to remove and writes the header once;
// later calls do nothing.
func (rw *ResponseWriter) WriteHeader(statusCode int) {
	if rw.wroteHeader {
		return
	}
	rw.statusCode = statusCode

	rw.removeHeaders()

	rw.w.WriteHeader(statusCode)
	rw.wroteHeader = true
}

// GetRemovedHeaders returns the removed headers' values (nil when none were
// to be removed).
func (rw *ResponseWriter) GetRemovedHeaders() http.Header {
	return rw.removedHeaders
}

// GetStatusCode returns the status written (0 before).
func (rw *ResponseWriter) GetStatusCode() int {
	return rw.statusCode
}

// GetSize returns the body bytes written.
func (rw *ResponseWriter) GetSize() int {
	return rw.size
}

// Flush implements http.Flusher. It delegates to the underlying
// ResponseWriter if it supports flushing (required for streaming/SSE).
func (rw *ResponseWriter) Flush() {
	if !rw.wroteHeader { // removed headers must not go out with an early flush
		rw.WriteHeader(http.StatusOK)
	}
	_ = http.NewResponseController(rw.w).Flush() // also through a writer that only unwraps
}

// Hijack hands the connection over (a protocol upgrade: WebSocket): the
// status is 101 and the headers to remove go, as WriteHeader would do.
func (rw *ResponseWriter) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	conn, brw, err := http.NewResponseController(rw.w).Hijack()
	if err == nil && !rw.wroteHeader {
		rw.statusCode = http.StatusSwitchingProtocols
		rw.removeHeaders()
		rw.wroteHeader = true
	}
	return conn, brw, err
}

// Unwrap lets http.ResponseController reach the server's writer.
func (rw *ResponseWriter) Unwrap() http.ResponseWriter { return rw.w }

// compile-time check: ResponseWriter must implement http.Flusher
var _ http.Flusher = (*ResponseWriter)(nil)
