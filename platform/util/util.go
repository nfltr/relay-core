// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

// Package util holds small helpers the relay shares: a typed sync.Map,
// a bounded worker pool, a response writer that strips headers, and
// conversions between HTTP headers and the agent wire's frames.
package util

import (
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"time"

	"github.com/nfltr/relay-core/internal/platform/logging"
	"github.com/nfltr/relay/proto/pb"
	"google.golang.org/protobuf/proto"
)

// Cast asserts x, found when ok, to T: the zero T and false when not found
// or not a T.
func Cast[T any](x any, ok bool) (T, bool) {
	if !ok {
		var z T
		return z, false
	}
	v, ok := x.(T)
	return v, ok
}

// SkipHeaderIf reports whether HeadersToProto leaves header out.
type SkipHeaderIf func(header string) bool

// SkipHeaderAlways leaves a header out always.
func SkipHeaderAlways(_ string) bool { return true }

// HeadersToProto converts header to the agent wire's headers, leaving out a
// header whose skipHeaders entry reports true.
func HeadersToProto(header http.Header, skipHeaders map[string]SkipHeaderIf) map[string]*pb.HeaderValueList {
	headers := make(map[string]*pb.HeaderValueList, len(header))
	for k, headerValues := range header {
		if condition, ok := skipHeaders[k]; ok && condition(k) {
			continue
		}
		headers[k] = &pb.HeaderValueList{}
		headers[k].HeaderValues = headerValues
	}
	return headers
}

// CopyProtoHeadersToHTTPHeaders adds the agent wire's headers to resHeader.
func CopyProtoHeadersToHTTPHeaders(headers map[string]*pb.HeaderValueList, resHeader http.Header) {
	for h, vl := range headers {
		for _, v := range vl.HeaderValues {
			resHeader.Add(h, v)
		}
	}
}

// NewLogger returns a logger instance with the log level set
func NewLogger(level string) *slog.Logger {
	return logging.New(level)
}

// GetListeners creates TCP listeners for the given ports, each bound to
// hostFor(port) (all interfaces when hostFor is nil or returns "").
// Duplicate ports are silently deduped. On error every already-opened
// listener is closed so the caller never has to worry about leaks.
func GetListeners(ports []string, hostFor func(port string) string) (map[string]net.Listener, error) {
	listeners := make(map[string]net.Listener, len(ports))
	for _, port := range ports {
		if _, exists := listeners[port]; exists {
			continue // already listening on this port
		}
		host := "0.0.0.0"
		if hostFor != nil && hostFor(port) != "" {
			host = hostFor(port)
		}
		l, err := net.Listen("tcp", net.JoinHostPort(host, port))
		if err != nil {
			// Close everything we opened so far.
			for _, ol := range listeners {
				_ = ol.Close()
			}
			return nil, fmt.Errorf("could not create listener for port %s: %w", port, err)
		}
		listeners[port] = l
	}

	return listeners, nil
}

// IsPortListening returns true if something is actively listening on the given port.
// This uses net.DialTimeout which directly checks if a TCP connection can be established.
// Note: there is an inherent TOCTOU race, but this is acceptable for health-check usage.
func IsPortListening(port string) bool {
	conn, err := net.DialTimeout("tcp", net.JoinHostPort("127.0.0.1", port), 500*time.Millisecond)
	if err != nil {
		return false
	}
	_ = conn.Close()
	return true
}

// protoMarshal is a package-level variable to allow test injection of marshal failures.
var protoMarshal = proto.Marshal

// ProtoToTunnelFrame marshals m into a tunnel frame for agentID
// (BytesToTunnelFrame).
func ProtoToTunnelFrame(m proto.Message, messageType, agentID string, eof bool) (*pb.TunnelFrame, error) {
	buf, err := protoMarshal(m)
	if err != nil {
		return nil, err
	}

	return BytesToTunnelFrame(buf, messageType, agentID, eof), nil
}

// BytesToTunnelFrame is a tunnel frame carrying buf, of messageType, routed
// to agentID; eom marks the message's last frame.
func BytesToTunnelFrame(buf []byte, messageType, agentID string, eom bool) *pb.TunnelFrame {
	return &pb.TunnelFrame{
		Payload:     buf,
		MessageType: messageType,
		Routing: &pb.RoutingInfo{
			AgentId: agentID,
		},
		Eom: eom,
	}
}
