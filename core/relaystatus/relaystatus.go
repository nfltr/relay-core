// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

// Package relaystatus is the one typed relay error, a leaf package so a
// service answers in the relay's error shape without linking the session
// core.
package relaystatus

import (
	"net/http"

	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"

	relayv1 "github.com/nfltr/relay/proto/nfltr/relay/v1"
)

// Domain is the google.rpc.ErrorInfo domain of every typed relay error.
const Domain = "relay.nfltr.xyz"

// New is a google.rpc.Status whose single ErrorInfo carries reason (the
// nfltr.relay.v1.ErrorReason value name) and Domain. Clients branch on the
// reason, never on msg.
func New(code codes.Code, reason relayv1.ErrorReason, msg string) *status.Status {
	st := status.New(code, msg)
	if detailed, err := st.WithDetails(&errdetails.ErrorInfo{Reason: reason.String(), Domain: Domain}); err == nil {
		return detailed
	}
	return st
}

// WriteHTTP answers an HTTP request with New(code, reason, msg) as JSON,
// the form grpc-gateway renders it in, so clients read one error shape.
func WriteHTTP(w http.ResponseWriter, httpStatus int, code codes.Code, reason relayv1.ErrorReason, msg string) {
	body, err := protojson.Marshal(New(code, reason, msg).Proto())
	if err != nil {
		http.Error(w, msg, httpStatus)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(httpStatus)
	_, _ = w.Write(body) // #nosec G705 -- protojson body served as application/json
}
