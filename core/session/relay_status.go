// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package session

import (
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/nfltr/relay-core/core/relaystatus"
	relayv1 "github.com/nfltr/relay/proto/nfltr/relay/v1"
)

// RelayErrorDomain is the google.rpc.ErrorInfo domain of every typed relay
// error.
const RelayErrorDomain = relaystatus.Domain

// RelayStatus is the one typed relay error (core/relaystatus). A typed
// (gateway-served) RPC returns RelayStatus(...).Err(); grpc-gateway renders
// it as the same Status JSON.
func RelayStatus(code codes.Code, reason relayv1.ErrorReason, msg string) *status.Status {
	return relaystatus.New(code, reason, msg)
}

// errNoPrincipal refuses an agent stream that carries no principal: no
// credential, and the relay registers none with no account (nf1.anon
// off, or not capability addresses only). Typed, so a client reports
// auth rather than a reset.
func errNoPrincipal() error {
	return RelayStatus(codes.PermissionDenied, relayv1.ErrorReason_ERROR_REASON_UNAUTHENTICATED,
		"no credential: this relay serves no agent without an account (present a machine certificate or a token)").Err()
}
