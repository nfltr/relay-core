// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package session

import (
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// peerAnsweredStatus reports the status a peer relay's handler answered a
// forwarded request with, so the source relays it unchanged — the same typed
// answer a local route gives — instead of a generic Internal. It matches only
// codes the gRPC library never generates itself, plus the peer's typed
// target-agent disconnect, so a failure of the pod-to-pod hop itself is never
// mistaken for a peer answer. Such an answer means the peer relay is healthy.
func peerAnsweredStatus(err error) (*status.Status, bool) {
	if err == nil {
		return nil, false
	}
	st, ok := status.FromError(err)
	if !ok {
		return nil, false
	}
	switch st.Code() {
	case codes.InvalidArgument, codes.NotFound, codes.AlreadyExists, codes.PermissionDenied,
		codes.FailedPrecondition, codes.Aborted, codes.OutOfRange, codes.DataLoss:
		return st, true
	case codes.Unavailable:
		return st, st.Message() == errTargetAgentDisconnected.Error()
	}
	return nil, false
}

// peerAnswerError carries a peer relay's typed answer out of the forwarding
// errgroup, distinct from a pod-to-pod stream failure.
type peerAnswerError struct{ st *status.Status }

// Error implements error. allow:wrapper
func (e *peerAnswerError) Error() string { return e.st.Err().Error() }

// GRPCStatus keeps the peer's status visible to status.FromError.
func (e *peerAnswerError) GRPCStatus() *status.Status { return e.st }
