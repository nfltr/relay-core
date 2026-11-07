// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package grpcauth

import (
	"context"
	"strings"
	"testing"

	"github.com/nfltr/relay-core/core/identity"
	"github.com/nfltr/relay-core/keyfile"
	relayv1 "github.com/nfltr/relay/proto/nfltr/relay/v1"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// refusing is a provider that knows its credentials and refuses them for
// err (a prepaid account whose balance is exhausted).
type refusing struct {
	keyfile.Provider
	err error
}

func (r refusing) Authenticate(ctx context.Context, c identity.Credential) (identity.Principal, error) {
	p, err := r.Provider.Authenticate(ctx, c)
	if err == nil && r.err != nil {
		return identity.Principal{}, r.err
	}
	return p, err
}

func wantBalanceStatus(t *testing.T, where string, err error) {
	t.Helper()
	s := status.Convert(err)
	reason := ""
	for _, d := range s.Details() {
		if info, ok := d.(*errdetails.ErrorInfo); ok {
			reason = info.GetReason()
		}
	}
	if s.Code() != codes.FailedPrecondition || reason != relayv1.ErrorReason_ERROR_REASON_BALANCE_EXHAUSTED.String() {
		t.Fatalf("%s: %v (reason %q), want FailedPrecondition with reason ERROR_REASON_BALANCE_EXHAUSTED: never a bad key", where, err, reason)
	}
}

// TestBalanceExhaustedTypedOnGRPC: a provider's balance refusal is answered
// FailedPrecondition with reason ERROR_REASON_BALANCE_EXHAUSTED at admission,
// and a stream its cut ended (RevokeCause) ends with the same: a client
// that stops on Unauthenticated keeps retrying instead.
func TestBalanceExhaustedTypedOnGRPC(t *testing.T) {
	store := keyfile.NewAPIKeyStore()
	store.Add(testMachine, "k")
	ctx := metadata.NewIncomingContext(t.Context(), metadata.Pairs("x-api-key", "k"))

	refuse := identity.NewAuthenticator("accounts", refusing{keyfile.Provider{Store: store}, identity.ErrBalanceExhausted})
	_, err := MetadataPrincipal(ctx, refuse)
	wantBalanceStatus(t, "admission", err)

	auth := identity.NewAuthenticator("accounts", keyfile.Provider{Store: store})
	err = AuthStreamInterceptor(auth)(nil, &fakeServerStream{ctx: ctx}, &grpc.StreamServerInfo{}, func(_ any, ss grpc.ServerStream) error {
		p, _ := identity.FromContext(ss.Context())
		auth.RevokeCause(p.Key(), identity.ErrBalanceExhausted)
		<-ss.Context().Done()
		return ss.Context().Err()
	})
	wantBalanceStatus(t, "cut", err)
}

// TestUnenrolledKeyRefusedOnGRPC: a key whose provider states no machine
// is refused at admission as Unauthenticated with the enrolment message,
// never as an invalid key and never with a principal.
func TestUnenrolledKeyRefusedOnGRPC(t *testing.T) {
	auth := identity.NewAuthenticator("accounts", noMachine{})
	ctx := metadata.NewIncomingContext(t.Context(), metadata.Pairs("x-api-key", "k", "x-agent-id", "svc"))
	p, err := MetadataPrincipal(ctx, auth)
	s, _ := status.FromError(err)
	if s.Code() != codes.Unauthenticated || !strings.Contains(s.Message(), "machine_not_enrolled") || p.Provider != "" || !hasReason(s, relayv1.ErrorReason_ERROR_REASON_MACHINE_NOT_ENROLLED) {
		t.Fatalf("unenrolled key: %+v, %v; want Unauthenticated machine_not_enrolled", p, err)
	}
}

// noMachine knows every token as a key with no machine.
type noMachine struct{}

func (noMachine) Name() string { return "accounts" }
func (noMachine) Authenticate(context.Context, identity.Credential) (identity.Principal, error) {
	return identity.Principal{Owner: "o", Subject: "cli-1", Names: []string{"svc"}}, nil
}

// hasReason reports whether s carries reason as its ErrorInfo.
func hasReason(s *status.Status, reason relayv1.ErrorReason) bool {
	for _, d := range s.Details() {
		if info, ok := d.(*errdetails.ErrorInfo); ok && info.GetReason() == reason.String() {
			return true
		}
	}
	return false
}
