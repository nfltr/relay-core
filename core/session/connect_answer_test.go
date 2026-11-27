// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package session

import (
	"fmt"
	"slices"
	"strings"
	"testing"

	"github.com/nfltr/link"
	"github.com/nfltr/relay-core/platform/config"
	relayv1 "github.com/nfltr/relay/proto/nfltr/relay/v1"
	"github.com/nfltr/relay/proto/wire/agentservices"
	"github.com/nfltr/relay/proto/wire/dataplane"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// TestReleasedAgentServices: an agent that does not read modes
// (agentservices.ModesHeader: nfltr before v2.1.6, which requires the echo
// to equal its declaration) gets its declaration back as sent and its tcp
// services plain, whatever tcp.e2ee_default; under require it is refused
// at connect, saying to update nfltr where it prints it. An agent that reads modes gets them
// resolved.
func TestReleasedAgentServices(t *testing.T) {
	for _, tc := range []struct {
		policy string
		def    bool
		modes  bool
		echo   string // "" refused
		stored string
	}{
		{config.E2EEAllow, true, false, "ssh=tcp,web=http", "ssh=tcp+plain,web=http"},
		{config.E2EEAllow, false, false, "ssh=tcp,web=http", "ssh=tcp+plain,web=http"},
		{config.E2EEForbid, false, false, "ssh=tcp,web=http", "ssh=tcp+plain,web=http"},
		{config.E2EERequire, false, false, "", ""},
		{config.E2EEAllow, true, true, "ssh=tcp+e2ee,web=http", "ssh=tcp+e2ee,web=http"},
		{config.E2EERequire, false, true, "ssh=tcp+e2ee,web=http", "ssh=tcp+e2ee,web=http"},
	} {
		t.Run(fmt.Sprintf("%s/default=%v/modes=%v", tc.policy, tc.def, tc.modes), func(t *testing.T) {
			d, cancel := newTestDispatcher(t)
			defer cancel()
			d.Config.E2EEPolicy, d.Config.E2EEDefault = tc.policy, tc.def
			client, ctx := dialDispatcher(t, serveDispatcherAs(t, d, "svc-agent"))
			ctx = metadata.AppendToOutgoingContext(ctx, agentservices.Header, "ssh=tcp,web=http")
			if tc.modes {
				ctx = metadata.AppendToOutgoingContext(ctx, agentservices.ModesHeader, "1")
			}
			stream, err := client.ConnectHTTP(ctx)
			if err != nil {
				t.Fatal(err)
			}
			hdr, err := stream.Header()
			if tc.echo == "" {
				// A released agent reads only the header: its declaration
				// echoed as sent and the reason as the notice it prints,
				// then the status it stops on.
				if err != nil || !slices.Equal(hdr.Get(agentservices.Header), []string{"ssh=tcp,web=http"}) ||
					len(hdr.Get("x-admin-message")) != 1 || !strings.Contains(hdr.Get("x-admin-message")[0], "update nfltr") {
					t.Fatalf("header %v, %v: want the echo and the notice", hdr, err)
				}
				if _, err = stream.Recv(); status.Code(err) != codes.FailedPrecondition || !strings.Contains(err.Error(), "update nfltr") {
					t.Fatalf("refusal %v, want FailedPrecondition saying to update nfltr", err)
				}
				if _, connected := d.AgentServices("svc-agent"); connected {
					t.Fatal("a refused agent is registered")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if got := hdr.Get(agentservices.Header); len(got) != 1 || got[0] != tc.echo {
				t.Fatalf("echo %q, want %q", got, tc.echo)
			}
			if services, _ := d.AgentServices("svc-agent"); agentservices.Format(services) != tc.stored {
				t.Fatalf("served %q, want %q", agentservices.Format(services), tc.stored)
			}
		})
	}
}

// answerReason is a refusal's reason (UNSPECIFIED for none, -1 for link's own
// codes).
func answerReason(a *link.Abort) relayv1.ErrorReason {
	if a == nil {
		return relayv1.ErrorReason_ERROR_REASON_UNSPECIFIED
	}
	if r, ok := dataplane.AbortReason(a); ok {
		return r
	}
	return -1
}
