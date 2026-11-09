// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package session

// The bindings spec/proverif/e2ee_share.pv relies on, against the real
// code: each BIND:<id> the model lists has one row here (or in the
// repository that holds the check: ids relay.* and relayserver.*) that
// sends a protected share's connection where the model's attacker can send
// it and requires a refusal. `make spec-bindings`.

import (
	"context"
	"errors"
	"log/slog"
	"net"
	"os"
	"regexp"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/nfltr/link"
	"github.com/nfltr/relay-core/core/directory"
	"github.com/nfltr/relay-core/core/session/sessiontest/fake"
	relayv1 "github.com/nfltr/relay/proto/nfltr/relay/v1"
	"github.com/nfltr/relay/proto/wire/dataplane"
)

const shareBindModel = "../../spec/proverif/e2ee_share.pv"

// shareForeignRows are the model's ids whose rows are in the repository
// that holds the check: relay client/agent/e2ee_share_bindings_test.go (the
// agent's credential check) and relay-server
// core/replicas/e2ee_share_bindings_test.go (the replica link carries the
// share). A change here changes those rows.
var shareForeignRows = []string{"relay.agent.cred", "relayserver.link.share"}

var shareBindings = map[string]func(t *testing.T){
	// Gate on replica X: a protected share reaches only a stream whose
	// primary names it, also after attach moved the session
	"gate.here": func(t *testing.T) {
		t.Run("aux", TestProtectedShareOnAuxStreams)
		t.Run("handed-on", TestShareHandedOnToUnenforcingStreamRefused)
	},
	// Gate on replica Y (peer_link_tcp.go): a protected share the link
	// brought, to a stream that does not name it, on its frame streams and
	// on an agent that uses the agent link (where the open goes to the
	// agent before attach's recheck): refused before the agent is asked
	"gate.link": func(t *testing.T) {
		t.Run("frame streams", func(t *testing.T) {
			d := NewTCPDispatcher(slog.New(slog.DiscardHandler), nil, nil)
			pass := &fake.EndableStream{End: make(chan struct{})} // names no share
			defer close(pass.End)
			go func() { _ = d.RegisterStream("r", pass, directory.Session{}, nil) }()
			waitStreamsOf(t, d, "r", 1)
			refusedOnLink(t, d)
		})
		t.Run("agent link", func(t *testing.T) {
			d := NewTCPDispatcher(slog.New(slog.DiscardHandler), nil, nil)
			d.Link = newAgentLink(d, &net.TCPAddr{})
			p := NewTCPAgentStream() // names no share; no send loop, so SendCh keeps what the agent was sent
			p.agent, p.token, p.link.asked = "r", "pass-token", dataplane.AgentLinkTCP
			d.Mu.Lock()
			d.Streams["r"], d.All["r"] = p, []*tcpAgentStream{p}
			d.Mu.Unlock()
			refusedOnLink(t, d)
			if n := len(p.SendCh); n != 0 {
				t.Fatalf("the passthrough agent was sent %d frames (a dial request for the share's session)", n)
			}
		})
	},
}

// refusedOnLink requires a protected share the link brought to agent r to
// be refused SHARE_POLICY_UNENFORCED.
func refusedOnLink(t *testing.T, d *TCPDispatcher) {
	t.Helper()
	o, in := chainPair(t)
	go d.ServeLinkTCP(in, TCPEndpoint{AgentID: "r", TargetAddr: "__e2ee__", E2EE: true, Share: "swift-bay"})
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	var ab *link.Abort
	if err := o.Answered(ctx); !errors.As(err, &ab) || answerReason(ab) != relayv1.ErrorReason_ERROR_REASON_SHARE_POLICY_UNENFORCED {
		t.Fatalf("a protected share on the link to a passthrough stream: %v, want SHARE_POLICY_UNENFORCED", err)
	}
}

// TestE2EEShareModelBindings runs every row, and fails when the model's
// BIND ids and the rows differ.
func TestE2EEShareModelBindings(t *testing.T) {
	src, err := os.ReadFile(shareBindModel)
	if err != nil {
		t.Fatal(err)
	}
	var model, foreign []string
	for _, m := range regexp.MustCompile(`BIND:([a-z0-9.]+)`).FindAllSubmatch(src, -1) {
		if id := string(m[1]); strings.HasPrefix(id, "relay.") || strings.HasPrefix(id, "relayserver.") {
			foreign = append(foreign, id)
		} else {
			model = append(model, id)
		}
	}
	var rows []string
	for id := range shareBindings {
		rows = append(rows, id)
	}
	slices.Sort(model)
	slices.Sort(rows)
	slices.Sort(foreign)
	if !slices.Equal(model, rows) || !slices.Equal(foreign, shareForeignRows) {
		t.Fatalf("%s BIND ids %v and %v, rows %v and the other repositories' %v: each binding of the model needs one row", shareBindModel, model, foreign, rows, shareForeignRows)
	}
	for _, id := range rows {
		t.Run(id, shareBindings[id])
	}
}
