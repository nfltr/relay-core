// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

//go:build relaytrace

package session

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/nfltr/relay/proto/wire/agentwire"
	"google.golang.org/grpc/metadata"
)

// Trace mode (go test -tags relaytrace, RELAYTRACE=<dir>) also writes the
// endpoint's step of spec/Failover.tla, Accept(e), for every agent
// connect: "accept" (the record claimed, the agent registered) or "refuse"
// (ALREADY_ATTACHED, nothing claimed), with the relay ids the connect
// named, one file per dispatcher, <dir>/failover-<relay id>-<n>.ndjson,
// each line under traceMu (trace_on.go). For a later TraceFailover.tla
// (lane RFTRACE), merged with the agent's recorder.

type failoverEvent struct {
	Ev    string   `json:"ev"`    // accept | refuse
	Relay string   `json:"relay"` // this relay's id
	Agent string   `json:"agent"`
	Kind  string   `json:"kind"`            // ConnectHTTP | AgentTunnel | ConnectTCP
	Named []string `json:"named,omitempty"` // the AgentRelaysKey values
}

// failoverTraces is each dispatcher's file; under traceMu.
var failoverTraces = map[*Dispatcher]*os.File{}

func traceFailover(d *Dispatcher, ctx context.Context, ev, agentID, kind string) {
	dir := os.Getenv("RELAYTRACE")
	if dir == "" {
		return
	}
	traceMu.Lock()
	defer traceMu.Unlock()
	f := failoverTraces[d]
	if f == nil {
		name := strings.NewReplacer("/", "_", ":", "_").Replace(d.RelayID)
		var err error
		f, err = os.OpenFile(filepath.Join(dir, fmt.Sprintf("failover-%s-%d.ndjson", name, traceN.Add(1))), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600) // #nosec G304 -- the tester's RELAYTRACE
		if err != nil {
			panic(err)
		}
		failoverTraces[d] = f
	}
	b, err := json.Marshal(failoverEvent{Ev: ev, Relay: d.RelayID, Agent: agentID, Kind: kind, Named: metadata.ValueFromIncomingContext(ctx, agentwire.AgentRelaysKey)})
	if err != nil {
		panic(err)
	}
	if _, err := f.Write(append(b, '\n')); err != nil {
		panic(err)
	}
}
