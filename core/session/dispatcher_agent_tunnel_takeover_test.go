// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package session

import (
	"context"
	"testing"
	"time"
	"uuid"

	"github.com/nfltr/relay/client/rpcclient"
	"google.golang.org/grpc/metadata"
)

// First-user audit #2: a second AgentTunnel with a live agent's id (a second
// `nfltr node join` on one machine) replaced the first in the routing table
// but left its stream open, so when the second exited the first stayed
// connected yet unroutable ("target agent not connected") with nothing in
// its log. The newer session takes over and the older one is ended with the
// final "replaced" status its client acts on.
func TestAgentTunnelSecondConnectionEndsTheFirst(t *testing.T) {
	client, d, cleanup := newA2ABufConnEnv(t)
	defer cleanup()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	const id = "node-host"
	connect := func() *rpcclient.Agent {
		tunnel, err := client.AgentTunnel(metadata.NewOutgoingContext(ctx, metadata.Pairs("x-apikey-agentID", id, "x-auth-method", "apikey", rpcclient.InstanceHeader, uuid.New().String())))
		if err != nil {
			t.Fatal(err)
		}
		return rpcclient.NewAgent(id, tunnel)
	}
	first := connect()
	defer first.Close() //nolint:errcheck
	awaitUpstreamRegistered(t, d, id)
	firstStream, _ := d.Upstreams.Load(id)
	second := connect()
	select {
	case <-first.Done():
	case <-ctx.Done():
		t.Fatal("the first session stayed open after a second connection took its id")
	}
	if !rpcclient.IsSessionReplaced(first.Err()) {
		t.Fatalf("first session ended with %v, want the replaced status", first.Err())
	}
	for deadline := time.Now().Add(5 * time.Second); ; time.Sleep(10 * time.Millisecond) {
		if cur, ok := d.Upstreams.Load(id); ok && cur != firstStream {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("the second session is not the routed one")
		}
	}
	// The first session's teardown left the second's registration alone.
	time.Sleep(100 * time.Millisecond)
	if _, ok := d.Upstreams.Load(id); !ok {
		t.Fatal("the replaced session's teardown removed the newer session")
	}
	if _, ok := d.agentTunnelCancels.Load(id); !ok {
		t.Fatal("the replaced session's teardown removed the newer session's cancel")
	}
	_ = second.Close()
}

// Network is fickle: the same process reconnecting while the relay still
// holds its half-dead stream is a reconnect, not a takeover. The relay
// drops the older stream with a retryable status (never the final
// "replaced" one) and the process keeps running on its new connection;
// only another process (another instance id) taking the id ends the
// current one as replaced. Workers, nodes and hubs share this path
// (rpcclient.Dial sends the instance id).
func TestAgentTunnelReconnectOfTheSameProcessIsNotATakeover(t *testing.T) {
	client, d, cleanup := newA2ABufConnEnv(t)
	defer cleanup()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	const id = "node-host"
	connect := func(instance string) *rpcclient.Agent {
		tunnel, err := client.AgentTunnel(metadata.NewOutgoingContext(ctx, metadata.Pairs("x-apikey-agentID", id, "x-auth-method", "apikey", rpcclient.InstanceHeader, instance)))
		if err != nil {
			t.Fatal(err)
		}
		return rpcclient.NewAgent(id, tunnel)
	}
	ended := func(a *rpcclient.Agent, what string) error {
		select {
		case <-a.Done():
			return a.Err()
		case <-ctx.Done():
			t.Fatalf("%s stayed open", what)
			return nil
		}
	}
	halfDead := connect("process-1") // still open at the relay: a drop it has not seen
	defer halfDead.Close()           //nolint:errcheck
	awaitUpstreamRegistered(t, d, id)
	old, _ := d.Upstreams.Load(id)
	current := connect("process-1") // the same process's new connection
	defer current.Close()           //nolint:errcheck
	err := ended(halfDead, "the older stream of a reconnecting process")
	if rpcclient.IsSessionReplaced(err) {
		t.Fatalf("a reconnect of the same process told its older stream it was replaced: %v", err)
	}
	if !rpcclient.IsRetryableTransportError(err) {
		t.Fatalf("the older stream of a reconnect ended with a final status: %v", err)
	}
	for deadline := time.Now().Add(5 * time.Second); ; time.Sleep(10 * time.Millisecond) {
		if cur, ok := d.Upstreams.Load(id); ok && cur != old {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("the reconnected stream is not the routed one")
		}
	}
	select {
	case <-current.Done():
		t.Fatalf("the process's new connection ended: %v", current.Err())
	case <-time.After(200 * time.Millisecond):
	}

	other := connect("process-2") // another process takes the id
	defer other.Close()           //nolint:errcheck
	if err := ended(current, "the session another process took over"); !rpcclient.IsSessionReplaced(err) {
		t.Fatalf("a takeover by another process ended the session with %v, want the replaced status", err)
	}
	// Without an instance id (an older client) a second connection is a takeover.
	legacy := connect("")
	defer legacy.Close() //nolint:errcheck
	if err := ended(other, "the session a client without an instance id took over"); !rpcclient.IsSessionReplaced(err) {
		t.Fatalf("legacy takeover = %v", err)
	}
}
