// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package session

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nfltr/relay/proto/pb"
	"github.com/spf13/viper"
)

// afterFuncCountingCtx counts the cancel children registered on it (the
// context package registers them through AfterFunc) that are still live.
type afterFuncCountingCtx struct {
	context.Context
	live, registered atomic.Int64
}

// Value hides the embedded cancelCtx so children register through AfterFunc.
func (c *afterFuncCountingCtx) Value(any) any { return nil }

func (c *afterFuncCountingCtx) AfterFunc(f func()) func() bool {
	c.live.Add(1)
	c.registered.Add(1)
	stop := context.AfterFunc(c.Context, f)
	var once sync.Once
	return func() bool {
		once.Do(func() { c.live.Add(-1) })
		return stop()
	}
}

// Every A2A transaction's session context is a child of the source agent's
// stream context. A finished transaction must release it: a long-lived
// stream (a hub, a worker) otherwise retains one context per transaction
// for its lifetime (long soak: relay heap floor +8 MB/h, profile top
// context.propagateCancel under routeAgentToAgent).
func TestDispatcher_routeAgentToAgent_ReleasesTheSessionContext(t *testing.T) {
	d, cancel := newTestDispatcher(t)
	defer cancel()
	defer viper.Reset()

	streamCtx := &afterFuncCountingCtx{Context: t.Context()}
	sourceStreamer := &channelStreamer{sendCh: make(chan *pb.TunnelFrame, 10), ctx: context.Background()}
	sourceStream := newTestGRPCStream("worker-1", sourceStreamer, context.Background())
	frame := multiPodAssignFrame("absent-agent")
	frame.Routing.AgentId = "worker-1"
	frame.Payload = []byte(`{"type":"task.progress"}`)

	d.dispatchAgentToAgent(streamCtx, sourceStream, frame)
	select {
	case <-sourceStreamer.sendCh:
	case <-time.After(4 * time.Second):
		t.Fatal("transaction never finished")
	}
	deadline := time.Now().Add(2 * time.Second)
	for streamCtx.live.Load() != 0 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if streamCtx.registered.Load() == 0 {
		t.Fatal("the transaction registered no context on the stream context; the probe observes nothing")
	}
	if n := streamCtx.live.Load(); n != 0 {
		t.Fatalf("%d transaction context(s) still registered on the stream context after the transaction ended", n)
	}
}
