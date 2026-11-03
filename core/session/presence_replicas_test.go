// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package session

import (
	"errors"
	"maps"
	"os"
	"testing"
	"time"

	"github.com/spf13/viper"

	"github.com/nfltr/relay-core/core/directory"
	"github.com/nfltr/relay-core/core/session/sessiontest/fake"
)

const presAgent = "agent-1"

// presReplicas is one agent's sessions on two replicas, a and b, sharing a
// directory (mc) and a bus, and a watcher on a that re-lists on every wake
// (presenceclient.Wait, then a list: its stream here, else the record).
// With -tags relaytrace its steps are a presence trace (spec/README.md
// "Presence traces"). The pod address is process-wide
// (directory.GetPodIP): each replica's step sets it.
type presReplicas struct {
	t      *testing.T
	mc     *fake.MockCache
	a, b   *Dispatcher
	toA    chan []byte
	ip     map[*Dispatcher]string
	cursor string
}

func newPresReplicas(t *testing.T) *presReplicas {
	viper.Set("pod_ip.env_var", "TEST_POD_IP")
	t.Cleanup(viper.Reset)
	t.Setenv("TEST_POD_IP", "")
	p := &presReplicas{t: t, mc: newMockCache(), toA: make(chan []byte, 16), ip: map[*Dispatcher]string{}}
	toB := make(chan []byte, 16)
	g := tracePresenceStart(presAgent)
	replica := func(name, ip string, pub, sub chan []byte) *Dispatcher {
		d, cancel := newTestDispatcher(t)
		t.Cleanup(cancel)
		inv, err := directory.NewIPInventory(fake.TestLogger(), p.mc, time.Second, "127.0.0.1", directory.GetStreamerCacheKey)
		if err != nil {
			t.Fatal(err)
		}
		d.StreamerIPInventory = inv
		tracePresenceJoin(&d.Presence, g, name)
		d.Presence.BindBus(t.Context(), fakePresenceBus{pub: pub, sub: sub})
		p.ip[d] = ip
		return d
	}
	p.a, p.b = replica("a", "10.0.0.1", toB, p.toA), replica("b", "10.0.0.2", p.toA, toB)
	p.cursor = p.a.Presence.Watch(t.Context(), "", 0, nil).GetCursor() // the watcher's baseline
	return p
}

func (p *presReplicas) on(d *Dispatcher) { _ = os.Setenv("TEST_POD_IP", p.ip[d]) }

func (p *presReplicas) connect(d *Dispatcher) *GRPCStream {
	p.on(d)
	l := map[string]string{"host": "h"}
	s, err := claimAgentRecord(p.t.Context(), d.StreamerIPInventory, presAgent, directory.Session{Since: time.Now(), Credential: "cred", Account: "acct"}, l, nil, 5*time.Second)
	if err != nil {
		p.t.Fatal(err)
	}
	st := NewGRPCStream(presAgent, nil, nil, nil, nil)
	st.connectedAt, st.seq, st.credential, st.account = s.Since, s.Seq, s.Credential, s.Account
	st.connectLabels = maps.Clone(l)
	st.SetLabels(l)
	d.Upstreams.Store(presAgent, st)
	d.notifyUpstreamRegistered(presAgent)
	return st
}

func (p *presReplicas) update(d *Dispatcher) {
	p.on(d)
	if _, ok := d.UpdateAgentLabels(presAgent, map[string]string{"k": "v"}, true); !ok {
		p.t.Fatal("update refused")
	}
}

func (p *presReplicas) renew(d *Dispatcher, st *GRPCStream) error {
	p.on(d)
	d.InventoryRefreshByAgentID.Delete(presAgent)
	return d.refreshAgentInventoryOnInboundActivity(p.t.Context(), presAgent, d.StreamerIPInventory, st)
}

func (p *presReplicas) end(d *Dispatcher, st *GRPCStream) {
	p.on(d)
	d.Upstreams.Delete(presAgent)
	d.departed(presAgent, st.Session(), st)
}

// relist waits for the watcher's wake, takes the cursor and lists: the
// agent must be listed (want) with label k = wantK.
func (p *presReplicas) relist(want bool, wantK string) {
	p.t.Helper()
	a := p.a
	if r := a.Presence.Watch(p.t.Context(), p.cursor, 10*time.Second, nil); len(r.GetEvents()) == 0 && !r.GetReset_() {
		p.t.Fatal("no wake")
	}
	p.cursor = a.Presence.Cursor()
	var got bool
	var labels map[string]string
	if s, ok := a.Upstreams.Load(presAgent); ok {
		got, labels = true, s.(*GRPCStream).Labels()
	} else {
		p.mc.Mu.Lock()
		if v := p.mc.Data[directory.GetStreamerCacheKey(presAgent)]; v != nil {
			got, labels = true, maps.Clone(v.Labels)
		}
		p.mc.Mu.Unlock()
	}
	tracePresSnap(&a.Presence, got, labels)
	if got != want || labels["k"] != wantK {
		p.t.Fatalf("listed=%v labels=%v, want listed=%v k=%q", got, labels, want, wantK)
	}
}

// TestPresenceAcrossReplicas: each re-list answers the agent as it is now:
// connected on b, its labels updated there, after the bus's resync, its
// newer session on a superseding b's, and gone.
func TestPresenceAcrossReplicas(t *testing.T) {
	p := newPresReplicas(t)
	sb := p.connect(p.b)
	p.relist(true, "")
	p.update(p.b)
	p.relist(true, "v")
	if err := p.renew(p.b, sb); err != nil {
		t.Fatal(err)
	}
	p.toA <- []byte{} // the bus resubscribed: a resync
	p.relist(true, "v")
	sa := p.connect(p.a) // a newer session here
	p.relist(true, "")
	if err := p.renew(p.b, sb); !errors.Is(err, directory.ErrSuperseded) {
		t.Fatalf("renewal of the older session on b = %v, want superseded", err)
	}
	p.end(p.b, sb)
	p.end(p.a, sa)
	p.relist(false, "")
}

// TestPresenceAcrossReplicasFailedWrites (finding 27): a label update whose
// record write failed is listed on a once a renewal writes it, and a
// release that failed leaves the agent listed until the record's TTL, then
// a second down wakes the watcher to list it gone.
func TestPresenceAcrossReplicasFailedWrites(t *testing.T) {
	p := newPresReplicas(t)
	p.b.Config.HeartbeatRecvThreshold = 50 * time.Millisecond
	sb := p.connect(p.b)
	p.relist(true, "")
	setCacheErr(p.mc, errors.New("store down"))
	p.update(p.b)
	p.relist(true, "") // the record keeps the connect labels
	setCacheErr(p.mc, nil)
	if err := p.renew(p.b, sb); err != nil {
		t.Fatal(err)
	}
	p.relist(true, "v")
	setCacheErr(p.mc, errors.New("store down"))
	p.end(p.b, sb)
	p.relist(true, "v") // the record stays until its TTL
	setCacheErr(p.mc, nil)
	p.mc.Mu.Lock()
	delete(p.mc.Data, directory.GetStreamerCacheKey(presAgent)) // its TTL passed
	p.mc.Mu.Unlock()
	p.relist(false, "") // woken by the second down
}
