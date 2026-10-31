// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package session

import (
	"context"
	"maps"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

	presencev1 "github.com/nfltr/relay/proto/nfltr/presence/v1"
)

// presenceHistory bounds the events kept for resume; a cursor older than it
// answers reset (the watcher re-lists).
const presenceHistory = 256

var presenceEvents = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "nfltr_relay_presence_events_total",
	Help: "Presence changes (up, down, labels) published on the relay presence feed, by origin (local, peer; folded: a local change not yet sent to peers that folded into the agent's next); change resync: the bus's subscription was re-established, a new feed epoch makes every watcher re-list.",
}, []string{"change", "origin"})

// PresenceBus carries presence events between relay replicas sharing one
// deployment (Redis pub/sub in relayserver). Publish must not block on a slow
// peer for long; Subscribe delivers peer payloads until ctx ends. An empty
// payload is a resync: the subscription was (re)established and may have
// missed peer payloads, so the feed starts a new epoch and its watchers
// re-list.
type PresenceBus interface {
	Publish(ctx context.Context, payload []byte) error
	Subscribe(ctx context.Context) <-chan []byte
}

// presenceFeed is the relay's presence change signal. The zero value is
// ready to use.
type presenceFeed struct {
	Mu      sync.Mutex
	epoch   string
	rev     uint64
	recent  []*presencev1.PresenceEvent // revisions rev-len(recent)+1 .. rev
	changed chan struct{}
	Fanout  func(ev *presencev1.PresenceEvent)
}

func (f *presenceFeed) initLocked() {
	if f.changed == nil {
		f.changed = make(chan struct{})
		f.epoch = strconv.FormatInt(time.Now().UnixNano(), 36)
	}
}

// resync starts a new epoch: the feed may have missed peer changes, so
// every cursor of the old one answers reset (its watcher re-lists), a
// waiting watcher at once.
func (f *presenceFeed) resync() {
	f.Mu.Lock()
	f.initLocked()
	old := f.epoch
	for n := time.Now().UnixNano(); f.epoch == old; n++ {
		f.epoch = strconv.FormatInt(n, 36)
	}
	f.recent = nil
	close(f.changed)
	f.changed = make(chan struct{})
	f.Mu.Unlock()
	presenceEvents.WithLabelValues("resync", "peer").Inc()
}

func (f *presenceFeed) cursorLocked() string {
	f.initLocked()
	return f.epoch + "." + strconv.FormatUint(f.rev, 10)
}

func (f *presenceFeed) Cursor() string {
	f.Mu.Lock()
	defer f.Mu.Unlock()
	return f.cursorLocked()
}

func presenceChangeName(ev *presencev1.PresenceEvent) string {
	switch ev.GetChange().(type) {
	case *presencev1.PresenceEvent_Up:
		return "up"
	case *presencev1.PresenceEvent_Down:
		return "down"
	default:
		return "labels"
	}
}

// up publishes an agent connecting on this replica. allow:wrapper (event builder)
func (f *presenceFeed) up(agentID string, labels map[string]string, capabilities []string) {
	f.publish(&presencev1.PresenceEvent{AgentId: agentID, Change: &presencev1.PresenceEvent_Up{Up: &presencev1.AgentUp{
		Labels: maps.Clone(labels), Capabilities: slices.Clone(capabilities)}}})
}

// down publishes an agent leaving this replica. allow:wrapper (event builder)
func (f *presenceFeed) down(agentID string) {
	f.publish(&presencev1.PresenceEvent{AgentId: agentID, Change: &presencev1.PresenceEvent_Down{Down: &presencev1.AgentDown{}}})
}

// labels publishes an agent's new label set. allow:wrapper (event builder)
func (f *presenceFeed) labels(agentID string, labels map[string]string) {
	f.publish(&presencev1.PresenceEvent{AgentId: agentID, Change: &presencev1.PresenceEvent_Labels{Labels: &presencev1.LabelsChanged{Labels: maps.Clone(labels)}}})
}

func (f *presenceFeed) publish(ev *presencev1.PresenceEvent) {
	f.bump(ev, "local")
	f.Mu.Lock()
	fanout := f.Fanout
	f.Mu.Unlock()
	if fanout != nil {
		fanout(ev)
	}
}

// bump assigns ev this replica's next revision and wakes every waiter.
func (f *presenceFeed) bump(ev *presencev1.PresenceEvent, origin string) {
	f.Mu.Lock()
	f.initLocked()
	f.rev++
	ev.Revision = f.rev
	if len(f.recent) == presenceHistory {
		f.recent = append(f.recent[:0], f.recent[1:]...)
	}
	f.recent = append(f.recent, ev)
	close(f.changed)
	f.changed = make(chan struct{})
	f.Mu.Unlock()
	presenceEvents.WithLabelValues(presenceChangeName(ev), origin).Inc()
}

// parseCursor returns the revision a cursor names on this replica, or
// ok=false for one from another epoch or malformed.
func (f *presenceFeed) parseCursorLocked(cursor string) (uint64, bool) {
	gotEpoch, gotRev, found := strings.Cut(cursor, ".")
	if !found || gotEpoch != f.epoch {
		return 0, false
	}
	n, err := strconv.ParseUint(gotRev, 10, 64)
	if err != nil || n > f.rev {
		return 0, false
	}
	return n, true
}

// sinceLocked returns the events after since that visible admits, and
// reset=true when since is older than the kept history.
func (f *presenceFeed) sinceLocked(since uint64, visible func(string) bool) (events []*presencev1.PresenceEvent, reset bool) {
	if since >= f.rev {
		return nil, false
	}
	first := f.rev - uint64(len(f.recent)) + 1
	if since+1 < first {
		reset = true
		since = first - 1
	}
	for _, ev := range f.recent[since+1-first:] {
		if visible == nil || visible(ev.GetAgentId()) {
			events = append(events, ev)
		}
	}
	return events, reset
}

// Watch answers one long-poll for a waiter that sees the agents visible
// admits (nil: all). An empty cursor returns the current cursor at once. A
// cursor from another epoch or malformed answers reset at once. Otherwise it
// returns the visible events after cursor as soon as there is one, or none at
// maxWait or ctx end.
func (f *presenceFeed) Watch(ctx context.Context, cursor string, maxWait time.Duration, visible func(string) bool) *presencev1.WatchResponse {
	f.Mu.Lock()
	f.initLocked()
	if cursor == "" {
		defer f.Mu.Unlock()
		return &presencev1.WatchResponse{Cursor: f.cursorLocked()}
	}
	since, ok := f.parseCursorLocked(cursor)
	if !ok {
		defer f.Mu.Unlock()
		return &presencev1.WatchResponse{Cursor: f.cursorLocked(), Reset_: true}
	}
	epoch := f.epoch
	f.Mu.Unlock()
	var timeout <-chan time.Time
	if maxWait > 0 {
		timer := time.NewTimer(maxWait)
		defer timer.Stop()
		timeout = timer.C
	}
	for {
		f.Mu.Lock()
		if f.epoch != epoch { // resync
			defer f.Mu.Unlock()
			return &presencev1.WatchResponse{Cursor: f.cursorLocked(), Reset_: true}
		}
		events, reset := f.sinceLocked(since, visible)
		current, changed := f.cursorLocked(), f.changed
		since = f.rev
		f.Mu.Unlock()
		if len(events) > 0 || reset || timeout == nil {
			return &presencev1.WatchResponse{Cursor: current, Events: events, Reset_: reset}
		}
		select {
		case <-changed:
		case <-timeout:
			return &presencev1.WatchResponse{Cursor: current}
		case <-ctx.Done():
			return &presencev1.WatchResponse{Cursor: current}
		}
	}
}

// BindBus connects this replica's feed to peers: local changes are
// published, peer changes are appended under a local revision. A nil bus
// (single replica) leaves the feed local.
func (f *presenceFeed) BindBus(ctx context.Context, bus PresenceBus) {
	if bus == nil {
		return
	}
	// Off the frame-relay path: a slow bus must never stall relaying. One
	// goroutine publishes, in each agent's order; changes not yet
	// published fold into the agent's latest (presenceOutbox), so a slow
	// bus holds one pending change per agent, not one goroutine per change.
	out := &presenceOutbox{pending: map[string]*presencev1.PresenceEvent{}, wake: make(chan struct{}, 1)}
	f.Mu.Lock()
	f.Fanout = out.add
	f.Mu.Unlock()
	go out.run(ctx, bus)
	peer := bus.Subscribe(ctx)
	go func() {
		for payload := range peer {
			if len(payload) == 0 {
				f.resync()
				continue
			}
			var ev presencev1.PresenceEvent
			if protojson.Unmarshal(payload, &ev) != nil || ev.GetAgentId() == "" || ev.GetChange() == nil {
				continue
			}
			f.bump(&ev, "peer")
		}
	}()
}

// notifyUpstreamRegistered publishes agentID's arrival to presence and
// wakes sessiontest.AwaitUpstream's waiters.
func (d *Dispatcher) notifyUpstreamRegistered(agentID string) {
	if d == nil {
		return
	}
	if d.Upstreams != nil {
		if stream, ok := d.Upstreams.Load(agentID); ok {
			labels, _ := stream.(interface{ Labels() map[string]string })
			caps, _ := stream.(interface{ Capabilities() []string })
			var l map[string]string
			var c []string
			if labels != nil {
				l = labels.Labels()
			}
			if caps != nil {
				c = caps.Capabilities()
			}
			d.Presence.up(agentID, l, c)
		}
	}
	if d.UpstreamRegCond == nil {
		return
	}
	d.UpstreamRegMu.Lock()
	d.UpstreamRegCond.Broadcast()
	d.UpstreamRegMu.Unlock()
}

// presenceOutbox holds the local changes not yet published to peers: at
// most one per agent, in the order the agents first changed.
type presenceOutbox struct {
	mu      sync.Mutex
	order   []string
	pending map[string]*presencev1.PresenceEvent
	wake    chan struct{}
}

// add queues ev, folded into the agent's pending change: an up or down
// replaces it; a label change updates a pending up's labels and is dropped
// after a pending down. Each fold is counted (presence_events, "folded").
func (o *presenceOutbox) add(ev *presencev1.PresenceEvent) {
	ev = proto.Clone(ev).(*presencev1.PresenceEvent) // the feed keeps ev
	id := ev.GetAgentId()
	o.mu.Lock()
	prev, ok := o.pending[id]
	switch {
	case !ok:
		o.order = append(o.order, id)
		o.pending[id] = ev
	case ev.GetLabels() != nil && prev.GetUp() != nil:
		prev.GetUp().Labels = ev.GetLabels().GetLabels()
	case ev.GetLabels() != nil && prev.GetDown() != nil:
	default:
		o.pending[id] = ev
	}
	o.mu.Unlock()
	if ok {
		presenceEvents.WithLabelValues(presenceChangeName(ev), "folded").Inc()
	}
	select {
	case o.wake <- struct{}{}:
	default:
	}
}

// run publishes the pending changes until ctx ends, then what is still
// pending within 5 s (an agent's last down reaches peers at shutdown).
func (o *presenceOutbox) run(ctx context.Context, bus PresenceBus) {
	for {
		select {
		case <-ctx.Done():
			drain, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
			o.publish(drain, bus)
			cancel()
			return
		case <-o.wake:
			o.publish(ctx, bus)
		}
	}
}

// publish sends the pending changes, in order, until none is left or ctx
// ends; each send is bounded at 5 s.
func (o *presenceOutbox) publish(ctx context.Context, bus PresenceBus) {
	for ctx.Err() == nil {
		o.mu.Lock()
		if len(o.order) == 0 {
			o.order = nil // let the backing array go after a burst
			o.mu.Unlock()
			return
		}
		id := o.order[0]
		o.order = o.order[1:]
		ev := o.pending[id]
		delete(o.pending, id)
		o.mu.Unlock()
		payload, err := protojson.Marshal(ev)
		if err != nil {
			continue
		}
		pubCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		_ = bus.Publish(pubCtx, payload)
		cancel()
	}
}
