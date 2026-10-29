// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package directory

import (
	"context"
	"errors"
	"fmt"
	"time"
)

// An agent's record names the session that wrote it, so replicas agree on
// which of an agent's sessions is current when the agent has more than one:
// it redialled another replica while the replica it left still holds its old
// stream (a partition, a dead peer the old replica has not noticed). The
// newer session wins: it is admitted although the record names another pod
// (same credential), and the old session's renewal finds the newer record,
// writes nothing and ends instead of pointing the agent back at itself.
//
// "Newer" is the agent's session order in the shared store, not the
// replicas' clocks (a replica whose clock ran behind kept or took back the
// record): each claim takes the agent's next Seq from a counter there
// (Cache.Incr), which outlives the agent's record (seqTTL) and never gives
// a claim a Seq at or below the current record's. Records and sessions
// without a Seq (an older relay's, during a rolling upgrade) are
// ordered by their start times, as those replicas order them; Since is
// still written for them. The limit: a store that restarts empty loses the
// counters. A claim that comes after a surviving session re-announced its
// record still takes a larger Seq; one that comes before it takes a small
// Seq, so that stale session (the agent left it, its replica has not
// noticed) takes the record back and the new session yields at its next
// renewal; the agent then reconnects and its next claim, above the stale
// record's Seq, wins.

// ErrSuperseded is returned by a renewal when a newer session of the agent
// on another pod holds its record.
var ErrSuperseded = errors.New("a newer session of this agent on another relay replica holds its record")

// Session is one agent session as its record names it: when it started,
// its place in the agent's session order (0: none), a tag of the
// credential it connected with (empty: none) and the account owning the
// agent (empty: none, or a record an older relay wrote).
type Session struct {
	Since      time.Time
	Seq        int64
	Credential string
	Account    string
	// Services is what the session declared (agentservices.Format; empty:
	// none): every replica resolves the agent's services from its record.
	Services string
}

// SessionStore is implemented by the shared inventory (not by the no-op
// one): agent records that name their session.
type SessionStore interface {
	// GetSession returns the pod the agent's record names (the loopback
	// address for this pod) and its session; a record an older relay wrote
	// has a zero Session.
	GetSession(ctx context.Context, agentID string) (podIP string, s Session, err error)
	// PutSession writes the agent's record for this pod's session s. With
	// renew it first reads the record and writes nothing, returning
	// ErrSuperseded, when the record names another pod's session that is
	// newer than s (Session.newer).
	PutSession(ctx context.Context, agentID string, s Session, renew bool, labels map[string]string, capabilities []string, expiration time.Duration) error
	// ClaimSession starts this pod's session s of the agent: it takes the
	// agent's next Seq and writes the record (PutSession without renew).
	// It returns s with its Seq (none when the store had none to give).
	ClaimSession(ctx context.Context, agentID string, s Session, labels map[string]string, capabilities []string, expiration time.Duration) (Session, error)
}

var _ SessionStore = (*ipInventory)(nil)

func (d *ipInventory) GetSession(ctx context.Context, agentID string) (string, Session, error) {
	ctx, cancel := context.WithTimeout(ctx, d.opTimeout)
	defer cancel()
	v, err := d.inventory.Get(ctx, d.getCacheKey(agentID))
	if err != nil && !errors.Is(err, ErrNotFound) {
		return "", Session{}, fmt.Errorf("%w: %w", ErrUnavailable, err)
	}
	if v == nil || v.PodIP == "" {
		return "", Session{}, ErrAgentNotConnected
	}
	pod := v.PodIP
	if pod == GetPodIP() {
		pod = d.loopback
	}
	return pod, recordSession(v), nil
}

func (d *ipInventory) PutSession(ctx context.Context, agentID string, s Session, renew bool, labels map[string]string, capabilities []string, expiration time.Duration) error {
	ctx, cancel := context.WithTimeout(ctx, d.opTimeout)
	defer cancel()
	key := d.getCacheKey(agentID)
	if renew {
		// ponytail: get-then-set, not atomic; a session that loses the
		// microsecond race is corrected by the newer one's next renewal.
		if v, err := d.inventory.Get(ctx, key); err == nil && v != nil && v.PodIP != GetPodIP() && recordSession(v).newer(s) {
			return ErrSuperseded
		}
	}
	value := Value{
		PodIP:        GetPodIP(),
		Labels:       cloneStringMap(labels),
		Capabilities: cloneStringSlice(capabilities),
		Since:        s.Since.UnixNano(),
		Seq:          s.Seq,
		Credential:   s.Credential,
		Account:      s.Account,
		Services:     s.Services,
	}
	if _, err := d.inventory.Set(ctx, key, value, expiration); err != nil {
		return err
	}
	_, err := d.inventory.SAdd(ctx, d.activeAgentSetKey(), agentID)
	return err
}

// seqTTL is how long an agent's session counter outlives its last claim:
// longer than any partition, so a record that expired meanwhile does not
// reset the order.
const seqTTL = 30 * 24 * time.Hour

func seqKey(agentID string) string { return "nfltr:agent-session-seq:" + agentID }

func (d *ipInventory) ClaimSession(ctx context.Context, agentID string, s Session, labels map[string]string, capabilities []string, expiration time.Duration) (Session, error) {
	seq, err := d.nextSeq(ctx, agentID)
	if err != nil {
		return s, err
	}
	s.Seq = seq
	return s, d.PutSession(ctx, agentID, s, false, labels, capabilities, expiration)
}

// nextSeq takes the agent's next Seq, above the current record's (the
// counter was lost or expired under a long session).
func (d *ipInventory) nextSeq(ctx context.Context, agentID string) (int64, error) {
	ctx, cancel := context.WithTimeout(ctx, d.opTimeout)
	defer cancel()
	seq, err := d.inventory.Incr(ctx, seqKey(agentID), 1, seqTTL)
	if err != nil {
		return 0, err
	}
	if v, err := d.inventory.Get(ctx, d.getCacheKey(agentID)); err == nil && v != nil && v.Seq >= seq {
		return d.inventory.Incr(ctx, seqKey(agentID), v.Seq-seq+1, seqTTL)
	}
	return seq, nil
}

// newer reports whether a is a later session of the agent than b: by their
// Seqs when both have one, else by their start times.
func (a Session) newer(b Session) bool {
	if a.Seq != 0 && b.Seq != 0 {
		return a.Seq > b.Seq
	}
	return a.Since.After(b.Since)
}

func recordSession(v *Value) Session {
	s := Session{Seq: v.Seq, Credential: v.Credential, Account: v.Account, Services: v.Services}
	if v.Since != 0 {
		s.Since = time.Unix(0, v.Since)
	}
	return s
}
