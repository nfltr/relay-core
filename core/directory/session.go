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
// record): each claim takes the record's next Seq from a counter there
// (Cache.Incr, one per record: seqKey), which outlives the record (seqTTL) and never gives
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
// One that takes the same Seq as the stale session's is ordered by their
// start times, so one of them yields (relay-server spec/README.md finding
// R7).

// ErrSuperseded is returned by a renewal when a newer session of the agent
// on another pod holds its record, or held it and has ended.
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
	// PutSession writes the agent's record for this pod's session s.
	// Without renew (a claim) it writes nothing over a later claim's
	// record (a larger Seq). With renew it writes nothing over a newer
	// session's record (Session.Newer), returning ErrSuperseded when that
	// record names another pod, and writes nothing, returning
	// ErrSuperseded, when there is no record and the agent's Seq counter is
	// past s's. The check and the write are one store step
	// (Cache.SetAddIf).
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
	// The checks, the record and its set entries in one store step: no
	// claim lands between the checks and the write (relay-server
	// spec/README.md findings 2 and 3, spec/README.md finding 38), and no
	// reader finds the record without its set entries (finding 47). A
	// renewal restores what a restarted store lost.
	superseded := false
	match := func(v *Value, n int64) bool {
		superseded = false
		if !renew {
			// A claim writes over any record but a later claim's (two
			// claims racing: relay-server finding 2).
			return v == nil || s.Seq == 0 || v.Seq <= s.Seq
		}
		if v != nil && recordSession(v).Newer(s) {
			// A newer session's record: another pod's supersedes this one;
			// on this pod, write nothing (lowering the Seq would let this
			// one's DeleteSession take it).
			superseded = v.PodIP != GetPodIP()
			return false
		}
		// No record, but the agent's counter went past this session's
		// Seq: a newer session claimed it and has ended. Writing it back
		// would list the agent again with nothing announcing it (finding
		// 13); this session ends and the agent's reconnect claims anew. A
		// record without a Seq (a session whose claim got none) does not
		// hide the counter (finding 37).
		superseded = (v == nil || v.Seq == 0) && s.Seq != 0 && n > s.Seq
		return !superseded
	}
	if _, err := d.inventory.SetAddIf(ctx, key, seqKey(key), match, value, expiration, agentID, d.sessionSets(s)...); err != nil {
		return err
	}
	if superseded {
		return ErrSuperseded
	}
	return nil
}

// DeleteSession removes the agent's record when it names this pod's
// session s (its Seq and start time: a store that restarted empty can give
// a later session here the same Seq, spec/README.md finding 36; s without
// a Seq, a claim that got none, by its start time alone, finding 37; a
// record without a session: this pod): another session's record, here or
// on another pod, is kept.
func (d *ipInventory) DeleteSession(ctx context.Context, agentID string, s Session) error {
	ctx, cancel := context.WithTimeout(ctx, d.opTimeout)
	defer cancel()
	// The check and the delete are one store step (Cache.DelIf): a claim
	// landing meanwhile is kept, with its set entries (relay-server
	// spec/README.md finding 1b; spec/README.md findings 38, 47). An error
	// leaves the record: the caller's release failed (finding 27).
	self := GetPodIP()
	_, err := d.inventory.DelIf(ctx, d.getCacheKey(agentID), func(v *Value) bool {
		// A record without a session here (a renewal by SetIP: the relay's
		// own agent) is deleted by its pod, as DeleteIPs does; another
		// session's, here or on another pod, is kept.
		return v != nil && v.PodIP == self && (v.Seq == 0 && v.Since == 0 || v.Seq == s.Seq && recordSession(v).Since.Equal(s.Since))
	}, agentID, d.sessionSets(s)...)
	return err
}

// seqTTL is how long an agent's session counter outlives its last claim:
// longer than any partition, so a record that expired meanwhile does not
// reset the order.
const seqTTL = 30 * 24 * time.Hour

// seqKey is the session counter of the record at key: each record of an
// agent (dispatcher, streamer) counts its own sessions, so one record's
// claim never reads as a newer session of the other (spec/README.md
// finding 34).
func seqKey(recordKey string) string { return "nfltr:agent-session-seq:" + recordKey }

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
	key := d.getCacheKey(agentID)
	seq, err := d.inventory.Incr(ctx, seqKey(key), 1, seqTTL)
	if err != nil {
		return 0, err
	}
	if v, err := d.inventory.Get(ctx, key); err == nil && v != nil && v.Seq >= seq {
		return d.inventory.Incr(ctx, seqKey(key), v.Seq-seq+1, seqTTL)
	}
	return seq, nil
}

// Newer reports whether a is a later session of the agent than b: by their
// Seqs when both have one, an equal Seq (a store that restarted empty gave
// it twice: relay-server spec/README.md finding R7) or none by their start
// times.
func (a Session) Newer(b Session) bool {
	if a.Seq != 0 && b.Seq != 0 && a.Seq != b.Seq {
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
