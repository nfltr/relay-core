// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package connect

import (
	"errors"
	"sync"

	"github.com/nfltr/link"
	"github.com/nfltr/relay-core/core/identity"
	relayv1 "github.com/nfltr/relay/proto/nfltr/relay/v1"
	"github.com/nfltr/relay/proto/wire/dataplane"
)

// sessionSet is a data connection's open sessions, so that its
// credential's revocation ends each one typed before the connection closes
// (nfltr docs/decisions/key-revocation-ends-sessions-2026-11-01.md). The
// zero value is ready to use.
type sessionSet struct {
	mu sync.Mutex
	s  map[*dataplane.Session]struct{}
}

// add records s; sessions that ended since are dropped here.
// ponytail: O(open sessions) per add, no goroutine per session; index by
// Done if a data connection ever holds thousands at once.
func (o *sessionSet) add(s *dataplane.Session) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.s == nil {
		o.s = make(map[*dataplane.Session]struct{})
	}
	for x := range o.s {
		select {
		case <-x.Done():
			delete(o.s, x)
		default:
		}
	}
	o.s[s] = struct{}{}
}

// abort ends every open session with a; nil aborts none.
func (o *sessionSet) abort(a *link.Abort) {
	if a == nil {
		return
	}
	o.mu.Lock()
	open := make([]*dataplane.Session, 0, len(o.s))
	for s := range o.s {
		open = append(open, s)
	}
	o.mu.Unlock()
	for _, s := range open {
		_ = s.AbortWith(a) // flushed before it returns: ahead of the connection's close
	}
}

// credentialAbort is the end of a session whose connection's hold ended
// with cause: ERROR_REASON_BALANCE_EXHAUSTED when its account's prepaid
// balance is exhausted, ERROR_REASON_UNAUTHENTICATED when its credential
// was revoked or expired, nil otherwise (the client left: nothing to tell
// it).
func credentialAbort(cause error) *link.Abort {
	switch {
	case errors.Is(cause, identity.ErrBalanceExhausted):
		return &link.Abort{Code: dataplane.AbortCode(relayv1.ErrorReason_ERROR_REASON_BALANCE_EXHAUSTED), Msg: cause.Error()}
	case identity.Ended(cause):
		return &link.Abort{Code: dataplane.AbortCode(relayv1.ErrorReason_ERROR_REASON_UNAUTHENTICATED), Msg: cause.Error()}
	}
	return nil
}
