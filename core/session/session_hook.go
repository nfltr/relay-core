// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package session

import (
	"context"

	"github.com/nfltr/relay/proto/pb"
	"google.golang.org/grpc/metadata"
)

// SessionHook is a frontend's part in agents' sessions (ConnectHTTP),
// set on the dispatcher before it serves (Dispatcher.SessionHook): Join is
// asked of each session before its accept, Claims of each frame's
// transaction ID. The core knows nothing of what a hook does.
type SessionHook interface {
	// Join returns the hook's part in the session of agentID (keyBase is
	// the key's own identity, "" for none), or nil for none. It may add to
	// the accept's metadata, or refuse the session with an error.
	Join(ctx context.Context, agentID, keyBase string, accept metadata.MD) (HookedSession, error)
	// Claims reports whether a frame with this transaction ID is the
	// hook's: it goes to the session's part (Frame), and is dropped when
	// the session has none.
	Claims(txnID string) bool
}

// HookedSession is a hook's part in one agent session.
type HookedSession interface {
	// Start runs once the session is registered, before any of its frames
	// is read: send writes a frame to the agent, end ends the session. An
	// error refuses the session.
	Start(send func(*pb.HTTPProxyRequest) error, end func()) error
	// Frame takes a frame the hook claims.
	Frame(txnID string, payload []byte)
	// Renew runs on each of the session's heartbeats.
	Renew()
	// Close runs when the session ends, after a Start that succeeded.
	Close()
}
