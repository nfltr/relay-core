// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package session

import (
	"context"
	"errors"
	"github.com/nfltr/relay-core/core/session/sessiontest/fake"
	"runtime"
	"strings"
	"testing"
	"time"

	"golang.org/x/sync/errgroup"
)

// runSession runs the housekeeping on the handler's goroutine and returns
// what eg.Wait would have returned with the housekeeping in the group.
func TestRunSessionReturnsTheFirstErrorAndEndsTheGroup(t *testing.T) {
	errReceive := errors.New("receive loop ended")
	errMember := errors.New("relay loop failed")
	d := &Dispatcher{globalContext: context.Background(), Logger: fake.TestLogger()}

	t.Run("housekeeping first", func(t *testing.T) {
		parent, cancel := context.WithCancelCause(context.Background())
		defer cancel(nil)
		eg, ctx := errgroup.WithContext(parent)
		eg.Go(func() error { <-ctx.Done(); return ctx.Err() }) // a member that runs until the group ends
		received := make(chan error, 1)
		received <- errReceive
		if err := d.runSession(eg, ctx, cancel, "a", nil, nil, received); !errors.Is(err, errReceive) {
			t.Fatalf("runSession = %v, want the housekeeping's error", err)
		}
		if !errors.Is(context.Cause(parent), errReceive) {
			t.Fatalf("session cause = %v, want the housekeeping's error", context.Cause(parent))
		}
	})

	t.Run("group member first", func(t *testing.T) {
		parent, cancel := context.WithCancelCause(context.Background())
		defer cancel(nil)
		eg, ctx := errgroup.WithContext(parent)
		eg.Go(func() error { return errMember })
		if err := d.runSession(eg, ctx, cancel, "a", nil, nil, nil); !errors.Is(err, errMember) {
			t.Fatalf("runSession = %v, want the member's error", err)
		}
	})

	t.Run("session ended from outside", func(t *testing.T) {
		parent, cancel := context.WithCancelCause(context.Background())
		eg, ctx := errgroup.WithContext(parent)
		eg.Go(func() error { <-ctx.Done(); return ctx.Err() })
		cancel(errSessionReplaced)
		if err := d.runSession(eg, ctx, cancel, "a", nil, nil, nil); !errors.Is(err, context.Canceled) {
			t.Fatalf("runSession = %v, want context.Canceled", err)
		}
		if !errors.Is(context.Cause(parent), errSessionReplaced) {
			t.Fatalf("session cause = %v, want errSessionReplaced kept", context.Cause(parent))
		}
	})
}

// The housekeeping holds no goroutine of its own while the session is idle:
// WatchAgentSession runs on the goroutine that called runSession.
func TestRunSessionStartsNoGoroutine(t *testing.T) {
	d := &Dispatcher{globalContext: context.Background(), Logger: fake.TestLogger()}
	parent, cancel := context.WithCancelCause(context.Background())
	defer cancel(nil)
	eg, ctx := errgroup.WithContext(parent)
	received := make(chan error, 1)
	done := make(chan error, 1)
	go func() { done <- d.runSession(eg, ctx, cancel, "a", nil, nil, received) }()
	// Other tests' sessions may still be ending: look for this shape only.
	inline := func() (onCaller, inGroup int) {
		for _, g := range goroutinesIn("session.(*Dispatcher).WatchAgentSession") {
			switch {
			case strings.Contains(g, "session.(*Dispatcher).runSession"):
				onCaller++
			case strings.Contains(g, "errgroup.(*Group).Go"):
				inGroup++
			}
		}
		return onCaller, inGroup
	}
	onCaller, inGroup := 0, 0
	for deadline := time.Now().Add(2 * time.Second); onCaller == 0 && time.Now().Before(deadline); time.Sleep(5 * time.Millisecond) {
		onCaller, inGroup = inline()
	}
	if onCaller == 0 || inGroup != 0 {
		t.Fatalf("housekeeping on runSession's goroutine: %d, in an errgroup goroutine: %d; want it only on the caller's", onCaller, inGroup)
	}
	received <- nil
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

// goroutinesIn returns the stacks of the goroutines with frame in them.
func goroutinesIn(frame string) []string {
	buf := make([]byte, 1<<20)
	for n := runtime.Stack(buf, true); ; n = runtime.Stack(buf, true) {
		if n < len(buf) { // the dump fit: other tests' goroutines may fill a small buffer
			buf = buf[:n]
			break
		}
		buf = make([]byte, 2*len(buf))
	}
	var out []string
	for g := range strings.SplitSeq(string(buf), "\n\n") {
		if strings.Contains(g, frame) {
			out = append(out, g)
		}
	}
	return out
}
