// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

//go:build relaytrace

package session

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// The Handoff trace (go test -tags relaytrace, RELAYTRACE=<dir>): the
// relay-side steps of spec/Handoff.tla of one agent on one TCPDispatcher
// whose agent link is on, to <dir>/handoff-<agent>-<n>.ndjson (spec/README.md
// "Trace validation"). Primaries are numbered 1.. in registration order
// (Handoff's Gens), a token by the primary that minted it, client sessions
// c1.. in arrival order. Each line is written under traceMu at the state
// change it records, inside the lock that orders that change (d.Mu, the
// link's Mu): traceMu is the innermost lock. The agent's steps (Dial,
// AgentEnd, Adopt, AdoptRefused) and its path's failure are not logged:
// spec/TraceHandoff.tla lets TLC choose them.
type handoffTrace struct {
	f    *os.File
	gens int
	toks map[string]int // token -> the primary that minted it
	sess int
}

// handoffEvent is one line: ev the model step, g a primary, t a token's
// primary, s a session, via its transport (link, grpc).
type handoffEvent struct {
	Ev  string `json:"ev"`
	G   int    `json:"g,omitempty"`
	T   int    `json:"t,omitempty"`
	S   string `json:"s,omitempty"`
	Via string `json:"via,omitempty"`
}

type handoffKey struct {
	d     *TCPDispatcher
	agent string
}

var (
	handoffTraces = map[handoffKey]*handoffTrace{} // under traceMu
	handoffGen    = map[*tcpAgentStream]int{}      // a traced primary's number
	handoffTokens = map[string]*handoffTrace{}     // a traced token's trace
)

func (h *handoffTrace) log(e handoffEvent) {
	b := fmt.Appendf(nil, `{"ev":%q,"g":%d,"t":%d,"s":%q,"via":%q}`+"\n", e.Ev, e.G, e.T, e.S, e.Via)
	if _, err := h.f.Write(b); err != nil {
		panic(err)
	}
}

// traceHRegister: primary as registered (bound[token] = as), under d.Mu.
func traceHRegister(d *TCPDispatcher, as *tcpAgentStream) {
	dir := os.Getenv("RELAYTRACE")
	if dir == "" || d.Link == nil || as.link.asked == "" {
		return
	}
	traceMu.Lock()
	defer traceMu.Unlock()
	k := handoffKey{d, as.agent}
	h := handoffTraces[k]
	if h == nil {
		name := strings.NewReplacer("/", "_", ":", "_").Replace(as.agent)
		f, err := os.OpenFile(filepath.Join(dir, fmt.Sprintf("handoff-%s-%d.ndjson", name, traceN.Add(1))), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600) // #nosec G304 -- the tester's RELAYTRACE
		if err != nil {
			panic(err)
		}
		h = &handoffTrace{f: f, toks: map[string]int{}}
		handoffTraces[k] = h
	}
	h.gens++
	handoffGen[as] = h.gens
	t, ok := h.toks[as.token]
	if !ok {
		t = h.gens
		h.toks[as.token] = t
		handoffTokens[as.token] = h
	}
	h.log(handoffEvent{Ev: "Register", G: h.gens, T: t})
}

// handoffOf is p's trace and number; nil when p is not traced. Under traceMu.
func handoffOf(p *tcpAgentStream) (*handoffTrace, int) {
	g, ok := handoffGen[p]
	if !ok {
		return nil, 0
	}
	return handoffTokens[p.token], g
}

// traceHPrimEnd: primary as left the rotation (unregister), under d.Mu.
func traceHPrimEnd(as *tcpAgentStream) {
	traceMu.Lock()
	defer traceMu.Unlock()
	if h, g := handoffOf(as); h != nil {
		h.log(handoffEvent{Ev: "PrimEnd", G: g})
	}
}

// traceHConn: token's link went from no connection to one (up) or back,
// under the link's Mu.
func traceHConn(token string, up bool) {
	traceMu.Lock()
	defer traceMu.Unlock()
	if h := handoffTokens[token]; h != nil {
		ev := "LinkDrop"
		if up {
			ev = "Bind"
		}
		h.log(handoffEvent{Ev: ev, T: h.toks[token]})
	}
}

// traceHSilent: token's connection on transport (tcp, quic) went silent
// and its primary ends (agentLink.silent).
func traceHSilent(token, transport string) {
	traceMu.Lock()
	defer traceMu.Unlock()
	if h := handoffTokens[token]; h != nil {
		h.log(handoffEvent{Ev: "Silent", T: h.toks[token], Via: transport})
	}
}

// traceHCarries: carriesSessions answered c (via "yes" or "no").
func traceHCarries(d *TCPDispatcher, agentID string, c bool) {
	traceMu.Lock()
	defer traceMu.Unlock()
	if h := handoffTraces[handoffKey{d, agentID}]; h != nil {
		via := "no"
		if c {
			via = "yes"
		}
		h.log(handoffEvent{Ev: "Carries", Via: via})
	}
}

type handoffSessKey struct{}

// handoffSess is a client session's name in its trace.
type handoffSess struct {
	h *handoffTrace
	s string
}

// traceHArrive: a client session arrives for the agent of primary p; ctx
// carries its name to open and the caller's placement.
func traceHArrive(ctx context.Context, p *tcpAgentStream) context.Context {
	if p == nil {
		return ctx
	}
	traceMu.Lock()
	defer traceMu.Unlock()
	h, _ := handoffOf(p)
	if h == nil {
		return ctx
	}
	h.sess++
	s := &handoffSess{h: h, s: fmt.Sprintf("c%d", h.sess)}
	h.log(handoffEvent{Ev: "Arrive", S: s.s})
	return context.WithValue(ctx, handoffSessKey{}, s)
}

func handoffSessOf(ctx context.Context) *handoffSess {
	s, _ := ctx.Value(handoffSessKey{}).(*handoffSess)
	return s
}

// traceHWait: the session waits for p's link (a dial asked), under the link's Mu.
func traceHWait(ctx context.Context) {
	if s := handoffSessOf(ctx); s != nil {
		traceMu.Lock()
		defer traceMu.Unlock()
		s.h.log(handoffEvent{Ev: "Wait", S: s.s})
	}
}

// traceHPlaced: the session is open on p's link (via "link": on p's token)
// or on p's frame streams (via "grpc"); p nil: refused.
func traceHPlaced(ctx context.Context, p *tcpAgentStream, via string) {
	s := handoffSessOf(ctx)
	if s == nil {
		return
	}
	traceMu.Lock()
	defer traceMu.Unlock()
	if p == nil {
		s.h.log(handoffEvent{Ev: "Refused", S: s.s})
		return
	}
	p = primaryOf(p)
	e := handoffEvent{Ev: "Open", S: s.s, Via: via}
	if via == "link" {
		e.T = s.h.toks[p.token]
	} else {
		e.G = handoffGen[p]
	}
	s.h.log(e)
}

// traceHEnd: the session ended here.
func traceHEnd(ctx context.Context) {
	if s := handoffSessOf(ctx); s != nil {
		traceMu.Lock()
		defer traceMu.Unlock()
		s.h.log(handoffEvent{Ev: "End", S: s.s})
	}
}
