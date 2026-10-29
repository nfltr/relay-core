// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package compose

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"slices"
	"strings"
	"time"

	"github.com/nfltr/relay-core/core/frontend"
	"github.com/nfltr/relay-core/core/identity"
	"github.com/nfltr/relay-core/core/session"
	"github.com/nfltr/relay-core/frontend/connect"
	"github.com/spf13/viper"
)

// accountsService is the tcp service an accounts service agent declares.
const accountsService = "accounts"

// shareGrants verifies share codes the accounts service issued
// (accounts.service: <owner>/<agent>[,...]), asking the service for each
// code it has not cached (frontend.ServiceGrantLookup over a session to
// the agent's accounts service) and reporting each code it granted, so the
// service keeps it reserved for its agent (frontend.ShareUses,
// http.share_reservation_ttl); nil without accounts.service: no shares.
// The core issues, lists and revokes none. journal is the service's word
// that it revoked something (sharecode.JournalPath, with the key of the
// service's owner): the core then reads every code again.
func shareGrants(c *session.Core, auth *identity.Authenticator, logger *slog.Logger, spec string) (_ frontend.GrantVerifier, journal http.Handler, _ error) {
	if spec == "" {
		return nil, nil, nil
	}
	type agent struct{ owner, id string }
	var agents []agent
	for e := range strings.SplitSeq(spec, ",") {
		owner, id, ok := strings.Cut(strings.TrimSpace(e), "/")
		if !ok || owner == "" || id == "" || strings.ContainsAny(id, "/\\") {
			return nil, nil, fmt.Errorf("accounts.service %q: want <owner>/<agent>", e)
		}
		agents = append(agents, agent{owner, id})
	}
	dial := func(ctx context.Context, _, _ string) (net.Conn, error) {
		var last error = session.ErrOffline
		for _, a := range agents {
			// Never another account's agent of that name.
			if got, err := c.Dispatcher.OwnerForAgent(a.id); err != nil || !strings.EqualFold(got, a.owner) {
				if err != nil {
					last = err
				}
				continue
			}
			conn, err := c.ServiceConn(ctx, a.id, accountsService)
			if err == nil {
				return conn, nil
			}
			last = err
		}
		return nil, last
	}
	client := &http.Client{Timeout: 10 * time.Second, Transport: &http.Transport{DialContext: dial, MaxIdleConnsPerHost: -1, DisableCompression: true}}
	base := "http://" + accountsService + ".service"
	uses := &frontend.ShareUses{
		Send:    frontend.ServiceShareUses(client, base),
		Reserve: func() time.Duration { return viper.GetDuration("http.share_reservation_ttl") },
		After:   shareUsesFlush, Log: logger.Warn,
	}
	grants := frontend.NewCachedGrants(frontend.ServiceGrantLookup(client, base), uses.Used)
	journal = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		p, _, ok := connect.RequestPrincipal(auth, r, identity.Credential{})
		if !ok || r.Method != http.MethodPost || !slices.ContainsFunc(agents, func(a agent) bool { return strings.EqualFold(p.Owner, a.owner) }) {
			http.NotFound(w, r)
			return
		}
		grants.ForgetAll()
		w.WriteHeader(http.StatusNoContent)
	})
	return grants, journal, nil
}

// shareUsesFlush is how long granted codes accumulate before the core
// reports them (the relay's credential and share facts flush alike).
const shareUsesFlush = 30 * time.Second
