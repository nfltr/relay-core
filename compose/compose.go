// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

// Package compose is a relay composition: the core (core/session, its
// in-process directory) with its frontends attached and its ports served,
// as nfltr-relay-core runs it (Serve), and the pieces a larger relay
// composes on its own (Composition, AgentPort, Listen, Metrics). It lives
// beside core/, not in it: it attaches frontends (frontend/connect), which
// core/ does not link.
package compose

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/nfltr/relay-core/broker"
	"github.com/nfltr/relay-core/core/directory"
	"github.com/nfltr/relay-core/core/frontend"
	"github.com/nfltr/relay-core/core/identity"
	"github.com/nfltr/relay-core/core/session"
	"github.com/nfltr/relay-core/core/sharecode"
	"github.com/nfltr/relay-core/frontend/connect"
	httpingress "github.com/nfltr/relay-core/frontend/http"
	"github.com/nfltr/relay-core/keyfile"
	"github.com/nfltr/relay-core/platform/clientgen"
	"github.com/nfltr/relay-core/platform/config"
	"github.com/nfltr/relay-core/platform/ratelimit"
	"github.com/nfltr/relay-core/services"
	"github.com/spf13/viper"
)

// Ports are a composition's listeners: proxy (clients' /connect, keys' own
// requests), gRPC (agents), internal (/ready), metrics, and the
// frontends' own (Ports: the SNI router's when
// e2ee.tls_passthrough.enabled).
type Ports struct {
	Proxy, GRPC, Internal, Metrics net.Listener
	Ports                          map[string]net.Listener
}

// Serve runs the core on ls until ctx ends, frontends attached
// (frontend.CoreHost): the dispatchers, the agents' gRPC port (AgentPort),
// the internal port's /ready and the metrics port's /metrics (and pprof
// with profiling.enable). It reads its settings from the process
// configuration (platform/config.SetDefaultFlags, then viper).
//
// Agents and clients authenticate with providers, asked in order, the
// first one primary (identity.NewAuthenticator). Without providers the
// key file (security.apikey.file) is the one provider, else the single key
// in the environment variable security.apikey.env names; with neither,
// Serve returns an error wrapping ErrNoKeys.
func Serve(ctx context.Context, logger *slog.Logger, ls Ports, frontends []frontend.Frontend, providers ...identity.Provider) error {
	keys := -1 // keys loaded from the key file or environment; -1: providers given
	if len(providers) == 0 {
		store, err := keyfile.LoadAPIKeys(viper.GetString("security.apikey.file"))
		if err != nil { // as nfltr-relay: the key file, else the key in the environment
			if store, err = keyfile.LoadAPIKeysFromEnv(viper.GetString("security.apikey.env")); err != nil {
				return fmt.Errorf("%w: %w", ErrNoKeys, err)
			}
		}
		keys, providers = store.Count(), []identity.Provider{keyfile.Provider{Store: store}}
	}
	grace := time.Duration(viper.GetInt("termination.grace.periodseconds")) * time.Second
	rules := session.TargetRules{TargetHost: viper.GetString("ssh.proxy.target_host"), E2EEPolicy: viper.GetString("tcp.e2ee_policy"), E2EEDefault: viper.GetBool("tcp.e2ee_default")}
	if rules.E2EEPolicy == "" {
		rules.E2EEPolicy = config.E2EEAllow
	}
	dialRetries, dialInterval := max(viper.GetInt("tcp.dial_max_retries"), 1), viper.GetDuration("tcp.dial_retry_interval")
	if dialInterval <= 0 {
		dialInterval = 500 * time.Millisecond
	}
	podID := os.Getenv(viper.GetString("pod_id.env_var"))
	dir := directory.NewMemoryCache()
	loopback := viper.GetString("loopback")
	dispInv, err := directory.NewIPInventory(logger, dir, viper.GetDuration("redis.ops_timeout"), loopback, directory.GetDispatcherCacheKey)
	if err != nil {
		return err
	}
	streamInv, err := directory.NewIPInventory(logger, dir, viper.GetDuration("redis.ops_timeout"), loopback, directory.GetStreamerCacheKey)
	if err != nil {
		return err
	}
	b := broker.NewInMemoryMessageBroker()
	defer b.Close()
	if err := b.CreateTopic(podID, 0, true); err != nil {
		return err
	}
	producer, err := b.CreateProducer(broker.ProducerOptions{Topic: podID})
	if err != nil {
		return err
	}
	defer producer.Close()

	subscriber, err := b.Subscribe(broker.ConsumerOptions{Topic: podID, SubscriptionName: podID + "-core", Type: broker.ConsumerTypeExclusive})
	if err != nil {
		return err
	}
	defer subscriber.Close()

	c := &session.Core{
		PodID:                 podID,
		Directory:             dir,
		DispatcherIPInventory: dispInv,
		StreamerIPInventory:   streamInv,
		Broker:                b,
		PodIDTopicSubscriber:  subscriber,
		PodIDTopicProducer:    producer,
		DrainBy:               sync.OnceValue(func() time.Time { return time.Now().Add(grace) }),
	}
	auth := identity.NewAuthenticator(providers[0].Name(), providers...)
	front := &connect.Frontend{Core: c, Logger: logger, Config: func() connect.Config {
		return connect.Config{Enabled: viper.GetBool("tcp.enabled"), MinClientGeneration: clientgen.Min(), E2EEPolicy: rules.E2EEPolicy}
	}}
	front.Caller = func(r *http.Request) identity.Principal {
		p, _, _ := connect.RequestPrincipal(auth, r, identity.Credential{})
		return p
	}
	front.Hold = func(ctx context.Context, p identity.Principal) (context.Context, func(), bool) {
		held := auth.Hold(ctx, p)
		return held, held.Release, true
	}

	d := session.NewDispatcher(ctx, logger.With("type", "dispatcher"), b, producer, dispInv, streamInv, grace)
	d.AgentsGo = front.Clients.Drained() // no peers: agents go once clients drained
	c.Dispatcher = d
	c.PublishedDispatcher.Store(d)
	if viper.GetBool("tcp.enabled") {
		c.WireTCP(ctx, logger, grace, front.Clients.Drained())
	}
	c.Reach = d.KeyReach(func() session.TargetRules { return rules })
	c.DialPolicy = func() (int, time.Duration) { return dialRetries, dialInterval }

	shares, journal, err := shareGrants(c, auth, logger, viper.GetString("accounts.service"))
	if err != nil {
		return err
	}
	internal := http.NewServeMux()
	internal.HandleFunc("/ready", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	internal.HandleFunc("/health", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	host := &frontend.CoreHost{C: c, Log: logger, Ports: ls.Ports, Shares: shares}
	proxy := &http.Server{ReadHeaderTimeout: 10 * time.Second, HTTP2: connect.HTTP2}
	comp := &Composition{
		Core: c, Logger: logger, Internal: ls.Internal, InternalHandler: internal, Metrics: ls.Metrics, Clients: &front.Clients,
		// The frontends this binary links (frontends.go) attach to the core.
		Start: func(ctx context.Context, run func(func() error)) error {
			host.Run = run
			if err := host.Attach(ctx, frontends...); err != nil {
				return err
			}
			ingress := host.HTTP()
			if ingress.AgentGone != nil {
				d.OnAgentDisconnect = ingress.AgentGone
			}
			mux := http.NewServeMux()
			mux.Handle(connect.Path, front)
			if journal != nil {
				mux.Handle(sharecode.JournalPath, journal)
			}
			if ingress.Serve != nil {
				var visits *httpingress.Shares
				if shares != nil { // as the relay's: its verified-credential cache, its miss budget per client IP
					visits = &httpingress.Shares{Grants: shares, Verified: httpingress.NewShareVerified(), Misses: ratelimit.New(1, 30, time.Minute, 10*time.Minute), Log: logger}
				}
				mux.Handle("/", ownRequests(auth, ingress.Serve, visits))
			}
			proxy.Handler = mux
			attrs := []any{"proxy", ls.Proxy.Addr().String(), "grpc", ls.GRPC.Addr().String(), "internal", ls.Internal.Addr().String(), "shares", shares != nil}
			if keys >= 0 {
				attrs = append(attrs, "keys", keys)
			}
			if l := ls.Ports[config.SNIRouterPort]; l != nil {
				attrs = append(attrs, "sni", l.Addr().String())
			}
			logger.Info("relay core serving", attrs...)
			return nil
		},
		// The agents' gRPC port, the reaper and HTTP ingress's fanout until
		// the drain lets the port stop.
		Agents: func() *AgentPort {
			return &AgentPort{Core: c, Logger: logger, Auth: auth, Listener: ls.GRPC, Fanout: host.HTTP().Fanout, Grace: grace, WaitClients: front.Clients.WaitDrained}
		},
		// The proxy port: on the drain (SIGTERM) it takes no new client; its
		// open requests drain to the deadline (hijacked data connections are
		// not its: Clients drains them), then the agents go and the gRPC port
		// stops (Core.ServeAgents).
		Serve: []func(context.Context) error{
			func(context.Context) error { return serveHTTP(proxy, ls.Proxy) },
			func(ctx context.Context) error {
				<-ctx.Done()
				sctx, cancel := context.WithDeadline(context.Background(), c.DrainBy())
				defer cancel()
				if proxy.Shutdown(sctx) != nil {
					_ = proxy.Close()
				}
				return nil
			},
		},
	}
	return comp.Run(ctx)
}

// ErrNoKeys is Serve's error when it has no provider and finds no API key
// in the key file or the environment.
var ErrNoKeys = errors.New("no API keys")

// ownRequests routes a share's visitor (<code>.<http.share_domain>,
// visits: nil without shares) to the shared agent, and a key's own HTTP
// request to its agent: the key (X-API-Key, Authorization: Bearer or
// ApiKey) and X-Agent-Id name the agent as the gRPC wire does (the key's
// namespace); the relay's credential never reaches the agent, and no
// client sets the identity headers the relay sets. Anything else is 401.
func ownRequests(auth *identity.Authenticator, serve http.Handler, visits *httpingress.Shares) http.Handler {
	domain := "." + strings.ToLower(strings.TrimSuffix(strings.TrimSpace(viper.GetString("http.share_domain")), "."))
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		for _, h := range []string{"X-Authenticated-Agent", "X-Auth-Method", "X-Fleet-Token-ID", services.CallerHeader, services.CallerPrincipalHeader, services.CallerMethodHeader, sharecode.RelayHeader} {
			r.Header.Del(h)
		}
		host := r.Host
		if h, _, err := net.SplitHostPort(host); err == nil {
			host = h
		}
		if code, ok := strings.CutSuffix(strings.ToLower(host), domain); ok && visits != nil && domain != "." && code != "" {
			visits.Serve(w, r, serve, code)
			return
		}
		p, header, ok := connect.RequestPrincipal(auth, r, identity.Credential{Name: r.Header.Get("X-Agent-Id")})
		if !ok || p.AgentID == "" {
			http.Error(w, "missing authentication credentials", http.StatusUnauthorized)
			return
		}
		r.Header.Del(header)
		r.Header.Del("X-Agent-Id")
		r.Header.Set(session.AgentIDKey, p.AgentID)
		serve.ServeHTTP(w, r.WithContext(auth.Carry(r.Context(), p)))
	})
}
