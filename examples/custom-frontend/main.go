// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

// Command custom-frontend attaches the program's own frontend
// (frontend.Frontend) to a relay core: a plain TCP port on the relay that
// carries each connection to one agent's service, as the key's owner. A
// client with no relay protocol at all reaches the echo server behind the
// agent.
package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"log/slog"
	"net"
	"os"
	"time"

	"github.com/nfltr/relay-core/compose"
	"github.com/nfltr/relay-core/core/frontend"
	"github.com/nfltr/relay-core/core/identity"
	"github.com/nfltr/relay-core/core/session"
	"github.com/nfltr/relay-core/keyfile"
	"github.com/nfltr/relay-core/platform/config"
	"github.com/nfltr/relay/client/agent"
	"github.com/spf13/viper"
)

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	os.Args = os.Args[:1]
	config.SetDefaultFlags() // the relay's settings: defaults, then the environment
	viper.Set("tcp.enabled", true)
	const key = "rpc_minimal-example-key-0123456789"
	keys := keyfile.NewAPIKeyStore()
	keys.Add("demo", key)

	var ls compose.Ports // the relay's ports: clients, agents, /ready
	for _, l := range []*net.Listener{&ls.Proxy, &ls.GRPC, &ls.Internal} {
		*l = must(net.Listen("tcp", "127.0.0.1:0"))
	}
	port := must(net.Listen("tcp", "127.0.0.1:0")) // the frontend's own port
	go func() {
		log.Fatal(compose.Serve(ctx, slog.New(slog.DiscardHandler), ls, []frontend.Frontend{&tcpPort{l: port, agent: "echo", service: "22"}}, keyfile.Provider{Store: keys}))
	}()

	echo := must(net.Listen("tcp", "127.0.0.1:0")) // the service behind the agent
	go func() {
		for c, err := echo.Accept(); err == nil; c, err = echo.Accept() {
			go func() { _, _ = io.Copy(c, c); _ = c.Close() }()
		}
	}()
	a := agent.New(agent.Config{ServerAddr: ls.GRPC.Addr().String(), AgentID: "echo", APIKey: key,
		TCPTunnel: true, TCPForwardAddr: echo.Addr().String(), Logger: slog.New(slog.DiscardHandler)})
	if err := a.Connect(ctx); err != nil {
		log.Fatal(err)
	}
	defer a.Close()

	for ctx.Err() == nil { // the agent registers its TCP stream just after Connect
		c, err := net.Dial("tcp", port.Addr().String())
		if err == nil {
			msg := []byte("hello through a custom frontend")
			buf := make([]byte, len(msg))
			if _, err = c.Write(msg); err == nil {
				if _, err = io.ReadFull(c, buf); err == nil {
					fmt.Printf("echoed: %s\n", buf)
					return
				}
			}
			_ = c.Close()
		}
		time.Sleep(100 * time.Millisecond)
	}
	log.Fatal(ctx.Err())
}

// tcpPort is a frontend: on l, each connection is a session to agent's
// service, opened on behalf of the key "demo" (the relay's reach rule
// decides, as for any client).
type tcpPort struct {
	l              net.Listener
	agent, service string
}

func (*tcpPort) Name() string                 { return "tcp-port" }
func (*tcpPort) Settings() []frontend.Setting { return nil }

// Attach serves l with the relay's run (h.Go): it ends with the relay.
func (f *tcpPort) Attach(ctx context.Context, h frontend.Host) error {
	owner := identity.Principal{Provider: keyfile.ProviderName, Owner: "demo", Subject: "demo", AgentID: "demo"}
	h.Go(func() error {
		go func() { <-ctx.Done(); _ = f.l.Close() }()
		for {
			c, err := f.l.Accept()
			if err != nil {
				return nil
			}
			go func() {
				defer c.Close()
				s, err := h.Core().Open(ctx, owner, session.Target{Agent: f.agent, Service: f.service})
				if err != nil {
					h.Logger().Info("open refused", "err", err)
					return
				}
				defer s.Release()
				defer s.Close()
				go func() { _, _ = io.Copy(s, c); _ = s.CloseWrite() }()
				_, _ = io.Copy(c, s)
			}()
		}
	})
	return nil
}

func must[T any](v T, err error) T {
	if err != nil {
		log.Fatal(err)
	}
	return v
}
