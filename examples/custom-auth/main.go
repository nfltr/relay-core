// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

// Command custom-auth runs a relay core whose agents and clients
// authenticate with the program's own identity.Provider (tokens), not the
// key file: a token it does not know is refused, a token it knows carries
// bytes to its agent.
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
	"github.com/nfltr/relay-core/core/identity"
	"github.com/nfltr/relay-core/platform/config"
	"github.com/nfltr/relay/client/agent"
	pb "github.com/nfltr/relay/proto/pb"
	"github.com/nfltr/relay/proto/wire/dataplane"
	"github.com/spf13/viper"
)

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	os.Args = os.Args[:1]
	config.SetDefaultFlags() // the relay's settings: defaults, then the environment
	viper.Set("tcp.enabled", true)
	const key = "team-blue-token"

	var ls compose.Ports // the relay's ports: clients, agents, /ready
	for _, l := range []*net.Listener{&ls.Proxy, &ls.GRPC, &ls.Internal} {
		*l = must(net.Listen("tcp", "127.0.0.1:0"))
	}
	go func() {
		log.Fatal(compose.Serve(ctx, slog.New(slog.DiscardHandler), ls, nil, tokens{"team-blue-token": "blue"}))
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

	if _, _, err := dataplane.Upgrade(ctx, must(net.Dial("tcp", ls.Proxy.Addr().String())), "relay", "a-stranger", 1<<20); err == nil {
		log.Fatal("a token the provider does not know was admitted")
	}
	m, _ := must2(dataplane.Upgrade(ctx, must(net.Dial("tcp", ls.Proxy.Addr().String())), "relay", key, 1<<20))
	go func() { _ = m.Serve(nil) }()
	for ctx.Err() == nil { // the agent registers its TCP stream just after Connect
		s, err := m.Open(&pb.TCPData{AgentId: "echo", TargetAddr: "22"})
		if err == nil {
			msg := []byte("hello through the relay core")
			buf := make([]byte, len(msg))
			if _, err = s.Write(msg); err == nil {
				if _, err = io.ReadFull(s, buf); err == nil {
					fmt.Printf("echoed: %s\n", buf)
					return
				}
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
	log.Fatal(ctx.Err())
}

// tokens is an identity.Provider: each token owns its team, whose agents
// take any name under it; any other token is unknown.
type tokens map[string]string

func (tokens) Name() string { return "tokens" }

func (t tokens) Authenticate(_ context.Context, c identity.Credential) (identity.Principal, error) {
	team, ok := t[c.Token]
	if !ok {
		return identity.Principal{}, identity.ErrUnknownCredential
	}
	return identity.Principal{Owner: team, Subject: team, Names: identity.AnyName}, nil
}

func must[T any](v T, err error) T {
	if err != nil {
		log.Fatal(err)
	}
	return v
}

func must2[T, U any](v T, u U, err error) (T, U) { return must(v, err), u }
