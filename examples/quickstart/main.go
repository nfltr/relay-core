// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

// Command quickstart is the README's agent and client against a running
// nfltr-relay-core: it connects an agent "echo" in front of a TCP echo
// server, then, as a client of the same key, sends bytes through the relay
// to it and prints what came back.
//
//	quickstart -key-file nfltr-relay-core.keys -agents 127.0.0.1:9090 -clients 127.0.0.1:8080
package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log"
	"log/slog"
	"net"
	"os"
	"strings"
	"time"

	"github.com/nfltr/relay/client/agent"
	pb "github.com/nfltr/relay/proto/pb"
	"github.com/nfltr/relay/proto/wire/dataplane"
)

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	keyFile := flag.String("key-file", "nfltr-relay-core.keys", "the relay's key file; its last line is <agent id>:<key>")
	agents := flag.String("agents", "127.0.0.1:9090", "the relay's gRPC address (agents)")
	clients := flag.String("clients", "127.0.0.1:8080", "the relay's proxy address (clients)")
	flag.Parse()
	data, err := os.ReadFile(*keyFile)
	for ; err != nil && ctx.Err() == nil; data, err = os.ReadFile(*keyFile) { // the relay writes it as it starts
		time.Sleep(100 * time.Millisecond)
	}
	lines := strings.Split(strings.TrimSpace(string(must(data, err))), "\n")
	_, key, _ := strings.Cut(lines[len(lines)-1], ":")

	echo := must(net.Listen("tcp", "127.0.0.1:0")) // the service behind the agent
	go func() {
		for c, err := echo.Accept(); err == nil; c, err = echo.Accept() {
			go func() { _, _ = io.Copy(c, c); _ = c.Close() }()
		}
	}()
	a := agent.New(agent.Config{ServerAddr: *agents, AgentID: "echo", APIKey: key,
		TCPTunnel: true, TCPForwardAddr: echo.Addr().String(), Logger: slog.New(slog.DiscardHandler)})
	if err := a.Connect(ctx); err != nil {
		log.Fatal(err)
	}
	defer a.Close()

	m, _ := must2(dataplane.Upgrade(ctx, must(net.Dial("tcp", *clients)), "relay", key, 1<<20))
	go func() { _ = m.Serve(nil) }()
	for ctx.Err() == nil { // the agent registers its TCP stream just after Connect
		s, err := m.Open(&pb.TCPData{AgentId: "echo", TargetAddr: "22"})
		if err == nil {
			msg := []byte("hello through nfltr-relay-core")
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

func must[T any](v T, err error) T {
	if err != nil {
		log.Fatal(err)
	}
	return v
}

func must2[T, U any](v T, u U, err error) (T, U) { return must(v, err), u }
