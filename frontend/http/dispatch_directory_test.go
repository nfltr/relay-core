// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package httpingress

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/nfltr/relay-core/broker"
	"github.com/nfltr/relay-core/core/directory"
	"github.com/nfltr/relay-core/core/session"
	relayv1 "github.com/nfltr/relay/proto/nfltr/relay/v1"
	"github.com/spf13/viper"
)

// A request whose agent the relay cannot place, because its directory read
// failed (Redis failing fast after a partition), gets the relay's own 503
// typed ERROR_REASON_DIRECTORY_UNAVAILABLE: not the "machine is offline"
// page a registered agent that is not connected gets, nor a 404. Before,
// the directory's failure was answered as the agent being offline.
func TestDispatchAnswersADirectoryFailureAsTheRelays(t *testing.T) {
	viper.Set("http.server.producers.cache_size", 10)
	viper.Set("http.server.producers.cache_expiry", time.Minute)
	defer viper.Reset()

	b := broker.NewInMemoryMessageBroker()
	inv, err := directory.NewIPInventory(testLogger(), &mockCache{data: map[string]*directory.Value{}, err: errors.New("redis: circuit open")}, time.Second, "127.0.0.1", directory.GetDispatcherCacheKey)
	if err != nil {
		t.Fatal(err)
	}
	ing := newTestIngress(&session.Core{Broker: b, DispatcherIPInventory: inv})
	ing.core.Dispatcher = session.NewDispatcher(context.Background(), testLogger(), b, &testProducer{}, inv, inv, 5*time.Second, session.DispatcherConfig{ConnectedAgentsThreshold: 100})
	ing.registered = func(context.Context, string) bool { return true }

	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.Header.Set(session.AgentIDKey, "some-agent")
	http.HandlerFunc(ing.dispatch).ServeHTTP(w, req)
	if w.Code != http.StatusServiceUnavailable {
		t.Fatalf("status %d, want 503", w.Code)
	}
	body := w.Body.String()
	if !strings.Contains(body, relayv1.ErrorReason_ERROR_REASON_DIRECTORY_UNAVAILABLE.String()) || strings.Contains(body, agentOfflineMessage) {
		t.Fatalf("body %q: want the relay's own DIRECTORY_UNAVAILABLE status, not the agent-offline page", body)
	}
}
