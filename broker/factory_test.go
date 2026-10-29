// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package broker

import (
	"fmt"
	"testing"
	"time"
)

// ---------------------------------------------------------------------------
// GetBrokerNamespace
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// GetBrokerTopicName
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// GetMessageBroker
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// GetSubscriptionMode
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// CreateTopic
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// NewProducers
// ---------------------------------------------------------------------------

func TestNewProducers_GetCacheHit(t *testing.T) {
	b := NewInMemoryMessageBroker()
	_ = b.CreateTopic("t1", 1, false)

	producers := NewProducers[string](10, time.Minute, nil)
	p, _, err := producers.Get("key1", b, ProducerOptions{Topic: "t1"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if p == nil {
		t.Fatal("expected producer")
	}

	// Second get should return cached
	p2, _, err := producers.Get("key1", b, ProducerOptions{Topic: "t1"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if p2 == nil {
		t.Fatal("expected cached producer")
	}
}

// TestProducersUnboundedKeepsEveryAgentRemoveRecreates: a size of 0 holds
// any number of agents' producers (a cap of 100 closed a producer per
// request once more than 100 agents were busy), and Remove closes one so the
// next Get makes a fresh producer on the same topic.
func TestProducersUnboundedKeepsEveryAgentRemoveRecreates(t *testing.T) {
	b := NewInMemoryMessageBroker()
	producers := NewProducers[string](0, time.Minute, nil)
	first := map[string]Producer{}
	for i := range 300 {
		topic := fmt.Sprintf("agent-%d", i)
		_ = b.CreateTopic(topic, 1, false)
		p, release, err := producers.Get(topic, b, ProducerOptions{Topic: topic})
		if err != nil {
			t.Fatal(err)
		}
		release()
		first[topic] = p
	}
	for topic, p := range first {
		again, release, _ := producers.Get(topic, b, ProducerOptions{Topic: topic})
		release()
		if again != p {
			t.Fatalf("%s: producer replaced while cached", topic)
		}
	}
	producers.Remove("agent-0")
	p, _, err := producers.Get("agent-0", b, ProducerOptions{Topic: "agent-0"})
	if err != nil || p == first["agent-0"] {
		t.Fatalf("after Remove: %v, same producer %v; want a new one", err, p == first["agent-0"])
	}
}

func TestNewProducers_GetCreateError(t *testing.T) {
	b := NewInMemoryMessageBroker()
	// Don't create topic — should fail
	producers := NewProducers[string](10, time.Minute, nil)
	_, _, err := producers.Get("k", b, ProducerOptions{Topic: "nonexistent"})
	if err == nil {
		t.Fatal("expected error for non-existent topic")
	}
}

// TestProducersNeverCloseAProducerInUse: a producer that leaves the cache
// (expiry, counted from creation and found on a read: the cache has no
// janitor; size eviction; Remove) while a request
// holds it is closed when that request releases it, not under it, and a Get
// meanwhile takes it back rather than making a second producer on its topic.
func TestProducersNeverCloseAProducerInUse(t *testing.T) {
	for _, tc := range []struct {
		name   string
		size   int
		expiry time.Duration
		leave  func(p *Producers[string], b MessageBroker)
	}{
		{"expiry", 0, 20 * time.Millisecond, func(p *Producers[string], _ MessageBroker) {
			time.Sleep(50 * time.Millisecond)
			_, _ = p.cache.Get("k") // a read finds it expired
		}},
		{"size eviction", 1, time.Minute, func(p *Producers[string], b MessageBroker) {
			_, release, _ := p.Get("other", b, ProducerOptions{Topic: "o"})
			release()
		}},
		{"remove", 0, time.Minute, func(p *Producers[string], _ MessageBroker) { p.Remove("k") }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			closedK := make(chan struct{}, 4)
			b := NewInMemoryMessageBroker()
			holdAndLeave := func() (*Producers[string], Producer, func()) {
				b = NewInMemoryMessageBroker()
				_ = b.CreateTopic("t", 1, false)
				_ = b.CreateTopic("o", 1, false)
				producers := NewProducers[string](tc.size, tc.expiry, func(k string, p Producer) {
					p.Close()
					if k == "k" {
						closedK <- struct{}{}
					}
				})
				held, release, err := producers.Get("k", b, ProducerOptions{Topic: "t"})
				if err != nil {
					t.Fatal(err)
				}
				tc.leave(producers, b)
				select {
				case <-closedK:
					t.Fatal("producer closed while a request was using it")
				case <-time.After(50 * time.Millisecond):
				}
				return producers, held, release
			}

			// The last holder's release closes it.
			_, _, release := holdAndLeave()
			release()
			select {
			case <-closedK:
			case <-time.After(time.Second):
				t.Fatal("released producer was not closed")
			}

			// A Get while it is held takes it back.
			producers, held, release := holdAndLeave()
			defer release()
			again, releaseAgain, err := producers.Get("k", b, ProducerOptions{Topic: "t"})
			if err != nil {
				t.Fatalf("Get while the producer that left is held: %v", err)
			}
			releaseAgain()
			if again != held {
				t.Fatal("Get made a second producer instead of taking the held one back")
			}
		})
	}
}
