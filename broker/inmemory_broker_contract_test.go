// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package broker

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// brokerKeys counts what the broker keeps by name.
func brokerKeys(b FullBroker) (topics, producers int) {
	m := b.(*inMemoryMessageBroker)
	m.topics.Range(func(string, *inMemoryTopic) bool { topics++; return true })
	m.producers.Range(func(string, *inMemoryProducer) bool { producers++; return true })
	return topics, producers
}

// An agent that leaves leaves nothing keyed by its id in the broker: its
// session subscribes to a topic named by its id, a sender may hold a
// producer on it, and once both are closed the broker forgets the topic,
// its subscription and its producer.
func TestInMemoryBrokerForgetsWhatLeft(t *testing.T) {
	b := NewInMemoryMessageBroker()
	defer b.Close()
	for i := range 1000 {
		id := fmt.Sprintf("agent-%d", i)
		require.NoError(t, b.CreateTopic(id, 0, true))
		c, err := b.Subscribe(ConsumerOptions{Topic: id, SubscriptionName: id, Type: ConsumerTypeExclusive})
		require.NoError(t, err)
		c.Chan()
		if i%2 == 0 { // a sender reached half of them; it leaves after the agent
			p, err := b.CreateProducer(ProducerOptions{Topic: id})
			require.NoError(t, err)
			require.NoError(t, c.Unsubscribe())
			c.Close()
			_, err = p.SendBytes(context.Background(), []byte("x"))
			require.ErrorIs(t, err, ErrNoConsumer) // as before: the sender answers for it
			p.Close()
			continue
		}
		require.NoError(t, c.Unsubscribe())
		c.Close()
	}
	topics, producers := brokerKeys(b)
	require.Zero(t, topics, "topics left")
	require.Zero(t, producers, "producers left")
}

// A topic stays while a producer or a subscriber holds it: the other side
// comes back to the same topic.
func TestInMemoryBrokerKeepsAHeldTopic(t *testing.T) {
	b := NewInMemoryMessageBroker()
	defer b.Close()
	require.NoError(t, b.CreateTopic("t", 0, false))
	p, err := b.CreateProducer(ProducerOptions{Topic: "t"})
	require.NoError(t, err)
	c, err := b.Subscribe(ConsumerOptions{Topic: "t", SubscriptionName: "s"})
	require.NoError(t, err)
	c.Close()
	// The producer holds the topic: the reconnect finds it and gets what
	// the held producer sends.
	require.ErrorIs(t, b.CreateTopic("t", 0, false), ErrTopicExists)
	c2, err := b.Subscribe(ConsumerOptions{Topic: "t", SubscriptionName: "s"})
	require.NoError(t, err)
	ch := c2.Chan()
	_, err = p.SendBytes(context.Background(), []byte("x"))
	require.NoError(t, err)
	require.Equal(t, []byte("x"), (<-ch).Payload())
	p.Close()
	topics, _ := brokerKeys(b) // c2 still holds it
	require.Equal(t, 1, topics)
	c2.Close()
	topics, producers := brokerKeys(b)
	require.Zero(t, topics)
	require.Zero(t, producers)
	// Gone, it is created again.
	require.NoError(t, b.CreateTopic("t", 0, false))
}

func recv(t *testing.T, c Consumer) Message {
	t.Helper()
	select {
	case m, ok := <-c.Chan():
		require.True(t, ok, "channel closed")
		return m
	case <-time.After(2 * time.Second):
		t.Fatal("no message")
		return nil
	}
}

func requireClosed(t *testing.T, c Consumer) {
	t.Helper()
	select {
	case _, ok := <-c.Chan():
		require.False(t, ok, "a message on a channel that should be closed")
	case <-time.After(2 * time.Second):
		t.Fatal("channel not closed")
	}
}

func waitErr(t *testing.T, ch <-chan error) error {
	t.Helper()
	select {
	case err := <-ch:
		return err
	case <-time.After(2 * time.Second):
		t.Fatal("send still blocked")
		return nil
	}
}

// The rules of the package doc, one test each.
func TestInMemoryBrokerContract(t *testing.T) {
	ctx := context.Background()
	joined := func(t *testing.T) (FullBroker, Producer, Consumer) {
		b := NewInMemoryMessageBroker()
		t.Cleanup(b.Close)
		require.NoError(t, b.CreateTopic("t", 0, false))
		p, err := b.CreateProducer(ProducerOptions{Topic: "t"})
		require.NoError(t, err)
		c, err := b.Subscribe(ConsumerOptions{Topic: "t", SubscriptionName: "s"})
		require.NoError(t, err)
		return b, p, c
	}
	fill := func(t *testing.T, p Producer) {
		for range consumerBuffer {
			_, err := p.SendBytes(ctx, nil)
			require.NoError(t, err)
		}
	}
	for _, tc := range []struct {
		name string
		run  func(t *testing.T)
	}{
		{"send with no consumer fails", func(t *testing.T) {
			b := NewInMemoryMessageBroker()
			defer b.Close()
			require.NoError(t, b.CreateTopic("t", 0, false))
			p, err := b.CreateProducer(ProducerOptions{Topic: "t"})
			require.NoError(t, err)
			_, err = p.SendBytes(ctx, nil)
			require.ErrorIs(t, err, ErrNoConsumer)
		}},
		{"send after the consumer closed fails", func(t *testing.T) {
			_, p, c := joined(t)
			c.Close()
			_, err := p.SendBytes(ctx, nil)
			require.ErrorIs(t, err, ErrNoConsumer)
		}},
		{"a consumer gets what was sent before its first Chan", func(t *testing.T) {
			_, p, c := joined(t)
			_, err := p.SendBytes(ctx, []byte("early"))
			require.NoError(t, err)
			require.Equal(t, []byte("early"), recv(t, c).Payload())
		}},
		{"one sender's messages arrive in order", func(t *testing.T) {
			_, p, c := joined(t)
			for i := range consumerBuffer {
				_, err := p.SendBytes(ctx, []byte{byte(i)})
				require.NoError(t, err)
			}
			for i := range consumerBuffer {
				require.Equal(t, []byte{byte(i)}, recv(t, c).Payload())
			}
		}},
		{"a send with an ended ctx delivers nothing", func(t *testing.T) {
			_, p, c := joined(t)
			cctx, cancel := context.WithCancel(ctx)
			cancel()
			_, err := p.SendBytes(cctx, nil)
			require.ErrorIs(t, err, context.Canceled)
			c.Close()
			requireClosed(t, c)
		}},
		{"a full consumer holds the send until its ctx ends", func(t *testing.T) {
			_, p, _ := joined(t)
			fill(t, p)
			cctx, cancel := context.WithTimeout(ctx, 20*time.Millisecond)
			defer cancel()
			_, err := p.SendBytes(cctx, nil)
			require.ErrorIs(t, err, context.DeadlineExceeded)
		}},
		{"a full consumer's Close wakes the send", func(t *testing.T) {
			_, p, c := joined(t)
			fill(t, p)
			sent := make(chan error, 1)
			go func() { _, err := p.SendBytes(ctx, nil); sent <- err }()
			time.Sleep(10 * time.Millisecond) // the send waits (else it fails the same)
			c.Close()
			require.ErrorIs(t, waitErr(t, sent), ErrNoConsumer)
		}},
		{"a full consumer's read lets the send in", func(t *testing.T) {
			_, p, c := joined(t)
			fill(t, p)
			sent := make(chan error, 1)
			go func() { _, err := p.SendBytes(ctx, []byte("last")); sent <- err }()
			recv(t, c)
			require.NoError(t, waitErr(t, sent))
		}},
		{"Close keeps what is on the channel, then closes it", func(t *testing.T) {
			_, p, c := joined(t)
			_, err := p.SendBytes(ctx, []byte("kept"))
			require.NoError(t, err)
			c.Close()
			c.Close()
			require.NoError(t, c.Unsubscribe())
			require.Equal(t, []byte("kept"), recv(t, c).Payload())
			requireClosed(t, c)
		}},
		{"one consumer per topic, whatever its name", func(t *testing.T) {
			b, _, c := joined(t)
			_, err := b.Subscribe(ConsumerOptions{Topic: "t", SubscriptionName: "other"})
			require.ErrorIs(t, err, ErrSubscriptionExists)
			_, err = b.Subscribe(ConsumerOptions{Topic: "t", SubscriptionName: "s"})
			require.ErrorIs(t, err, ErrSubscriptionExists)
			c.Close()
			_, err = b.Subscribe(ConsumerOptions{Topic: "t", SubscriptionName: "s"})
			require.NoError(t, err)
		}},
		{"of concurrent subscribes exactly one wins", func(t *testing.T) {
			b := NewInMemoryMessageBroker()
			defer b.Close()
			require.NoError(t, b.SetTopicAutoCreation("", true))
			var won atomic.Int32
			var wg sync.WaitGroup
			for range 32 {
				wg.Go(func() {
					if _, err := b.Subscribe(ConsumerOptions{Topic: "t", SubscriptionName: "s"}); err == nil {
						won.Add(1)
					} else {
						require.ErrorIs(t, err, ErrSubscriptionExists)
					}
				})
			}
			wg.Wait()
			require.Equal(t, int32(1), won.Load())
		}},
		{"one producer per topic", func(t *testing.T) {
			b, p, _ := joined(t)
			_, err := b.CreateProducer(ProducerOptions{Topic: "t"})
			require.ErrorIs(t, err, ErrProducerExists)
			p.Close()
			p.Close()
			_, err = p.SendBytes(ctx, nil)
			require.ErrorIs(t, err, ErrProducerClosed)
			_, err = b.CreateProducer(ProducerOptions{Topic: "t"})
			require.NoError(t, err)
		}},
		{"broker Close closes everything", func(t *testing.T) {
			b, p, c := joined(t)
			b.Close()
			b.Close()
			_, err := p.SendBytes(ctx, nil)
			require.ErrorIs(t, err, ErrProducerClosed)
			requireClosed(t, c)
			require.ErrorIs(t, b.CreateTopic("u", 0, false), ErrBrokerClosed)
			_, err = b.Subscribe(ConsumerOptions{Topic: "t"})
			require.ErrorIs(t, err, ErrBrokerClosed)
			_, err = b.CreateProducer(ProducerOptions{Topic: "t"})
			require.ErrorIs(t, err, ErrBrokerClosed)
			require.ErrorIs(t, b.SetTopicAutoCreation("", true), ErrBrokerClosed)
		}},
	} {
		t.Run(tc.name, tc.run)
	}
}

// Consumers and producers of a few topics come and go at random while
// sends run: no panic, no deadlock, at most one consumer per topic at a
// time, every send that returned nil reached exactly one consumer's reader
// (and none other), every other send failed, and the broker ends empty.
// The leak check in TestMain fails it on a goroutine left behind.
func TestInMemoryBrokerStress(t *testing.T) {
	seed := uint64(time.Now().UnixNano())
	t.Logf("seed %d", seed)
	b := NewInMemoryMessageBroker()
	defer b.Close()
	require.NoError(t, b.SetTopicAutoCreation("", true))
	const topics, workers, rounds = 3, 12, 300
	var next atomic.Uint64
	var sentOK, received sync.Map // message id -> topic
	var holders [topics]atomic.Int32
	var errs = make(chan error, workers*rounds*4)
	var wg sync.WaitGroup
	for w := range workers {
		rng := rand.New(rand.NewPCG(seed, uint64(w)))
		wg.Go(func() {
			for range rounds {
				ti := rng.IntN(topics)
				topic := fmt.Sprint("t", ti)
				if rng.IntN(2) == 0 {
					c, err := b.Subscribe(ConsumerOptions{Topic: topic, SubscriptionName: fmt.Sprint(w)})
					if err != nil {
						if !errors.Is(err, ErrSubscriptionExists) {
							errs <- err
						}
						continue
					}
					if holders[ti].Add(1) != 1 {
						errs <- fmt.Errorf("%s: two consumers at once", topic)
					}
					reads := rng.IntN(8)
					for range reads {
						select {
						case m := <-c.Chan():
							if _, dup := received.LoadOrStore(string(m.Payload()), topic); dup {
								errs <- fmt.Errorf("%s: delivered twice", m.Payload())
							}
						case <-time.After(time.Millisecond):
						}
					}
					holders[ti].Add(-1)
					c.Close()
					for m := range c.Chan() { // the closing side answers for these
						if _, dup := received.LoadOrStore(string(m.Payload()), topic); dup {
							errs <- fmt.Errorf("%s: delivered twice", m.Payload())
						}
					}
					continue
				}
				p, err := b.CreateProducer(ProducerOptions{Topic: topic})
				if err != nil {
					if !errors.Is(err, ErrProducerExists) {
						errs <- err
					}
					continue
				}
				for range rng.IntN(100) {
					id := fmt.Sprint(next.Add(1))
					sctx, cancel := context.WithTimeout(context.Background(), time.Duration(rng.IntN(3))*time.Millisecond)
					_, err := p.SendBytes(sctx, []byte(id))
					cancel()
					switch {
					case err == nil:
						sentOK.Store(id, topic)
					case errors.Is(err, ErrNoConsumer), errors.Is(err, context.DeadlineExceeded):
					default:
						errs <- err
					}
				}
				p.Close()
			}
		})
	}
	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(60 * time.Second):
		t.Fatalf("deadlock (seed %d)", seed)
	}
	close(errs)
	for err := range errs {
		t.Errorf("seed %d: %v", seed, err)
	}
	delivered := 0
	sentOK.Range(func(id, topic any) bool {
		delivered++
		got, ok := received.Load(id)
		if !ok || got != topic {
			t.Errorf("seed %d: %v sent on %v, received on %v (%v)", seed, id, topic, got, ok)
		}
		return true
	})
	received.Range(func(id, _ any) bool {
		if _, ok := sentOK.Load(id); !ok {
			t.Errorf("seed %d: %v received but its send failed", seed, id)
		}
		return true
	})
	t.Logf("%d of %d sends delivered", delivered, next.Load())
	topicsLeft, producersLeft := brokerKeys(b)
	require.Zero(t, topicsLeft, "seed %d", seed)
	require.Zero(t, producersLeft, "seed %d", seed)
}
