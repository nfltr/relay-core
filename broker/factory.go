// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package broker

import (
	"sync"
	"time"

	"github.com/nfltr/relay-core/internal/platform/expiring"
	"golang.org/x/sync/singleflight"
)

// Cacheable is a type constraint for cache keys (currently only string).
type Cacheable interface {
	string
}

// Producers caches producers by key: each key's producer is made once
// (Get), shared by concurrent holders and closed by onEvict once it left the
// cache and its last holder released it.
type Producers[C Cacheable] struct {
	sg      singleflight.Group
	cache   *expiring.Cache[C, *sharedProducer[C]] // expires lazily: no janitor goroutine
	onEvict func(C, Producer)
	mu      sync.Mutex
	leaving map[C]*sharedProducer[C] // left the cache while held
}

// sharedProducer is a cached producer and its holders. One that leaves the
// cache (expiry, size eviction, Remove) while a request sends on it is closed
// when its last holder releases it, never under it; a Get meanwhile takes it
// back instead of making a second producer on its topic (the in-memory
// broker allows one per topic). Lock order: the cache, s.mu, p.mu.
type sharedProducer[C Cacheable] struct {
	Producer
	key     C
	mu      sync.Mutex
	refs    int
	evicted bool
	closed  bool
}

// DefaultOnEvict closes an evicted producer.
func DefaultOnEvict[C Cacheable](_ C, producer Producer) {
	producer.Close()
}

// NewProducers caches up to cacheSize producers, each for expiryDuration;
// onEvict (nil: DefaultOnEvict) gets each producer once it is evicted and
// released.
func NewProducers[C Cacheable](cacheSize int, expiryDuration time.Duration, onEvict func(C, Producer)) *Producers[C] {
	if onEvict == nil {
		onEvict = DefaultOnEvict[C]
	}
	p := &Producers[C]{onEvict: onEvict, leaving: map[C]*sharedProducer[C]{}}
	p.cache = expiring.New[C, *sharedProducer[C]](cacheSize, expiryDuration, p.evict)
	return p
}

// evict is the cache's eviction callback: for an entry evicted, expired
// (found so on a read), removed or purged.
func (p *Producers[C]) evict(_ C, s *sharedProducer[C]) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.evicted = true
	if s.refs == 0 {
		p.closeLocked(s)
		return
	}
	p.mu.Lock()
	p.leaving[s.key] = s
	p.mu.Unlock()
}

func (p *Producers[C]) closeLocked(s *sharedProducer[C]) {
	s.closed = true
	p.onEvict(s.key, s.Producer)
}

func (p *Producers[C]) acquire(s *sharedProducer[C]) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.evicted {
		return false
	}
	s.refs++
	return true
}

func (p *Producers[C]) release(s *sharedProducer[C]) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.refs--
	if !s.evicted || s.refs > 0 {
		return
	}
	p.mu.Lock()
	if p.leaving[s.key] == s {
		delete(p.leaving, s.key)
	}
	p.mu.Unlock()
	p.closeLocked(s)
}

// takeBack returns key's producer that left the cache while held, unless
// its last holder has closed it meanwhile.
func (p *Producers[C]) takeBack(key C) *sharedProducer[C] {
	p.mu.Lock()
	s := p.leaving[key]
	delete(p.leaving, key)
	p.mu.Unlock()
	if s == nil {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil
	}
	s.evicted = false
	return s
}

// AddToCache directly stores a producer in the cache under the given key.
// This is primarily useful for testing.
func (p *Producers[C]) AddToCache(key C, producer Producer) {
	p.cache.Add(key, &sharedProducer[C]{Producer: producer, key: key})
}

// Remove forgets the producer for key, if any, and closes it once no caller
// holds it. allow:wrapper (the cache is unexported; the relay drops a
// departed agent's producer)
func (p *Producers[C]) Remove(key C) {
	p.cache.Remove(key)
}

// Get returns a producer for the given key and the release the caller must
// call when done with it; the producer is not closed before then.
// If the producer is not in the cache, it is created with the given ProducerOptions
// and added to the cache.
// If the producer is in the cache then existing producer is returned.
func (p *Producers[C]) Get(key C, b MessageBroker, producerOpts ProducerOptions) (Producer, func(), error) {
	for {
		s, err := p.lookup(key, b, producerOpts)
		if err != nil {
			return nil, nil, err
		}
		// One evicted between lookup and acquire: look again, which takes
		// it back or makes its successor.
		if p.acquire(s) {
			return s.Producer, func() { p.release(s) }, nil
		}
	}
}

func (p *Producers[C]) lookup(key C, b MessageBroker, producerOpts ProducerOptions) (*sharedProducer[C], error) {
	// Fast path: return cached producer without entering singleflight.
	if s, ok := p.cache.Get(key); ok {
		return s, nil
	}

	// Slow path: deduplicate concurrent producer creation.
	sgKey := string(key)
	defer p.sg.Forget(sgKey)
	v, err, _ := p.sg.Do(sgKey, func() (any, error) {
		// Re-check after acquiring the singleflight slot.
		if s, ok := p.cache.Get(key); ok {
			return s, nil
		}
		// An expired entry not yet purged is still in the cache and Add
		// would replace it unclosed: remove it (closed, or left if held).
		p.cache.Remove(key)
		s := p.takeBack(key)
		if s == nil {
			producer, err := b.CreateProducer(producerOpts)
			if err != nil {
				return nil, err
			}
			s = &sharedProducer[C]{Producer: producer, key: key}
		}
		p.cache.Add(key, s)
		return s, nil
	})
	if err != nil {
		return nil, err
	}
	return v.(*sharedProducer[C]), nil
}
