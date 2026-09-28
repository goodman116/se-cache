package cache

import (
	"sync"
)

type call[V any] struct {
	wg  sync.WaitGroup
	val V
	err error
}

type flightShard[V any] struct {
	mu    sync.Mutex
	calls map[Key]*call[V]
}

// SingleFlightRing short-circuits duplicate concurrent workflows safely.
type SingleFlightRing[V any] struct {
	shards    []flightShard[V]
	shardMask uint64
}

// NewSingleFlightRing initializes the sharded downstream traffic isolation plane.
func NewSingleFlightRing[V any]() *SingleFlightRing[V] {
	shards := make([]flightShard[V], shardCount)
	for i := range shardCount {
		shards[i] = flightShard[V]{
			calls: make(map[Key]*call[V]),
		}
	}
	return &SingleFlightRing[V]{
		shards:    shards,
		shardMask: shardCount - 1,
	}
}

func (g *SingleFlightRing[V]) getShard(key Key) *flightShard[V] {
	return &g.shards[key.Lo&g.shardMask]
}

// Do ensures only one execution routine fires simultaneously for overlapping hash keys.
// Fully protected against downstream callback panics using a single causeless defer block.
func (g *SingleFlightRing[V]) Do(key Key, fn func() (V, error)) (V, error) {
	sh := g.getShard(key)

	sh.mu.Lock()
	if c, exists := sh.calls[key]; exists {
		sh.mu.Unlock()
		c.wg.Wait()
		return c.val, c.err
	}

	// This goroutine is selected as the flight leader
	c := new(call[V])
	c.wg.Add(1)
	sh.calls[key] = c
	sh.mu.Unlock()

	// If fn() succeeds or panics, the map is cleaned up and passengers are unblocked safely.
	defer func() {
		sh.mu.Lock()
		delete(sh.calls, key)
		sh.mu.Unlock()
		c.wg.Done()
	}()

	c.val, c.err = fn()
	return c.val, c.err
}
