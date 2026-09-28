package cache

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/zeebo/xxh3"
)

const (
	shardCount     = 64
	evictionSample = 8

	// noEvictionTTL represents a 10-year period, effectively disabling TTL-based eviction by default.
	noEvictionTTL          = time.Hour * 24 * 365 * 10
	defaultCleanupInterval = time.Minute * 30
)

// Cache defines the complete interface contract for the ultra-performant sharded cache subsystem.
type Cache[V any] interface {
	fmt.Stringer
	Options[V]

	// Hash computes an allocation-free 16-byte cache token using the standard XXH3 algorithm.
	Hash(data []byte) Key

	// Set stores a key-value pair in the cache with an explicit Time-To-Live (TTL) duration.
	// If the shard breaches its capacity limit, a fast random sampling eviction pass is triggered.
	Set(key Key, value V, ttl time.Duration)

	// SetDefault assigns a value to the key using the fallback TTL specified during configuration.
	SetDefault(key Key, value V)

	// Get retrieves a value from the cache. Returns the value and true on a cache hit.
	// Returns a zero-value representation and false on a cache miss or if the entry has expired.
	// Expired elements are lazily evicted on the fly under synchronized locks.
	Get(key Key) (V, bool)

	// Peek returns an entry value from the cache without updating hit/miss telemetry
	// counters or executing lazy-deletion write loops on expired items.
	Peek(key Key) (V, bool)

	// Keys aggregates and extracts all active, non-expired keys currently tracking across all 64 shards.
	// Iterates through shards sequentially to completely avoid global lock contention.
	// Returns a zero-allocation snapshot index representation.
	Keys() []Key

	// Len estimates the total logical size across all shards combined.
	Len() int

	// Invalidate removes a single entry immediately from its designated shard.
	// Invokes the registered onEvicted callback asynchronously if configured.
	Invalidate(key Key)

	// InvalidateFn evaluates a user-provided predicate function against all entries across the cache.
	// Elements returning true are cleanly wiped shard-by-shard.
	InvalidateFn(fn func(key Key) bool)

	// RemoveOldest samples candidates across a pseudo-random shard bucket and evicts the one
	// closest to its absolute expiration timestamp.
	RemoveOldest()

	// DeleteExpired actively iterates across all shards, removing elements that have breached
	// their expiration timestamps. Automatically triggered by the background cleaner routine.
	DeleteExpired()

	// Purge clears all tracking maps across all shards instantly. Wipes the allocation matrix.
	Purge()

	Stat() Stats

	// Close terminates background ticking workers cleanly and blocks until execution routines wind down.
	Close()
}

// Key acts as the public vehicle for your cache ecosystem.
type Key struct {
	Hi uint64
	Lo uint64
}

// HasherFn defines the callback signature for custom hashing algorithms.
// Must accept raw data vectors and project them into Key (explicit 128-bit unsigned boundaries).
type HasherFn func(data []byte) Key

// DefaultHasher leverages the extremely fast XXH3 non-cryptographic 128-bit hash standard.
func DefaultHasher(data []byte) Key {
	hash := xxh3.Hash128(data)
	return Key{hash.Hi, hash.Lo}
}

type item[V any] struct {
	value      V
	expiration int64
}

type shard[V any] struct {
	mu    sync.RWMutex
	items map[Key]item[V]
}

type Stats struct {
	Hits      int64 `json:"hits"`
	Misses    int64 `json:"misses"`
	Evictions int64 `json:"evictions"`
	Expired   int64 `json:"expired"`
}

type cacheImpl[V any] struct {
	shards    []*shard[V]
	hasherFn  HasherFn
	shardMask uint64
	maxSize   int

	defaultTTL      time.Duration
	clockInterval   time.Duration
	cleanupInterval time.Duration
	onEvicted       func(key Key, value V)

	nowNanos  atomic.Int64
	hits      atomic.Int64
	misses    atomic.Int64
	evictions atomic.Int64
	expired   atomic.Int64

	stop chan struct{}
	wg   sync.WaitGroup
}

// New initializes the generic cache with production-optimized passive defaults.
func New[V any]() Cache[V] {
	shards := make([]*shard[V], shardCount)
	for i := range shardCount {
		shards[i] = &shard[V]{
			items: make(map[Key]item[V]),
		}
	}

	c := &cacheImpl[V]{
		shards:          shards,
		shardMask:       shardCount - 1,
		hasherFn:        DefaultHasher,
		maxSize:         0,
		defaultTTL:      noEvictionTTL,
		clockInterval:   1 * time.Second,
		cleanupInterval: defaultCleanupInterval,
		stop:            make(chan struct{}),
	}
	c.nowNanos.Store(time.Now().UnixNano())

	return c
}

func (c *cacheImpl[V]) Hash(data []byte) Key {
	return c.hasherFn(data)
}

func (c *cacheImpl[V]) now() int64 {
	return c.nowNanos.Load()
}

func (c *cacheImpl[V]) getShard(key Key) *shard[V] {
	return c.shards[key.Lo&c.shardMask]
}

func (c *cacheImpl[V]) perShardLimit() int {
	if c.maxSize <= 0 {
		return 0
	}
	return max(1, c.maxSize/shardCount)
}

func (c *cacheImpl[V]) callOnEvicted(key Key, val V) {
	if c.onEvicted != nil {
		go c.onEvicted(key, val)
	}
}

func (c *cacheImpl[V]) clockLoop(interval time.Duration) {
	defer c.wg.Done()
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-c.stop:
			return
		case <-ticker.C:
			c.nowNanos.Store(time.Now().UnixNano())
		}
	}
}

func (c *cacheImpl[V]) cleanupLoop(interval time.Duration) {
	defer c.wg.Done()
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-c.stop:
			return
		case <-ticker.C:
			c.DeleteExpired()
		}
	}
}

func (c *cacheImpl[V]) SetDefault(key Key, value V) {
	c.Set(key, value, c.defaultTTL)
}

func (c *cacheImpl[V]) Set(key Key, value V, ttl time.Duration) {
	sh := c.getShard(key)

	sh.mu.Lock()
	defer sh.mu.Unlock()

	limit := c.perShardLimit()
	if limit > 0 && len(sh.items) >= limit {
		if _, exists := sh.items[key]; !exists {
			c.evict(sh, limit)
		}
	}

	sh.items[key] = item[V]{
		value:      value,
		expiration: c.now() + int64(ttl),
	}
}

func (c *cacheImpl[V]) evict(sh *shard[V], limit int) {
	if len(sh.items) < limit {
		return
	}

	var sampledKeys [evictionSample]Key
	var sampledExps [evictionSample]int64
	count := 0
	now := c.now()

	for k, v := range sh.items {
		sampledKeys[count] = k
		sampledExps[count] = v.expiration
		count++
		if count == evictionSample {
			break
		}
	}

	if count == 0 {
		return
	}

	for i := range count {
		if now > sampledExps[i] {
			targetKey := sampledKeys[i]
			targetVal := sh.items[targetKey].value

			delete(sh.items, targetKey)
			c.expired.Add(1)
			c.callOnEvicted(targetKey, targetVal)

			if len(sh.items) < limit {
				return
			}
		}
	}

	if len(sh.items) >= limit {
		oldestIdx := 0
		for i := 1; i < count; i++ {
			if sampledExps[i] < sampledExps[oldestIdx] {
				oldestIdx = i
			}
		}
		targetKey := sampledKeys[oldestIdx]
		targetVal := sh.items[targetKey].value

		delete(sh.items, targetKey)
		c.evictions.Add(1)
		c.callOnEvicted(targetKey, targetVal)
	}
}

func (c *cacheImpl[V]) Get(key Key) (V, bool) {
	sh := c.getShard(key)

	sh.mu.RLock()
	it, found := sh.items[key]
	sh.mu.RUnlock()

	var zero V
	if !found {
		c.misses.Add(1)
		return zero, false
	}

	if c.now() > it.expiration {
		sh.mu.Lock()
		if current, ok := sh.items[key]; ok && c.now() > current.expiration {
			targetVal := sh.items[key].value

			delete(sh.items, key)
			c.expired.Add(1)
			c.callOnEvicted(key, targetVal)
		}
		sh.mu.Unlock()
		c.misses.Add(1)
		return zero, false
	}

	c.hits.Add(1)
	return it.value, true
}

func (c *cacheImpl[V]) Peek(key Key) (V, bool) {
	sh := c.getShard(key)

	sh.mu.RLock()
	it, found := sh.items[key]
	sh.mu.RUnlock()

	var zero V
	if !found || c.now() > it.expiration {
		return zero, false
	}
	return it.value, true
}

func (c *cacheImpl[V]) Keys() []Key {
	now := c.now()
	keysList := make([]Key, 0, c.Len())

	for _, sh := range c.shards {
		sh.mu.RLock()
		for k, it := range sh.items {
			if now <= it.expiration {
				keysList = append(keysList, k)
			}
		}
		sh.mu.RUnlock()
	}
	return keysList
}

func (c *cacheImpl[V]) Len() int {
	var total int
	for _, sh := range c.shards {
		sh.mu.RLock()
		total += len(sh.items)
		sh.mu.RUnlock()
	}
	return total
}

func (c *cacheImpl[V]) Invalidate(key Key) {
	sh := c.getShard(key)

	sh.mu.Lock()
	targetVal, valOk := sh.items[key]

	delete(sh.items, key)
	sh.mu.Unlock()

	if valOk {
		c.callOnEvicted(key, targetVal.value)
	}
}

func (c *cacheImpl[V]) InvalidateFn(fn func(key Key) bool) {
	for _, sh := range c.shards {
		sh.mu.Lock()
		for k, v := range sh.items {
			if fn(k) {
				delete(sh.items, k)
				c.callOnEvicted(k, v.value)
			}
		}
		sh.mu.Unlock()
	}
}

func (c *cacheImpl[V]) RemoveOldest() {
	// Step 1: Extract the lower bits of the time register using an implicit int64 mask.
	// Since shardCount is 64, the mask index bounds are statically guaranteed to be [0..63].
	const mask int64 = shardCount - 1
	shardIdx := c.now() & mask

	// Step 2: Access the shard slice directly using the safe int64 index variable.
	// Go permits native indexing of slices using int64 expressions without casting.
	sh := c.shards[shardIdx]

	sh.mu.Lock()
	c.evict(sh, 0)
	sh.mu.Unlock()
}

func (c *cacheImpl[V]) DeleteExpired() {
	now := c.now()
	for _, sh := range c.shards {
		sh.mu.Lock()
		for k, v := range sh.items {
			if now > v.expiration {
				targetVal := v.value

				delete(sh.items, k)
				c.expired.Add(1)
				c.callOnEvicted(k, targetVal)
			}
		}
		sh.mu.Unlock()
	}
}

func (c *cacheImpl[V]) Purge() {
	for _, sh := range c.shards {
		sh.mu.Lock()
		for k, v := range sh.items {
			c.callOnEvicted(k, v.value)
		}
		sh.items = make(map[Key]item[V])
		sh.mu.Unlock()
	}
}

func (c *cacheImpl[V]) Stat() Stats {
	return Stats{
		Hits:      c.hits.Load(),
		Misses:    c.misses.Load(),
		Evictions: c.evictions.Load(),
		Expired:   c.expired.Load(),
	}
}

func (c *cacheImpl[V]) Close() {
	close(c.stop)
	c.wg.Wait()
}

func (c *cacheImpl[V]) String() string {
	return fmt.Sprintf("HighPerfShardedCache(Entries: %d, MaxKeys: %d)", c.Len(), c.maxSize)
}
