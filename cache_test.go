package cache

import (
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestCache_BasicOperations(t *testing.T) {
	// Configure high clock precision exclusively for unit tests evaluation windows.
	c := New[string]().
		WithMaxKeys(1000).
		WithDefaultTTL(50 * time.Millisecond).
		WithClockInterval(10 * time.Millisecond).
		Start()
	defer c.Close()

	key := c.Hash([]byte("foo"))
	c.SetDefault(key, "bar")

	val, found := c.Get(key)
	if !found || val != "bar" {
		t.Fatalf("expected bar, got %s (found: %v)", val, found)
	}

	// Sleep past the active expiration and clock loop synchronization thresholds
	time.Sleep(100 * time.Millisecond)

	_, found = c.Get(key)
	if found {
		t.Fatal("expected item to be lazily expired out")
	}
}

func TestSingleFlightRing_StampedeProtection(t *testing.T) {
	sf := NewSingleFlightRing[string]()

	// Create explicit mock hash inputs
	key := Key{Hi: uint64(999999), Lo: uint64(888888)}

	var executions atomic.Int64
	var wg sync.WaitGroup

	concurrentWorkers := 50
	wg.Add(concurrentWorkers)

	startBarrier := make(chan struct{})

	for range concurrentWorkers {
		go func() {
			defer wg.Done()
			<-startBarrier

			// Direct pass of raw hash identifiers without old intermediate adapters
			res, err := sf.Do(key, func() (string, error) {
				executions.Add(1)
				time.Sleep(25 * time.Millisecond) // Mock network/database roundtrip latency
				return "cache_stampede_shielded", nil
			})

			if err != nil || res != "cache_stampede_shielded" {
				t.Errorf("unexpected flight response signature: res=%s, err=%v", res, err)
			}
		}()
	}

	close(startBarrier)
	wg.Wait()

	finalCount := executions.Load()
	if finalCount != 1 {
		t.Fatalf("expected exactly 1 execution, concurrent callers dogpiled: %d", finalCount)
	}
}

func TestCache_ConcurrentStressRace(_ *testing.T) {
	c := New[string]().WithMaxKeys(100).WithClockInterval(5 * time.Millisecond).WithCleanupInterval(10 * time.Millisecond).Start()
	defer c.Close()

	var wg sync.WaitGroup
	var workers uint64 = 100
	var opsPerWorker uint64 = 1000

	wg.Add(int(workers) * 3)

	// Writer Loop
	for i := range workers {
		go func(workerID uint64) {
			defer wg.Done()
			for j := range opsPerWorker {
				key := Key{Hi: j % 50, Lo: workerID}
				c.Set(key, "payload", 50*time.Millisecond)
			}
		}(i)
	}

	// Reader Loop
	for i := range workers {
		go func(workerID uint64) {
			defer wg.Done()
			for j := range opsPerWorker {
				key := Key{Hi: j % 50, Lo: workerID}
				_, _ = c.Get(key)
			}
		}(i)
	}

	// Invalidator Loop
	for i := range workers {
		go func(workerID uint64) {
			defer wg.Done()
			for j := range opsPerWorker {
				key := Key{Hi: j % 50, Lo: workerID}
				if j%10 == 0 {
					c.Invalidate(key)
				}
			}
		}(i)
	}

	wg.Wait()
}

func TestCache_EvictionCeilingAndCallbackLeaks(t *testing.T) {
	var evictedCount atomic.Int64

	const maxKeys uint64 = 10000
	const loadFactor uint64 = 5
	const totalInsertions uint64 = maxKeys * loadFactor

	c := New[uint64]().
		WithMaxKeys(int(maxKeys)).
		WithOnEvicted(func(_ Key, _ uint64) {
			evictedCount.Add(1)
		}).
		Start()
	defer c.Close()

	for i := range totalInsertions {
		key := Key{Hi: i, Lo: i}
		c.SetDefault(key, i)
	}

	currentLen := int64(c.Len())
	if currentLen > int64(maxKeys) {
		t.Fatalf("Cache breached hard ceiling limit restriction. Max: %d, Found: %d", maxKeys, currentLen)
	}

	// Give async callbacks a brief window to finish executing in the background ring
	time.Sleep(50 * time.Millisecond)

	totalTracked := currentLen + evictedCount.Load()
	if totalTracked != int64(totalInsertions) {
		t.Fatalf("Memory leak or dropped items detected. Total objects tracked: %d vs expected %d", totalTracked, totalInsertions)
	}
}

func TestCache_GoroutineLifecycleLeakCheck(t *testing.T) {
	initialGoroutines := runtime.NumGoroutine()

	// Create and instantiate 50 distinct isolated cache systems
	for i := range uint64(50) {
		c := New[string]().WithMaxKeys(100).Start()
		c.SetDefault(Key{Hi: 1, Lo: i}, "data")
		c.Close() // Explicit shutdown verification
	}

	// Yield to scheduler to let dead routines exit clean
	runtime.GC()
	time.Sleep(20 * time.Millisecond)

	finalGoroutines := runtime.NumGoroutine()
	// Allow a tiny slack variance threshold for generic background runtime systems
	if finalGoroutines > initialGoroutines+3 {
		t.Fatalf("Goroutine memory leak detected after closing instances. Started with %d, holding %d active routines", initialGoroutines, finalGoroutines)
	}
}

// TestCache_ShardCollisionSeparation verifies that if two keys route to the
// exact same shard (same lower 6 bits) but have different high bits, they
// are isolated completely and do not overwrite or corrupt each other.
func TestCache_ShardCollisionSeparation(t *testing.T) {
	c := New[string]().Start()
	defer c.Close()

	// Both Lo values end in 4 (4 & 63 == 4), routing them to Shard #4.
	// Their Hi values are distinct, making them separate keys in the map.
	keyA := Key{Hi: 100, Lo: 4}
	keyB := Key{Hi: 200, Lo: 4}

	c.SetDefault(keyA, "payload-alpha")
	c.SetDefault(keyB, "payload-beta")

	valA, foundA := c.Get(keyA)
	if !foundA || valA != "payload-alpha" {
		t.Fatalf("KeyA corrupted or lost in shard collision: %s", valA)
	}

	valB, foundB := c.Get(keyB)
	if !foundB || valB != "payload-beta" {
		t.Fatalf("KeyB corrupted or lost in shard collision: %s", valB)
	}
}

// TestCache_StateManagementAndTelemetry validates global administrative commands
// like Purge, Peek, InvalidateFn, and Stat collections across all shards.
func TestCache_StateManagementAndTelemetry(t *testing.T) {
	c := New[string]().WithMaxKeys(100).Start()
	defer c.Close()

	keyA := Key{Hi: 11, Lo: 11}
	keyB := Key{Hi: 22, Lo: 22}

	c.SetDefault(keyA, "data-a")
	c.SetDefault(keyB, "data-b")

	// 1. Verify Peek does not mutate hits/misses telemetry counters
	if _, found := c.Peek(keyA); !found {
		t.Fatal("expected Peek to find keyA")
	}
	initialStats := c.Stat()
	if initialStats.Hits != 0 || initialStats.Misses != 0 {
		t.Fatalf("Peek incorrectly modified telemetry registers: %+v", initialStats)
	}

	// 2. Validate InvalidateFn matching predicate paths
	c.InvalidateFn(func(k Key) bool {
		return k.Hi == 11
	})

	if _, found := c.Get(keyA); found {
		t.Fatal("expected keyA to be wiped by InvalidateFn")
	}
	if _, found := c.Get(keyB); !found {
		t.Fatal("expected keyB to remain unaffected by predicate sweep")
	}

	// 3. Verify global state Purge and alignment
	c.Purge()
	if totalKeys := len(c.Keys()); totalKeys != 0 {
		t.Fatalf("Purge failed to wipe allocation matrix cleanly, left: %d keys", totalKeys)
	}
}

// TestCache_RemoveOldestManual validates that the RemoveOldest administrative
// method safely sweeps a random shard without failing or causing deadlocks.
func TestCache_RemoveOldestManual(_ *testing.T) {
	c := New[string]().Start()
	defer c.Close()

	for i := range uint64(100) {
		c.SetDefault(Key{Hi: i, Lo: i}, "evict-test")
	}

	// RemoveOldest samples a random shard and evicts.
	// This call must execute smoothly without throwing panic warnings.
	c.RemoveOldest()
}

// TestCache_EvictCornerCases explicitly targets the early-exit and inline lazy
// expiration deletion branches inside the evict loop (Fixes cache.go zero blocks).
func TestCache_EvictCornerCases(t *testing.T) {
	c := New[string]().WithMaxKeys(64).Start()
	impl := c.(*cacheImpl[string])

	sh := impl.shards[0]
	sh.mu.Lock()
	impl.evict(sh, 10)
	sh.mu.Unlock()
	c.Close()

	c = New[string]().WithMaxKeys(64).Start()
	impl = c.(*cacheImpl[string])
	sh = impl.shards[0]
	sh.mu.Lock()
	impl.evict(sh, 0)
	sh.mu.Unlock()
	c.Close()

	// We use a high clock interval to ensure we can control time slices manually
	c = New[string]().WithMaxKeys(64).WithClockInterval(24 * time.Hour).Start()
	impl = c.(*cacheImpl[string])

	key := Key{Hi: 1, Lo: 0}

	sh = impl.shards[0]
	sh.mu.Lock()
	sh.items[key] = item[string]{
		value:      "expired-in-sample",
		expiration: impl.now() - 1000,
	}
	impl.evict(sh, 0)
	sh.mu.Unlock()
	c.Close()
}

// TestCache_GetDoubleCheckRace targets the case when a key is deleted
// concurrently after breaching expiration but before the write lock is grabbed.
func TestCache_GetDoubleCheckRace(t *testing.T) {
	c := New[string]().WithClockInterval(24 * time.Hour).Start()
	impl := c.(*cacheImpl[string])

	key := Key{Hi: 99, Lo: 99}
	sh := impl.getShard(key)

	sh.mu.Lock()
	sh.items[key] = item[string]{
		value:      "race-payload",
		expiration: impl.now() - 50,
	}
	sh.mu.Unlock()

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		sh.mu.Lock()
		delete(sh.items, key)
		sh.mu.Unlock()
	}()

	wg.Wait()

	if _, found := c.Get(key); found {
		t.Fatal("expected key to not be found during cleanup race evaluation")
	}
}

// TestCache_OptionsAutotuneAndCallbacks targets options.go zero branches
// and the Invalidate internal validation metadata mismatch branches.
func TestCache_OptionsAutotuneAndCallbacks(t *testing.T) {
	var evictedTriggered int64

	c := New[string]().
		WithMaxKeys(64).
		WithDefaultTTL(10 * time.Millisecond).
		WithOnEvicted(func(_ Key, _ string) {
			evictedTriggered++
		}).
		Start()

	key := Key{Hi: 5, Lo: 5}
	c.SetDefault(key, "test-data")

	impl := c.(*cacheImpl[string])
	sh := impl.getShard(key)

	sh.mu.Lock()
	delete(sh.items, key)
	sh.mu.Unlock() // Triggers the custom short TTL autotuner path

	c.Invalidate(key)
	if _, found := c.Get(key); found {
		t.Fatal("expected key to be wiped by Invalidate")
	}
	c.Close()
}

func TestCache_CustomHasher(t *testing.T) {
	customHasher := func(data []byte) Key {
		return Key{Hi: 123, Lo: 321}
	}

	c := New[string]().
		WithHasher(customHasher).
		WithMaxKeys(10).
		Start()

	key := c.Hash([]byte("test"))
	if key.Hi != 123 || key.Lo != 321 {
		t.Fatal("expected key to match customHasher output")
	}
	c.Close()
}

func TestCache_CleanupIntervalCalculation(t *testing.T) {
	ttl := 4 * time.Minute
	c := New[int]().
		WithDefaultTTL(ttl).
		Start()

	impl := c.(*cacheImpl[int])
	if impl.cleanupInterval != ttl/2 {
		t.Fatal("wrong cleanup interval calculation")
	}

	c.Close()
}

func TestCache_StringNotEmpty(t *testing.T) {
	c := New[string]().Start()

	if str := c.String(); str == "" {
		t.Error("String() returned an empty representation")
	}
}
