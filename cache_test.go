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
