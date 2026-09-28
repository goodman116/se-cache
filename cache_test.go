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

	var executions int64
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
				atomic.AddInt64(&executions, 1)
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

	finalCount := atomic.LoadInt64(&executions)
	if finalCount != 1 {
		t.Fatalf("expected exactly 1 execution, concurrent callers dogpiled: %d", finalCount)
	}
}

func TestCache_ConcurrentStressRace(t *testing.T) {
	c := New[string]().WithMaxKeys(1000).WithClockInterval(5 * time.Millisecond).WithCleanupInterval(10 * time.Millisecond).Start()
	defer c.Close()

	var wg sync.WaitGroup
	workers := 100
	opsPerWorker := 1000

	wg.Add(workers * 3)

	// Writer Loop
	for i := 0; i < workers; i++ {
		uWorkerID := uint64(i)
		go func(workerID uint64) {
			defer wg.Done()
			for j := 0; j < opsPerWorker; j++ {
				key := Key{Hi: uint64(j % 50), Lo: workerID}
				c.Set(key, "payload", 50*time.Millisecond)
			}
		}(uWorkerID)
	}

	// Reader Loop
	for i := 0; i < workers; i++ {
		uWorkerID := uint64(i)
		go func(workerID uint64) {
			defer wg.Done()
			for j := 0; j < opsPerWorker; j++ {
				key := Key{Hi: uint64(j % 50), Lo: workerID}
				_, _ = c.Get(key)
			}
		}(uWorkerID)
	}

	// Invalidator Loop
	for i := 0; i < workers; i++ {
		uWorkerID := uint64(i)
		go func(workerID uint64) {
			defer wg.Done()
			for j := 0; j < opsPerWorker; j++ {
				key := Key{Hi: uint64(j % 50), Lo: workerID}
				if j%10 == 0 {
					c.Invalidate(key)
				}
			}
		}(uWorkerID)
	}

	wg.Wait()
}

func TestCache_EvictionCeilingAndCallbackLeaks(t *testing.T) {
	var evictedCount atomic.Int64
	maxKeys := 10000

	c := New[int]().
		WithMaxKeys(maxKeys).
		WithOnEvicted(func(key Key, val int) {
			evictedCount.Add(1)
		}).
		Start()
	defer c.Close()

	// Push 5x more elements than the configured ceiling capacity
	for i := 0; i < maxKeys*5; i++ {
		key := Key{Hi: uint64(i), Lo: uint64(i)}
		c.SetDefault(key, i)
	}

	// Shard limit is integer division based, minor padding account can exist per shard baseline
	currentLen := c.Len()
	if currentLen > maxKeys {
		t.Fatalf("Cache breached hard ceiling limit restriction. Max: %d, Found: %d", maxKeys, currentLen)
	}

	// Give async callbacks a brief window to finish executing
	time.Sleep(50 * time.Millisecond)

	totalTracked := int64(currentLen) + evictedCount.Load()
	if totalTracked != int64(maxKeys*5) {
		t.Fatalf("Memory leak or dropped items detected. Total acknowledged objects tracking dropped: %d vs expected %d", totalTracked, maxKeys*5)
	}
}

func TestCache_GoroutineLifecycleLeakCheck(t *testing.T) {
	initialGoroutines := runtime.NumGoroutine()

	// Create and instantiate 50 distinct isolated cache systems
	for i := 0; i < 50; i++ {
		c := New[string]().WithMaxKeys(100).Start()
		c.SetDefault(Key{Hi: 1, Lo: uint64(i)}, "data")
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
