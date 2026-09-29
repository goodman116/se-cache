package cache

import (
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestSingleFlight_PanicResilience verifies that if a flight leader panics,
// subsequent requests can retry and execute normally instead of deadlocking.
func TestSingleFlight_PanicResilience(t *testing.T) {
	sf := NewSingleFlightRing[string]()
	key := Key{Hi: 1, Lo: 1}

	var wg sync.WaitGroup
	wg.Add(2)

	// Worker 1: Designated flight leader that will panic
	go func() {
		defer wg.Done()
		defer func() {
			if r := recover(); r == nil {
				t.Error("expected flight leader execution callback to panic")
			}
		}()

		_, _ = sf.Do(key, func() (string, error) {
			time.Sleep(10 * time.Millisecond)
			panic("downstream internal system crash simulation")
		})
	}()

	// Worker 2: Passenger that should get unblocked when Worker 1 panics
	go func() {
		defer wg.Done()
		time.Sleep(5 * time.Millisecond) // Ensure Worker 1 registers first

		_, _ = sf.Do(key, func() (string, error) {
			return "fallback", nil
		})
	}()

	wg.Wait()

	// Critical Recovery Check: Ensure the cache map slot is clear and normal calls work again
	successVal, err := sf.Do(key, func() (string, error) {
		return "recovered_data", nil
	})

	if err != nil || successVal != "recovered_data" {
		t.Fatalf("SingleFlightRing permanently locked out after a panic: val=%s, err=%v", successVal, err)
	}
}

// TestSingleFlight_ConcurrentReadSerialization ensures that under immense parallel stampede loops,
// exactly ONE downstream call executes, and all passengers get identical payload results safely.
func TestSingleFlight_ConcurrentReadSerialization(t *testing.T) {
	sf := NewSingleFlightRing[int]()
	key := Key{Hi: 42, Lo: 42}

	var executedCount atomic.Int64
	var wg sync.WaitGroup

	workers := 100
	wg.Add(workers)

	barrier := make(chan struct{})

	for i := range workers {
		go func(id int) {
			defer wg.Done()
			<-barrier // Block to synchronize simultaneous goroutine wake-ups

			res, err := sf.Do(key, func() (int, error) {
				executedCount.Add(1)
				time.Sleep(15 * time.Millisecond) // Simulate slow database query
				return 2026, nil
			})

			if err != nil || res != 2026 {
				t.Errorf("Worker %d received corrupted data: res=%d, err=%v", id, res, err)
			}
		}(i)
	}

	// Release the barrier to trigger a cache stampede
	close(barrier)
	wg.Wait()

	if totalExecs := executedCount.Load(); totalExecs != 1 {
		t.Fatalf("Singleflight ring failed to short-circuit stampede. Downstream executed %d times", totalExecs)
	}
}

// TestSingleFlight_ErrorBubbling verifies that transient downstream database/network errors
// bubble up cleanly to all waiting passengers without corrupting the tracking ring maps.
func TestSingleFlight_ErrorBubbling(t *testing.T) {
	sf := NewSingleFlightRing[string]()
	key := Key{Hi: 777, Lo: 777}
	expectedErr := errors.New("timeout downstream network connection failure")

	var wg sync.WaitGroup
	wg.Add(5)

	for i := 0; i < 5; i++ {
		go func() {
			defer wg.Done()
			_, err := sf.Do(key, func() (string, error) {
				time.Sleep(10 * time.Millisecond)
				return "", expectedErr
			})

			if !errors.Is(err, expectedErr) {
				t.Errorf("expected error %v, got %v", expectedErr, err)
			}
		}()
	}

	wg.Wait()
}

// TestSingleFlight_TransientErrorIsolation confirms that when a downstream call returns
// an explicit error token, the ring clears and allows immediate subsequent retries.
func TestSingleFlight_TransientErrorIsolation(t *testing.T) {
	sf := NewSingleFlightRing[string]()
	key := Key{Hi: 555, Lo: 555}
	mockErr := errors.New("transient database connection timeout")

	// Execute an operation that fails
	_, err := sf.Do(key, func() (string, error) {
		return "", mockErr
	})
	if err == nil {
		t.Fatal("expected singleflight worker to bubble up error token")
	}

	// Immediate retry path must execute and succeed if the downstream has recovered
	successVal, err := sf.Do(key, func() (string, error) {
		return "recovered_payload", nil
	})

	if err != nil || successVal != "recovered_payload" {
		t.Fatalf("Singleflight ring failed to clear transient error tracking slots: err=%v", err)
	}
}

// TestSingleFlight_PassengerOrderOfOperations validates that passenger goroutines
// wait cleanly for the single leader execution to finalize before reading,
// ensuring proper sequential synchronization under heavy concurrency loops.
func TestSingleFlight_PassengerOrderOfOperations(t *testing.T) {
	sf := NewSingleFlightRing[string]()
	key := Key{Hi: 888, Lo: 888}

	var leaderStarted atomic.Int32
	var wg sync.WaitGroup

	wg.Add(2)

	// Leader Flight
	go func() {
		defer wg.Done()
		_, _ = sf.Do(key, func() (string, error) {
			leaderStarted.Store(1)
			time.Sleep(20 * time.Millisecond)
			return "done", nil
		})
	}()

	// Passenger Flight
	go func() {
		defer wg.Done()
		// Sleep briefly to guarantee the leader acquires the tracking flight seat first
		time.Sleep(5 * time.Millisecond)

		res, err := sf.Do(key, func() (string, error) {
			return "interrupted_invalid_execution", nil
		})

		if err != nil || res != "done" {
			t.Errorf("Passenger woke up prematurely or fetched invalid payload data: %s", res)
		}

		if leaderStarted.Load() != 1 {
			t.Error("Passenger bypassed synchronization plane execution rules")
		}
	}()

	wg.Wait()
}
