# [S]harded [E]xpirable [Cache] (se-cache)

[![Build Status](https://github.com/goodman116/se-cache/workflows/build/badge.svg)](https://github.com/goodman116/se-cache/actions)
[![Coverage Status](https://coveralls.io/repos/github/goodman116/se-cache/badge.svg?branch=master)](https://coveralls.io/github/goodman116/se-cache?branch=master)

An ultra-low overhead, sharded, concurrent TTL cache designed specifically to manage heavy read/write traffic under loads exceeding **20,000 RPS**. Built from scratch using **Go 1.22** native constraint rules.

This cache is widely inspired by [go-pkgz/expirable-cache](https://github.com/go-pkgz/expirable-cache).

## Key Optimizations

- **Single-Map Unification:** Memory budget is ~11.3 MB for 100,000 entries.
- **16-Byte Vector Key Alignment:** Public methods consume a lightweight, compact `Key` struct by value. This allows the Go compiler to map lookups onto highly optimized assembly arrays rather than slow generic runtime map lookups.
- **Lock Contention Shield:** Spreads concurrency uniformly over 64 independent, isolated shards to completely eliminate lock conflicts under parallel patterns.
- **Hard Absolute TTL Boundary:** Avoids "zombie data retention" common in traditional LRU structures. Accessing hot items via `Get` does *not* extend their lifespan, guaranteeing predictable freshness windows and letting downstream backends receive vital data updates.
- **Syscall Mitigation:** Employs an internal monotonic atomic clock loop to completely avoid high-frequency `time.Now()` hot-path syscalls.
- **Sharded SingleFlight Ring:** Aligns execution flights with your hash shards to shortcircuit concurrent backend cache stampedes without introducing global mutex bottlenecks.

## Installation

Ensure you are working inside a **Go 1.22** or higher environment:

```bash
go get github.com/goodman116/se-cache
```

## Quick Start

Initialize the cache using the factory constructor and leverage the unified `.Hash(data)` method to compute optimized cache tokens transparently. By default, the subsystem uses the extremely fast **XXH3** non-cryptographic 128-bit hash standard.

```go
package main

import (
	cache "github.com/goodman116/se-cache"
)

func main() {
	// Initialize with a pre-sized maximum cap and default fallback TTL
	c := cache.New[string]().
		WithMaxKeys(100000).
		WithDefaultTTL(4 * time.Hour).
		WithClockInterval(500 * time.Millisecond).   // Bounded time-slice register updates
		WithCleanupInterval(15 * time.Minute).       // Infrequent shard-sweeping janitor loops
		Start()                                      // Activates background tickers safely
	defer c.Close()

	requestBody := []byte("POST_REQUEST_BODY_PAYLOAD_DATA_HERE")

	// Option A: Calculate Key natively through the cache interface instance method
	key := c.Hash(requestBody)

	// Option B: Calculate Key globally via the package function utility fallback
	// key := cache.DefaultHasher(requestBody)

	// Set and Get values seamlessly with 0 heap allocations
	c.SetDefault(key, "authorized_session_verdict")

	if val, found := c.Get(key); found {
		fmt.Printf("Cache Hit! Key Hash: [%d:%d] -> %s\n", key.Hi, key.Lo, val)
	}
}
```

## Custom Hashing Strategy (Extensibility)

By default, the cache utilizes `DefaultHasher` (powered by [zeebo/xxh3](github.com/zeebo/xxh3)) under the hood for zero-allocation key token projections. If your application layer demands alternative constraints (e.g., cryptographic verification or custom cluster-wide salting registers), you can inject an explicit custom hasher signature block using the `WithHasher(fn HasherFn)` option.

```go
package main

import (
	"crypto/sha256"
	"encoding/binary"
	"time"

	cache "github.com/goodman116/se-cache"
)

// CustomSHA256Hasher maps standard cryptographic signatures into the 16-byte Key boundaries.
func CustomSHA256Hasher(data []byte) cache.Key {
	sum := sha256.Sum256(data)
	return cache.Key{
		Hi: binary.BigEndian.Uint64(sum[0:8]),
		Lo: binary.BigEndian.Uint64(sum[8:16]),
	}
}

func main() {
	c := cache.New[string]().
		WithMaxKeys(100000).
		WithHasher(CustomSHA256Hasher). // Swaps default XXH3 fallback algorithm safely
		Start()
	defer c.Close()

	// Operations using c.Hash(data) now execute through your SHA256 projection vector.
	key := c.Hash([]byte("transaction-payload"))
	c.SetDefault(key, "verified")
}
```

## Production Read-Through Usage (Cache-Aside + SingleFlight)

Below is a production-grade architecture integrating the **Sharded Cache** alongside the **`SingleFlightRing`** wrapper. It models a realistic **80% Read / 20% Write** distribution strategy to safely shield resource-intensive downstream engines (like an ICAP Bridge Engine or analytical parse cluster) from parallel cache stampedes.

```go
package main

import (
	"fmt"
	"time"

	cache "github.com/goodman116/se-cache"
)

// Verdict mirrors a real payload with multiple types.
type Verdict struct {
	ID        int64
	Blocked   bool
	Score     float64
	RuleName  string
	SHA256    string
	CreatedAt time.Time
}

type ProxyService struct {
	cache  cache.Cache[Verdict]
	sfRing *cache.SingleFlightRing[Verdict]
}

func NewProxyService() *ProxyService {
	return &ProxyService{
		cache:  cache.New[Verdict]().WithMaxKeys(150000).Start(),
		sfRing: cache.NewSingleFlightRing[Verdict](),
	}
}

func (s *ProxyService) ProcessRequest(body []byte) (Verdict, error) {
	// Step 1: Pre-compute the 16-byte key token natively on arrival (0 allocations)
	key := s.cache.Hash(body)

	// Step 2: High-Performance Read Path
	if res, found := s.cache.Get(key); found {
		return res, nil
	}

	// Step 3: Cache Miss. Trap concurrent dogpiling via the Sharded SingleFlight Ring.
	// Only ONE goroutine passes into the execution closure; the rest block and wait.
	return s.sfRing.Do(key, func() (Verdict, error) {
		// Double-Check Optimization: Inspect the cache again inside the synchronized window
		// since a previous concurrent worker might have just populated it.
		if res, found := s.cache.Get(key); found {
			return res, nil
		}

		// Simulate intensive parsing or network request to your downstream engine
		result := Verdict{
			ID:        9876543210,
			Blocked:   true,
			Score:     0.9945,
			RuleName:  "MALWARE_HEURISTIC_EXPLOIT_RULE_BLOCK_EXECUTE",
			SHA256:    "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
			CreatedAt: time.Now(),
		}
		
		// Populate cache using zero-rehash mechanics
		s.cache.Set(key, result, 2*time.Hour)
		return result, nil
	})
}

func main() {
	service := NewProxyService()
	res, _ := service.ProcessRequest([]byte("POST_RAW_REQUEST_BODY_HERE"))
	fmt.Printf("Processed request successfully. Verdict Blocked: %v, Confidence: %f\n", res.Blocked, res.Score)
}
```
