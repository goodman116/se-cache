package benchmarks

import (
	"strconv"
	"testing"
	"time"

	"github.com/dgraph-io/ristretto"
	expcache "github.com/go-pkgz/expirable-cache/v2"
	secache "github.com/goodman116/se-cache"
	"github.com/jellydator/ttlcache/v3"
	gocache "github.com/patrickmn/go-cache"
)

// ComplexStructure mirrors a real payload with multiple types.
type ComplexStructure struct {
	ID        int64
	Blocked   bool
	Score     float64
	RuleName  string
	SHA256    string
	CreatedAt time.Time
}

const (
	cacheSize = 10000
	maskSize  = 10000
)

var (
	sharedKeys []secache.Key
	mockValue  ComplexStructure
)

func init() {
	sharedKeys = make([]secache.Key, cacheSize)
	for i := 0; i < cacheSize; i++ {
		rawStr := "key-sequence-idx-" + strconv.Itoa(i)
		sharedKeys[i] = secache.DefaultHasher([]byte(rawStr))
	}

	mockValue = ComplexStructure{
		ID:        9876543210,
		Blocked:   true,
		Score:     0.9945,
		RuleName:  "MALWARE_HEURISTIC_EXPLOIT_RULE_BLOCK_EXECUTE",
		SHA256:    "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
		CreatedAt: time.Now(),
	}
}

// ==========================================
// 1. SECACHE (SHARDED EXPIRABLE CACHE)
// ==========================================

func BenchmarkSeCache_Get(b *testing.B) {
	c := secache.New[ComplexStructure]().WithMaxKeys(cacheSize).Start()
	defer c.Close()

	for i := 0; i < cacheSize; i++ {
		c.SetDefault(sharedKeys[i], mockValue)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = c.Get(sharedKeys[i%maskSize])
	}
}

func BenchmarkSeCache_Set(b *testing.B) {
	c := secache.New[ComplexStructure]().WithMaxKeys(cacheSize).Start()
	defer c.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		c.SetDefault(sharedKeys[i%maskSize], mockValue)
	}
}

func BenchmarkSeCache_SetAndGet(b *testing.B) {
	c := secache.New[ComplexStructure]().WithMaxKeys(cacheSize).Start()
	defer c.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := sharedKeys[i%maskSize]
		c.SetDefault(key, mockValue)
		_, _ = c.Get(key)
	}
}

func BenchmarkSeCache_RealisticScenario(b *testing.B) {
	c := secache.New[ComplexStructure]().WithMaxKeys(cacheSize).Start()
	defer c.Close()

	for i := 0; i < cacheSize; i++ {
		c.SetDefault(sharedKeys[i], mockValue)
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			key := sharedKeys[i%maskSize]
			if i%5 == 0 { // 20% Writes
				c.SetDefault(key, mockValue)
			} else { // 80% Reads
				_, _ = c.Get(key)
			}
			i++
		}
	})
}

// ==========================================
// 2. GO-PKGZ EXPIRABLE-CACHE (V2)
// ==========================================

func BenchmarkExpirableCache_Get(b *testing.B) {
	c := expcache.NewCache[secache.Key, ComplexStructure]().WithMaxKeys(cacheSize)
	for i := 0; i < cacheSize; i++ {
		c.Set(sharedKeys[i], mockValue, 4*time.Hour)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = c.Get(sharedKeys[i%maskSize])
	}
}

func BenchmarkExpirableCache_Set(b *testing.B) {
	c := expcache.NewCache[secache.Key, ComplexStructure]().WithMaxKeys(cacheSize)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		c.Set(sharedKeys[i%maskSize], mockValue, 4*time.Hour)
	}
}

func BenchmarkExpirableCache_SetAndGet(b *testing.B) {
	c := expcache.NewCache[secache.Key, ComplexStructure]().WithMaxKeys(cacheSize)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := sharedKeys[i%maskSize]
		c.Set(key, mockValue, 4*time.Hour)
		_, _ = c.Get(key)
	}
}

func BenchmarkExpirableCache_RealisticScenario(b *testing.B) {
	c := expcache.NewCache[secache.Key, ComplexStructure]().WithMaxKeys(cacheSize)
	for i := 0; i < cacheSize; i++ {
		c.Set(sharedKeys[i], mockValue, 4*time.Hour)
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			key := sharedKeys[i%maskSize]
			if i%5 == 0 {
				c.Set(key, mockValue, 4*time.Hour)
			} else {
				_, _ = c.Get(key)
			}
			i++
		}
	})
}

// ==========================================
// 3. PATRICKMN GO-CACHE
// ==========================================

func BenchmarkGoCache_Get(b *testing.B) {
	c := gocache.New(4*time.Hour, 30*time.Minute)
	for i := 0; i < cacheSize; i++ {
		c.Set(strconv.Itoa(i), mockValue, gocache.DefaultExpiration)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = c.Get(strconv.Itoa(i % maskSize))
	}
}

func BenchmarkGoCache_Set(b *testing.B) {
	c := gocache.New(4*time.Hour, 30*time.Minute)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		c.Set(strconv.Itoa(i%maskSize), mockValue, gocache.DefaultExpiration)
	}
}

func BenchmarkGoCache_SetAndGet(b *testing.B) {
	c := gocache.New(4*time.Hour, 30*time.Minute)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		k := strconv.Itoa(i % maskSize)
		c.Set(k, mockValue, gocache.DefaultExpiration)
		_, _ = c.Get(k)
	}
}

func BenchmarkGoCache_RealisticScenario(b *testing.B) {
	c := gocache.New(4*time.Hour, 30*time.Minute)
	for i := 0; i < cacheSize; i++ {
		c.Set(strconv.Itoa(i), mockValue, gocache.DefaultExpiration)
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			k := strconv.Itoa(i % maskSize)
			if i%5 == 0 {
				c.Set(k, mockValue, gocache.DefaultExpiration)
			} else {
				_, _ = c.Get(k)
			}
			i++
		}
	})
}

// ==========================================
// 4. JELLYDATOR TTLCACHE (V3)
// ==========================================

func BenchmarkTTLCache_Get(b *testing.B) {
	c := ttlcache.New[secache.Key, ComplexStructure](ttlcache.WithTTL[secache.Key, ComplexStructure](4 * time.Hour))
	for i := 0; i < cacheSize; i++ {
		c.Set(sharedKeys[i], mockValue, ttlcache.DefaultTTL)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = c.Get(sharedKeys[i%maskSize])
	}
}

func BenchmarkTTLCache_Set(b *testing.B) {
	c := ttlcache.New[secache.Key, ComplexStructure](ttlcache.WithTTL[secache.Key, ComplexStructure](4 * time.Hour))

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		c.Set(sharedKeys[i%maskSize], mockValue, ttlcache.DefaultTTL)
	}
}

func BenchmarkTTLCache_SetAndGet(b *testing.B) {
	c := ttlcache.New[secache.Key, ComplexStructure](ttlcache.WithTTL[secache.Key, ComplexStructure](4 * time.Hour))

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := sharedKeys[i%maskSize]
		c.Set(key, mockValue, ttlcache.DefaultTTL)
		_ = c.Get(key)
	}
}

func BenchmarkTTLCache_RealisticScenario(b *testing.B) {
	c := ttlcache.New[secache.Key, ComplexStructure](ttlcache.WithTTL[secache.Key, ComplexStructure](4 * time.Hour))
	for i := 0; i < cacheSize; i++ {
		c.Set(sharedKeys[i], mockValue, ttlcache.DefaultTTL)
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			key := sharedKeys[i%maskSize]
			if i%5 == 0 {
				c.Set(key, mockValue, ttlcache.DefaultTTL)
			} else {
				_ = c.Get(key)
			}
			i++
		}
	})
}

// ==========================================
// 5. DGRAPH-IO RISTRETTO
// ==========================================

func BenchmarkRistretto_Get(b *testing.B) {
	c, err := ristretto.NewCache(&ristretto.Config{
		NumCounters: 1e5,
		MaxCost:     1e6,
		BufferItems: 64,
		KeyToHash: func(k interface{}) (uint64, uint64) {
			ck := k.(secache.Key)
			return ck.Hi, ck.Lo
		},
	})
	if err != nil {
		b.Fatal(err)
	}
	for i := 0; i < cacheSize; i++ {
		c.Set(sharedKeys[i], mockValue, 1)
	}
	c.Wait()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = c.Get(sharedKeys[i%maskSize])
	}
}

func BenchmarkRistretto_Set(b *testing.B) {
	c, _ := ristretto.NewCache(&ristretto.Config{
		NumCounters: 1e5,
		MaxCost:     1e6,
		BufferItems: 64,
		KeyToHash: func(k interface{}) (uint64, uint64) {
			ck := k.(secache.Key)
			return ck.Hi, ck.Lo
		},
	})

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		c.Set(sharedKeys[i%maskSize], mockValue, 1)
	}
	c.Wait()
}

func BenchmarkRistretto_SetAndGet(b *testing.B) {
	c, _ := ristretto.NewCache(&ristretto.Config{
		NumCounters: 1e5,
		MaxCost:     1e6,
		BufferItems: 64,
		KeyToHash: func(k interface{}) (uint64, uint64) {
			ck := k.(secache.Key)
			return ck.Hi, ck.Lo
		},
	})

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := sharedKeys[i%maskSize]
		c.Set(key, mockValue, 1)
		_, _ = c.Get(key)
	}
	c.Wait()
}

func BenchmarkRistretto_RealisticScenario(b *testing.B) {
	c, _ := ristretto.NewCache(&ristretto.Config{
		NumCounters: 1e5,
		MaxCost:     1e6,
		BufferItems: 64,
		KeyToHash: func(k interface{}) (uint64, uint64) {
			ck := k.(secache.Key)
			return ck.Hi, ck.Lo
		},
	})
	for i := 0; i < cacheSize; i++ {
		c.Set(sharedKeys[i], mockValue, 1)
	}
	c.Wait()

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			key := sharedKeys[i%maskSize]
			if i%5 == 0 {
				c.Set(key, mockValue, 1)
			} else {
				_, _ = c.Get(key)
			}
			i++
		}
	})
}
