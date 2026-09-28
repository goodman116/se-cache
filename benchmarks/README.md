# Cache Implementation Performance Benchmarks

This sub-project evaluates the raw execution performance and memory overhead of this cache subsystem (published as `goodman116/se-cache`) right next to prominent industry-standard in-memory caching solutions in the Go ecosystem.

To guarantee an absolute apples-to-apples comparison, the test suites standardize the key tracking architecture. Every compatible contender is forced to look up and store identical, pre-computed 16-byte cryptographic key representations. This isolates the benchmark to measure structural mapping and concurrency coordination efficiency rather than generic interface wrapping bottlenecks.

## Tested Implementations

The following library versions were evaluated under a strict **Go 1.22** runtime environment:

- **[goodman116/se-cache](https://github.com/goodman116/se-cache)** - This project (Sharded array architecture using unified 16-byte `Key` structs and background atomic clocks).
- **[go-pkgz/expirable-cache](https://github.com/go-pkgz/expirable-cache)** (v2) - Simple time-to-live cache leveraging traditional sequential double-linked lists for LRU.
- **[patrickmn/go-cache](https://github.com/patrickmn/go-cache)** - Standard thread-safe in-memory key-value store with clean global lock expiration intervals.
- **[jellydator/ttlcache](https://github.com/jellydator/ttlcache)** (v3) - Feature-rich generic thread-safe concurrent cache tracking precise absolute element lifetimes.
- **[dgraph-io/ristretto](https://github.com/dgraph-io/ristretto)** - High-performance, contention-resistant memory-bound caching engine utilizing custom hash override functions.

## Environment

- **CPU:** Intel(R) Core(TM) i5-8350U CPU @ 1.70GHz (4 Physical Cores, 8 Logical Threads)
- **Go Runtime:** Go 1.22
- **OS:** Linux / Native Journaled Partition

## Benchmark Metrics

```text
BenchmarkSeCache_Get-8                        	12662337	       86.35 ns/op	       0 B/op	       0 allocs/op
BenchmarkSeCache_Set-8                        	 9054436	       119.1 ns/op	       0 B/op	       0 allocs/op
BenchmarkSeCache_SetAndGet-8                  	 6682357	       180.2 ns/op	       0 B/op	       0 allocs/op
BenchmarkSeCache_RealisticScenario-8          	14268046	       80.23 ns/op	       0 B/op	       0 allocs/op
BenchmarkExpirableCache_Get-8                 	10046748	       115.5 ns/op	       0 B/op	       0 allocs/op
BenchmarkExpirableCache_Set-8                 	10016638	       115.0 ns/op	       0 B/op	       0 allocs/op
BenchmarkExpirableCache_SetAndGet-8           	 5436398	       212.1 ns/op	       0 B/op	       0 allocs/op
BenchmarkExpirableCache_RealisticScenario-8   	 5526524	       205.7 ns/op	       0 B/op	       0 allocs/op
BenchmarkGoCache_Get-8                        	10100485	       121.7 ns/op	       3 B/op	       0 allocs/op
BenchmarkGoCache_Set-8                        	 5390377	       221.7 ns/op	      84 B/op	       1 allocs/op
BenchmarkGoCache_SetAndGet-8                  	 4022012	       290.7 ns/op	      84 B/op	       1 allocs/op
BenchmarkGoCache_RealisticScenario-8          	 6055032	       189.5 ns/op	      19 B/op	       1 allocs/op
BenchmarkTTLCache_Get-8                       	 1557692	       767.9 ns/op	      48 B/op	       1 allocs/op
BenchmarkTTLCache_Set-8                       	 1842085	       650.6 ns/op	       1 B/op	       0 allocs/op
BenchmarkTTLCache_SetAndGet-8                 	 1219364	       1003  ns/op	      50 B/op	       1 allocs/op
BenchmarkTTLCache_RealisticScenario-8         	 1931068	       631.1 ns/op	      38 B/op	       0 allocs/op
BenchmarkRistretto_Get-8                      	 9311458	       131.6 ns/op	      20 B/op	       1 allocs/op
BenchmarkRistretto_Set-8                      	 2153608	       592.0 ns/op	     177 B/op	       3 allocs/op
BenchmarkRistretto_SetAndGet-8                	 1633762	       740.9 ns/op	     202 B/op	       4 allocs/op
BenchmarkRistretto_RealisticScenario-8        	 7538133	       163.8 ns/op	      53 B/op	       1 allocs/op
```

## Key Findings

1. **Read Path Dominance (`Get`):** `se-cache` handles lookups at **86.40 ns/op**. This performance advantage comes from splitting the workload across 64 independent shards, which prevents global lock contention.
2. **Write Path Efficiency (`Set`):** Streamlining the entry parameters into an optimized 16-byte key type enables `se-cache` to process writes quickly while maintaining **0 B/op and 0 allocs/op**.
3. **Target Load Headroom:** At an average operational rate of ~119.1 ns per write, this cache can handle roughly **8,400,000 requests per second** per thread. 

## Execution

To reproduce these metrics on your local environment, navigate into the benchmarks subfolder and run:

```bash
go test -bench=. -benchmem
```
