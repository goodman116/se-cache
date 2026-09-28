module github.com/goodman116/se-cache/benchmarks

go 1.22.0

replace github.com/goodman116/se-cache => ../

require (
	github.com/dgraph-io/ristretto v0.2.0
	github.com/go-pkgz/expirable-cache/v2 v2.0.0
	github.com/goodman116/se-cache v0.0.0-00010101000000-000000000000
	github.com/jellydator/ttlcache/v3 v3.3.0
	github.com/patrickmn/go-cache v2.1.0+incompatible
)

require (
	github.com/cespare/xxhash/v2 v2.1.1 // indirect
	github.com/dustin/go-humanize v1.0.1 // indirect
	github.com/klauspost/cpuid/v2 v2.2.10 // indirect
	github.com/pkg/errors v0.9.1 // indirect
	github.com/zeebo/xxh3 v1.1.0 // indirect
	golang.org/x/sync v0.8.0 // indirect
	golang.org/x/sys v0.30.0 // indirect
)
