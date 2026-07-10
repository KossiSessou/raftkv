# Benchmark Results

Performance benchmarks for the KV store and WAL subsystems.

**Environment:**
- Go 1.24+
- macOS, Apple M3
- `go test -bench=. -benchmem -benchtime=3s -count=5`

---

## KV Store

Two implementations of the `Store` interface (`Get`/`Set`/`Delete`) benchmarked
across a matrix of concurrency levels (1 / 8 / 64 goroutines) and read/write
ratios (90/10, 50/50, 10/90). All operations use keys from a pool of 8 values;
0 B/op across the board (map value type is pointer-free, no allocations on the
hot path).

### Results

| Config | Goroutines | Read/Write | MutexStore (ns/op) | ShardedStore (ns/op) | Winner |
|--------|------------|------------|-------------------:|---------------------:|--------|
| 1G_50R50W | 1 | 50/50 | 29.5 | 29.5 | tie |
| 8G_50R50W | 8 | 50/50 | 63.3 | 40.9 | ShardedStore (1.5x) |
| 64G_50R50W | 64 | 50/50 | 62.0 | 59.3 | ShardedStore |
| 1G_90R10W | 1 | 90/10 | 23.9 | 23.1 | tie |
| 8G_90R10W | 8 | 90/10 | 56.4 | 34.0 | ShardedStore (1.7x) |
| 64G_90R10W | 64 | 90/10 | 54.5 | 53.4 | tie |
| 1G_10R90W | 1 | 10/90 | 27.4 | 27.8 | tie |
| 8G_10R90W | 8 | 10/90 | 98.0 | 42.5 | ShardedStore (2.3x) |
| 64G_10R90W | 64 | 10/90 | 110.5 | 75.1 | ShardedStore (1.5x) |

### Analysis

- **Single goroutine:** both implementations are identical (no contention to
  avoid), so either is fine.
- **8 goroutines:** ShardedStore pulls ahead significantly, especially under
  write-heavy loads (2.3x faster at 10R/90W) because writes target different
  shards and rarely collide on the same lock.
- **64 goroutines:** the gap narrows at high concurrency — the machine has
  limited cores and lock contention is dominated by OS scheduling, but
  ShardedStore still edges ahead.
- **Zero allocations** for both implementations across all configurations.

---

## Write-Ahead Log

Append throughput for 100-byte records under each sync policy, plus tail latency
distributions.

### Throughput

| Sync Mode | ns/op | ops/s | Allocs |
|-----------|------:|------:|-------:|
| SyncNever | 1,250 | ~800K | 1 B/op, 1 alloc |
| SyncInterval | 1,970 | ~508K | 1 B/op, 1 alloc |
| SyncAlways | 2,761,000 | ~362 | 1 B/op, 1 alloc |

SyncNever is **~2,200x** faster than SyncAlways. SyncInterval lands at a
~1.6x penalty over SyncNever — the background goroutine's interval flush only
touches the file descriptor on a timer, not per-append.

### Latency Distribution (100-byte records)

Measured with 100K samples (SyncNever, SyncInterval) or 2K samples
(SyncAlways, limited by fsync cost):

| Sync Mode | p50 | p99 | p99.9 |
|-----------|----:|----:|------:|
| SyncNever | 1.17 µs | 4.25 µs | 7.67 µs |
| SyncInterval | 1.17 µs | 3.50 µs | 7.08 µs |
| SyncAlways | 2.99 ms | 3.38 ms | 8.75 ms |

SyncNever and SyncInterval have sub-10 µs tail latencies; the interval flush
does not introduce measurable jitter in the hot path. SyncAlways p99.9 spikes
to ~9 ms, dominated by `fsync` syscall variance.

### How to reproduce

```sh
make bench                    # throughput benchmarks (all packages)
go test -v -run TestAppendLatencyPercentiles ./internal/wal/  # tail latency
```
