# raftkv

A sharded, Raft-replicated, linearizable key-value database, built in Go.

> **Status:** Phase 0 (Foundations) is complete. The concurrent in-memory KV store and crash-safe write-ahead log below are implemented and tested under the race detector in CI. The persistent storage engine is the next milestone.

## What's implemented

### Concurrent in-memory KV store

A small `Store` interface (`Get` / `Set` / `Delete`) with two interchangeable
implementations and a benchmark suite comparing them:

- **`MutexStore`:** a single map guarded by an `RWMutex`. Simple baseline.
- **`ShardedStore`:** keys are distributed across 16 independent shards using an FNV-1a hash and a power-of-two mask, so concurrent operations on different key rarely contend on the same lock.

Both are exercised by table-driven correctness tests and a benchmark matrix that sweeps goroutine counts (1 / 8 / 64) against read/write mixes (90/10, 50/50, 10/90).

### Write-ahead log (`wal/`)

An append-only, crash-safe log that durably records operations before they're
applied to the state machine. Written entirely from scratch with no external
dependencies.

**Record format.** Each record is framed with a fixed 8-byte header:

| Field    | Width   | Description                              |
|----------|---------|------------------------------------------|
| Length   | 4 bytes | Payload length (uint32, little-endian)   |
| Checksum | 4 bytes | CRC32 over the length + payload          |
| Payload  | N bytes | Caller-supplied record bytes             |

**Sync policies.** Configurable durability tradeoffs:

- **`SyncAlways`:** `fsync` after every append (safest, slowest).
- **`SyncNever`:**  rely on the OS page cache (fastest, least durable).
- **`SyncInterval`:** a background goroutine flushes on a fixed interval, trading
  a bounded window of un-synced writes for throughput.

**Segments.** The log is split into numbered segments (e.g. `0000000001.wal`).
When the active segment reaches a configurable max size, the WAL atomically
rotates to a new segment. Naming, parsing, and directory listing are handled by
a dedicated `segment` module.

**Crash recovery.** `Open` inspects the active segment on startup and
transparently truncates a torn tail — partial writes left by a crash — so the
log resumes at the last valid offset. Sealed-segment corruption is treated as a
hard error (not expected under normal operation).

**Offset-based replay.** An `Offset` type locates any record by `(segment,
position)`. The `Replay(from Offset)` method returns an `Iterator` that yields
records sequentially, enabling incremental reconstruction of state after an
interruption.

The on-disk format and design rationale are documented in
[`docs/wal-format.md`](docs/wal-format.md) and
[`docs/wal-design.md`](docs/wal-design.md).

## Project layout

```
.
├── cmd/
│   └── raftkv/        # main entry point (placeholder)
├── internal/
│   ├── kv/            # Store interface + MutexStore / ShardedStore
│   └── wal/           # write-ahead log + segment utilities
├── docs/
│   ├── benchmark.md     # KV store & WAL performance results
│   ├── wal-design.md    # WAL runtime behaviour and concurrency contract
│   ├── wal-format.md    # on-disk binary format specification
│   └── LIMITATIONS.md   # known limitations and design tradeoffs
└── Makefile
```

## Building and testing

Requires Go 1.24+.

```sh
make test    # go test ./...
make lint    # go vet + golangci-lint
make bench   # go test -bench=. -benchmem ./...
```

CI runs `go vet`, `golangci-lint`, and the full test suite **with the race
detector** on every push and pull request to `main`.

## Benchmarks

Quick summary of key results (Apple M3, Go 1.24, 5 iterations × 3s):

| What | Highlight |
|------|-----------|
| KV 8 goroutines, 50/50 R/W | ShardedStore **1.5x** faster than MutexStore |
| KV 8 goroutines, 10/90 R/W | ShardedStore **2.3x** faster (writes rarely contend) |
| WAL SyncNever append | ~800K ops/s, p99 latency **4.25 µs** |
| WAL SyncAlways append | ~362 ops/s, p99 latency **3.38 ms** |

Both KV implementations allocate zero bytes per operation. Full results and analysis:
[`docs/benchmark.md`](docs/benchmark.md)

## Next up

**Phase 1 — Storage engine.** A persistent, crash-safe single-node KV engine in the Bitcask model: values live in WAL segments, an in-memory hash index maps each key to its `(segment, offset)`, and `Open` rebuilds that index by replaying the log so acknowledged writes survive a `kill -9`. Compaction will reclaim space from overwritten and deleted keys while reads and writes continue uninterrupted.
