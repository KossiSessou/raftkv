# raftkv

A sharded, Raft-replicated, linearizable key-value database, built from scratch in Go.



> **Status:** early and active. The foundational pieces below are implemented and tested under the race detector in CI. The persistent storage engine is the next milestone.

## What's implemented

### Concurrent in-memory KV store

A small `Store` interface (`Get` / `Set` / `Delete`) with two interchangeable
implementations and a benchmark suite comparing them:

- **`MutexStore`:** a single map guarded by an `RWMutex`. Simple baseline.
- **`ShardedStore`:** keys are distributed across 16 independent shards using an FNV-1a hash and a power-of-two mask, so concurrent operations on different key rarely contend on the same lock.

Both are exercised by table-driven correctness tests and a benchmark matrix that sweeps goroutine counts (1 / 8 / 64) against read/write mixes (90/10, 50/50, 10/90).

### Write-ahead log (`wal/`)

An append-only, crash-safe log that durably records records before they're
applied. Each record is framed with a fixed 8-byte header:

| Field    | Width   | Description                              |
|----------|---------|------------------------------------------|
| Length   | 4 bytes | Payload length (uint32, little-endian)   |
| Checksum | 4 bytes | CRC32 over the length + payload          |
| Payload  | N bytes | Caller-supplied record bytes             |

Durability is configurable via the sync policy:

- **`SyncAlways`:** `fsync` after every append (safest, slowest).
- **`SyncNever`:**  rely on the OS page cache (fastest, least durable).
- **`SyncInterval`:** a background goroutine flushes on a fixed interval, trading
  a bounded window of un-synced writes for throughput.

The full on-disk format and the rationale behind each field is documented in [`docs/wal-format.md`](docs/wal-format.md).

## Project layout

```
.
├── cmd/
│   └── raftkv/       # main entry point (placeholder for now)
├── internal/
│   ├── kv/           # Store interface + MutexStore / ShardedStore
│   └── wal/          # write-ahead log package
├── docs/             # design + format documentation
└── requirements/     # phase-by-phase specifications driving the build
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

## Next up

**Phase 1 — Storage engine.** A persistent, crash-safe single-node KV engine in the Bitcask model: values live in WAL segments, an in-memory hash index maps each key to its `(segment, offset)`, and `Open` rebuilds that index by replaying the log so acknowledged writes survive a `kill -9`. Compaction will reclaim space from overwritten and deleted keys while reads and writes continue uninterrupted.
