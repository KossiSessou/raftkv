# Runtime Behavior

**status:** stable
**last updated:** 2026-07-09

## Sync Policies
The WAL implement three policies:
`SyncAlways`: every successful `Append` return means the record survives any crash. Slowest, strongest.
`SyncInterval(d)`: after a crash, records appended in the last ~d may be lost. Everything older survives.
`SyncNever`: after a crash, an unbounded amount of trailing data may be lost. The OS flushes on its own schedule. Fastest, weakest.

There is also a manual `Sync()` that lets a caller force durability on demand in any mode.
`Close()` performs a best-effort sync before closing (which strengthens Close's contract to "when Close returns, everything appended so far is durable"). This applies regardless of the sync mode — even `SyncNever` syncs on close.

## Concurrency Contract
`Append` and `Sync` are safe to call concurrently from multiple goroutines; the WAL serializes them internally via mutex. `Close` is safe to call multiple times (via sync.Once) but must be called at most once from a lifecycle perspective. Callers holding a returned `Offset` across a `Close` and `Open` are fine, offsets are stable identities on disk.

## Lifecycle

A WAL instance moves through three states:
```txt
                              Open(dir, cfg)             Close()
[Unopened] ─────────────────▶ [Open/running] ──────────▶ [Closed]
no fd,                        fd held,                   fd closed,
no goroutine                  mutex live                 ErrClosed on all ops
```
#### Open (running)

While open, three operations coexist, all serialized by the internal mutex:

| Operation          | Who drives it                | What it does                                    |
|--------------------|------------------------------|-------------------------------------------------|
| `Append(record)`   | caller                       | frames + writes a record; rotates segment if full |
| `Sync()`           | caller                       | forces fsync of the active segment              |
| `syncLoop`         | internal goroutine           | periodic fsync; only exists in `SyncInterval` mode |

#### Close sequence

`Close()` is idempotent (`sync.Once`) and performs, in order:

1. Mark closed under the lock; best-effort `fd.Sync()` so buffered
   appends are durable before shutdown.
2. `close(done)` signals the syncLoop goroutine to exit.
3. If in `SyncInterval` mode, block on the `stopped` rendezvous until
   the goroutine has fully exited (prevents fsync racing fd close).
4. `fd.Close()`.

Once closed, every public method returns `ErrClosed`. There is no reopen
on the same instance. Call `Open` again for a fresh one.

#### Failure path

`closeLocked` is also called from `rotate()` and `Append()` when an I/O error
occurs. In those paths the WAL bricks itself immediately — any subsequent
operation returns `ErrClosed`. This is intentional: once the WAL has observed
a write or sync failure there is no safe way to continue, because the in-memory
position may have diverged from the on-disk state.

### Invariants

- The state machine is one-way: Unopened → Open → Closed. No transitions backward.
- The syncLoop goroutine's lifetime is strictly contained within the Open state —
  it is spawned by `Open` and provably exited before `Close` returns.
- After `Close` returns nil, everything ever appended is durable (the step-1 sync
  strengthens Close's contract beyond just resource cleanup).

## Decision Log

Decision 1 — `(segment_id, position)` tuple over a single global offset. Replay is trivial: open `segment_id`, seek to `position`. The index stores it as a struct, slightly larger per entry, but unambiguous. Rotation does not require any global accounting; each segment is internally self-consistent.

Decision 2 — rotate-sync ordering: sync the old segment file *before* creating the new one. The existence of segment N+1 is the signal that segment N is complete. If we created N+1 first and then crashed before syncing N, recovery would see N+1 and trust that N is sealed, even though N may have unsynced data. This would violate the invariant that sealed segments are fully durable.

Decision 3 — Iterator distinguishes torn tails from sealed corruption. When a partial or corrupt record is encountered:
  - If the segment is the **active** segment → treat it as a torn tail. Set `LastValid` to the truncation boundary, keep `err` nil. `Open` uses this to truncate the file and resume appending.
  - If the segment is a **sealed** (non-active) segment → return `ErrCorrupt`. Sealed segments should never have damaged data; if they do, it signals hardware faults or bugs, and the operator must intervene.

Decision 4 — `sync.Once` for `Close` idempotency. Multiple goroutines may call `Close` concurrently; only the first call executes the shutdown sequence. Subsequent calls return the stored error (or nil).

Decision 5 — goroutine rendezvous protocol. The sync loop is signalled via `close(w.done)` and confirms exit via `close(w.stopped)`. The `Close` method drops the mutex while waiting on `<-stopped` to avoid deadlock — the goroutine needs the mutex to call `w.Sync()`. By the time the wait completes, `w.closed` is already true, so any `Append` that grabs the lock in the gap short-circuits with `ErrClosed`.

Decision 6 — brick-on-failure. If `rotate()` or `Append()` encounters an I/O error (fsync failure, write failure), the WAL calls `closeLocked` and all subsequent operations return `ErrClosed`. This is safer than attempting partial recovery: the in-memory position may have diverged from the on-disk state, and continuing could silently lose or duplicate data.

Decision 7 — zero-length payloads are valid. A record with an empty byte slice is framed, written, and replayed normally. The reader treats `length == 0` as a valid record (not as EOF). This keeps the framing uniform — the caller decides what constitutes a meaningful record.