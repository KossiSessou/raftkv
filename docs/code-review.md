# Code Review — WAL segment rotation (`wal-open-with-offset-struct`)

**Scope:** `git diff main...HEAD` — segment rotation, `Offset{SegmentID, Position}`, and the accompanying docs/tests.
**Method:** 8 finder angles (line-by-line, removed-behavior, cross-file, reuse, simplification, efficiency, altitude, conventions) → 33 raw candidates → deduped → 1 verifier per candidate.
**Result:** 9 findings survived verification (8 CONFIRMED, 1 refuted-in-part and downgraded, 1 refuted and dropped).

## Correctness

### 1. `rotate()` failure bricks the WAL while it still reports itself open — CONFIRMED
`internal/wal/wal.go:158`

If `os.OpenFile` fails after the old fd is synced and closed, `w.fd` is assigned `nil`, `w.closed` stays `false`, and `w.position` is never reset. There is no failed/poisoned state on the struct.

**Failure scenario:** a transient `OpenFile` failure during rotation (`ENOSPC`, `EMFILE`, permissions) leaves `w.fd == nil` while the WAL reports itself open. Every subsequent `Append` re-triggers rotate (position still exceeds the threshold), whose first step `w.fd.Sync()` on the nil `*os.File` returns `os.ErrInvalid` — the WAL is bricked forever, returning an opaque error unrelated to the root cause. `Close()` also returns `os.ErrInvalid` instead of shutting down cleanly. `activeID` was already incremented, so accounting references a segment file that never got created.

### 2. Partial write leaves torn bytes and desyncs offsets — PLAUSIBLE
`internal/wal/wal.go:205`

A short `fd.Write` followed by an error leaves torn bytes on disk without advancing `w.position` or poisoning the WAL.

**Failure scenario:** `fd.Write` returns `(n>0, err)` mid-frame (e.g. disk fills). `Append` returns the error with `w.position` unadvanced while `n` bytes sit at the file tail. The caller retries after freeing space: `O_APPEND` lands the new record at true EOF (after the torn bytes), but `Append` returns `Offset{Position: <stale w.position>}`. Offsets no longer match disk layout, and the torn frame is now mid-segment — which WAL-4 defines as unrecoverable corruption, not a truncatable tail.

### 3. `Close()` violates its own documented durability contract — CONFIRMED
`internal/wal/wal.go:110`

`_ = w.fd.Sync()` discards the pre-close flush error, so `Close` can return `nil` after a failed fsync — contradicting `docs/wal-design.md`'s stated contract ("after Close returns nil, everything ever appended is durable"). Repeated `Close()` calls also unconditionally return `nil` (fresh local `err` each call, `closeOnce` skips the body), swallowing the first call's real error.

**Failure scenario:** in `SyncNever` mode with buffered appends, the final fsync fails with an I/O error but `fd.Close()` succeeds → `Close` returns `nil`. A caller trusting the documented contract believes all appends are durable; they are lost on power failure.

### 4. Panic on a legal config — CONFIRMED
`internal/wal/wal.go:97`

`Open(dir, Config{Mode: SyncInterval})` with `Interval` left unset panics in `time.NewTicker(0)` ("non-positive interval for NewTicker"). `MaxSize` is defaulted when zero; `Interval` is not.

### 5. uint32 length truncation on large records — CONFIRMED
`internal/wal/wal.go:186`

The frame's Length field is uint32, but the only size guard compares against the configurable **uint64** `maxSize`.

**Failure scenario:** `Config{MaxSize: 8<<30}` and a 5GiB record: `uint64(8+recordLen) ≈ 5GiB` passes the 8GiB guard; `PutUint32` stores `recordLen mod 2^32` (1GiB) while the full 5GiB payload is written. Replay computing the next boundary as `offset+8+N` lands mid-payload — the format invariant breaks undetectably until read time. The real constraint (`recordLen ≤ math.MaxUint32`) is never enforced at the framing boundary.

## Test gap

### 6. No test resumes a non-empty active segment — CONFIRMED
`internal/wal/wal_test.go:66`

`TestOpenResumesActiveSegment` creates only empty files and never checks `w.position`.

**Failure scenario:** a regression leaves `w.position` at 0 when reopening a segment that already holds data. `O_APPEND` still writes at true EOF, but `Append` returns `Offset{Position: 0}` — every returned offset points at the wrong byte, and no existing test (`TestAppendBasic`, `TestAppendSyncInterval`, `TestRotation`, `TestOpenResumesActiveSegment`) catches it, because none does append → close → reopen → verify offset continuity.

## Cleanup


### 7. Dead `WAL.interval` field — CONFIRMED
`internal/wal/wal.go:36`

Assigned in `Open`'s struct literal but never read — the diff switched `time.NewTicker(w.interval)` to `time.NewTicker(cfg.Interval)`, orphaning the field. Delete it or use it — not both paths half-alive.

### 8. Duplicated segment-open logic — PLAUSIBLE
`internal/wal/wal.go:158`

The segment-open sequence (`OpenFile` with `O_CREATE|O_APPEND|O_WRONLY, 0o644` on `filepath.Join(dir, formatSegmentName(id))`) is duplicated between `Open` and `rotate`. The two copies are currently identical, but nothing keeps them that way. An `openSegment(dir, id)` helper in `segment.go` — which already owns naming — removes the drift risk.

## Not flagged (by design)

- Missing torn-tail detection at `Open` — that's WAL-4, work not yet reached, not a bug in this diff.
- `t.Errorf("... = %d", off1)` passing the whole `Offset` struct to `%d` (garbage on failure), a doc typo ("Segment Formant") and an 11-digit filename in a `wal-format.md` example, and a few stray blank lines gofmt won't touch — all below the findings bar.

## Common thread

Findings 1–3 are the same defect wearing three costumes: **error paths return an error but never update WAL state.** Deciding once what state a WAL is in after any failed I/O (poisoned? closed? retryable?) fixes all three at the right altitude — and it's exactly the question WAL-4/5's crash-safety work will force anyway.
