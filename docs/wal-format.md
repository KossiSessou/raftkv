# <Component> On-Disk Format

**Status:** stable
**Last updated:** 2026-07-09

## Purpose
This format describes a WAL record so that readers with no external background can tell how far to read from each record via the length field and check the integrity of the data(payload field) by calculating the checksum.

## Record Layout

| Offset | Field        | Width    | Type            | Description                  |
|--------|--------------|----------|-----------------|------------------------------|
| 0      | Length       | 4 bytes  | uint32 LE       | Length of the payload        |
| 4      | Checksum     | 4 bytes  | uint32 LE       | Checksum of Length + Payload |
| 8      | Payload      | N bytes  | variable        | Payload                      |

Header = 8 bytes fixed. Total = 8 + N.

## Field Decisions

- **Length (offset 0):** This tells the reader how many bytes to read for the payload
- **Checksum before payload:** This is ideal for our design as we have our entire payload at hand when we call append. A checksum after payload is ideal for streaming which is not our case. We can also easily access the length and checksum easily as we have 8 bytes fixed size header.
- **Checksum Length + Payload(Offset 4):** reader must know whether the length or the payload has been corrupted. A corrupted length is dangerous as the reader might try to access invalid memory address that could cause the program to issue a seg fault or access garbage data.
- **Little-endian:** matches the host architecture (x86/ARM); reader
  and writer must agree, and this is the project-wide convention.

## Segment Formant and Naming
A segment is a file of record appended back to back.
```txt
0000000001.wal:  [record A][record B][record C]...[record M]
                ↑0       ↑89       ↑184
                (byte positions inside THIS file)

0000000002.wal:  [record N][record O][record P]...
                ↑0       ↑42

0000000003.wal:  [record X][record Y]...
                ↑0       ↑1247  ← Offset{SegmentID: 3, Position: 1247} points here
```
Each follow the form `%010d.wal`. Zero-padded 10 digit.  
  
```txt
/data/mywal/
    0000000001.wal     ← oldest, immutable (frozen, no longer being written)  
    0000000002.wal     ← immutable  
    0000000003.wal     ← immutable  
    0000000004.wal     ← ACTIVE — currently being appended to  
```

## The Offset Model
The offset is a `struct {SegmentID uint64, Position uint64}`. The SegementID field refers to the current segment in use an the Postion field refers to the byte offset of the record first byte within that file.

## Segment Rotation Rule
A segment is 64MB large. If a to be appended record size exceed it, it will be rejected with `ErrRecordTooLarge`. If appending a record will make the existing file exceed that capacity, we trigger a rotation. Once rotated away, segments are never written to again.

## `Open(dir, cfg)` Logic

`Open` initialises a WAL instance and recovers any partial state from disk:

1. Create the directory if it does not exist.
2. List all `.wal` segment files. Pick the highest-numbered segment as the
   active one. If none exist, start at `0000000001.wal`.
3. Open the active segment file for appending.
4. **Crash recovery:** replay the active segment through an `Iterator`. When
   the iterator encounters a torn record (partial header or payload), it records
   the truncation boundary — the byte offset of the first invalid byte — and
   stops without error. The file is then truncated to that boundary, so the
   log is ready for new appends. Sealed-segment corruption is never expected
   here because `rotate()` syncs a segment before creating its successor.
5. If `SyncInterval` mode is configured, start a background goroutine that
   calls `Sync()` on a fixed interval.

`Config` defaults when zero-valued:

| Field    | Default                |
|----------|------------------------|
| MaxSize  | 64 MiB                 |
| Interval | 100 ms (SyncInterval)  |

## `readRecordAt` Return Semantics

| Condition                          | Return                     |
|------------------------------------|----------------------------|
| Clean read (valid CRC)            | `(record, size, nil)`      |
| Zero bytes read (`io.EOF`)        | `(nil, 0, io.EOF)`         |
| Zero-length field detected        | `(nil, 0, io.EOF)`         |
| Partial header (< 8 bytes)        | `(nil, 0, ErrCorrupt)`     |
| Partial payload (short read)      | `(nil, 0, ErrCorrupt)`     |
| Checksum mismatch (full payload)  | `(nil, 0, ErrCRCMismatch)` |

## Invariants
Things that are always true if the file is valid:
- Every record begins with Length.
- A reader at any record boundary can compute the next boundary as
  current_offset + 8 + N. 
- No segment exceeds MaxSegmentSize

## Decision Log

Decision 1 — zero-padded sequence numbers (e.g., `0000000001.wal`). Two practical wins: it sorts lexicographically the same as numerically (so `ls` shows them in order), and operational tools handle them cleanly.

Decision 2 — bump-on-write: close the active segment when the *next* record would exceed `MaxSegmentSize`, not when the file reaches `MaxSegmentSize` exactly. This makes `MaxSegmentSize` a hard upper bound that is never violated, instead of a soft target. Replay code can assume "no segment exceeds `MaxSegmentSize`" as an invariant.

Decision 3 — record framing: 4-byte length + 4-byte CRC32 + variable payload. The checksum covers both the length field and the payload so a corrupted length (which could cause the reader to seek into garbage memory) is always detected. Little-endian matches the host architecture and is the project-wide convention.

Decision 4 — CRC32-IEEE table: zero external dependency, fast hardware-accelerated path on x86 and ARM, widely understood.

Decision 5 — maximum record size of 1 GiB (`maxRecordSize`). Records larger than this are rejected with `ErrRecordTooLarge`. This bounds memory allocation in both the writer and the reader.

Decision 6 — zero-length payload (`length == 0`) means end-of-log: `readRecordAt` returns `io.EOF`. This avoids ambiguous states between "empty record" and "unused tail" — empty records have no meaning in this system so zero is an unambiguous sentinel.

Decision 7 — `Open` only validates the active segment during recovery. Sealed segments are trusted because `rotate()` syncs a segment before creating its successor. If sealed-segment corruption is encountered during a full replay, the `Iterator` returns `ErrCorrupt` — a hard error that must be surfaced to the operator.

Decision 8 — `Open` truncates a torn tail to the last valid offset so the WAL is immediately append-able after recovery. No manual repair or explicit `Recover()` call is needed.

## Decisions Deferred

- **No versioning:** format is frozen for this project; revisit if it
  must evolve.
- **No compression:** out of scope.
- **No tombstone/delete markers:** deletion lives at the KV layer, not the WAL.