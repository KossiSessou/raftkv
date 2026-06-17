# <Component> On-Disk Format

**Status:** draft
**Last updated:** 2026-06-17

## Purpose
This format describes a WAL record so that readers with no external background can tell how far to read from each record via the length field and check the integrity of the data(payload field) by calculating the checksum.

## Record Layout

| Offset | Field        | Width    | Type            | Description                  |
|--------|--------------|----------|-----------------|------------------------------|
| 0      | Length       | 4 bytes  | uint32 LE       | Length of the payload        |
| 4      | Checksum     | 4 bytes   | uint32 LE       | Checksum of Length + Payload |
| 8      | Payload      | N bytes  | variable        | Payload                      |

Header = 8 bytes fixed. Total = 8 + N.

## Field Decisions

- **Length (offset 0):** This tells the reader how many bytes to read for the payload
- **Checksum before payload:** This is ideal for our design as we have our entire payload at hand when we call append. A checksum after payload is ideal for streaming which is not our case. We can also easily access the length and checksum easily as we have 8 bytes fixed size header.
- **Checksum Length + Payload(Offset 4):** reader must know whether the length or the payload has been corrupted. A corrupted length is dangerous as the reader might try to access invalid memory address that could cause the program to issue a seg fault or access garbage data.
- **Little-endian:** matches the host architecture (x86/ARM); reader
  and writer must agree, and this is the project-wide convention.

## Invariants
Things that are always true if the file is valid:
- Every record begins with Length.
- A reader at any record boundary can compute the next boundary as
  current_offset + 8 + N. An offset is the byte position of a record's first byte (its Length field); Append returns this, and Replay seeks to it.


## Decisions Deferred
What this format intentionally does NOT handle, and why:
- **No versioning:** format is frozen for this project; revisit if it
  must evolve.
- **No compression:** out of scope.

## Open Questions
Things not yet resolved:
- How is the end-of-file / unused tail distinguished from a real record?