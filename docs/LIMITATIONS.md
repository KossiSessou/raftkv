# Limitations

## Active-segment recovery

During crash recovery, damage at the tail of the active segment is
handled pragmatically: replay stops at the first invalid record and the
tail is truncated. This treats all active-segment damage as torn
writes — the common case during crash recovery.

What is **not** done is a rigorous scan past the damage to distinguish a
torn write from bit-rot. Doing so would require byte-by-byte scanning
for a valid CRC-length-CRC frame, which is expensive and itself
heuristic. This is an accepted limitation: simpler, ships.
