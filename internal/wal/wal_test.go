package wal

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"math/rand"
	"os"
	"path/filepath"
	"slices"
	"testing"
	"time"
)

func TestAppendBasic(t *testing.T) {
	dir := t.TempDir()

	w, err := Open(dir, Config{Mode: SyncNever})

	if err != nil {
		t.Fatal(err)
	}

	off1, err := w.Append([]byte("hello"))

	if err != nil {
		t.Fatal(err)
	}

	off2, err := w.Append([]byte("world"))

	if err != nil {
		t.Fatal(err)
	}

	if off1.Position != 0 {
		t.Errorf("First offset = %d; want 0", off1)
	}

	if off2.Position != 13 {
		t.Errorf("Second offset = %d; want 13", off2)
	}

	info, _ := w.fd.Stat()

	if info.Size() != 26 {
		t.Errorf("File size = %d; want 26", info.Size())
	}
}

func TestAppendSyncInterval(t *testing.T) {
	dir := t.TempDir()

	w, err := Open(dir, Config{Mode: SyncInterval, Interval: 50 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	for range 1000 {
		_, err := w.Append([]byte("hello"))
		if err != nil {
			t.Fatal(err)
		}
	}
	time.Sleep(500 * time.Millisecond)
	err = w.Close()
	if err != nil {
		t.Fatal(err)
	}
}

func TestOpenResumesActiveSegment(t *testing.T) {
	dir := t.TempDir()
	// simulate a WAL that previously rotated through 3 segments
	for _, id := range []uint64{1, 2, 3} {
		f, err := os.Create(filepath.Join(dir, formatSegmentName(id)))
		if err != nil {
			t.Fatal(err)
		}
		_ = f.Close()
	}

	w, err := Open(dir, Config{Mode: SyncNever})
	if err != nil {
		t.Fatal(err)
	}

	if w.activeID != 3 {
		t.Errorf("activeID = %d; want 3 (highest existing segment)", w.activeID)
	}

	_ = w.Close()
}

func TestOpenResumesActiveSegmentPosition(t *testing.T) {
	dir := t.TempDir()

	w1, err := Open(dir, Config{Mode: SyncNever})

	if err != nil {
		t.Fatal(err)
	}

	off1, err := w1.Append([]byte("Hello"))

	if err != nil {
		t.Fatal(err)
	}

	_, err = w1.Append([]byte("World"))
	if err != nil {
		t.Fatal(err)
	}

	err = w1.Close()
	if err != nil {
		t.Fatal(err)
	}

	segmentPath := filepath.Join(dir, formatSegmentName(1))

	info, err := os.Stat(segmentPath)
	if err != nil {
		t.Fatal(err)
	}

	expectedResumePosition := uint64(info.Size())

	w2, err := Open(dir, Config{Mode: SyncNever})
	if err != nil {
		t.Fatal(err)
	}

	off2, err := w2.Append([]byte("Resumed"))

	if err != nil {
		t.Fatal(err)
	}

	if off2.Position == off1.Position {
		t.Errorf("new append after reopen collided with existing record at position %d", off2.Position)
	}

	if off2.Position != expectedResumePosition {
		t.Errorf("new append after reopen at Position=%d; want %d (post-reopen EOF)",
			off2.Position, expectedResumePosition)
	}
}

func TestRotation(t *testing.T) {
	dir := t.TempDir()
	w, _ := Open(dir, Config{Mode: SyncNever, MaxSize: 40})

	// each record: 8 header + 10 payload = 18 bytes. Two fit in 40; third forces rotation.
	o1, _ := w.Append(make([]byte, 10)) // seg 1, pos 0
	o2, _ := w.Append(make([]byte, 10)) // seg 1, pos 18
	o3, _ := w.Append(make([]byte, 10)) // 18+18+18=54 > 40 → rotate → seg 2, pos 0

	if o1.SegmentID != 1 || o2.SegmentID != 1 {
		t.Errorf("first two records should be in segment 1")
	}
	if o3.SegmentID != 2 || o3.Position != 0 {
		t.Errorf("third record should start segment 2 at position 0, got seg=%d pos=%d", o3.SegmentID, o3.Position)
	}

	ids, _ := listSegments(dir)
	if len(ids) != 2 {
		t.Errorf("expected 2 segment files, got %d", len(ids))
	}

	_ = w.Close()
}

func TestReadRecordAt(t *testing.T) {
	t.Run("good record", func(t *testing.T) {
		dir := t.TempDir()
		w, err := Open(dir, Config{Mode: SyncAlways})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := w.Append([]byte("Hello")); err != nil {
			t.Fatal(err)
		}
		if err := w.Close(); err != nil {
			t.Fatal(err)
		}

		rfd, err := os.OpenFile(filepath.Join(dir, formatSegmentName(1)), os.O_RDONLY, 0)
		if err != nil {
			t.Fatal(err)
		}

		res, err := readRecordAt(rfd, 0)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(res.record, []byte("Hello")) {
			t.Errorf("record mismatch: got %q, want %q", res.record, "Hello")
		}
		if res.size != 13 {
			t.Errorf("size mismatch: got %d, want 13", res.size)
		}
		if err := rfd.Close(); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("clean EOF", func(t *testing.T) {
		dir := t.TempDir()
		w, err := Open(dir, Config{Mode: SyncAlways})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := w.Append([]byte("Hello")); err != nil {
			t.Fatal(err)
		}
		if err := w.Close(); err != nil {
			t.Fatal(err)
		}

		rfd, err := os.OpenFile(filepath.Join(dir, formatSegmentName(1)), os.O_RDONLY, 0)
		if err != nil {
			t.Fatal(err)
		}

		// position 13 is exactly end-of-data (one 13-byte record)
		if _, err := readRecordAt(rfd, 13); err != io.EOF {
			t.Fatalf("got %v, want io.EOF", err)
		}
		if err := rfd.Close(); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("torn header", func(t *testing.T) {
		dir := t.TempDir()
		path := filepath.Join(dir, "torn-header.dat")

		// a valid 13-byte record, then 3 stray bytes (less than an 8-byte header)
		buf := makeRecord(t, []byte("Hello"))
		buf = append(buf, []byte("XyZ")...)
		if err := os.WriteFile(path, buf, 0o644); err != nil {
			t.Fatal(err)
		}

		f, err := os.Open(path)
		if err != nil {
			t.Fatal(err)
		}

		// reading at position 13 hits only 3 bytes -> torn header
		if _, err := readRecordAt(f, 13); err != ErrCorrupt {
			t.Fatalf("got %v, want ErrCorrupt", err)
		}
		if err := f.Close(); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("torn payload", func(t *testing.T) {
		dir := t.TempDir()
		path := filepath.Join(dir, "torn-payload.dat")

		// header claims length 100, but only 20 payload bytes follow
		header := make([]byte, 8)
		payload := []byte("abcdefghijklmnopqrst") // 20 bytes
		binary.LittleEndian.PutUint32(header[0:4], 100)
		crc := crc32.Update(0, crc32.IEEETable, header[0:4])
		crc = crc32.Update(crc, crc32.IEEETable, payload)
		binary.LittleEndian.PutUint32(header[4:8], crc)

		buf := append(header, payload...)
		if err := os.WriteFile(path, buf, 0o644); err != nil {
			t.Fatal(err)
		}

		f, err := os.Open(path)
		if err != nil {
			t.Fatal(err)
		}

		if _, err := readRecordAt(f, 0); err != ErrCorrupt {
			t.Fatalf("got %v, want ErrCorrupt", err)
		}
		if err := f.Close(); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("bad checksum", func(t *testing.T) {
		dir := t.TempDir()
		path := filepath.Join(dir, "bad-crc.dat")

		buf := makeRecord(t, []byte("abcdefghijklmnopqrst"))
		if err := os.WriteFile(path, buf, 0o644); err != nil {
			t.Fatal(err)
		}

		// flip one byte in the payload region (offset 13 = first payload byte + 5)
		f, err := os.OpenFile(path, os.O_RDWR, 0o644)
		if err != nil {
			t.Fatal(err)
		}
		var b [1]byte
		if _, err := f.ReadAt(b[:], 13); err != nil {
			t.Fatal(err)
		}
		b[0] = ^b[0]
		if _, err := f.WriteAt(b[:], 13); err != nil {
			t.Fatal(err)
		}
		if err := f.Close(); err != nil {
			t.Fatal(err)
		}

		f2, err := os.Open(path)
		if err != nil {
			t.Fatal(err)
		}

		if _, err := readRecordAt(f2, 0); err != ErrCRCMismatch {
			t.Fatalf("got %v, want ErrCRCMismatch", err)
		}
		if err := f2.Close(); err != nil {
			t.Fatal(err)
		}
	})
}

func TestReplayRoundTrip(t *testing.T) {
	dir := t.TempDir()

	w, err := Open(dir, Config{Mode: SyncNever})
	if err != nil {
		t.Fatal(err)
	}

	off1, err := w.Append([]byte("alpha"))
	if err != nil {
		t.Fatal(err)
	}

	off2, err := w.Append([]byte("beta"))
	if err != nil {
		t.Fatal(err)
	}

	off3, err := w.Append([]byte("gamma"))
	if err != nil {
		t.Fatal(err)
	}

	it, err := w.Replay(Offset{SegmentID: 1, Position: 0})
	if err != nil {
		t.Fatal(err)
	}

	records, offsets, err := drainReplay(t, it)
	if err != nil {
		t.Fatal(err)
	}
	wantRecord := []string{"alpha", "beta", "gamma"}
	wantOffset := []Offset{off1, off2, off3}

	if len(wantRecord) != len(records) {
		t.Fatalf("record count: want %d; got %d", len(wantRecord), len(records))
	}

	for i := range wantRecord {
		if string(records[i]) != wantRecord[i] {
			t.Errorf("record %d mismatch: want %q; got %q", i, wantRecord[i], records[i])
		}

		if offsets[i].SegmentID != wantOffset[i].SegmentID || offsets[i].Position != wantOffset[i].Position {
			t.Errorf("offset %d mismatch: want %q; got %q", i, wantOffset[i], offsets[i])
		}

	}

	if err := it.Err(); err != nil {
		t.Fatal(err)
	}

	info, err := w.fd.Stat()
	if err != nil {
		t.Fatal(err)
	}

	want := Offset{Position: uint64(info.Size()), SegmentID: 1}
	if it.LastValid() != want {
		t.Errorf("Last valid offset mismatch: want %+v, got %+v", want, it.LastValid())
	}

	// - `Close()` nil, and a second`Close()` also nil
	if err := it.Close(); err != nil {
		t.Fatal(err)
	}
	if err := it.Close(); err != nil {
		t.Fatal(err)
	}

	_, err = w.Append([]byte("delta"))
	if err != nil {
		t.Fatal(err)
	}

}

func TestReplayEmptyWAL(t *testing.T) {

	dir := t.TempDir()

	w, err := Open(dir, Config{Mode: SyncNever})
	if err != nil {
		t.Fatal(err)
	}

	it, err := w.Replay(Offset{SegmentID: 1, Position: 0})
	if err != nil {
		t.Fatal(err)
	}
	got := it.Next()
	if got != false {
		t.Errorf("Expected it.Next() to be false; got %v", got)
	}

	if err := it.Err(); err != nil {
		t.Fatal(err)
	}
	want := Offset{SegmentID: 1, Position: 0}
	if it.LastValid() != want {
		t.Errorf("Last valid offset mismatch: want %v, got %v", want, it.LastValid())
	}

	if err := it.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestReplayMultiSegment(t *testing.T) {
	dir := t.TempDir()
	w, _ := Open(dir, Config{Mode: SyncNever, MaxSize: 40})

	// each record: 8 header + 10 payload = 18 bytes. Two fit in 40; third forces rotation.
	o1, _ := w.Append([]byte("abcdefghij")) // seg 1, pos 0
	o2, _ := w.Append([]byte("klmnopqrst")) // seg 1, pos 18
	o3, _ := w.Append([]byte("uvwxyz7890")) // 18+18+18=54 > 40 → rotate → seg 2, pos 0

	it, err := w.Replay(Offset{SegmentID: 1, Position: 0})
	if err != nil {
		t.Fatal(err)
	}

	_, offsets, err := drainReplay(t, it)
	if err != nil {
		t.Fatal(err)
	}
	wantOffset := []Offset{o1, o2, o3}

	if len(wantOffset) != len(offsets) {
		t.Fatalf("record count: want %d; got %d", len(wantOffset), len(offsets))
	}

	for i := range wantOffset {
		if wantOffset[i] != offsets[i] {
			t.Errorf("offset %d mismatch: want %q; got %q", i, wantOffset[i], offsets[i])
		}
	}

	if err := it.Err(); err != nil {
		t.Fatal(err)
	}
	want := Offset{SegmentID: 2, Position: 18}
	if it.LastValid() != want {
		t.Errorf("Last valid offset mismatch: want %v, got %v", want, it.LastValid())

	}

	if err := it.Close(); err != nil {
		t.Fatal(err)
	}

}

func TestReplayFromOffset(t *testing.T) {
	dir := t.TempDir()

	w, err := Open(dir, Config{Mode: SyncNever})
	if err != nil {
		t.Fatal(err)
	}

	_, err = w.Append([]byte("alpha"))
	if err != nil {
		t.Fatal(err)
	}

	off2, err := w.Append([]byte("beta"))
	if err != nil {
		t.Fatal(err)
	}

	_, err = w.Append([]byte("gamma"))
	if err != nil {
		t.Fatal(err)
	}

	it, err := w.Replay(off2)
	if err != nil {
		t.Fatal(err)
	}

	records, offsets, err := drainReplay(t, it)

	if err != nil {
		t.Fatal(err)
	}

	if len(records) != 2 {
		t.Fatalf("record count: want %d; got %d", 2, len(records))
	}

	if string(records[0]) != "beta" || string(records[1]) != "gamma" {
		t.Errorf("records mismatch: want [beta gamma]; got [%s %s]", records[0], records[1])
	}

	if offsets[0] != off2 {
		t.Errorf("offset 0 mismatch: want %q; got %q", off2, offsets[0])
	}

	if err := it.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestReplayTornTail(t *testing.T) {

	t.Run("mid segment", func(t *testing.T) {
		// cut=22 leaves record 2's full header + partial payload (torn payload);
		// cut=16 leaves record 2 fewer than 8 header bytes (torn header).
		for _, cut := range []int64{22, 16} {
			dir := t.TempDir()
			w, err := Open(dir, Config{Mode: SyncAlways})
			if err != nil {
				t.Fatal(err)
			}

			_, err = w.Append([]byte("abcde"))
			if err != nil {
				t.Fatal(err)
			}

			_, err = w.Append([]byte("fghij"))
			if err != nil {
				t.Fatal(err)
			}

			if err := w.Close(); err != nil {
				t.Fatal(err)
			}

			// reopen BEFORE tearing the tail: Open's own recovery would repair
			// the file first, so the damage is done behind the live WAL's back
			// and the iterator — not Open — is what encounters the torn record.
			w2, err := Open(dir, Config{Mode: SyncAlways})
			if err != nil {
				t.Fatal(err)
			}

			path := filepath.Join(dir, formatSegmentName(1))
			if err := os.Truncate(path, cut); err != nil {
				t.Fatal(err)
			}

			it, err := w2.Replay(Offset{1, 0})
			if err != nil {
				t.Fatal(err)
			}

			records, _, err := drainReplay(t, it)
			if err != nil {
				t.Fatal(err)
			}

			if len(records) != 1 {
				t.Fatalf("cut=%d: record count: want 1; got %d", cut, len(records))
			}

			if string(records[0]) != "abcde" {
				t.Errorf("cut=%d: record 0: want %q; got %q", cut, "abcde", records[0])
			}

			// torn tail is normal recovery, not failure — WAL-4's soul.
			if err := it.Err(); err != nil {
				t.Errorf("cut=%d: torn tail should not be an error, got %v", cut, err)
			}

			// recovery reports the end of the last good record.
			if got := it.LastValid(); got != (Offset{SegmentID: 1, Position: 13}) {
				t.Errorf("cut=%d: LastValid: want {1, 13}; got %v", cut, got)
			}

			if err := it.Close(); err != nil {
				t.Fatal(err)
			}
			_ = w2.Close()
		}
	})

	t.Run("first record of fresh segment", func(t *testing.T) {
		dir := t.TempDir()
		w, _ := Open(dir, Config{Mode: SyncNever, MaxSize: 40})

		// each record: 8 header + 10 payload = 18 bytes. Two fit in 40; third forces rotation.
		_, _ = w.Append([]byte("abcdefghij")) // seg 1, pos 0
		_, _ = w.Append([]byte("klmnopqrst")) // seg 1, pos 18
		_, _ = w.Append([]byte("uvwxyz7890")) // 18+18+18=54 > 40 → rotate → seg 2, pos 0

		if err := w.Close(); err != nil {
			t.Fatal(err)
		}

		// reopen BEFORE tearing the tail — see "mid segment" for why.
		w2, err := Open(dir, Config{Mode: SyncAlways})
		if err != nil {
			t.Fatal(err)
		}

		path := filepath.Join(dir, formatSegmentName(2))
		if err := os.Truncate(path, 3); err != nil {
			t.Fatal(err)
		}

		it, err := w2.Replay(Offset{1, 0})
		if err != nil {
			t.Fatal(err)
		}

		records, _, err := drainReplay(t, it)
		if err != nil {
			t.Fatal(err)
		}

		if len(records) != 2 {
			t.Fatalf("record count: want 2; got %d", len(records))
		}

		if string(records[0]) != "abcdefghij" {
			t.Errorf("record 0: want %q; got %q", "abcdefghij", records[0])
		}

		// torn tail is normal recovery, not failure — WAL-4's soul.
		if err := it.Err(); err != nil {
			t.Errorf("torn tail should not be an error, got %v", err)
		}

		// the boundary is the start of the bad record — {2, 0}, not the end of
		// the last good read in segment 1. The stale-boundary regression.
		if got := it.LastValid(); got != (Offset{SegmentID: 2, Position: 0}) {
			t.Errorf("LastValid: want {2, 0}; got %v", got)
		}

		if err := it.Close(); err != nil {
			t.Fatal(err)
		}

		_ = w2.Close()

	})

}

// Open must truncate a torn tail so the log is append-able again (WAL-4).
func TestOpenTruncatesTornTail(t *testing.T) {
	dir := t.TempDir()

	w, err := Open(dir, Config{Mode: SyncAlways})
	if err != nil {
		t.Fatal(err)
	}

	_, err = w.Append([]byte("abcde"))
	if err != nil {
		t.Fatal(err)
	}

	_, err = w.Append([]byte("fghij"))
	if err != nil {
		t.Fatal(err)
	}

	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	path := filepath.Join(dir, formatSegmentName(1))
	if err := os.Truncate(path, 22); err != nil {
		t.Fatal(err)
	}

	w2, err := Open(dir, Config{Mode: SyncAlways})
	if err != nil {
		t.Fatal(err)
	}

	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if info.Size() != 13 {
		t.Errorf("Open should truncate the torn tail: size = %d; want 13", info.Size())
	}

	off, err := w2.Append([]byte("delta"))
	if err != nil {
		t.Fatal(err)
	}
	if off != (Offset{SegmentID: 1, Position: 13}) {
		t.Errorf("append after recovery at %+v; want {1, 13}", off)
	}

	it, err := w2.Replay(Offset{SegmentID: 1, Position: 0})
	if err != nil {
		t.Fatal(err)
	}

	records, _, err := drainReplay(t, it)
	if err != nil {
		t.Fatal(err)
	}

	want := []string{"abcde", "delta"}
	if len(records) != len(want) {
		t.Fatalf("record count: want %d; got %d", len(want), len(records))
	}
	for i := range want {
		if string(records[i]) != want[i] {
			t.Errorf("record %d: want %q; got %q", i, want[i], records[i])
		}
	}

	if err := it.Close(); err != nil {
		t.Fatal(err)
	}
	_ = w2.Close()
}

func TestReplaySealedCorruption(t *testing.T) {
	dir := t.TempDir()
	w, _ := Open(dir, Config{Mode: SyncAlways, MaxSize: 40})

	// each record: 8 header + 10 payload = 18 bytes. Two fit in 40; third forces rotation.
	_, _ = w.Append([]byte("abcdefghij")) // seg 1, pos 0
	_, _ = w.Append([]byte("klmnopqrst")) // seg 1, pos 18
	_, _ = w.Append([]byte("uvwxyz7890")) // 18+18+18=54 > 40 → rotate → seg 2, pos 0

	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	path := filepath.Join(dir, formatSegmentName(1))
	f, err := os.OpenFile(path, os.O_RDWR, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	var b [1]byte
	if _, err := f.ReadAt(b[:], 10); err != nil {
		t.Fatal(err)
	}
	b[0] = ^b[0]
	if _, err := f.WriteAt(b[:], 10); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}

	w2, err := Open(dir, Config{Mode: SyncAlways})
	if err != nil {
		t.Fatal(err)
	}

	it, err := w2.Replay(Offset{1, 0})
	if err != nil {
		t.Fatal(err)
	}

	records, _, err := drainReplay(t, it)

	if !errors.Is(err, ErrCorrupt) {
		t.Fatalf("want ErrCorrupt; got %v", err)
	}

	if len(records) != 0 {
		t.Fatalf("record count: want 0; got %d", len(records))
	}

	if err := it.Close(); err != nil {
		t.Fatal(err)
	}

}

func TestIteratorCloseEarly(t *testing.T) {
	dir := t.TempDir()

	w, err := Open(dir, Config{Mode: SyncNever})
	if err != nil {
		t.Fatal(err)
	}

	_, err = w.Append([]byte("alpha"))
	if err != nil {
		t.Fatal(err)
	}

	_, err = w.Append([]byte("beta"))
	if err != nil {
		t.Fatal(err)
	}

	_, err = w.Append([]byte("gamma"))
	if err != nil {
		t.Fatal(err)
	}

	it, err := w.Replay(Offset{1, 0})
	if err != nil {
		t.Fatal(err)
	}

	if !it.Next() {
		t.Fatal("Next() = false on a log with records")
	}

	// first Close does the real work — the nil-before-close regression
	if err := it.Close(); err != nil {
		t.Fatal(err)
	}

	// second Close is the idempotency check
	if err := it.Close(); err != nil {
		t.Fatal(err)
	}

	if it.Next() {
		t.Errorf("it.Next() expected to be false; got true")
	}

}

func TestCrashRecoveryProperty(t *testing.T) {
	// PROPERTY: recovered records are a PREFIX of acknowledged records.
	// Note: active-segment corruption truncates at the first bad record, so
	// acknowledged records AFTER the corruption point in the active segment
	// are legitimately lost (documented limitation — active-segment mid-corruption
	// is treated as a torn tail). The prefix property permits this. It does NOT
	// permit gaps, reorders, altered records, or extra records.
	for iter := 0; iter < 200; iter++ {
		seed := time.Now().UnixNano() + int64(iter)
		t.Run(fmt.Sprintf("seed=%d", seed), func(t *testing.T) {
			runCrashScenario(t, seed)
		})
	}
}

// makeRecord builds a framed record (length + crc + payload) the same way
// Append does — a test helper so corrupt-file construction stays readable.
func makeRecord(t *testing.T, payload []byte) []byte {
	t.Helper()
	buf := make([]byte, 8+len(payload))
	binary.LittleEndian.PutUint32(buf[0:4], uint32(len(payload)))
	copy(buf[8:], payload)
	crc := crc32.Update(0, crc32.IEEETable, buf[0:4])
	crc = crc32.Update(crc, crc32.IEEETable, buf[8:])
	binary.LittleEndian.PutUint32(buf[4:8], crc)
	return buf
}

func drainReplay(t *testing.T, it *Iterator) ([][]byte, []Offset, error) {

	t.Helper()
	var records [][]byte
	var offsets []Offset
	for it.Next() {

		records = append(records, append([]byte(nil), it.Record()...))
		offsets = append(offsets, it.Offset())
	}

	if err := it.Err(); err != nil {
		return nil, nil, err
	}

	return records, offsets, nil
}

func randomBytes(t *testing.T, rng *rand.Rand) []byte {
	t.Helper()
	n := rng.Intn(100) + 1
	b := make([]byte, n)

	rng.Read(b)
	return b

}

func runCrashScenario(t *testing.T, seed int64) {
	t.Helper()
	rng := rand.New(rand.NewSource(seed))
	// 1. open a WAL, append a random number of random-sized records,
	//    remembering exactly which ones Append acknowledged (returned nil).
	dir := t.TempDir()
	w, err := Open(dir, Config{Mode: SyncAlways, MaxSize: 200})
	if err != nil {
		t.Fatal(err)
	}

	var acked [][]byte
	n := rng.Intn(50) + 1

	for range n {
		rec := randomBytes(t, rng)

		_, err := w.Append(rec)
		if err != nil {
			t.Fatal(err)
		}
		acked = append(acked, rec)

	}

	// 2. close.
	active := w.activeID
	_ = w.Close()

	path := filepath.Join(dir, formatSegmentName(active))
	f, err := os.OpenFile(path, os.O_RDWR, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	info, err := f.Stat()
	if err != nil {
		t.Fatal(err)
	}

	size := info.Size()

	if size == 0 {
		t.Skip("empty log — nothing to corrupt")
	}

	if rng.Intn(2) == 0 {
		cut := rng.Int63n(size)

		if err := f.Truncate(cut); err != nil {
			t.Fatal(err)
		}
	} else {
		off := rng.Int63n(size)

		var b [1]byte

		if _, err := f.ReadAt(b[:], off); err != nil {
			t.Fatal(err)
		}

		b[0] ^= 1 << uint(rng.Intn(8))
		if _, err := f.WriteAt(b[:], off); err != nil {
			t.Fatal(err)
		}
	}

	// 4. reopen (recovery runs) — or replay directly.
	w2, err := Open(dir, Config{Mode: SyncAlways, MaxSize: 200})
	if err != nil {
		t.Fatal(err)
	}

	it, err := w2.Replay(Offset{1, 0})
	if err != nil {
		t.Fatal(err)
	}

	records, _, err := drainReplay(t, it)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("seed=%d: %d segments, %d acked, %d recovered", seed, active, len(acked), len(records))

	// 5. PROPERTY: the records that come back must be a PREFIX of the
	//    acknowledged records. Not a superset, not reordered, not a
	//    record that differs. A proper prefix (some suffix may be lost).
	if len(records) > len(acked) {
		t.Fatalf("seed=%d: got %d records, but only %d were acknowledged",
			seed, len(records), len(acked))
	}

	for i := range records {
		if !bytes.Equal(records[i], acked[i]) {
			t.Fatalf("seed=%d: record %d mismatch: got %q, want %q",
				seed, i, records[i], acked[i])
		}
	}

	if err := it.Err(); err != nil {
		t.Fatal(err)
	}
	// 6. on failure, the seed is in the subtest name → reproducible.
}

func BenchmarkAppendSyncNever(b *testing.B)    { benchAppend(b, SyncNever) }
func BenchmarkAppendSyncInterval(b *testing.B) { benchAppend(b, SyncInterval) }
func BenchmarkAppendSyncAlways(b *testing.B)   { benchAppend(b, SyncAlways) }

func benchAppend(b *testing.B, mode SyncMode) {

	dir := b.TempDir()

	w, err := Open(dir, Config{Mode: mode, Interval: 10 * time.Millisecond})
	if err != nil {
		b.Fatal(err)
	}

	record := make([]byte, 100)

	b.ResetTimer()

	for range b.N {
		if _, err := w.Append(record); err != nil {
			b.Fatal(err)
		}
	}

	_ = w.Close()
}

func TestAppendLatencyPercentiles(t *testing.T) {

	modes := []struct {
		name string
		mode SyncMode
		n    int
	}{
		{"SyncNever", SyncNever, 100000},
		{"SyncInterval", SyncInterval, 100000},
		{"SyncAlways", SyncAlways, 2000},
	}

	record := make([]byte, 100)

	for _, m := range modes {
		dir := t.TempDir()

		w, err := Open(dir, Config{Mode: m.mode, Interval: 10 * time.Millisecond})
		if err != nil {
			t.Fatal(err)
		}

		latencies := make([]time.Duration, m.n)

		for i := 0; i < m.n; i++ {
			start := time.Now()

			if _, err := w.Append(record); err != nil {
				t.Fatal(err)
			}

			latencies[i] = time.Since(start)
		}

		_ = w.Close()

		slices.Sort(latencies)

		p50 := latencies[m.n*50/100]
		p99 := latencies[m.n*99/100]
		p999 := latencies[m.n*999/1000]

		t.Logf("%-13s p50=%-12v p99=%-12v p999=%-12v",
			m.name, p50, p99, p999)

	}
}
