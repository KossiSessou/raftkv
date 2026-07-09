package wal

import (
	"bytes"
	"encoding/binary"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
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
		t.Errorf("record count: want %d; got %d", len(wantRecord), len(records))
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
	if it.lastValid != want {
		t.Errorf("Last valid offset mismatch: want %+v, got %+v", want, it.lastValid)
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
		t.Errorf("Last valid offset mismatch: want %v, got %v", want, it.lastValid)
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
