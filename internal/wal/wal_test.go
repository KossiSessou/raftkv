package wal

import (
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
