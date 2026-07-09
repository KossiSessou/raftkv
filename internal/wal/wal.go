package wal

import (
	"encoding/binary"
	"errors"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"
)

type SyncMode int

const (
	defaultMaxSegmentSize = 64 << 20
	maxRecordSize         = 1 << 30
	defaultSyncInterval   = 100 * time.Millisecond
)

const (
	SyncNever SyncMode = iota
	SyncAlways
	SyncInterval
)

var (
	ErrClosed         = errors.New("wal: file closed")
	ErrRecordTooLarge = errors.New("wal: record too large")
	ErrCRCMismatch    = errors.New("wal: CRC mismatch")
	ErrCorrupt        = errors.New("wal: torn file")
)

type Offset struct {
	SegmentID uint64
	Position  uint64
}

type readResult struct {
	record []byte
	size   uint64
}

type WAL struct {
	dir       string
	fd        *os.File
	mu        sync.Mutex
	mode      SyncMode
	maxSize   uint64
	activeID  uint64
	position  uint64
	closed    bool
	done      chan struct{}
	stopped   chan struct{}
	closeOnce sync.Once
}

type Config struct {
	Mode     SyncMode
	Interval time.Duration
	MaxSize  uint64
}

type Iterator struct {
	dir       string
	segID     uint64
	fd        *os.File
	pos       uint64
	activeID  uint64
	record    []byte
	off       Offset
	err       error
	lastValid Offset
	done      bool
}

func Open(dir string, cfg Config) (*WAL, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}

	ids, err := listSegments(dir)
	if err != nil {
		return nil, err
	}

	maxSize := cfg.MaxSize

	if maxSize == 0 {
		maxSize = defaultMaxSegmentSize
	}

	if cfg.Mode == SyncInterval && cfg.Interval <= 0 {
		cfg.Interval = defaultSyncInterval
	}
	w := &WAL{
		dir:     dir,
		mode:    cfg.Mode,
		maxSize: maxSize,
		done:    make(chan struct{}),
	}
	if len(ids) == 0 {
		w.activeID = 1
	} else {
		w.activeID = ids[len(ids)-1]
	}

	path := filepath.Join(dir, formatSegmentName(w.activeID))

	fi, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return nil, err
	}

	it, err := w.Replay(Offset{SegmentID: w.activeID, Position: 0})

	if err != nil {
		return nil, err
	}

	for it.Next() {

	}

	if err := it.Err(); err != nil {
		return nil, err
	}

	boundary := it.LastValid().Position

	w.fd = fi
	w.position = boundary

	if err := w.fd.Truncate(int64(boundary)); err != nil {
		return nil, err
	}

	if cfg.Mode == SyncInterval {

		ticker := time.NewTicker(cfg.Interval)
		w.stopped = make(chan struct{})
		go w.syncLoop(ticker)
	}

	return w, nil
}

func (w *WAL) closeLocked() error {
	if w.closed {
		return nil
	}
	w.closed = true

	var firstErr error

	if err := w.fd.Sync(); err != nil {
		firstErr = err
	}

	close(w.done)

	// If there IS a goroutine, wait for it to exit. We must drop the lock
	// during the wait, because the goroutine is trying to acquire this
	// same lock inside its w.Sync() call. Holding it would deadlock.
	// Safety: w.closed is already true, so any Append that grabs the lock
	// in the gap will short-circuit with ErrClosed and do nothing.
	if w.mode == SyncInterval {
		w.mu.Unlock()
		<-w.stopped
		w.mu.Lock()
	}

	if err := w.fd.Close(); err != nil && firstErr == nil {
		firstErr = err
	}

	return firstErr
}

func (w *WAL) Close() error {
	var err error
	w.closeOnce.Do(func() {
		w.mu.Lock()
		err = w.closeLocked()
		w.mu.Unlock()
	})

	return err
}

func (w *WAL) Sync() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return ErrClosed
	}

	return w.fd.Sync()
}

func (w *WAL) syncLoop(ticker *time.Ticker) {
	defer close(w.stopped)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			_ = w.Sync()
		case <-w.done:
			return
		}
	}
}

func (w *WAL) rotate() (err error) {
	if err = w.fd.Sync(); err != nil {
		_ = w.closeLocked()
		return err
	}

	fi, err := os.OpenFile(filepath.Join(w.dir, formatSegmentName(w.activeID+1)), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		_ = w.closeLocked()
		return err
	}

	committed := false

	defer func() {
		if !committed {
			_ = fi.Close()
		}
	}()

	if err = w.fd.Close(); err != nil {
		_ = w.closeLocked()
		return err
	}

	w.fd = fi
	w.activeID++
	w.position = 0
	committed = true
	return nil
}

func (w *WAL) Append(record []byte) (offset Offset, err error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.closed {
		return Offset{}, ErrClosed
	}

	recordLen := len(record)

	if 8+recordLen > maxRecordSize {
		return Offset{}, ErrRecordTooLarge
	}

	buf := make([]byte, 8+recordLen)
	binary.LittleEndian.PutUint32(buf[0:4], uint32(recordLen))

	copy(buf[8:], record)

	crc := crc32.Update(0, crc32.IEEETable, buf[0:4])
	crc = crc32.Update(crc, crc32.IEEETable, buf[8:])
	binary.LittleEndian.PutUint32(buf[4:8], crc)

	if uint64(len(buf))+w.position > w.maxSize {

		err = w.rotate()
		if err != nil {
			return Offset{}, err
		}

	}

	var written int

	for written < len(buf) {
		n, err := w.fd.Write(buf[written:])
		if err != nil {
			_ = w.closeLocked()
			return Offset{}, err
		}
		written += n

	}
	pos := w.position
	w.position += uint64(written)
	offset = Offset{SegmentID: w.activeID, Position: uint64(pos)}

	if w.mode == SyncAlways {
		if err := w.fd.Sync(); err != nil {
			_ = w.closeLocked()
			return Offset{}, err
		}
	}

	return offset, nil
}

func (w *WAL) Replay(from Offset) (*Iterator, error) {
	w.mu.Lock()
	if w.closed {
		w.mu.Unlock()
		return nil, ErrClosed

	}
	activeID := w.activeID
	w.mu.Unlock()

	fi, err := os.OpenFile(filepath.Join(w.dir, formatSegmentName(from.SegmentID)), os.O_RDONLY, 0o644)
	if err != nil {
		return nil, err
	}

	return &Iterator{
		dir:      w.dir,
		activeID: activeID,
		segID:    from.SegmentID,
		fd:       fi,
		pos:      from.Position,
	}, nil

}

func readRecordAt(f *os.File, pos uint64) (readResult, error) {
	header := make([]byte, 8)

	n, err := f.ReadAt(header, int64(pos))

	if n == 0 && err == io.EOF {
		return readResult{}, io.EOF
	}
	if err != nil && err != io.EOF {
		return readResult{}, err
	}

	if n < len(header) {
		return readResult{}, ErrCorrupt
	}

	length := binary.LittleEndian.Uint32(header[0:4])

	if length == 0 {
		return readResult{}, io.EOF
	}

	storedCrc := binary.LittleEndian.Uint32(header[4:8])
	payload := make([]byte, length)

	n, err = f.ReadAt(payload, int64(pos+8))

	if err != nil && err != io.EOF {
		return readResult{}, err
	}

	if n < len(payload) {
		return readResult{}, ErrCorrupt
	}

	crc := crc32.Update(0, crc32.IEEETable, header[0:4])
	crc = crc32.Update(crc, crc32.IEEETable, payload)

	if crc != storedCrc {
		return readResult{}, ErrCRCMismatch
	}

	return readResult{record: payload, size: uint64(8 + length)}, nil
}

func (it *Iterator) Next() bool {
	//   1. if done → return false
	if it.done {
		return false
	}

	for {
		//   2. res, err := readRecordAt(fd, pos)
		res, err := readRecordAt(it.fd, it.pos)

		//   3. switch on err:
		switch err {
		//nil (good record):
		case nil:

			//  - store res.record and the offset {segID, pos} for accessors
			it.record = res.record
			it.off = Offset{SegmentID: it.segID, Position: it.pos}

			//         - advance pos += res.size
			it.pos += res.size
			//         - return true

			return true

		//
		//io.EOF (clean end of this segment):
		case io.EOF:

			it.lastValid = Offset{SegmentID: it.segID, Position: it.pos}
			//  - close current fd
			_ = it.fd.Close()
			it.fd = nil
			//         - try to open segment segID+1 for reading
			fi, err := os.OpenFile(filepath.Join(it.dir, formatSegmentName(it.segID+1)), os.O_RDONLY, 0644)

			//           - doesn't exist (os.IsNotExist) → done = true, return false (no error, clean end of log)

			if os.IsNotExist(err) {
				it.done = true

				return false
			}
			//           - other open error → set err, done, return false
			if err != nil {
				it.err = err
				it.done = true
				return false
			}
			//	- opened successfully → set fd, segID++, pos = 0, LOOP back to step 2
			it.fd = fi
			it.segID++
			it.pos = 0
			continue

		// damage (ErrCorrupt / ErrCRCMismatch):
		case ErrCRCMismatch, ErrCorrupt:
			it.done = true
			//		- is this the active segment? (segID == activeID)
			if it.segID != it.activeID {
				//		- NO  → sealed-segment corruption. done = true.
				//                   err = ErrCorrupt (hard error, recovery must refuse).
				//                   return false.
				it.err = ErrCorrupt
				_ = it.fd.Close()
				it.fd = nil
				return false

			}
			//		- YES → torn tail. done = true. Record the truncation boundary
			//                   (= current pos, the start of the bad record = end of last good).
			//                   err stays nil (this is normal recovery, not failure).
			//                   return false.

			//
			it.lastValid = Offset{SegmentID: it.segID, Position: it.pos}
			_ = it.fd.Close()
			it.fd = nil
			return false
			//

		default:

			//      other (real I/O error):
			//         - set err, done, return false
			it.err = err
			it.done = true
			_ = it.fd.Close()
			it.fd = nil
			return false

		}

	}

}

func (it *Iterator) Record() []byte    { return it.record }
func (it *Iterator) Offset() Offset    { return it.off }
func (it *Iterator) Err() error        { return it.err }
func (it *Iterator) LastValid() Offset { return it.lastValid }
func (it *Iterator) Close() error {

	if it.fd == nil {
		return nil
	}

	err := it.fd.Close()
	it.fd = nil
	return err
}
