package wal

import (
	"encoding/binary"
	"errors"
	"hash/crc32"
	"os"
	"sync"
	"time"
)

type SyncMode int

const (
	SyncNever SyncMode = iota
	SyncAlways
	SyncInterval
)

var ErrClosed = errors.New("wal closed")

type WAL struct {
	fd        *os.File
	mu        sync.Mutex
	mode      SyncMode
	interval  time.Duration
	offset    uint64
	closed    bool
	done      chan struct{}
	stopped   chan struct{}
	closeOnce sync.Once
}

type Config struct {
	Mode     SyncMode
	Interval time.Duration
}

func Open(path string, cfg Config) (*WAL, error) {

	fi, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return nil, err
	}

	info, err := fi.Stat()
	if err != nil {
		return nil, err
	}

	w := &WAL{
		fd:       fi,
		mode:     cfg.Mode,
		offset:   uint64(info.Size()),
		interval: cfg.Interval,
		done:     make(chan struct{}),
	}

	if cfg.Mode == SyncInterval {
		ticker := time.NewTicker(w.interval)
		w.stopped = make(chan struct{})

		go w.syncLoop(ticker)

	}

	return w, nil
}

func (w *WAL) Close() error {
	var err error
	w.closeOnce.Do(func() {
		w.mu.Lock()
		w.closed = true
		_ = w.fd.Sync()
		w.mu.Unlock()

		close(w.done)
		if w.mode == SyncInterval {
			<-w.stopped
		}
		err = w.fd.Close()
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

func (w *WAL) Append(record []byte) (offset uint64, err error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.closed {
		return 0, ErrClosed
	}

	recordLen := len(record)
	buf := make([]byte, 8+recordLen)
	binary.LittleEndian.PutUint32(buf[0:4], uint32(recordLen))

	copy(buf[8:], record)

	crc := crc32.Update(0, crc32.IEEETable, buf[0:4])
	crc = crc32.Update(crc, crc32.IEEETable, buf[8:])
	binary.LittleEndian.PutUint32(buf[4:8], crc)

	var written int

	for written < len(buf) {
		n, err := w.fd.Write(buf[written:])
		if err != nil {
			return 0, err
		}
		written += n
	}
	pos := w.offset
	w.offset += uint64(written)

	if w.mode == SyncAlways {
		if err := w.fd.Sync(); err != nil {
			return pos, err
		}
	}

	return pos, nil

}
