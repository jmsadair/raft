package raft

import (
	"io"
	"os"
	"path/filepath"
	"sync"

	"github.com/jmsadair/raft/internal/errors"
)

// Error strings.
const (
	errInvalidIndex = "index %d does not exist"
	errLogOpen      = "persistent log %s is open"
	errLogClosed    = "persistent log %s is closed"
)

// PersistentLog implements the Log interface.
type PersistentLog struct {
	path string
	file *os.File
	vlog *VolatileLog
	mu   sync.Mutex
}

func NewPersistentLog(path string) *PersistentLog {
	return &PersistentLog{path: filepath.Join(path, "log"), vlog: NewVolatileLog()}
}

func (l *PersistentLog) Open() error {
	if l.file != nil {
		return errors.WrapError(nil, errLogOpen, l.path)
	}

	file, err := os.OpenFile(l.path, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0666)
	if err != nil {
		return errors.WrapError(err, err.Error())
	}
	l.file = file

	for {
		var err error
		entry := NewLogEntry(0, 0, nil)

		if entry.offset, err = l.file.Seek(0, os.SEEK_CUR); err != nil {
			return err
		}

		if _, err = entry.Decode(file); err != nil {
			if err == io.EOF {
				break
			}
			return errors.WrapError(err, err.Error())
		}

		l.vlog.AppendEntries(entry)
	}

	return nil
}

func (l *PersistentLog) Close() error {
	if l.file == nil {
		return errors.WrapError(nil, errLogClosed, l.path)
	}
	l.file.Close()
	l.file = nil
	l.vlog.Clear()
	return nil
}

func (l *PersistentLog) IsOpen() bool {
	l.mu.Lock()
	defer l.mu.Unlock()

	return !(l.file == nil)
}

func (l *PersistentLog) GetEntry(index uint64) (*LogEntry, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.file == nil {
		return nil, errors.WrapError(nil, errLogClosed, l.path)
	}
	if !l.vlog.Contains(index) {
		return nil, errors.WrapError(nil, errInvalidIndex, index)
	}

	entry := l.vlog.GetEntry(index)

	return entry, nil
}

func (l *PersistentLog) Contains(index uint64) bool {
	l.mu.Lock()
	defer l.mu.Unlock()

	return !l.vlog.Contains(index)
}

func (l *PersistentLog) AppendEntries(entries ...*LogEntry) (uint64, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.file == nil {
		return 0, errors.WrapError(nil, errLogClosed, l.path)
	}

	var toAppend []*LogEntry

	for i, entry := range entries {
		if l.vlog.LastIndex() < entry.Index() {
			toAppend = entries[i:]
			break
		}

		existing := l.vlog.GetEntry(entry.Index())

		if existing != nil && existing.IsConflict(entry) {
			l.truncate(entry.Index())
			toAppend = entries[i:]
			break
		}
	}

	l.persistEntries(toAppend...)
	l.vlog.AppendEntries(toAppend...)

	if len(toAppend) != 0 {
		return toAppend[len(toAppend)-1].Index(), nil
	}

	return 0, nil
}

func (l *PersistentLog) Truncate(index uint64) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.truncate(index)
}

func (l *PersistentLog) LastTerm() uint64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.vlog.LastTerm()
}

func (l *PersistentLog) FirstIndex() uint64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.vlog.FirstIndex()
}

func (l *PersistentLog) LastIndex() uint64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.vlog.LastIndex()
}

func (l *PersistentLog) Path() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.path
}

func (l *PersistentLog) File() *os.File {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.file
}

func (l *PersistentLog) Size() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.vlog.Size()
}

func (l *PersistentLog) persistEntries(entries ...*LogEntry) error {
	// Expects log mutex to be locked - not concurrent safe.
	if l.file == nil {
		return errors.WrapError(nil, errLogClosed, l.path)
	}

	for _, entry := range entries {
		var err error
		if entry.offset, err = l.file.Seek(0, os.SEEK_CUR); err != nil {
			return errors.WrapError(err, err.Error())
		}
		if _, err = entry.Encode(l.file); err != nil {
			return errors.WrapError(err, err.Error())
		}
	}

	return nil
}

func (l *PersistentLog) truncate(index uint64) error {
	// Expects log mutex to be locked - not concurrent safe.
	if l.file == nil {
		return errors.WrapError(nil, errLogClosed, l.path)
	}

	if !l.vlog.Contains(index) {
		return errors.WrapError(nil, errInvalidIndex, index)
	}

	entry := l.vlog.GetEntry(index)

	if err := l.file.Truncate(entry.offset); err != nil {
		return errors.WrapError(err, err.Error())
	}

	l.vlog.Truncate(index)

	return nil
}
