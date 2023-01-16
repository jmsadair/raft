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

type Log struct {
	path        string
	file        *os.File
	entries     []*LogEntry
	commitIndex uint64
	firstIndex  uint64
	lastIndex   uint64
	lastTerm    uint64
	mu          sync.RWMutex
}

func NewLog(path string) *Log {
	return &Log{path: filepath.Join(path, "log"), entries: make([]*LogEntry, 0)}
}

// Open opens the log for retrieving and storing log entries. Require that the log is closed.
func (l *Log) Open() error {
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

		l.entries = append(l.entries, entry)
	}

	if len(l.entries) != 0 {
		l.firstIndex = l.entries[0].Index()
		l.lastIndex = l.entries[len(l.entries)-1].Index()
		l.lastTerm = l.entries[len(l.entries)-1].Term()
	}

	return nil
}

// Close closes the log. Require that the log is open.
func (l *Log) Close() error {
	if l.file == nil {
		return errors.WrapError(nil, errLogClosed, l.path)
	}
	l.file.Close()
	l.file = nil
	l.entries = make([]*LogEntry, 0)
	l.firstIndex = 0
	l.lastIndex = 0
	l.lastTerm = 0
	return nil
}

// IsOpen returns true if the log is open and false otherwise.
func (l *Log) IsOpen() bool {
	l.mu.RLock()
	defer l.mu.RUnlock()

	return !(l.file == nil)
}

// GetEntry retrieves the log entry with the provided index from
// the log. Require that a log entry with the provided index exists
// in the log and that the log is open.
func (l *Log) GetEntry(index uint64) (*LogEntry, error) {
	l.mu.RLock()
	defer l.mu.RUnlock()

	if l.file == nil {
		return nil, errors.WrapError(nil, errLogClosed, l.path)
	}
	if l.firstIndex > index || l.lastIndex < index {
		return nil, errors.WrapError(nil, errInvalidIndex, index)
	}

	entry := l.entries[index-l.firstIndex]

	return entry, nil
}

// Contains returns true if the log contains the provided index
// and false otherwise.
func (l *Log) Contains(index uint64) bool {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.firstIndex <= index && index <= l.lastIndex
}

// AppendEntries appends the provided log entries to the log;
// returns the index of the last entry appended to the log or 0
// if no entries were appended. Require that the log is is open.
func (l *Log) AppendEntries(entries ...*LogEntry) (uint64, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.file == nil {
		return 0, errors.WrapError(nil, errLogClosed, l.path)
	}

	for _, entry := range entries {
		var err error
		if entry.offset, err = l.file.Seek(0, os.SEEK_CUR); err != nil {
			return 0, errors.WrapError(err, err.Error())
		}
		if _, err = entry.Encode(l.file); err != nil {
			return 0, errors.WrapError(err, err.Error())
		}
	}

	if err := l.file.Sync(); err != nil {
		return 0, errors.WrapError(err, err.Error())
	}

	l.entries = append(l.entries, entries...)

	if len(l.entries) != 0 {
		l.firstIndex = l.entries[0].Index()
		l.lastIndex = l.entries[len(l.entries)-1].Index()
		l.lastTerm = l.entries[len(l.entries)-1].Term()
	}

	return l.lastIndex, nil
}

// Truncate truncates the log starting from the provided index.
// Require that a log entry with the provided index exists in the log
// and that the log is open.
func (l *Log) Truncate(index uint64) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.file == nil {
		return errors.WrapError(nil, errLogClosed, l.path)
	}

	if l.firstIndex > index || l.lastIndex < index {
		return errors.WrapError(nil, errInvalidIndex, index)
	}

	entry := l.entries[index-l.firstIndex]

	if err := l.file.Truncate(entry.offset); err != nil {
		return errors.WrapError(err, err.Error())
	}
	if err := l.file.Sync(); err != nil {
		return errors.WrapError(err, err.Error())
	}

	l.entries = l.entries[:entry.Index()-l.firstIndex]

	if len(l.entries) != 0 {
		l.lastIndex = l.entries[len(l.entries)-1].Index()
		l.lastTerm = l.entries[len(l.entries)-1].Term()
	}

	return nil
}

// LastTerm returns the last term written to the log. If the log is
// empty, returns 0.
func (l *Log) LastTerm() uint64 {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.lastTerm
}

// FirstIndex returns the earliest index written to the log. If the log
// is empty, returns 0.
func (l *Log) FirstIndex() uint64 {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.firstIndex
}

// LastIndex returns the last index written to the log. If the log is empty,
// returns 0.
func (l *Log) LastIndex() uint64 {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.lastIndex
}

// Path returns the path to the file associated with the log.
func (l *Log) Path() string {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.path
}

// File returns the file that is used to persist the log. If the log is
// not open, returns nil.
func (l *Log) File() *os.File {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.file
}

// Size returns the number of log entries in the log.
func (l *Log) Size() int {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return len(l.entries)
}

// SetCommitIndex updates the commit index associated with the log
// to the provided index.
func (l *Log) SetCommitIndex(index uint64) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.commitIndex = index
}

// CommitIndex returns the commit index associated with the log.
func (l *Log) CommitIndex() uint64 {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.commitIndex
}
