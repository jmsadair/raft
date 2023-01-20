package raft

import (
	"sync"

	"github.com/jmsadair/raft/internal/errors"
)

const (
	errIndexDoesNotExist = "index %d does not exist"
)

// VolatileLog implements the Log interface.
// It is completely in-memory and should only be used for testing purposes.
type VolatileLog struct {
	entries    []*LogEntry
	firstIndex uint64
	lastIndex  uint64
	lastTerm   uint64
	mu         sync.RWMutex
}

func NewVolatileLog() *VolatileLog {
	return &VolatileLog{entries: make([]*LogEntry, 0)}
}

func (l *VolatileLog) GetEntry(index uint64) (*LogEntry, error) {
	l.mu.RLock()
	defer l.mu.RUnlock()

	if l.firstIndex == 0 || l.firstIndex > index || l.lastIndex < index {
		return nil, errors.WrapError(nil, errIndexDoesNotExist, index)
	}

	entry := l.entries[index-l.firstIndex]

	return entry, nil
}

func (l *VolatileLog) Contains(index uint64) bool {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.firstIndex <= index && index <= l.lastIndex
}

func (l *VolatileLog) AppendEntry(entry *LogEntry) error {
	return l.AppendEntries([]*LogEntry{entry})
}

func (l *VolatileLog) AppendEntries(entries []*LogEntry) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	l.entries = append(l.entries, entries...)

	if len(l.entries) != 0 {
		l.firstIndex = l.entries[0].Index()
		l.lastIndex = l.entries[len(l.entries)-1].Index()
		l.lastTerm = l.entries[len(l.entries)-1].Term()
	}

	return nil
}

func (l *VolatileLog) Truncate(index uint64) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.firstIndex > index || l.lastIndex < index {
		return errors.WrapError(nil, errIndexDoesNotExist, index)
	}

	entry := l.entries[index-l.firstIndex]

	l.entries = l.entries[:entry.Index()-l.firstIndex]

	if len(l.entries) != 0 {
		l.lastIndex = l.entries[len(l.entries)-1].Index()
		l.lastTerm = l.entries[len(l.entries)-1].Term()
	}

	return nil
}

func (l *VolatileLog) LastTerm() uint64 {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.lastTerm
}

func (l *VolatileLog) FirstIndex() uint64 {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.firstIndex
}

func (l *VolatileLog) LastIndex() uint64 {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.lastIndex
}
