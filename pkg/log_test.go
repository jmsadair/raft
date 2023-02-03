package raft

import (
	"sync"
	"testing"

	"github.com/jmsadair/raft/internal/errors"
	"github.com/stretchr/testify/assert"
)

const (
	errIndexDoesNotExist = "index %d does not exist"
)

// LogMock implements the Log interface.
// It is completely in-memory and should only be used for testing purposes.
type LogMock struct {
	entries    []*LogEntry
	firstIndex uint64
	lastIndex  uint64
	lastTerm   uint64
	mu         sync.RWMutex
}

func NewLogMock() *LogMock {
	return &LogMock{entries: make([]*LogEntry, 0)}
}

func (l *LogMock) GetEntry(index uint64) (*LogEntry, error) {
	l.mu.RLock()
	defer l.mu.RUnlock()

	if l.firstIndex == 0 || l.firstIndex > index || l.lastIndex < index {
		return nil, errors.WrapError(nil, errIndexDoesNotExist, index)
	}

	entry := l.entries[index-l.firstIndex]

	return entry, nil
}

func (l *LogMock) Contains(index uint64) bool {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.firstIndex <= index && index <= l.lastIndex
}

func (l *LogMock) AppendEntry(entry *LogEntry) error {
	return l.AppendEntries([]*LogEntry{entry})
}

func (l *LogMock) AppendEntries(entries []*LogEntry) error {
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

func (l *LogMock) Truncate(index uint64) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.firstIndex > index || l.lastIndex < index {
		return errors.WrapError(nil, errIndexDoesNotExist, index)
	}

	l.entries = l.entries[:index-l.firstIndex]
	if len(l.entries) != 0 {
		l.lastIndex = l.entries[len(l.entries)-1].Index()
		l.lastTerm = l.entries[len(l.entries)-1].Term()
	}

	return nil
}

func (l *LogMock) Compact(index, term uint64) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	newEntries := []*LogEntry{NewLogEntry(index, term, nil)}
	for i := len(l.entries) - 1; i >= 0; i-- {
		if l.entries[i].Index() == index && l.entries[i].Term() == term {
			newEntries = append(newEntries, l.entries[i+1:]...)
			break
		}
	}
	l.entries = newEntries
	l.firstIndex = l.entries[0].Index()
	l.lastIndex = l.entries[len(l.entries)-1].Index()
	l.lastTerm = l.entries[len(l.entries)-1].Term()
	return nil
}

func (l *LogMock) LastTerm() uint64 {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.lastTerm
}

func (l *LogMock) FirstIndex() uint64 {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.firstIndex
}

func (l *LogMock) LastIndex() uint64 {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.lastIndex
}

func validateLogEntry(t *testing.T, entry *LogEntry, expectedIndex uint64, expectedTerm uint64, expectedData []byte) {
	assert.Equal(t, expectedIndex, entry.Index(), "entry has incorrect index")
	assert.Equal(t, expectedTerm, entry.Term(), "entry has incorrect term")
	assert.Equal(t, expectedData, entry.Data(), "entry has incorrect data")
}

func TestNewLog(t *testing.T) {
	log := NewLogMock()
	assert.Zero(t, log.FirstIndex())
	assert.Zero(t, log.LastIndex())
	assert.Zero(t, log.LastTerm())
}

func TestAppendEntries(t *testing.T) {
	log := NewLogMock()

	var entry1, entry2 *LogEntry

	var entry1Index uint64 = 1
	var entry1Term uint64 = 1
	entry1Data := []byte("entry1")
	entry1 = NewLogEntry(entry1Index, entry1Term, entry1Data)

	var entry2Index uint64 = 2
	var entry2Term uint64 = 2
	entry2Data := []byte("entry2")
	entry2 = NewLogEntry(entry2Index, entry2Term, entry2Data)

	log.AppendEntries([]*LogEntry{entry1, entry2})

	entry1, _ = log.GetEntry(entry1Index)
	validateLogEntry(t, entry1, entry1Index, entry1Term, entry1Data)

	entry2, _ = log.GetEntry(entry2Index)
	validateLogEntry(t, entry2, entry2Index, entry2Term, entry2Data)

	assert.Equal(t, log.FirstIndex(), entry1Index)
	assert.Equal(t, log.LastTerm(), entry2Term)
	assert.Equal(t, log.LastIndex(), entry2Index)
}

func TestTruncate(t *testing.T) {
	log := NewLogMock()

	var entry1, entry2, entry3 *LogEntry

	var entry1Index uint64 = 1
	var entry1Term uint64 = 1
	entry1Data := []byte("entry1")
	entry1 = NewLogEntry(entry1Index, entry1Term, entry1Data)

	var entry2Index uint64 = 2
	var entry2Term uint64 = 2
	entry2Data := []byte("entry2")
	entry2 = NewLogEntry(entry2Index, entry2Term, entry2Data)

	var entry3Index uint64 = 3
	var entry3Term uint64 = 3
	entry3Data := []byte("entry3")
	entry3 = NewLogEntry(entry3Index, entry3Term, entry3Data)

	log.AppendEntries([]*LogEntry{entry1, entry2, entry3})

	log.Truncate(entry2Index)

	entry1, _ = log.GetEntry(entry1Index)
	validateLogEntry(t, entry1, entry1Index, entry1Term, entry1Data)

	assert.Equal(t, log.LastTerm(), entry1Term)
	assert.Equal(t, log.LastIndex(), entry1Index)
}

func TestCompact(t *testing.T) {
	log := NewLogMock()

	var entry1, entry2, entry3 *LogEntry

	var entry1Index uint64 = 3
	var entry1Term uint64 = 3
	entry1Data := []byte("entry1")
	entry1 = NewLogEntry(entry1Index, entry1Term, entry1Data)

	var entry2Index uint64 = 4
	var entry2Term uint64 = 4
	entry2Data := []byte("entry2")
	entry2 = NewLogEntry(entry2Index, entry2Term, entry2Data)

	var entry3Index uint64 = 5
	var entry3Term uint64 = 5
	entry3Data := []byte("entry3")
	entry3 = NewLogEntry(entry3Index, entry3Term, entry3Data)

	log.AppendEntries([]*LogEntry{entry1, entry2, entry3})

	log.Compact(entry2Index, entry2Term)

	entry3, _ = log.GetEntry(entry3Index)
	validateLogEntry(t, entry3, entry3Index, entry3Term, entry3Data)

	assert.Equal(t, log.LastTerm(), entry3Term)
	assert.Equal(t, log.LastIndex(), entry3Index)
}
