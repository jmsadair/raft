package raft

import (
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
	entries []*LogEntry
}

func NewLogMock(initialIndex uint64, initialTerm uint64) *LogMock {
	return &LogMock{entries: []*LogEntry{{index: initialIndex, term: initialTerm}}}
}

func (l *LogMock) GetEntry(index uint64) (*LogEntry, error) {
	logIndex := index - l.entries[0].index
	lastIndex := l.entries[len(l.entries)-1].index
	if logIndex <= 0 || logIndex > lastIndex {
		return nil, errors.WrapError(nil, errIndexDoesNotExist, index)
	}
	entry := l.entries[logIndex]
	return entry, nil
}

func (l *LogMock) Contains(index uint64) bool {
	logIndex := index - l.entries[0].index
	lastIndex := l.entries[len(l.entries)-1].index
	return !(logIndex <= 0 || logIndex > lastIndex)
}

func (l *LogMock) AppendEntry(entry *LogEntry) {
	l.AppendEntries([]*LogEntry{entry})
}

func (l *LogMock) AppendEntries(entries []*LogEntry) {
	l.entries = append(l.entries, entries...)
}

func (l *LogMock) Truncate(index uint64) error {
	logIndex := index - l.entries[0].index
	lastIndex := l.entries[len(l.entries)-1].index
	if logIndex <= 0 || logIndex > lastIndex {
		return errors.WrapError(nil, errIndexDoesNotExist, index)
	}
	l.entries = l.entries[:logIndex]
	return nil
}

func (l *LogMock) Compact(index uint64) error {
	logIndex := index - l.entries[0].index
	lastIndex := l.entries[len(l.entries)-1].index
	if logIndex <= 0 || logIndex > lastIndex {
		return errors.WrapError(nil, errIndexDoesNotExist, index)
	}
	newEntries := make([]*LogEntry, len(l.entries)-int(logIndex))
	copy(newEntries[:], l.entries[logIndex:])
	l.entries = newEntries
	l.entries[0].data = nil
	return nil
}

func (l *LogMock) LastTerm() uint64 {
	return l.entries[len(l.entries)-1].term
}

func (l *LogMock) LastIndex() uint64 {
	return l.entries[len(l.entries)-1].index
}

func (l *LogMock) NextIndex() uint64 {
	return l.entries[len(l.entries)-1].index + 1
}

func validateLogEntry(t *testing.T, entry *LogEntry, expectedIndex uint64, expectedTerm uint64, expectedData []byte) {
	assert.Equal(t, expectedIndex, entry.Index(), "entry has incorrect index")
	assert.Equal(t, expectedTerm, entry.Term(), "entry has incorrect term")
	assert.Equal(t, expectedData, entry.Data(), "entry has incorrect data")
}

func TestNewLog(t *testing.T) {
	log := NewLogMock(0, 0)
	assert.Zero(t, log.LastIndex())
	assert.Zero(t, log.LastTerm())
}

func TestAppendEntries(t *testing.T) {
	log := NewLogMock(0, 0)

	var entry1, entry2 *LogEntry
	var err error

	var entry1Index uint64 = 1
	var entry1Term uint64 = 1
	entry1Data := []byte("entry1")
	entry1 = NewLogEntry(entry1Index, entry1Term, entry1Data)

	var entry2Index uint64 = 2
	var entry2Term uint64 = 1
	entry2Data := []byte("entry2")
	entry2 = NewLogEntry(entry2Index, entry2Term, entry2Data)

	log.AppendEntries([]*LogEntry{entry1, entry2})

	entry1, err = log.GetEntry(entry1Index)
	if err != nil {
		t.Fatalf("error getting entry from log: %s", err.Error())
	}
	validateLogEntry(t, entry1, entry1Index, entry1Term, entry1Data)

	entry2, err = log.GetEntry(entry2Index)
	if err != nil {
		t.Fatalf("error getting entry from log: %s", err.Error())
	}
	validateLogEntry(t, entry2, entry2Index, entry2Term, entry2Data)

	assert.Equal(t, log.LastTerm(), entry2Term)
	assert.Equal(t, log.LastIndex(), entry2Index)
}

func TestTruncate(t *testing.T) {
	log := NewLogMock(0, 0)

	var err error
	var entry1, entry2, entry3 *LogEntry

	var entry1Index uint64 = 1
	var entry1Term uint64 = 1
	entry1Data := []byte("entry1")
	entry1 = NewLogEntry(entry1Index, entry1Term, entry1Data)

	var entry2Index uint64 = 2
	var entry2Term uint64 = 1
	entry2Data := []byte("entry2")
	entry2 = NewLogEntry(entry2Index, entry2Term, entry2Data)

	var entry3Index uint64 = 3
	var entry3Term uint64 = 2
	entry3Data := []byte("entry3")
	entry3 = NewLogEntry(entry3Index, entry3Term, entry3Data)

	log.AppendEntries([]*LogEntry{entry1, entry2, entry3})

	if err := log.Truncate(entry2Index); err != nil {
		t.Fatalf("error truncating log: %s", err.Error())
	}

	entry1, err = log.GetEntry(entry1Index)
	if err != nil {
		t.Fatalf("error getting entry from log: %s", err.Error())
	}
	validateLogEntry(t, entry1, entry1Index, entry1Term, entry1Data)

	assert.Equal(t, log.LastTerm(), entry1Term)
	assert.Equal(t, log.LastIndex(), entry1Index)
}

func TestCompact(t *testing.T) {
	log := NewLogMock(0, 0)

	var err error
	var entry1, entry2, entry3, entry4 *LogEntry

	var entry1Index uint64 = 1
	var entry1Term uint64 = 1
	entry1Data := []byte("entry1")
	entry1 = NewLogEntry(entry1Index, entry1Term, entry1Data)

	var entry2Index uint64 = 2
	var entry2Term uint64 = 2
	entry2Data := []byte("entry2")
	entry2 = NewLogEntry(entry2Index, entry2Term, entry2Data)

	var entry3Index uint64 = 3
	var entry3Term uint64 = 2
	entry3Data := []byte("entry3")
	entry3 = NewLogEntry(entry3Index, entry3Term, entry3Data)

	log.AppendEntries([]*LogEntry{entry1, entry2, entry3})

	if err := log.Compact(entry2Index); err != nil {
		t.Fatalf("error compacting log: %s", err.Error())
	}

	entry3, err = log.GetEntry(entry3Index)
	if err != nil {
		t.Fatalf("error getting entry from log: %s", err.Error())
	}
	validateLogEntry(t, entry3, entry3Index, entry3Term, entry3Data)

	assert.Equal(t, log.LastTerm(), entry3Term)
	assert.Equal(t, log.LastIndex(), entry3Index)

	var entry4Index uint64 = 4
	var entry4Term uint64 = 2
	entry4Data := []byte("entry4")
	entry4 = NewLogEntry(entry4Index, entry4Term, entry4Data)

	log.AppendEntry(entry4)

	entry4, err = log.GetEntry(entry4Index)
	if err != nil {
		t.Fatalf("error getting entry from log: %s", err.Error())
	}
	validateLogEntry(t, entry4, entry4Index, entry4Term, entry4Data)
}
