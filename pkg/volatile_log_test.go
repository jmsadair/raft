package raft

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func validateLogEntry(t *testing.T, entry *LogEntry, expectedIndex uint64, expectedTerm uint64, expectedData []byte) {
	assert.Equal(t, expectedIndex, entry.Index(), "entry has incorrect index")
	assert.Equal(t, expectedTerm, entry.Term(), "entry has incorrect term")
	assert.Equal(t, expectedData, entry.Data(), "entry has incorrect data")
}

func TestNewLog(t *testing.T) {
	log := NewVolatileLog()
	assert.Zero(t, log.FirstIndex())
	assert.Zero(t, log.LastIndex())
	assert.Zero(t, log.LastTerm())
}

func TestAppendEntries(t *testing.T) {
	log := NewVolatileLog()

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
	log := NewVolatileLog()

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
	log := NewVolatileLog()

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

	log.Compact(entry2Index)

	entry3, _ = log.GetEntry(entry3Index)
	validateLogEntry(t, entry3, entry3Index, entry3Term, entry3Data)

	assert.Equal(t, log.LastTerm(), entry3Term)
	assert.Equal(t, log.LastIndex(), entry3Index)
}
