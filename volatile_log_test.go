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
	log := NewLog()
	assert.Zero(t, log.FirstIndex())
	assert.Zero(t, log.LastIndex())
	assert.Zero(t, log.LastTerm())
}

func TestAppendEntries(t *testing.T) {
	log := NewLog()

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
	log := NewLog()

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

	log.Truncate(entry2Index)

	entry1, _ = log.GetEntry(entry1Index)
	validateLogEntry(t, entry1, entry1Index, entry1Term, entry1Data)

	assert.Equal(t, log.LastTerm(), entry1Term)
	assert.Equal(t, log.LastIndex(), entry1Index)
}
