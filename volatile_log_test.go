package raft

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestVolatileAppendEntries(t *testing.T) {
	log := NewVolatileLog()

	var err error
	var entry1Index uint64 = 1
	var entry1Term uint64 = 1
	entry1Data := []byte("entry1")
	entry1 := NewLogEntry(entry1Index, entry1Term, entry1Data)

	log.AppendEntries(entry1)

	entry1, err = log.GetEntry(entry1Index)

	assert.NoError(t, err)
	validateLogEntry(t, entry1, entry1Index, entry1Term, entry1Data)
	assert.Equal(t, log.Size(), 1)
	assert.Equal(t, log.FirstIndex(), entry1Index)
	assert.Equal(t, log.LastIndex(), entry1Index)
	assert.Equal(t, log.LastTerm(), entry1Term)
}

func TestVolatileTruncate(t *testing.T) {
	log := NewVolatileLog()

	var entry1Index uint64 = 1
	var entry1Term uint64 = 1
	entry1Data := []byte("entry1")
	entry1 := NewLogEntry(entry1Index, entry1Term, entry1Data)

	var entry2Index uint64 = 2
	var entry2Term uint64 = 2
	entry2Data := []byte("entry2")
	entry2 := NewLogEntry(entry2Index, entry2Term, entry2Data)

	var entry3Index uint64 = 3
	var entry3Term uint64 = 3
	entry3Data := []byte("entry3")
	entry3 := NewLogEntry(entry3Index, entry3Term, entry3Data)

	log.AppendEntries(entry1, entry2, entry3)

	err := log.Truncate(entry2Index)

	assert.NoError(t, err)
	assert.Equal(t, log.Size(), 1)
	assert.Equal(t, log.FirstIndex(), entry1Index)
	assert.Equal(t, log.LastIndex(), entry1Index)
}

func TestVolatileContains(t *testing.T) {
	log := NewVolatileLog()

	var entry1Index uint64 = 1
	var entry1Term uint64 = 1
	entry1Data := []byte("entry1")
	entry1 := NewLogEntry(entry1Index, entry1Term, entry1Data)

	var entry2Index uint64 = 2
	var entry2Term uint64 = 2
	entry2Data := []byte("entry2")
	entry2 := NewLogEntry(entry2Index, entry2Term, entry2Data)

	log.AppendEntries(entry1, entry2)

	assert.True(t, log.Contains(entry1Index))
	assert.True(t, log.Contains(entry2Index))
	assert.False(t, log.Contains(3))
}

func TestVolatileClear(t *testing.T) {
	log := NewVolatileLog()

	var entry1Index uint64 = 1
	var entry1Term uint64 = 1
	entry1Data := []byte("entry1")
	entry1 := NewLogEntry(entry1Index, entry1Term, entry1Data)

	log.AppendEntries(entry1)

	assert.Equal(t, log.Size(), 1)

	log.Clear()

	assert.Zero(t, log.Size())
}
