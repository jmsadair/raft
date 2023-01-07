package raft

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestVolatileAppendEntries(t *testing.T) {
	log := newVolatileLog()

	var err error
	var entry1Index uint64 = 1
	var entry1Term uint64 = 1
	entry1Data := []byte("entry1")
	entry1 := newLogEntry(entry1Index, entry1Term, entry1Data)

	log.appendEntries(entry1)

	entry1, err = log.getEntry(entry1Index)

	assert.NoError(t, err)
	validateLogEntry(t, entry1, entry1Index, entry1Term, entry1Data)
	assert.Equal(t, log.size(), 1)
	assert.Equal(t, log.startIndex(), entry1Index)
	assert.Equal(t, log.lastIndex(), entry1Index)
	assert.Equal(t, log.lastTerm(), entry1Term)
}

func TestVolatileTruncate(t *testing.T) {
	log := newVolatileLog()

	var entry1Index uint64 = 1
	var entry1Term uint64 = 1
	entry1Data := []byte("entry1")
	entry1 := newLogEntry(entry1Index, entry1Term, entry1Data)

	var entry2Index uint64 = 2
	var entry2Term uint64 = 2
	entry2Data := []byte("entry2")
	entry2 := newLogEntry(entry2Index, entry2Term, entry2Data)

	var entry3Index uint64 = 3
	var entry3Term uint64 = 3
	entry3Data := []byte("entry3")
	entry3 := newLogEntry(entry3Index, entry3Term, entry3Data)

	log.appendEntries(entry1, entry2, entry3)

	err := log.truncate(entry2Index)

	assert.NoError(t, err)
	assert.Equal(t, log.size(), 1)
	assert.Equal(t, log.startIndex(), entry1Index)
	assert.Equal(t, log.lastIndex(), entry1Index)
}

func TestVolatileContains(t *testing.T) {
	log := newVolatileLog()

	var entry1Index uint64 = 1
	var entry1Term uint64 = 1
	entry1Data := []byte("entry1")
	entry1 := newLogEntry(entry1Index, entry1Term, entry1Data)

	var entry2Index uint64 = 2
	var entry2Term uint64 = 2
	entry2Data := []byte("entry2")
	entry2 := newLogEntry(entry2Index, entry2Term, entry2Data)

	log.appendEntries(entry1, entry2)

	assert.True(t, log.contains(entry1Index))
	assert.True(t, log.contains(entry2Index))
	assert.False(t, log.contains(3))
}

func TestVolatileClear(t *testing.T) {
	log := newVolatileLog()

	var entry1Index uint64 = 1
	var entry1Term uint64 = 1
	entry1Data := []byte("entry1")
	entry1 := newLogEntry(entry1Index, entry1Term, entry1Data)

	log.appendEntries(entry1)

	assert.Equal(t, log.size(), 1)

	log.clear()

	assert.Zero(t, log.size())
}
