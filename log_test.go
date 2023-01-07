package raft

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewLog(t *testing.T) {
	log := newLog("raft/test")

	assert.Equal(t, "raft/test/log", log.logPath())
	assert.Nil(t, log.logFile())
	assert.Zero(t, log.size())
	assert.Zero(t, log.lastIndex())
	assert.Zero(t, log.lastTerm())
}

func TestOpenNew(t *testing.T) {
	path := t.TempDir()
	log := newLog(path)
	t.Cleanup(func() { log.close() })
	log.open()

	assert.NotNil(t, log.logFile())
	assert.Equal(t, log.logFile().Name(), log.logPath())
	assert.Zero(t, log.size())
}

func TestIsOpen(t *testing.T) {
	path := t.TempDir()
	log := newLog(path)
	t.Cleanup(func() { log.close() })

	assert.False(t, log.isOpen())
	log.open()
	assert.True(t, log.isOpen())
}

func TestAppendEntries(t *testing.T) {
	log := NewTestLog(t)

	var entry1, entry2 *LogEntry

	var entry1Index uint64 = 1
	var entry1Term uint64 = 1
	entry1Data := []byte("entry1")
	entry1 = NewLogEntry(entry1Index, entry1Term, entry1Data)

	var entry2Index uint64 = 2
	var entry2Term uint64 = 2
	entry2Data := []byte("entry2")
	entry2 = NewLogEntry(entry2Index, entry2Term, entry2Data)

	log.appendEntries(entry1, entry2)

	validateLogSize(t, log.size(), 2)

	entry1 = log.getEntry(entry1Index)
	validateLogEntry(t, entry1, entry1Index, entry1Term, entry1Data)

	entry2 = log.getEntry(entry2Index)
	validateLogEntry(t, entry2, entry2Index, entry2Term, entry2Data)

	assert.Equal(t, log.lastTerm(), entry2Term)
	assert.Equal(t, log.lastIndex(), entry2Index)
}

func TestTruncate(t *testing.T) {
	log := NewTestLog(t)

	var entry1, entry2 *LogEntry

	var entry1Index uint64 = 1
	var entry1Term uint64 = 1
	entry1Data := []byte("entry1")
	entry1 = NewLogEntry(entry1Index, entry1Term, entry1Data)

	var entry2Index uint64 = 2
	var entry2Term uint64 = 2
	entry2Data := []byte("entry2")
	entry2 = NewLogEntry(entry2Index, entry2Term, entry2Data)

	log.appendEntries(entry1, entry2)

	log.truncate(entry2Index)

	checkLog := func() {
		validateLogSize(t, log.size(), 1)
		entry1 = log.getEntry(entry1Index)
		validateLogEntry(t, entry1, entry1Index, entry1Term, entry1Data)
	}

	checkLog()

	log.close()
	log.open()

	checkLog()

	assert.Equal(t, log.lastTerm(), entry1Term)
	assert.Equal(t, log.lastIndex(), entry1Index)
}

func TestAppendEntriesTruncate(t *testing.T) {
	log := NewTestLog(t)

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

	log.appendEntries(entry1, entry2, entry3)

	var entry4Index uint64 = 2
	var entry4Term uint64 = 3
	entry4Data := []byte("entry4")
	entry4 := NewLogEntry(entry4Index, entry4Term, entry4Data)

	var entry5Index uint64 = 3
	var entry5Term uint64 = 3
	entry5Data := []byte("entry5")
	entry5 := NewLogEntry(entry5Index, entry5Term, entry5Data)

	log.appendEntries(entry4, entry5)

	checkLog := func() {
		validateLogSize(t, log.size(), 3)

		entry1 = log.getEntry(entry1Index)
		validateLogEntry(t, entry1, entry1Index, entry1Term, entry1Data)

		entry2 = log.getEntry(entry2Index)
		validateLogEntry(t, entry2, entry4Index, entry4Term, entry4Data)

		entry3 = log.getEntry(entry3Index)
		validateLogEntry(t, entry3, entry5Index, entry5Term, entry5Data)
	}

	checkLog()

	log.close()
	log.open()

	checkLog()

	assert.Equal(t, log.lastTerm(), entry5Term)
	assert.Equal(t, log.lastIndex(), entry5Index)
}
