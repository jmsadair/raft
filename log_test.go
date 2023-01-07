package raft

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewLog(t *testing.T) {
	log := NewLog("raft/test")

	expectedPath := "raft/test/log"
	actualPath := log.Path()
	assert.Equal(t, expectedPath, actualPath)

	expectedSize := 0
	actualSize := log.Size()
	assert.Equal(t, expectedSize, actualSize)
}

func TestOpenNew(t *testing.T) {
	path := t.TempDir()
	log := NewLog(path)
	t.Cleanup(func() { log.Close() })
	log.Open()

	assert.NotNil(t, log.File(), "open log should not have nil file")
	assert.Equal(t, log.File().Name(), log.Path(), "log file name should match log path")

	expectedSize := 0
	actualSize := log.Size()
	assert.Equal(t, expectedSize, actualSize)
}

func TestIsOpen(t *testing.T) {
	path := t.TempDir()
	log := NewLog(path)
	t.Cleanup(func() { log.Close() })

	assert.False(t, log.IsOpen(), "expected log to be closed")
	log.Open()
	assert.True(t, log.IsOpen(), "expected log to be open")
}

func TestAppendEntries(t *testing.T) {
	log := NewTestLog(t)

	var entry1, entry2 *LogEntry

	// Create entries and append to log.
	entry1Index := uint64(1)
	entry1Term := uint64(1)
	entry1Data := []byte("entry1")
	entry1 = NewLogEntry(entry1Index, entry1Term, entry1Data)

	entry2Index := uint64(2)
	entry2Term := uint64(2)
	entry2Data := []byte("entry2")
	entry2 = NewLogEntry(entry2Index, entry2Term, entry2Data)

	log.AppendEntries(entry1, entry2)

	expectedLogSize := uint64(2)
	actualLogSize := uint64(log.Size())
	validateLogSize(t, actualLogSize, expectedLogSize)

	// Check if entries have been added to in memory log.
	entry1 = log.GetEntry(uint64(entry1Index))
	validateLogEntry(t, entry1, entry1Index, entry1Term, entry1Data)

	entry2 = log.GetEntry(uint64(entry2Index))
	validateLogEntry(t, entry2, entry2Index, entry2Term, entry2Data)
}

func TestTruncate(t *testing.T) {
	log := NewTestLog(t)

	var entry1, entry2 *LogEntry

	// Create entries and append to log.
	entry1Index := uint64(1)
	entry1Term := uint64(1)
	entry1Data := []byte("entry1")
	entry1 = NewLogEntry(entry1Index, entry1Term, entry1Data)

	entry2Index := uint64(2)
	entry2Term := uint64(2)
	entry2Data := []byte("entry2")
	entry2 = NewLogEntry(entry2Index, entry2Term, entry2Data)

	log.AppendEntries(entry1, entry2)

	// Truncate second entry.
	log.Truncate(entry2Index)

	// Check in memory log.
	checkLog := func() {
		expectedLogSize := uint64(1)
		actualLogSize := uint64(log.Size())
		validateLogSize(t, actualLogSize, expectedLogSize)

		entry1 = log.GetEntry(entry1Index)
		validateLogEntry(t, entry1, entry1Index, entry1Term, entry1Data)
	}

	checkLog()

	// Check log file.
	log.Close()
	log.Open()
	checkLog()
}

func TestAppendEntriesTruncate(t *testing.T) {
	log := NewTestLog(t)

	// Create initial entries and append to log.
	entry1Index := uint64(1)
	entry1Term := uint64(1)
	entry1Data := []byte("entry1")
	entry1 := NewLogEntry(entry1Index, entry1Term, entry1Data)

	entry2Index := uint64(2)
	entry2Term := uint64(2)
	entry2Data := []byte("entry2")
	entry2 := NewLogEntry(entry2Index, entry2Term, entry2Data)

	entry3Index := uint64(3)
	entry3Term := uint64(3)
	entry3Data := []byte("entry3")
	entry3 := NewLogEntry(entry3Index, entry3Term, entry3Data)

	log.AppendEntries(entry1, entry2, entry3)

	// Create entries where one will cause conflict.
	entry4Index := uint64(2)
	entry4Term := uint64(3)
	entry4Data := []byte("entry4")
	entry4 := NewLogEntry(entry4Index, entry4Term, entry4Data)

	entry5Index := uint64(3)
	entry5Term := uint64(3)
	entry5Data := []byte("entry5")
	entry5 := NewLogEntry(entry5Index, entry5Term, entry5Data)

	// Append the entries - should cause log to truncate.
	log.AppendEntries(entry4, entry5)

	checkLog := func() {
		expectedSize := 3
		actualSize := log.Size()
		validateLogSize(t, uint64(actualSize), uint64(expectedSize))

		entry1 = log.GetEntry(entry1Index)
		validateLogEntry(t, entry1, entry1Index, entry1Term, entry1Data)

		entry2 = log.GetEntry(entry2Index)
		validateLogEntry(t, entry2, entry4Index, entry4Term, entry4Data)

		entry3 = log.GetEntry(entry3Index)
		validateLogEntry(t, entry3, entry5Index, entry5Term, entry5Data)
	}

	// Check in memory log
	checkLog()

	// Check log file
	log.Close()
	log.Open()
	checkLog()
}
