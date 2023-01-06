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

func TestAppendEntry(t *testing.T) {
	path := t.TempDir()
	log := NewLog(path)
	t.Cleanup(func() { log.Close() })
	log.Open()

	var entry1, entry2 *LogEntry
	var ok bool

	// Create entries and append to log.
	entry1Index := uint64(1)
	entry1Term := uint64(1)
	entry1Data := []byte("entry1")
	entry1 = NewLogEntry(entry1Index, entry1Term, entry1Data)

	assert.NoError(t, log.AppendEntry(entry1), "expected no error from appending entry to log")

	expectedLogSize := uint64(1)
	actualLogSize := uint64(log.Size())
	validateLogSize(t, actualLogSize, expectedLogSize)

	entry2Index := uint64(2)
	entry2Term := uint64(2)
	entry2Data := []byte("entry2")
	entry2 = NewLogEntry(entry2Index, entry2Term, entry2Data)

	assert.NoError(t, log.AppendEntry(entry2), "expected no error from appending entry to log")

	expectedLogSize = 2
	actualLogSize = uint64(log.Size())
	validateLogSize(t, actualLogSize, expectedLogSize)

	// Check if entries have been added to in memory log.
	entry1, ok = log.GetEntry(uint64(entry1Index))
	assert.True(t, ok, "expected OK response from getting entry")
	validateLogEntry(t, entry1, entry1Index, entry1Term, entry1Data)

	entry2, ok = log.GetEntry(uint64(entry2Index))
	assert.True(t, ok, "expected OK response from getting entry")
	validateLogEntry(t, entry2, entry2Index, entry2Term, entry2Data)
}

func TestPersist(t *testing.T) {
	path := t.TempDir()
	log := NewLog(path)
	t.Cleanup(func() { log.Close() })
	log.Open()

	var entry1, entry2 *LogEntry
	var ok bool

	// Create entries and append to log.
	entry1Index := uint64(1)
	entry1Term := uint64(1)
	entry1Data := []byte("entry1")
	entry1 = NewLogEntry(entry1Index, entry1Term, entry1Data)

	assert.NoError(t, log.AppendEntry(entry1), "expected no error from appending entry to log")

	expectedLogSize := uint64(1)
	actualLogSize := uint64(log.Size())
	validateLogSize(t, actualLogSize, expectedLogSize)

	entry2Index := uint64(2)
	entry2Term := uint64(2)
	entry2Data := []byte("entry2")
	entry2 = NewLogEntry(entry2Index, entry2Term, entry2Data)

	assert.NoError(t, log.AppendEntry(entry2), "expected no error from appending entry to log")

	expectedLogSize = 2
	actualLogSize = uint64(log.Size())
	validateLogSize(t, actualLogSize, expectedLogSize)

	// Close the log, open the log, and check entries.
	log.Close()
	log.Open()

	// Check if entries have been added to in memory log.
	entry1, ok = log.GetEntry(entry1Index)
	assert.True(t, ok, "expected OK response from getting entry")
	validateLogEntry(t, entry1, entry1Index, entry1Term, entry1Data)

	entry2, ok = log.GetEntry(entry2Index)
	assert.True(t, ok, "expected OK response from getting entry")
	validateLogEntry(t, entry2, entry2Index, entry2Term, entry2Data)
}

func TestTruncate(t *testing.T) {
	path := t.TempDir()
	log := NewLog(path)
	t.Cleanup(func() { log.Close() })
	log.Open()

	var entry1, entry2 *LogEntry
	var ok bool

	// Create entries and append to log.
	entry1Index := uint64(1)
	entry1Term := uint64(1)
	entry1Data := []byte("entry1")
	entry1 = NewLogEntry(entry1Index, entry1Term, entry1Data)

	assert.NoError(t, log.AppendEntry(entry1), "expected no error from appending entry to log")

	expectedLogSize := uint64(1)
	actualLogSize := uint64(log.Size())
	validateLogSize(t, actualLogSize, expectedLogSize)

	entry2Index := uint64(2)
	entry2Term := uint64(2)
	entry2Data := []byte("entry2")
	entry2 = NewLogEntry(entry2Index, entry2Term, entry2Data)

	assert.NoError(t, log.AppendEntry(entry2), "expected no error from appending entry to log")

	expectedLogSize = 2
	actualLogSize = uint64(log.Size())
	validateLogSize(t, actualLogSize, expectedLogSize)

	// Truncate second entry and check in memory log.
	log.Truncate(entry2Index)

	expectedLogSize = 1
	actualLogSize = uint64(log.Size())
	validateLogSize(t, actualLogSize, expectedLogSize)

	entry1, ok = log.GetEntry(entry1Index)
	assert.True(t, ok, "expected OK response from getting entry")
	validateLogEntry(t, entry1, entry1Index, entry1Term, entry1Data)

	// Check log file.
	log.Close()
	log.Open()

	expectedLogSize = 1
	actualLogSize = uint64(log.Size())
	validateLogSize(t, actualLogSize, expectedLogSize)

	entry1, ok = log.GetEntry(entry1Index)
	assert.True(t, ok, "expected OK response from getting entry")
	validateLogEntry(t, entry1, entry1Index, entry1Term, entry1Data)
}
