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
	assert.NoError(t, log.Open(), "expected no error opening new log")

	expectedSize := 0
	actualSize := log.Size()
	assert.Equal(t, expectedSize, actualSize)
}

func TestOpenExisting(t *testing.T) {
	path := t.TempDir()
	log := NewLog(path)
	t.Cleanup(func() { log.Close() })
	assert.NoError(t, log.Open(), "expected no error opening new log")

	entry1 := NewLogEntry(1, 0, []byte("test"))
	assert.NoError(t, log.AppendEntry(entry1), "expected no error from appending entry to log")
	entry2 := NewLogEntry(2, 1, []byte("test"))
	assert.NoError(t, log.AppendEntry(entry2), "expected no error from appending entry to log")

	expectedLogSize := 2
	actualLogSize := log.Size()
	assert.Equal(t, expectedLogSize, actualLogSize, "log has incorrect number of entries")

	log.Close()
	assert.NoError(t, log.Open(), "expected no error opening existing log")

	actualLogSize = log.Size()
	assert.Equal(t, expectedLogSize, actualLogSize, "log has incorrect number of entries")
}
