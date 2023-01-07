package raft

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func NewTestLog(t *testing.T) *Log {
	path := t.TempDir()
	log := newLog(path)
	t.Cleanup(func() { log.close() })
	log.open()
	return log
}

func validateLogEntry(t *testing.T, entry *LogEntry, expectedIndex uint64, expectedTerm uint64, expectedData []byte) {
	assert.Equal(t, expectedIndex, entry.index(), "entry has incorrect index")
	assert.Equal(t, expectedTerm, entry.term(), "entry has incorrect term")
	assert.Equal(t, expectedData, entry.data(), "entry has incorrect data")
}

func validateLogSize(t *testing.T, actualSize int, expectedSize int) {
	assert.Equal(t, actualSize, expectedSize, "log has incorrect size")
}
