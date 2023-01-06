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

func validateLogSize(t *testing.T, actualSize uint64, expectedSize uint64) {
	assert.Equal(t, actualSize, expectedSize, "log has incorrect size")
}
