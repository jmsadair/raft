package raft

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestAppendEntries(t *testing.T) {
	tmpDir := t.TempDir()
	path := tmpDir + "/test-log.bin"
	log := NewLog(path)

	require.NoError(t, log.Open())
	require.NoError(t, log.Replay())
	defer func() { require.NoError(t, log.Close()) }()

	// Add some entries to the log.
	var entry1, entry2 *LogEntry
	var err error

	var entry1Index uint64 = 1
	var entry1Term uint64 = 1
	entry1Data := []byte("entry1")
	entry1Type := OperationEntry
	entry1 = NewLogEntry(entry1Index, entry1Term, entry1Data, entry1Type)

	var entry2Index uint64 = 2
	var entry2Term uint64 = 1
	entry2Data := []byte("entry2")
	entry2Type := OperationEntry
	entry2 = NewLogEntry(entry2Index, entry2Term, entry2Data, entry2Type)

	require.NoError(t, log.AppendEntries([]*LogEntry{entry1, entry2}))

	// Make sure the added entries are correct.
	entry1, err = log.GetEntry(entry1Index)
	require.NoError(t, err)
	validateLogEntry(t, entry1, entry1Index, entry1Term, entry1Data, entry1Type)

	entry2, err = log.GetEntry(entry2Index)
	require.NoError(t, err)
	validateLogEntry(t, entry2, entry2Index, entry2Term, entry2Data, entry2Type)

	require.Equal(t, log.LastTerm(), entry2Term)
	require.Equal(t, log.LastIndex(), entry2Index)

	// Close and reopen to the log to check that it was persisted correctly.
	require.NoError(t, log.Close())
	require.NoError(t, log.Open())
	require.NoError(t, log.Replay())

	entry1, err = log.GetEntry(entry1Index)
	require.NoError(t, err)
	validateLogEntry(t, entry1, entry1Index, entry1Term, entry1Data, entry1Type)

	entry2, err = log.GetEntry(entry2Index)
	require.NoError(t, err)
	validateLogEntry(t, entry2, entry2Index, entry2Term, entry2Data, entry2Type)
}

func TestTruncate(t *testing.T) {
	tmpDir := t.TempDir()
	path := tmpDir + "/test-log.bin"
	log := NewLog(path)

	require.NoError(t, log.Open())
	require.NoError(t, log.Replay())
	defer func() { require.NoError(t, log.Close()) }()

	// Add some entries to the log.
	var err error
	var entry1, entry2, entry3 *LogEntry

	var entry1Index uint64 = 1
	var entry1Term uint64 = 1
	entry1Data := []byte("entry1")
	entry1Type := OperationEntry
	entry1 = NewLogEntry(entry1Index, entry1Term, entry1Data, entry1Type)

	var entry2Index uint64 = 2
	var entry2Term uint64 = 1
	entry2Data := []byte("entry2")
	entry2Type := OperationEntry
	entry2 = NewLogEntry(entry2Index, entry2Term, entry2Data, entry2Type)

	var entry3Index uint64 = 3
	var entry3Term uint64 = 2
	entry3Data := []byte("entry3")
	entry3Type := OperationEntry
	entry3 = NewLogEntry(entry3Index, entry3Term, entry3Data, entry3Type)

	require.NoError(t, log.AppendEntries([]*LogEntry{entry1, entry2, entry3}))

	// Truncate the log down to and including the second entry.
	require.NoError(t, log.Truncate(entry2Index))

	// Make sure the first entry is still present and correct.
	entry1, err = log.GetEntry(entry1Index)
	require.NoError(t, err)
	validateLogEntry(t, entry1, entry1Index, entry1Term, entry1Data, entry1Type)

	require.Equal(t, log.LastTerm(), entry1Term)
	require.Equal(t, log.LastIndex(), entry1Index)

	// Close and reopen to the log to check that it was persisted correctly.
	require.NoError(t, log.Close())
	require.NoError(t, log.Open())
	require.NoError(t, log.Replay())

	entry1, err = log.GetEntry(entry1Index)
	require.NoError(t, err)
	validateLogEntry(t, entry1, entry1Index, entry1Term, entry1Data, entry1Type)
}

func TestCompact(t *testing.T) {
	tmpDir := t.TempDir()
	path := tmpDir + "/test-log.bin"
	log := NewLog(path)

	require.NoError(t, log.Open())
	require.NoError(t, log.Replay())
	defer func() { require.NoError(t, log.Close()) }()

	// Add some entries to the log.
	var err error
	var entry1, entry2, entry3, entry4 *LogEntry

	var entry1Index uint64 = 1
	var entry1Term uint64 = 1
	entry1Data := []byte("entry1")
	entry1Type := NoOpEntry
	entry1 = NewLogEntry(entry1Index, entry1Term, entry1Data, entry1Type)

	var entry2Index uint64 = 2
	var entry2Term uint64 = 2
	entry2Data := []byte("entry2")
	entry2Type := OperationEntry
	entry2 = NewLogEntry(entry2Index, entry2Term, entry2Data, entry2Type)

	var entry3Index uint64 = 3
	var entry3Term uint64 = 2
	entry3Data := []byte("entry3")
	entry3Type := OperationEntry
	entry3 = NewLogEntry(entry3Index, entry3Term, entry3Data, entry3Type)

	require.NoError(t, log.AppendEntries([]*LogEntry{entry1, entry2, entry3}))

	// Compact the log up to and including the second index.
	require.NoError(t, log.Compact(entry2Index))

	// Make sure the third entry is still present and correct.
	entry3, err = log.GetEntry(entry3Index)
	require.NoError(t, err)
	validateLogEntry(t, entry3, entry3Index, entry3Term, entry3Data, entry3Type)

	require.Equal(t, log.LastTerm(), entry3Term)
	require.Equal(t, log.LastIndex(), entry3Index)

	// Make sure we can still add and retrieve entries from the log.
	var entry4Index uint64 = 4
	var entry4Term uint64 = 2
	entry4Data := []byte("entry4")
	entry4Type := NoOpEntry
	entry4 = NewLogEntry(entry4Index, entry4Term, entry4Data, entry4Type)

	require.NoError(t, log.AppendEntry(entry4))

	entry4, err = log.GetEntry(entry4Index)
	require.NoError(t, err)
	validateLogEntry(t, entry4, entry4Index, entry4Term, entry4Data, entry4Type)

	// Close and reopen to the log to make sure it was correctly persisted.
	require.NoError(t, log.Close())
	require.NoError(t, log.Open())
	require.NoError(t, log.Replay())

	entry3, err = log.GetEntry(entry3Index)
	require.NoError(t, err)
	validateLogEntry(t, entry3, entry3Index, entry3Term, entry3Data, entry3Type)

	entry4, err = log.GetEntry(entry4Index)
	require.NoError(t, err)
	validateLogEntry(t, entry4, entry4Index, entry4Term, entry4Data, entry4Type)
}

func TestDiscard(t *testing.T) {
	tmpDir := t.TempDir()
	path := tmpDir + "/test-log.bin"
	log := NewLog(path)

	require.NoError(t, log.Open())
	require.NoError(t, log.Replay())
	defer func() { require.NoError(t, log.Close()) }()

	// Add some log entries to the log.
	var entry1Index uint64 = 1
	var entry1Term uint64 = 1
	entry1Data := []byte("entry1")
	entry1Type := OperationEntry
	entry1 := NewLogEntry(entry1Index, entry1Term, entry1Data, entry1Type)

	var entry2Index uint64 = 2
	var entry2Term uint64 = 4
	entry2Data := []byte("entry2")
	entry2Type := OperationEntry
	entry2 := NewLogEntry(entry2Index, entry2Term, entry2Data, entry2Type)

	require.NoError(t, log.AppendEntries([]*LogEntry{entry1, entry2}))

	// Discard the log entries.
	var discardIndex uint64 = 5
	var discardTerm uint64 = 5
	require.NoError(t, log.DiscardEntries(discardIndex, discardTerm))

	// Make sure the last index and last term are correct.
	require.Equal(t, discardIndex, log.LastIndex())
	require.Equal(t, discardTerm, log.LastTerm())
}

func TestContains(t *testing.T) {
	tmpDir := t.TempDir()
	path := tmpDir + "/test-log.bin"
	log := NewLog(path)

	require.NoError(t, log.Open())
	require.NoError(t, log.Replay())
	defer func() { require.NoError(t, log.Close()) }()

	// Make sure that placeholder entry is not visible.
	require.False(t, log.Contains(0))

	// Add an entry to the log.
	var entry1Index uint64 = 1
	var entry1Term uint64 = 1
	entry1Data := []byte("entry1")
	entry1Type := OperationEntry
	entry1 := NewLogEntry(entry1Index, entry1Term, entry1Data, entry1Type)
	require.NoError(t, log.AppendEntry(entry1))

	// Ensure log contains newly added entry
	require.True(t, log.Contains(entry1Index))
}
