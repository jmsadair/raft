package raft

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestLogEncoderDecoder(t *testing.T) {
	entry := NewLogEntry(1, 1, []byte("test"), OperationEntry)
	buf := new(bytes.Buffer)

	require.NoError(t, encodeLogEntry(buf, entry))

	decodedEntry, err := decodeLogEntry(buf)
	require.NoError(t, err)

	checkLogEntry(t, entry, &decodedEntry)
}

func TestAppendEntries(t *testing.T) {
	tmpDir := t.TempDir()
	log, err := NewLog(tmpDir)
	require.NoError(t, err)

	require.NoError(t, log.Open())
	require.NoError(t, log.Replay())
	defer func() { require.NoError(t, log.Close()) }()

	// Add some entries to the log.
	entry1 := NewLogEntry(1, 1, []byte("1"), OperationEntry)
	entry2 := NewLogEntry(2, 1, []byte("2"), OperationEntry)
	require.NoError(t, log.AppendEntries([]*LogEntry{entry1, entry2}))

	// Make sure the added entries are correct.
	actualEntry1, err := log.GetEntry(entry1.Index)
	require.NoError(t, err)
	checkLogEntry(t, entry1, actualEntry1)
	actualEntry2, err := log.GetEntry(entry2.Index)
	require.NoError(t, err)
	checkLogEntry(t, entry2, actualEntry2)

	require.Equal(t, entry2.Term, log.LastTerm())
	require.Equal(t, entry2.Index, log.LastIndex())
	require.Equal(t, 2, log.Size())

	// Close and reopen to the log to check that it was persisted correctly.
	require.NoError(t, log.Close())
	require.NoError(t, log.Open())
	require.NoError(t, log.Replay())

	actualEntry1, err = log.GetEntry(entry1.Index)
	require.NoError(t, err)
	checkLogEntry(t, entry1, actualEntry1)
	actualEntry2, err = log.GetEntry(entry2.Index)
	require.NoError(t, err)
	checkLogEntry(t, entry2, actualEntry2)

	require.Equal(t, entry2.Term, log.LastTerm())
	require.Equal(t, entry2.Index, log.LastIndex())
	require.Equal(t, 2, log.Size())
}

func TestTruncate(t *testing.T) {
	tmpDir := t.TempDir()
	log, err := NewLog(tmpDir)
	require.NoError(t, err)

	require.NoError(t, log.Open())
	require.NoError(t, log.Replay())
	defer func() { require.NoError(t, log.Close()) }()

	// Add some entries to the log.
	entry1 := NewLogEntry(1, 1, []byte("1"), OperationEntry)
	entry2 := NewLogEntry(2, 1, []byte("2"), OperationEntry)
	entry3 := NewLogEntry(3, 2, []byte("3"), OperationEntry)
	require.NoError(t, log.AppendEntries([]*LogEntry{entry1, entry2, entry3}))

	// Truncate the log down to and including the second entry.
	require.NoError(t, log.Truncate(entry2.Index))

	// Make sure the first entry is still present and correct.
	actualEntry1, err := log.GetEntry(entry1.Index)
	require.NoError(t, err)
	checkLogEntry(t, entry1, actualEntry1)

	require.Equal(t, entry1.Term, log.LastTerm())
	require.Equal(t, entry1.Index, log.LastIndex())
	require.Equal(t, 1, log.Size())

	// Close and reopen to the log to check that it was persisted correctly.
	require.NoError(t, log.Close())
	require.NoError(t, log.Open())
	require.NoError(t, log.Replay())

	actualEntry1, err = log.GetEntry(entry1.Index)
	require.NoError(t, err)
	checkLogEntry(t, entry1, actualEntry1)

	require.Equal(t, entry1.Term, log.LastTerm())
	require.Equal(t, entry1.Index, log.LastIndex())
	require.Equal(t, 1, log.Size())
}

func TestCompact(t *testing.T) {
	tmpDir := t.TempDir()
	log, err := NewLog(tmpDir)
	require.NoError(t, err)

	require.NoError(t, log.Open())
	require.NoError(t, log.Replay())
	defer func() { require.NoError(t, log.Close()) }()

	// Add some entries to the log.
	entry1 := NewLogEntry(1, 1, []byte("1"), NoOpEntry)
	entry2 := NewLogEntry(2, 2, []byte("2"), OperationEntry)
	entry3 := NewLogEntry(3, 2, []byte("3"), OperationEntry)
	require.NoError(t, log.AppendEntries([]*LogEntry{entry1, entry2, entry3}))

	// Compact the log up to and including the second index.
	require.NoError(t, log.Compact(entry2.Index))

	// Make sure the third entry is still present and correct.
	actualEntry3, err := log.GetEntry(entry3.Index)
	require.NoError(t, err)
	checkLogEntry(t, entry3, actualEntry3)

	require.Equal(t, entry3.Term, log.LastTerm())
	require.Equal(t, entry3.Index, log.LastIndex())
	require.Equal(t, 1, log.Size())

	// Make sure we can still add and retrieve entries from the log.
	entry4 := NewLogEntry(4, 2, []byte("4"), NoOpEntry)
	require.NoError(t, log.AppendEntry(entry4))

	actualEntry4, err := log.GetEntry(entry4.Index)
	require.NoError(t, err)
	checkLogEntry(t, entry4, actualEntry4)

	// Close and reopen to the log to make sure it was correctly persisted.
	require.NoError(t, log.Close())
	require.NoError(t, log.Open())
	require.NoError(t, log.Replay())

	actualEntry3, err = log.GetEntry(entry3.Index)
	require.NoError(t, err)
	checkLogEntry(t, entry3, actualEntry3)

	actualEntry4, err = log.GetEntry(entry4.Index)
	require.NoError(t, err)
	checkLogEntry(t, entry4, actualEntry4)

	require.Equal(t, entry4.Term, log.LastTerm())
	require.Equal(t, entry4.Index, log.LastIndex())
	require.Equal(t, 2, log.Size())
}

func TestDiscard(t *testing.T) {
	tmpDir := t.TempDir()
	log, err := NewLog(tmpDir)
	require.NoError(t, err)

	require.NoError(t, log.Open())
	require.NoError(t, log.Replay())
	defer func() { require.NoError(t, log.Close()) }()

	// Add some log entries to the log.
	entry1 := NewLogEntry(1, 1, []byte("1"), NoOpEntry)
	entry2 := NewLogEntry(2, 2, []byte("2"), OperationEntry)
	require.NoError(t, log.AppendEntries([]*LogEntry{entry1, entry2}))

	// Discard the log entries.
	var discardIndex uint64 = 5
	var discardTerm uint64 = 5
	require.NoError(t, log.DiscardEntries(discardIndex, discardTerm))

	// Make sure the last index and last term are correct.
	require.Equal(t, discardIndex, log.LastIndex())
	require.Equal(t, discardTerm, log.LastTerm())

	// Make sure log is empty.
	require.Zero(t, log.Size())
}

func TestContains(t *testing.T) {
	tmpDir := t.TempDir()
	log, err := NewLog(tmpDir)
	require.NoError(t, err)

	require.NoError(t, log.Open())
	require.NoError(t, log.Replay())
	defer func() { require.NoError(t, log.Close()) }()

	// Make sure that placeholder entry is not visible.
	require.False(t, log.Contains(0))

	// Add an entry to the log.
	entry1 := NewLogEntry(1, 1, []byte("1"), NoOpEntry)
	require.NoError(t, log.AppendEntry(entry1))

	// Ensure log contains newly added entry
	require.True(t, log.Contains(entry1.Index))
}
