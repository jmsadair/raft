package raft

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestAppendEntries(t *testing.T) {
	tmpDir := t.TempDir()
	path := tmpDir + "/test-log.bin"
	log := newPersistentLog(path)

	if err := log.Open(); err != nil {
		t.Fatalf("error opening log: %s", err.Error())
	}

	if err := log.Replay(); err != nil {
		t.Fatalf("error replaying log: %s", err.Error())
	}

	defer log.Close()

	// Add some entries to the log.
	var entry1, entry2 *LogEntry
	var err error

	var entry1Index uint64 = 1
	var entry1Term uint64 = 1
	entry1Data := []byte("entry1")
	entry1 = NewLogEntry(entry1Index, entry1Term, entry1Data)

	var entry2Index uint64 = 2
	var entry2Term uint64 = 1
	entry2Data := []byte("entry2")
	entry2 = NewLogEntry(entry2Index, entry2Term, entry2Data)

	if err := log.AppendEntries([]*LogEntry{entry1, entry2}); err != nil {
		t.Fatalf("error appending entries to log: %s", err.Error())
	}

	// Make sure the added entries are correct.
	entry1, err = log.GetEntry(entry1Index)
	if err != nil {
		t.Fatalf("error getting entry from log: %s", err.Error())
	}
	validateLogEntry(t, entry1, entry1Index, entry1Term, entry1Data)

	entry2, err = log.GetEntry(entry2Index)
	if err != nil {
		t.Fatalf("error getting entry from log: %s", err.Error())
	}
	validateLogEntry(t, entry2, entry2Index, entry2Term, entry2Data)

	assert.Equal(t, log.LastTerm(), entry2Term)
	assert.Equal(t, log.LastIndex(), entry2Index)

	// Close and reopen to the log to check that it was persisted correctly.
	if err := log.Close(); err != nil {
		t.Fatalf("error closing log: %s", err.Error())
	}

	if err := log.Open(); err != nil {
		t.Fatalf("error opening log: %s", err.Error())
	}

	if err := log.Replay(); err != nil {
		t.Fatalf("error replaying log: %s", err.Error())
	}

	defer log.Close()

	entry1, err = log.GetEntry(entry1Index)
	if err != nil {
		t.Fatalf("error getting entry from log: %s", err.Error())
	}
	validateLogEntry(t, entry1, entry1Index, entry1Term, entry1Data)

	entry2, err = log.GetEntry(entry2Index)
	if err != nil {
		t.Fatalf("error getting entry from log: %s", err.Error())
	}
	validateLogEntry(t, entry2, entry2Index, entry2Term, entry2Data)
}

func TestTruncate(t *testing.T) {
	tmpDir := t.TempDir()
	path := tmpDir + "/test-log.bin"
	log := newPersistentLog(path)

	if err := log.Open(); err != nil {
		t.Fatalf("error opening log: %s", err.Error())
	}

	if err := log.Replay(); err != nil {
		t.Fatalf("error replaying log: %s", err.Error())
	}

	defer log.Close()

	// Add some entries to the log.
	var err error
	var entry1, entry2, entry3 *LogEntry

	var entry1Index uint64 = 1
	var entry1Term uint64 = 1
	entry1Data := []byte("entry1")
	entry1 = NewLogEntry(entry1Index, entry1Term, entry1Data)

	var entry2Index uint64 = 2
	var entry2Term uint64 = 1
	entry2Data := []byte("entry2")
	entry2 = NewLogEntry(entry2Index, entry2Term, entry2Data)

	var entry3Index uint64 = 3
	var entry3Term uint64 = 2
	entry3Data := []byte("entry3")
	entry3 = NewLogEntry(entry3Index, entry3Term, entry3Data)

	log.AppendEntries([]*LogEntry{entry1, entry2, entry3})

	// Truncate the log down to and including the second entry.
	if err := log.Truncate(entry2Index); err != nil {
		t.Fatalf("error truncating log: %s", err.Error())
	}

	// Make sure the first entry is still present and correct.
	entry1, err = log.GetEntry(entry1Index)
	if err != nil {
		t.Fatalf("error getting entry from log: %s", err.Error())
	}
	validateLogEntry(t, entry1, entry1Index, entry1Term, entry1Data)

	assert.Equal(t, log.LastTerm(), entry1Term)
	assert.Equal(t, log.LastIndex(), entry1Index)

	// Close and reopen the log to make sure it was persisted correctly.
	if err := log.Close(); err != nil {
		t.Fatalf("error closing log: %s", err.Error())
	}

	if err := log.Open(); err != nil {
		t.Fatalf("error opening log: %s", err.Error())
	}

	if err := log.Replay(); err != nil {
		t.Fatalf("error replaying log: %s", err.Error())
	}

	entry1, err = log.GetEntry(entry1Index)
	if err != nil {
		t.Fatalf("error getting entry from log: %s", err.Error())
	}
	validateLogEntry(t, entry1, entry1Index, entry1Term, entry1Data)
}

func TestCompact(t *testing.T) {
	tmpDir := t.TempDir()
	path := tmpDir + "/test-log.bin"
	log := newPersistentLog(path)

	if err := log.Open(); err != nil {
		t.Fatalf("error opening log: %s", err.Error())
	}

	if err := log.Replay(); err != nil {
		t.Fatalf("error replaying log: %s", err.Error())
	}

	defer log.Close()

	// Add some entries to the log.
	var err error
	var entry1, entry2, entry3, entry4 *LogEntry

	var entry1Index uint64 = 1
	var entry1Term uint64 = 1
	entry1Data := []byte("entry1")
	entry1 = NewLogEntry(entry1Index, entry1Term, entry1Data)

	var entry2Index uint64 = 2
	var entry2Term uint64 = 2
	entry2Data := []byte("entry2")
	entry2 = NewLogEntry(entry2Index, entry2Term, entry2Data)

	var entry3Index uint64 = 3
	var entry3Term uint64 = 2
	entry3Data := []byte("entry3")
	entry3 = NewLogEntry(entry3Index, entry3Term, entry3Data)

	log.AppendEntries([]*LogEntry{entry1, entry2, entry3})

	// Compact the log up to and including the second index.
	if err := log.Compact(entry2Index); err != nil {
		t.Fatalf("error compacting log: %s", err.Error())
	}

	// Make sure the third entry is still present and correct.
	entry3, err = log.GetEntry(entry3Index)
	if err != nil {
		t.Fatalf("error getting entry from log: %s", err.Error())
	}
	validateLogEntry(t, entry3, entry3Index, entry3Term, entry3Data)

	assert.Equal(t, log.LastTerm(), entry3Term)
	assert.Equal(t, log.LastIndex(), entry3Index)

	// Make sure we can still add and retrieve entries from the log.
	var entry4Index uint64 = 4
	var entry4Term uint64 = 2
	entry4Data := []byte("entry4")
	entry4 = NewLogEntry(entry4Index, entry4Term, entry4Data)

	log.AppendEntry(entry4)

	entry4, err = log.GetEntry(entry4Index)
	if err != nil {
		t.Fatalf("error getting entry from log: %s", err.Error())
	}
	validateLogEntry(t, entry4, entry4Index, entry4Term, entry4Data)

	// Close and reopen to the log to make sure the it was correctly persisted.
	if err := log.Close(); err != nil {
		t.Fatalf("error closing log: %s", err.Error())
	}

	if err := log.Open(); err != nil {
		t.Fatalf("error opening log: %s", err.Error())
	}

	if err := log.Replay(); err != nil {
		t.Fatalf("error replaying log: %s", err.Error())
	}

	entry3, err = log.GetEntry(entry3Index)
	if err != nil {
		t.Fatalf("error getting entry from log: %s", err.Error())
	}
	validateLogEntry(t, entry3, entry3Index, entry3Term, entry3Data)

	entry4, err = log.GetEntry(entry4Index)
	if err != nil {
		t.Fatalf("error getting entry from log: %s", err.Error())
	}
	validateLogEntry(t, entry4, entry4Index, entry4Term, entry4Data)
}

func TestDiscard(t *testing.T) {
	tmpDir := t.TempDir()
	path := tmpDir + "/test-log.bin"
	log := newPersistentLog(path)

	if err := log.Open(); err != nil {
		t.Fatalf("error opening log: %s", err.Error())
	}

	if err := log.Replay(); err != nil {
		t.Fatalf("error replaying log: %s", err.Error())
	}

	defer log.Close()

	// Add some log entries to the log.
	var entry1Index uint64 = 1
	var entry1Term uint64 = 1
	entry1Data := []byte("entry1")
	entry1 := NewLogEntry(entry1Index, entry1Term, entry1Data)

	var entry2Index uint64 = 2
	var entry2Term uint64 = 4
	entry2Data := []byte("entry2")
	entry2 := NewLogEntry(entry2Index, entry2Term, entry2Data)

	log.AppendEntries([]*LogEntry{entry1, entry2})

	// Discard the log entries.
	var discardIndex uint64 = 5
	var discardTerm uint64 = 5
	if err := log.DiscardEntries(discardIndex, discardTerm); err != nil {
		t.Fatalf("error discarding log entries: %s", err.Error())
	}

	// Make sure the last index and last term are correct.
	assert.Equal(t, discardIndex, log.LastIndex(), "last index not correct after discarding log entries")
	assert.Equal(t, discardTerm, log.LastTerm(), "last term not correct after discarding log entries")
}
