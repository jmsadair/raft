package raft

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func validateLogEntry(t *testing.T, entry *LogEntry, expectedIndex uint64, expectedTerm uint64, expectedData []byte) {
	assert.Equal(t, expectedIndex, entry.index, "entry has incorrect index")
	assert.Equal(t, expectedTerm, entry.term, "entry has incorrect term")
	assert.Equal(t, expectedData, entry.data, "entry has incorrect data")
}

func TestAppendEntries(t *testing.T) {
	tmpDir := t.TempDir()
	path := tmpDir + "/test-log.bin"
	log := NewPersistentLog(path, NewProtoLogEncoder(), NewProtoLogDecoder())
	if err := log.Open(); err != nil {
		t.Fatalf("error opening log: %s", err.Error())
	}

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

	if err := log.Close(); err != nil {
		t.Fatalf("error closing log: %s", err.Error())
	}

	if err := log.Open(); err != nil {
		t.Fatalf("error opening log: %s", err.Error())
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
	log := NewPersistentLog(path, NewProtoLogEncoder(), NewProtoLogDecoder())
	if err := log.Open(); err != nil {
		t.Fatalf("error opening log: %s", err.Error())
	}
	defer log.Close()

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

	if err := log.Truncate(entry2Index); err != nil {
		t.Fatalf("error truncating log: %s", err.Error())
	}

	entry1, err = log.GetEntry(entry1Index)
	if err != nil {
		t.Fatalf("error getting entry from log: %s", err.Error())
	}
	validateLogEntry(t, entry1, entry1Index, entry1Term, entry1Data)

	assert.Equal(t, log.LastTerm(), entry1Term)
	assert.Equal(t, log.LastIndex(), entry1Index)

	if err := log.Close(); err != nil {
		t.Fatalf("error closing log: %s", err.Error())
	}

	if err := log.Open(); err != nil {
		t.Fatalf("error opening log: %s", err.Error())
	}

	defer log.Close()

	entry1, err = log.GetEntry(entry1Index)
	if err != nil {
		t.Fatalf("error getting entry from log: %s", err.Error())
	}
	validateLogEntry(t, entry1, entry1Index, entry1Term, entry1Data)
}

func TestCompact(t *testing.T) {
	tmpDir := t.TempDir()
	path := tmpDir + "/test-log.bin"
	log := NewPersistentLog(path, NewProtoLogEncoder(), NewProtoLogDecoder())
	if err := log.Open(); err != nil {
		t.Fatalf("error opening log: %s", err.Error())
	}
	defer log.Close()

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

	if err := log.Compact(entry2Index); err != nil {
		t.Fatalf("error compacting log: %s", err.Error())
	}

	entry3, err = log.GetEntry(entry3Index)
	if err != nil {
		t.Fatalf("error getting entry from log: %s", err.Error())
	}
	validateLogEntry(t, entry3, entry3Index, entry3Term, entry3Data)

	assert.Equal(t, log.LastTerm(), entry3Term)
	assert.Equal(t, log.LastIndex(), entry3Index)

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

	if err := log.Close(); err != nil {
		t.Fatalf("error closing log: %s", err.Error())
	}

	if err := log.Open(); err != nil {
		t.Fatalf("error opening log: %s", err.Error())
	}

	defer log.Close()

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
