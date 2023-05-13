package raft

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestPersistentStorageSetGet(t *testing.T) {
	tmpDir := t.TempDir()
	storageFile := tmpDir + "/test-storage.bin"
	encoder := new(ProtoStorageEncoder)
	decoder := new(ProtoStorageDecoder)
	storage := NewPersistentStorage(storageFile, encoder, decoder)

	if err := storage.Open(); err != nil {
		t.Fatalf("failed to open storage: %s", err.Error())
	}
	persistentState := &PersistentState{term: uint64(1), votedFor: "test"}
	if err := storage.SetState(persistentState); err != nil {
		t.Fatalf("failed to set storage state: %s", err.Error())
	}

	if err := storage.Close(); err != nil {
		t.Fatalf("failed to close storage: %s", err.Error())
	}
	if err := storage.Open(); err != nil {
		t.Fatalf("failed to open storage: %s", err.Error())
	}
	defer storage.Close()

	recoveredPersistentState, err := storage.GetState()
	if err != nil {
		t.Fatalf("failed to get storage state: %s", err.Error())
	}
	assert.Equal(t, persistentState.term, recoveredPersistentState.term, "persisted term is incorrect")
	assert.Equal(t, persistentState.votedFor, recoveredPersistentState.votedFor, "persisted votedFor is incorrect")
}
