package raft

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestPersistentStorageSetGet(t *testing.T) {
	tmpDir := t.TempDir()
	storageFile := tmpDir + "/test-storage.bin"
	storage := newPersistentStorage(storageFile)

	if err := storage.Open(); err != nil {
		t.Fatalf("failed to open storage: %s", err.Error())
	}
	persistentState := &PersistentState{Term: uint64(1), VotedFor: "test"}
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
	assert.Equal(t, persistentState.Term, recoveredPersistentState.Term, "persisted term is incorrect")
	assert.Equal(t, persistentState.VotedFor, recoveredPersistentState.VotedFor, "persisted votedFor is incorrect")
}
