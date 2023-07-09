package raft

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPersistentStorageSetGet(t *testing.T) {
	tmpDir := t.TempDir()
	storageFile := tmpDir + "/test-storage.bin"
	storage := newPersistentStorage(storageFile)

	require.NoError(t, storage.Open())

	persistentState := &PersistentState{Term: uint64(1), VotedFor: "test"}
	require.NoError(t, storage.SetState(persistentState))

	require.NoError(t, storage.Close())
	require.NoError(t, storage.Open())
	defer func() { require.NoError(t, storage.Close()) }()

	recoveredPersistentState, err := storage.GetState()
	require.NoError(t, err)

	require.Equal(t, persistentState.Term, recoveredPersistentState.Term, "persisted term is incorrect")
	require.Equal(t, persistentState.VotedFor, recoveredPersistentState.VotedFor, "persisted votedFor is incorrect")
}
