package raft

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPersistentStorageSetGet(t *testing.T) {
	tmpDir := t.TempDir()
	storageFile := tmpDir + "/test-storage.bin"
	storage := NewStateStorage(storageFile)

	require.NoError(t, storage.Open())

	term := uint64(1)
	votedFor := "test"
	require.NoError(t, storage.SetState(term, votedFor))

	require.NoError(t, storage.Close())
	require.NoError(t, storage.Open())
	require.NoError(t, storage.Replay())
	defer func() { require.NoError(t, storage.Close()) }()

	recoveredTerm, recoveredVotedFor, err := storage.State()

	require.NoError(t, err)
	require.Equal(t, term, recoveredTerm)
	require.Equal(t, votedFor, recoveredVotedFor)
}
