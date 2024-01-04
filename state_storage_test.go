package raft

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestStateStorageSetGet(t *testing.T) {
	tmpDir := t.TempDir()
	storage, err := NewStateStorage(tmpDir)
	require.NoError(t, err)

	require.NoError(t, storage.Open())

	term := uint64(1)
	votedFor := "test"
	require.NoError(t, storage.SetState(term, votedFor))

	require.NoError(t, storage.Close())
	require.NoError(t, storage.Open())
	defer func() { require.NoError(t, storage.Close()) }()

	recoveredTerm, recoveredVotedFor, err := storage.State()

	require.NoError(t, err)
	require.Equal(t, term, recoveredTerm)
	require.Equal(t, votedFor, recoveredVotedFor)
}
