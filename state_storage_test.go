package raft

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestStateStorageSetGet(t *testing.T) {
	tmpDir := t.TempDir()
	storage, err := NewStateStorage(tmpDir)
	require.NoError(t, err)

	term := uint64(1)
	votedFor := "test"
	require.NoError(t, storage.SetState(term, votedFor))

	newStorage, err := NewStateStorage(tmpDir)
	require.NoError(t, err)
	recoveredTerm, recoveredVotedFor, err := newStorage.State()

	require.NoError(t, err)
	require.Equal(t, term, recoveredTerm)
	require.Equal(t, votedFor, recoveredVotedFor)
}
