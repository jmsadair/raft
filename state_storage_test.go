package raft

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestStateStorageEncoderDecoder(t *testing.T) {
	state := persistentState{term: 1, votedFor: "test"}
	buf := new(bytes.Buffer)

	require.NoError(t, encodePersistentState(buf, &state))

	decodedState, err := decodePersistentState(buf)
	require.NoError(t, err)

	require.Equal(t, state.term, decodedState.term)
	require.Equal(t, state.votedFor, decodedState.votedFor)
}

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
