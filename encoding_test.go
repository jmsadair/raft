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

	require.Equal(t, entry.Index, decodedEntry.Index)
	require.Equal(t, entry.Term, decodedEntry.Term)
	require.Equal(t, entry.Data, decodedEntry.Data)
	require.Equal(t, entry.EntryType, decodedEntry.EntryType)
}

func TestStorageEncoderDecoder(t *testing.T) {
	state := persistentState{term: 1, votedFor: "test"}
	buf := new(bytes.Buffer)

	require.NoError(t, encodePersistentState(buf, &state))

	decodedState, err := decodePersistentState(buf)
	require.NoError(t, err)

	require.Equal(t, state.term, decodedState.term)
	require.Equal(t, state.votedFor, decodedState.votedFor)
}
