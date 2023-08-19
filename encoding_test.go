package raft

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestLogEncoderDecoder(t *testing.T) {
	entry := NewLogEntry(1, 1, []byte("test"), nil)
	buf := new(bytes.Buffer)

	require.NoError(t, encodeLogEntry(buf, entry))

	decodedEntry, err := decodeLogEntry(buf)
	require.NoError(t, err)

	require.Equal(t, entry.Index, decodedEntry.Index)
	require.Equal(t, entry.Term, decodedEntry.Term)
	require.Equal(t, entry.Data, decodedEntry.Data)
}

func TestStorageEncoderDecoder(t *testing.T) {
	persistentState := PersistentState{Term: 1, VotedFor: "test"}
	buf := new(bytes.Buffer)

	require.NoError(t, encodePersistentState(buf, &persistentState))

	decodedState, err := decodePersistentState(buf)
	require.NoError(t, err)

	require.Equal(t, persistentState.Term, decodedState.Term)
	require.Equal(t, persistentState.VotedFor, decodedState.VotedFor)
}

func TestSnapshotEncoderDecoder(t *testing.T) {
	snapshot := Snapshot{LastIncludedIndex: 1, LastIncludedTerm: 1, Data: []byte("test")}
	buf := new(bytes.Buffer)

	require.NoError(t, encodeSnapshot(buf, &snapshot))

	decodedSnapshot, err := decodeSnapshot(buf)
	require.NoError(t, err)

	require.Equal(t, snapshot.LastIncludedIndex, decodedSnapshot.LastIncludedIndex)
	require.Equal(t, snapshot.LastIncludedTerm, decodedSnapshot.LastIncludedTerm)
	require.Equal(t, snapshot.Data, decodedSnapshot.Data)
}
