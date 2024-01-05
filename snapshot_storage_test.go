package raft

import (
	"bytes"
	"io"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSnapshotStorageWriterReader(t *testing.T) {
	tmpDir := t.TempDir()
	store, err := NewSnapshotStorage(tmpDir)
	require.NoError(t, err)

	// Write the first snapshot.
	lastIncludedIndex1 := uint64(1)
	lastIncludedTerm1 := uint64(1)
	data1 := []byte("snapshot1")
	writer, err := store.NewSnapshotFile(lastIncludedIndex1, lastIncludedTerm1)
	require.NoError(t, err)
	n, err := writer.Write(data1)
	require.NoError(t, err)
	require.Equal(t, len(data1), n)
	require.NoError(t, writer.Close())

	// Write the second snapshot.
	lastIncludedIndex2 := uint64(2)
	lastIncludedTerm2 := uint64(2)
	data2 := []byte("snapshot2")
	writer, err = store.NewSnapshotFile(lastIncludedIndex2, lastIncludedTerm2)
	require.NoError(t, err)
	n, err = writer.Write(data2)
	require.NoError(t, err)
	require.Equal(t, len(data2), n)
	require.NoError(t, writer.Close())

	snapshots, err := store.Snapshots()
	require.NoError(t, err)
	require.Len(t, snapshots, 2)

	// Check the first snapshot.
	metadata1 := snapshots[0]
	require.Equal(t, lastIncludedIndex1, metadata1.LastIncludedIndex)
	require.Equal(t, lastIncludedTerm1, metadata1.LastIncludedTerm)
	reader, err := store.SnapshotReader(metadata1.ID)
	require.NoError(t, err)
	var buf bytes.Buffer
	_, err = io.Copy(&buf, reader)
	require.NoError(t, err)
	require.Equal(t, string(data1), buf.String())

	// Check the second snapshot.
	metadata2 := snapshots[1]
	require.Equal(t, lastIncludedIndex2, metadata2.LastIncludedIndex)
	require.Equal(t, lastIncludedTerm2, metadata2.LastIncludedTerm)
	reader, err = store.SnapshotReader(metadata2.ID)
	require.NoError(t, err)
	buf.Reset()
	_, err = io.Copy(&buf, reader)
	require.NoError(t, err)
	require.Equal(t, string(data2), buf.String())
}
