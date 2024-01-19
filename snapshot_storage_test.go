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
	configuration1 := []byte("configuration1")
	data1 := []byte("snapshot1")
	file1, err := store.NewSnapshotFile(lastIncludedIndex1, lastIncludedTerm1, configuration1)
	require.NoError(t, err)
	n, err := file1.Write(data1)
	require.NoError(t, err)
	require.Equal(t, len(data1), n)
	require.NoError(t, file1.Close())

	// Check the first snapshot.
	file1, err = store.SnapshotFile()
	require.NoError(t, err)
	metadata1 := file1.Metadata()
	require.Equal(t, lastIncludedIndex1, metadata1.LastIncludedIndex)
	require.Equal(t, lastIncludedTerm1, metadata1.LastIncludedTerm)
	require.Equal(t, configuration1, metadata1.Configuration)
	var buf bytes.Buffer
	_, err = io.Copy(&buf, file1)
	require.NoError(t, err)
	require.Equal(t, string(data1), buf.String())
	require.NoError(t, file1.Close())

	// Write the second snapshot.
	lastIncludedIndex2 := uint64(2)
	lastIncludedTerm2 := uint64(2)
	configuration2 := []byte("configuration2")
	data2 := []byte("snapshot2")
	file2, err := store.NewSnapshotFile(lastIncludedIndex2, lastIncludedTerm2, configuration2)
	require.NoError(t, err)
	n, err = file2.Write(data2)
	require.NoError(t, err)
	require.Equal(t, len(data2), n)
	require.NoError(t, file2.Close())

	// Check the second snapshot.
	file2, err = store.SnapshotFile()
	require.NoError(t, err)
	metadata2 := file2.Metadata()
	require.Equal(t, lastIncludedIndex2, metadata2.LastIncludedIndex)
	require.Equal(t, lastIncludedTerm2, metadata2.LastIncludedTerm)
	require.Equal(t, configuration2, metadata2.Configuration)
	buf.Reset()
	_, err = io.Copy(&buf, file2)
	require.NoError(t, err)
	require.Equal(t, string(data2), buf.String())
}
