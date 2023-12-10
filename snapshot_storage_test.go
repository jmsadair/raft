package raft

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSnapshotStore(t *testing.T) {
	tmpDir := t.TempDir()
	storageFile := tmpDir + "/test-snap-storage.bin"
	snapshotStore := NewSnapshotStorage(storageFile)

	require.NoError(t, snapshotStore.Open())
	require.NoError(t, snapshotStore.Replay())
	defer func() { require.NoError(t, snapshotStore.Close()) }()

	snapshot1 := NewSnapshot(1, 1, []byte("test1"))
	require.NoError(t, snapshotStore.SaveSnapshot(snapshot1))

	last1, err := snapshotStore.LastSnapshot()
	require.NoError(t, err)
	require.NotNil(t, last1)
	validateSnapshot(t, snapshot1, last1)

	snapshot2 := NewSnapshot(2, 2, []byte("test2"))
	require.NoError(t, snapshotStore.SaveSnapshot(snapshot2))

	last2, err := snapshotStore.LastSnapshot()
	require.NoError(t, err)
	require.NotNil(t, last2)
	validateSnapshot(t, snapshot2, last2)

	snapshots, err := snapshotStore.ListSnapshots()
	require.NoError(t, err)
	require.NotNil(t, snapshots)
	require.Len(t, snapshots, 2)

	require.NoError(t, snapshotStore.Close())
	require.NoError(t, snapshotStore.Open())
	require.NoError(t, snapshotStore.Replay())

	last2, err = snapshotStore.LastSnapshot()
	require.NoError(t, err)
	require.NotNil(t, last2)
	validateSnapshot(t, snapshot2, last2)

	snapshots, err = snapshotStore.ListSnapshots()
	require.NoError(t, err)
	require.NotNil(t, snapshots)
	require.Len(t, snapshots, 2)
}
