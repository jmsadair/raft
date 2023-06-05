package raft

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSnapshotStore(t *testing.T) {
	tmpDir := t.TempDir()
	storageFile := tmpDir + "/test-snap-storage.bin"
	snapshotStore := newPersistentSnapshotStorage(storageFile)

	if err := snapshotStore.Open(); err != nil {
		t.Fatalf("error opening snapshot store: %s", err.Error())
	}

	if err := snapshotStore.Replay(); err != nil {
		t.Fatalf("error replaying snapshot store: %s", err.Error())
	}

	snapshot1 := NewSnapshot(1, 1, []byte("test1"))
	if err := snapshotStore.SaveSnapshot(snapshot1); err != nil {
		t.Fatalf("error saving snapshot: %s", err.Error())
	}

	last1, ok := snapshotStore.LastSnapshot()
	if !ok {
		t.Fatalf("expected last snapshot to be valid")
	}

	validateSnapshot(t, snapshot1, &last1)

	snapshot2 := NewSnapshot(2, 2, []byte("test2"))
	if err := snapshotStore.SaveSnapshot(snapshot2); err != nil {
		t.Fatalf("error saving snapshot: %s", err.Error())
	}

	last2, ok := snapshotStore.LastSnapshot()
	if !ok {
		t.Fatalf("expected last snapshot to be valid")
	}

	validateSnapshot(t, snapshot2, &last2)

	snapshots := snapshotStore.ListSnapshots()

	assert.Len(t, snapshots, 2, "incorrect number of snapshots")

	if err := snapshotStore.Close(); err != nil {
		t.Fatalf("error closing snapshot store: %s", err.Error())
	}

	if err := snapshotStore.Open(); err != nil {
		t.Fatalf("error opening snapshot store: %s", err.Error())
	}

	if err := snapshotStore.Replay(); err != nil {
		t.Fatalf("error replaying snapshot store: %s", err.Error())
	}

	last2, ok = snapshotStore.LastSnapshot()
	if !ok {
		t.Fatalf("expected last snapshot to be valid")
	}

	validateSnapshot(t, snapshot2, &last2)

	snapshots = snapshotStore.ListSnapshots()

	assert.Len(t, snapshots, 2, "incorrect number of snapshots")
}