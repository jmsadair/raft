package raft

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// TestWithLog checks that the log option only accepts non-nil logs.
func TestWithLog(t *testing.T) {
	options := &options{}

	// Test nil input
	require.Error(t, WithLog(nil)(options))

	// Test valid input
	log := &persistentLog{}
	require.NoError(t, WithLog(log)(options))
}

// TestWithStateStorage checks that the state storage option only accepts non-nil state storage.
func TestWithStateStorage(t *testing.T) {
	options := &options{}

	// Test nil input
	require.Error(t, WithStateStorage(nil)(options))

	// Test valid input
	stateStore := &persistentStateStorage{}
	require.NoError(t, WithStateStorage(stateStore)(options))
}

// TestWithSnapshotStorage checks that the snapshot storage option only accepts non-nil snapshot storage.
func TestWithSnapshotStorage(t *testing.T) {
	options := &options{}

	// Test nil input
	require.Error(t, WithSnapshotStorage(nil)(options))

	// Test valid input
	snapshotStore := &persistentSnapshotStorage{}
	require.NoError(t, WithSnapshotStorage(snapshotStore)(options))
}

// TestWithTransport checks that the transport option only accepts non-nil transport.
func TestWithTransport(t *testing.T) {
	options := &options{}

	// Test nil input
	require.Error(t, WithTransport(nil)(options))

	// Test valid input
	transport := &transport{}
	require.NoError(t, WithTransport(transport)(options))
}
