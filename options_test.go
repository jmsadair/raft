package raft

import (
	"testing"
	"time"

	"github.com/jmsadair/raft/internal/logger"
	"github.com/stretchr/testify/require"
)

// TestWithElectionTimeout checks that the election timeout option only accepts values within the
// defined range.
func TestWithElectionTimeout(t *testing.T) {
	options := &options{}

	// Test minimum bound
	require.Error(t, WithElectionTimeout(minElectionTimeout-time.Millisecond)(options))

	// Test maximum bound
	require.Error(t, WithElectionTimeout(maxElectionTimeout+time.Millisecond)(options))

	// Test valid input
	require.NoError(t, WithElectionTimeout(500*time.Millisecond)(options))
}

// TestWithHeartbeatInterval checks that the heartbeat interval option only accepts values within the
// defined range.
func TestWithHeartbeatInterval(t *testing.T) {
	options := &options{}

	// Test minimum bound
	require.Error(t, WithHeartbeatInterval(minHeartbeat-time.Millisecond)(options))

	// Test maximum bound
	require.Error(t, WithHeartbeatInterval(maxHeartbeat+time.Millisecond)(options))

	// Test valid input
	require.NoError(t, WithHeartbeatInterval(250*time.Millisecond)(options))
}

// TestWithLeaseDuration checks that the lease duration option only accepts values within the
// defined range.
func TestWithLeaseDuration(t *testing.T) {
	options := &options{}

	// Test minimum bound
	require.Error(t, WithLeaseDuration(minLeaseDuration-time.Millisecond)(options))

	// Test maximum bound
	require.Error(t, WithLeaseDuration(maxLeaseDuration+time.Millisecond)(options))

	// Test valid input
	require.NoError(t, WithLeaseDuration(500*time.Millisecond)(options))
}

// TestWithLogger checks that the logger option only accepts non-nil loggers.
func TestWithLogger(t *testing.T) {
	options := &options{}

	// Test nil input
	require.Error(t, WithLogger(nil)(options))

	// Test valid input
	testLogger, err := logger.NewLogger()
	require.NoError(t, err)
	require.NoError(t, WithLogger(testLogger)(options))
}

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
