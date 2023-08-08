package raft

import (
	"github.com/jmsadair/raft/internal/logger"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
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

// TestWithMaxEntriesPerRPC checks that the max entries per RPC option only accepts values within the
// defined range.
func TestWithMaxEntriesPerRPC(t *testing.T) {
	options := &options{}

	// Test minimum bound
	require.Error(t, WithMaxEntriesPerRPC(minMaxEntriesPerRPC-1)(options))

	// Test maximum bound
	require.Error(t, WithMaxEntriesPerRPC(maxMaxEntriesPerRPC+1)(options))

	// Test valid input
	require.NoError(t, WithMaxEntriesPerRPC(200)(options))
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

// TestWithLogger checks that the logger option only accepts non-nil logs.
func TestWithLogger(t *testing.T) {
	options := &options{}

	// Test nil input
	require.Error(t, WithLogger(nil)(options))

	// Test valid input
	testLogger, err := logger.NewLogger()
	require.NoError(t, err)
	require.NoError(t, WithLogger(testLogger)(options))
}
