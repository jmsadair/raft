package raft

import (
	"github.com/stretchr/testify/require"
	"testing"
)

// TestNewRaft checks that a newly created raft with no provided options does not have any persisted state and
// has the default options.
func TestNewRaft(t *testing.T) {
	args := makeRaftArgs(t, false, 0)

	raft, err := NewRaft(args.id, args.peers, args.log, args.storage, args.snapshotStorage, args.stateMachine, args.responseCh)
	require.NoError(t, err)

	require.Zero(t, raft.currentTerm)
	require.Zero(t, raft.lastApplied)
	require.Zero(t, raft.lastIncludedIndex)
	require.Zero(t, raft.lastIncludedTerm)
	require.Equal(t, "", raft.votedFor)
	require.Equal(t, Shutdown, raft.state)
	require.Nil(t, raft.lease)

	require.Equal(t, defaultHeartbeat, raft.options.heartbeatInterval)
	require.Equal(t, defaultElectionTimeout, raft.options.electionTimeout)
	require.Equal(t, defaultMaxEntriesPerRPC, raft.options.maxEntriesPerRPC)
	require.Equal(t, defaultLeaseDuration, raft.options.leaseDuration)
	require.NotNil(t, raft.options.logger)
}
