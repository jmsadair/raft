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

// TestAppendEntriesSuccess checks that raft handles a basic AppendEntries request that should be successful
// correctly.
func TestAppendEntriesSuccess(t *testing.T) {
	args := makeRaftArgs(t, false, 0)

	raft, err := NewRaft(args.id, args.peers, args.log, args.storage, args.snapshotStorage, args.stateMachine, args.responseCh)
	require.NoError(t, err)

	raft.currentTerm = 1
	raft.votedFor = "test-leader"
	raft.state = Follower

	entries := []*LogEntry{NewLogEntry(1, 1, []byte("test1"))}
	request := &AppendEntriesRequest{
		Entries:      entries,
		LeaderID:     "test-leader",
		LeaderCommit: 1,
		Term:         1,
		PrevLogIndex: 0,
		PrevLogTerm:  0,
	}
	response := &AppendEntriesResponse{}

	require.NoError(t, raft.AppendEntries(request, response))
	require.True(t, response.Success)
	require.Equal(t, uint64(1), response.Term)

	require.Equal(t, uint64(1), raft.commitIndex)

	entry, err := raft.log.GetEntry(1)
	require.NoError(t, err)
	validateLogEntry(t, entry, 1, 1, []byte("test1"))
}

// TestAppendEntriesConflictSuccess checks that raft correctly handles the case where its log entries and
// the log entries in a request are conflicting (a log entry in the request has a different term than a log
// entry at the same index in the log).
func TestAppendEntriesConflictSuccess(t *testing.T) {
	args := makeRaftArgs(t, false, 0)

	raft, err := NewRaft(args.id, args.peers, args.log, args.storage, args.snapshotStorage, args.stateMachine, args.responseCh)
	require.NoError(t, err)

	raft.currentTerm = 2
	raft.votedFor = "test-leader"
	raft.state = Follower

	entries := []*LogEntry{NewLogEntry(1, 1, []byte("test1")), NewLogEntry(2, 1, []byte("test2"))}
	request := &AppendEntriesRequest{
		Entries:      entries,
		LeaderID:     "test-leader",
		LeaderCommit: 0,
		Term:         2,
		PrevLogIndex: 0,
		PrevLogTerm:  0,
	}
	response := &AppendEntriesResponse{}

	require.NoError(t, raft.AppendEntries(request, response))
	require.True(t, response.Success)
	require.Equal(t, uint64(2), response.Term)

	entries = []*LogEntry{NewLogEntry(1, 1, []byte("test1")), NewLogEntry(2, 2, []byte("test2"))}
	request.Entries = entries
	response = &AppendEntriesResponse{}

	require.NoError(t, raft.AppendEntries(request, response))
	require.True(t, response.Success)
	require.Equal(t, uint64(2), response.Term)

	entry, err := raft.log.GetEntry(1)
	require.NoError(t, err)
	validateLogEntry(t, entry, 1, 1, []byte("test1"))

	entry, err = raft.log.GetEntry(2)
	require.NoError(t, err)
	validateLogEntry(t, entry, 2, 2, []byte("test2"))
}

// TestAppendEntriesLeaderStepDownSuccess checks that a raft instance in the leader state correctly steps down to the
// follower state when it receives an AppendEntries request with a greater term than its own.
func TestAppendEntriesLeaderStepDownSuccess(t *testing.T) {
	args := makeRaftArgs(t, false, 0)

	raft, err := NewRaft(args.id, args.peers, args.log, args.storage, args.snapshotStorage, args.stateMachine, args.responseCh)
	require.NoError(t, err)

	raft.currentTerm = 1
	raft.votedFor = "test-leader"
	raft.state = Leader

	request := &AppendEntriesRequest{
		Entries:      []*LogEntry{},
		LeaderID:     "other-test-leader",
		LeaderCommit: 0,
		Term:         3,
		PrevLogIndex: 0,
		PrevLogTerm:  0,
	}
	response := &AppendEntriesResponse{}

	require.NoError(t, raft.AppendEntries(request, response))
	require.True(t, response.Success)
	require.Equal(t, uint64(3), response.Term)

	require.Equal(t, uint64(3), raft.currentTerm)
	require.Equal(t, Follower, raft.state)
	require.Equal(t, "", raft.votedFor)
}
