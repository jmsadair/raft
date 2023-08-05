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
	raft.votedFor = "leader1"
	raft.state = Follower

	request := &AppendEntriesRequest{
		LeaderID:     "leader1",
		Term:         1,
		LeaderCommit: 1,
		Entries:      []*LogEntry{NewLogEntry(1, 1, []byte("entry1"))},
	}
	response := &AppendEntriesResponse{}

	require.NoError(t, raft.AppendEntries(request, response))
	require.True(t, response.Success)
	require.Equal(t, uint64(1), response.Term)

	require.Equal(t, uint64(1), raft.commitIndex)

	entry, err := raft.log.GetEntry(1)
	require.NoError(t, err)
	validateLogEntry(t, entry, 1, 1, []byte("entry1"))
}

// TestAppendEntriesConflictSuccess checks that raft correctly handles the case where its log entries and
// the log entries in a request are conflicting (a log entry in the request has a different term than a log
// entry at the same index in the log).
func TestAppendEntriesConflictSuccess(t *testing.T) {
	args := makeRaftArgs(t, false, 0)

	raft, err := NewRaft(args.id, args.peers, args.log, args.storage, args.snapshotStorage, args.stateMachine, args.responseCh)
	require.NoError(t, err)

	raft.currentTerm = 2
	raft.votedFor = "leader1"
	raft.state = Follower

	request := &AppendEntriesRequest{
		LeaderID: "leader1",
		Term:     2,
		Entries:  []*LogEntry{NewLogEntry(1, 1, []byte("entry1")), NewLogEntry(2, 1, []byte("entry2"))},
	}
	response := &AppendEntriesResponse{}

	require.NoError(t, raft.AppendEntries(request, response))
	require.True(t, response.Success)
	require.Equal(t, uint64(2), response.Term)

	request.Entries = []*LogEntry{NewLogEntry(1, 1, []byte("entry1")), NewLogEntry(2, 2, []byte("entry2"))}
	response = &AppendEntriesResponse{}

	require.NoError(t, raft.AppendEntries(request, response))
	require.True(t, response.Success)
	require.Equal(t, uint64(2), response.Term)

	entry, err := raft.log.GetEntry(1)
	require.NoError(t, err)
	validateLogEntry(t, entry, 1, 1, []byte("entry1"))

	entry, err = raft.log.GetEntry(2)
	require.NoError(t, err)
	validateLogEntry(t, entry, 2, 2, []byte("entry2"))
}

// TestAppendEntriesLeaderStepDownSuccess checks that a raft instance in the leader state correctly steps down to the
// follower state when it receives an AppendEntries request with a greater term than its own.
func TestAppendEntriesLeaderStepDownSuccess(t *testing.T) {
	args := makeRaftArgs(t, false, 0)

	raft, err := NewRaft(args.id, args.peers, args.log, args.storage, args.snapshotStorage, args.stateMachine, args.responseCh)
	require.NoError(t, err)

	raft.currentTerm = 1
	raft.votedFor = "leader1"
	raft.state = Leader

	request := &AppendEntriesRequest{
		LeaderID: "leader2",
		Term:     3,
		Entries:  []*LogEntry{},
	}
	response := &AppendEntriesResponse{}

	require.NoError(t, raft.AppendEntries(request, response))
	require.True(t, response.Success)
	require.Equal(t, uint64(3), response.Term)

	require.Equal(t, uint64(3), raft.currentTerm)
	require.Equal(t, Follower, raft.state)
	require.Equal(t, "", raft.votedFor)
}

// TestAppendEntriesOutOfDateTermFailure checks that an AppendEntries request received that has a term less than
// the term of server that received it is rejected.
func TestAppendEntriesOutOfDateTermFailure(t *testing.T) {
	args := makeRaftArgs(t, false, 0)

	raft, err := NewRaft(args.id, args.peers, args.log, args.storage, args.snapshotStorage, args.stateMachine, args.responseCh)
	require.NoError(t, err)

	raft.state = Follower
	raft.currentTerm = 2

	request := &AppendEntriesRequest{
		LeaderID: "test-leader",
		Term:     1,
		Entries:  []*LogEntry{},
	}
	response := &AppendEntriesResponse{}

	require.NoError(t, raft.AppendEntries(request, response))
	require.False(t, response.Success)
	require.Equal(t, uint64(2), response.Term)
}

// TestAppendEntriesPrevLogIndexFailure checks that an AppendEntries request is rejected when the log does not contain
// an entry at the previous log index whose term match the previous log term.
func TestAppendEntriesPrevLogIndexFailure(t *testing.T) {
	args := makeRaftArgs(t, false, 0)

	raft, err := NewRaft(args.id, args.peers, args.log, args.storage, args.snapshotStorage, args.stateMachine, args.responseCh)
	require.NoError(t, err)

	raft.state = Follower
	raft.currentTerm = 1
	require.NoError(t, raft.log.AppendEntry(NewLogEntry(1, 1, []byte("test"))))

	request := &AppendEntriesRequest{
		LeaderID:     "leader",
		Term:         1,
		Entries:      []*LogEntry{},
		PrevLogTerm:  2,
		PrevLogIndex: 1,
	}
	response := &AppendEntriesResponse{}

	require.NoError(t, raft.AppendEntries(request, response))
	require.False(t, response.Success)
	require.Equal(t, uint64(1), response.Term)
}

// TestAppendEntriesShutdownFailure checks that an error is returned when an AppendEntries request is received
// by a server in the shutdown state.
func TestAppendEntriesShutdownFailure(t *testing.T) {
	args := makeRaftArgs(t, false, 0)

	raft, err := NewRaft(args.id, args.peers, args.log, args.storage, args.snapshotStorage, args.stateMachine, args.responseCh)
	require.NoError(t, err)

	request := &AppendEntriesRequest{
		LeaderID: "leader1",
		Term:     1,
		Entries:  []*LogEntry{},
	}
	response := &AppendEntriesResponse{}

	require.Error(t, raft.AppendEntries(request, response))
	require.False(t, response.Success)
}

// TestRequestVoteSuccess checks that raft handles a RequestVote request that should be successful correctly.
func TestRequestVoteSuccess(t *testing.T) {
	args := makeRaftArgs(t, false, 0)

	raft, err := NewRaft(args.id, args.peers, args.log, args.storage, args.snapshotStorage, args.stateMachine, args.responseCh)
	require.NoError(t, err)

	raft.currentTerm = 1
	raft.state = Follower

	request := &RequestVoteRequest{
		CandidateID: "candidate1",
		Term:        1,
	}
	response := &RequestVoteResponse{}

	require.NoError(t, raft.RequestVote(request, response))
	require.True(t, response.VoteGranted)
	require.Equal(t, uint64(1), response.Term)

	require.Equal(t, "candidate1", raft.votedFor)
}

// TestRequestVoteLeaderStepDownSuccess checks that a raft instance in the leader state correctly steps down to the
// follower state when it receives an AppendEntries request with a greater term than its own.
func TestRequestVoteLeaderStepDownSuccess(t *testing.T) {
	args := makeRaftArgs(t, false, 0)

	raft, err := NewRaft(args.id, args.peers, args.log, args.storage, args.snapshotStorage, args.stateMachine, args.responseCh)
	require.NoError(t, err)

	raft.currentTerm = 1
	raft.state = Leader

	request := &RequestVoteRequest{
		CandidateID: "candidate1",
		Term:        2,
	}
	response := &RequestVoteResponse{}

	require.NoError(t, raft.RequestVote(request, response))
	require.True(t, response.VoteGranted)
	require.Equal(t, uint64(2), response.Term)

	require.Equal(t, uint64(2), raft.currentTerm)
	require.Equal(t, Follower, raft.state)
	require.Equal(t, "candidate1", raft.votedFor)
}

// TestRequestVoteAlreadyVotedSuccess checks that a raft instance that receives a RequestVote request from a server
// that it already voted for grants it vote again.
func TestRequestVoteAlreadyVotedSuccess(t *testing.T) {
	args := makeRaftArgs(t, false, 0)

	raft, err := NewRaft(args.id, args.peers, args.log, args.storage, args.snapshotStorage, args.stateMachine, args.responseCh)
	require.NoError(t, err)

	raft.currentTerm = 1
	raft.votedFor = "candidate1"
	raft.state = Leader

	request := &RequestVoteRequest{
		CandidateID: "candidate1",
		Term:        1,
	}
	response := &RequestVoteResponse{}

	require.NoError(t, raft.RequestVote(request, response))
	require.True(t, response.VoteGranted)
}

// TestRequestVoteAlreadyVotedFailure checks that a raft instance that receives a RequestVote request from a different
// server after it already voted for another server refuses to grant its vote.
func TestRequestVoteAlreadyVotedFailure(t *testing.T) {
	args := makeRaftArgs(t, false, 0)

	raft, err := NewRaft(args.id, args.peers, args.log, args.storage, args.snapshotStorage, args.stateMachine, args.responseCh)
	require.NoError(t, err)

	raft.currentTerm = 1
	raft.votedFor = "candidate1"
	raft.state = Follower

	request := &RequestVoteRequest{
		CandidateID: "candidate2",
		Term:        1,
	}
	response := &RequestVoteResponse{}

	require.NoError(t, raft.RequestVote(request, response))
	require.False(t, response.VoteGranted)

	require.Equal(t, "candidate1", raft.votedFor)
}

// TestRequestVoteOutOfDateTermFailure checks that a RequestVote request received that has a term less than
// the term of server that received it is rejected.
func TestRequestVoteOutOfDateTermFailure(t *testing.T) {
	args := makeRaftArgs(t, false, 0)

	raft, err := NewRaft(args.id, args.peers, args.log, args.storage, args.snapshotStorage, args.stateMachine, args.responseCh)
	require.NoError(t, err)

	raft.currentTerm = 2
	raft.votedFor = "candidate1"
	raft.state = Follower

	request := &RequestVoteRequest{
		CandidateID: "candidate2",
		Term:        1,
	}
	response := &RequestVoteResponse{}

	require.NoError(t, raft.RequestVote(request, response))
	require.False(t, response.VoteGranted)

	require.Equal(t, "candidate1", raft.votedFor)
}

// TestRequestVoteOutOfDateLogFailure checks that a RequestVote request received from a server with an out-of-date
// log is rejected.
func TestRequestVoteOutOfDateLogFailure(t *testing.T) {
	args := makeRaftArgs(t, false, 0)

	raft, err := NewRaft(args.id, args.peers, args.log, args.storage, args.snapshotStorage, args.stateMachine, args.responseCh)
	require.NoError(t, err)

	raft.currentTerm = 2
	raft.votedFor = "candidate1"
	raft.state = Follower
	require.NoError(t, raft.log.AppendEntries([]*LogEntry{NewLogEntry(2, 2, []byte("entry1"))}))

	request := &RequestVoteRequest{
		CandidateID:  "candidate2",
		Term:         2,
		LastLogIndex: 2,
		LastLogTerm:  1,
	}
	response := &RequestVoteResponse{}

	require.NoError(t, raft.RequestVote(request, response))
	require.False(t, response.VoteGranted)

	require.Equal(t, "candidate1", raft.votedFor)
}

// TestRequestVoteShutdownFailure checks that a RequestVote request received by a server in the shutdown state
// is rejected and an error is returned.
func TestRequestVoteShutdownFailure(t *testing.T) {
	args := makeRaftArgs(t, false, 0)

	raft, err := NewRaft(args.id, args.peers, args.log, args.storage, args.snapshotStorage, args.stateMachine, args.responseCh)
	require.NoError(t, err)

	request := &RequestVoteRequest{
		CandidateID: "candidate",
		Term:        1,
	}
	response := &RequestVoteResponse{}

	require.Error(t, raft.RequestVote(request, response))
	require.False(t, response.VoteGranted)
}
