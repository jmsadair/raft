package raft

import (
	"bytes"
	"testing"
	"time"

	"github.com/jmsadair/raft/logging"
	"github.com/stretchr/testify/require"
)

// TestNewRaft checks that a newly created raft with no provided options does not have any persisted state and
// has the default options.
func TestNewRaft(t *testing.T) {
	tmpDir := t.TempDir()

	fsm := newStateMachineMock(false, 0)
	id := "test"
	address := "127.0.0.1:8080"
	raft, err := NewRaft(id, address, fsm, tmpDir)
	require.NoError(t, err)

	require.Zero(t, raft.currentTerm)
	require.Zero(t, raft.lastApplied)
	require.Zero(t, raft.lastIncludedIndex)
	require.Zero(t, raft.lastIncludedTerm)
	require.Empty(t, raft.votedFor)
	require.Equal(t, Shutdown, raft.state)
	require.Equal(t, id, raft.id)
	require.Equal(t, address, raft.address)
	require.NotNil(t, raft.operationManager)

	require.Equal(t, defaultHeartbeat, raft.options.heartbeatInterval)
	require.Equal(t, defaultElectionTimeout, raft.options.electionTimeout)
	require.Equal(t, defaultLeaseDuration, raft.options.leaseDuration)
	require.Equal(t, logging.Info, raft.options.logLevel)
}

// TestAppendEntriesSuccess checks that raft handles a basic AppendEntries
// request that should be successful correctly.
func TestAppendEntriesSuccess(t *testing.T) {
	tmpDir := t.TempDir()

	raft, err := makeRaft("test", "127.0.0.1:8080", tmpDir, false, 0)
	require.NoError(t, err)

	raft.currentTerm = 1
	raft.votedFor = "leader"
	raft.state = Follower

	request := &AppendEntriesRequest{
		LeaderID:     "leader",
		Term:         1,
		LeaderCommit: 1,
		Entries:      []*LogEntry{NewLogEntry(1, 1, []byte("operation1"), OperationEntry)},
	}
	response := &AppendEntriesResponse{}

	require.NoError(t, raft.AppendEntries(request, response))
	require.True(t, response.Success)
	require.Equal(t, request.Term, response.Term)
	require.Equal(t, request.LeaderCommit, raft.commitIndex)

	entry, err := raft.log.GetEntry(1)
	require.NoError(t, err)
	checkLogEntry(t, request.Entries[0], entry)
}

// TestAppendEntriesConflictSuccess checks that raft correctly handles the case where its log entries and
// the log entries in a request are conflicting (a log entry in the request has a different term than a log
// entry at the same index in the log). The log should be truncated and the node should fall back to its
// committed configuration.
func TestAppendEntriesConflictSuccess(t *testing.T) {
	tmpDir := t.TempDir()

	raft, err := makeRaft("1", "127.0.0.0:8080", tmpDir, false, 0)
	require.NoError(t, err)
	defer func() { raft.transport.Shutdown() }()

	raft.currentTerm = 2
	raft.votedFor = "2"
	raft.state = Follower
	raft.committedConfiguration = &Configuration{
		Members: map[string]string{"1": "127.0.0.0:8080", "2": "127.0.0.1:8080"},
		IsVoter: map[string]bool{"1": true, "2": true},
		Index:   1,
	}
	raft.configuration = &Configuration{
		Members: map[string]string{
			"1": "127.0.0.0:8080",
			"2": "127.0.0.1:8080",
			"3": "127.0.0.2:8080",
		},
		IsVoter: map[string]bool{"1": true, "2": true, "3": false},
		Index:   2,
	}

	request := &AppendEntriesRequest{
		LeaderID: "2",
		Term:     2,
		Entries: []*LogEntry{
			NewLogEntry(1, 1, []byte("configuration1"), ConfigurationEntry),
			NewLogEntry(2, 1, []byte("configuration2"), ConfigurationEntry),
			NewLogEntry(3, 1, []byte("operation1"), OperationEntry),
		},
	}
	response := &AppendEntriesResponse{}

	require.NoError(t, raft.AppendEntries(request, response))
	require.True(t, response.Success)
	require.Equal(t, request.Term, response.Term)

	request.Entries = []*LogEntry{
		NewLogEntry(1, 1, []byte("configuration1"), ConfigurationEntry),
		NewLogEntry(2, 2, []byte("operation1"), OperationEntry),
	}
	response = &AppendEntriesResponse{}

	require.NoError(t, raft.AppendEntries(request, response))
	require.True(t, response.Success)
	require.Equal(t, request.Term, response.Term)

	entry, err := raft.log.GetEntry(1)
	require.NoError(t, err)
	checkLogEntry(t, request.Entries[0], entry)

	entry, err = raft.log.GetEntry(2)
	require.NoError(t, err)
	checkLogEntry(t, request.Entries[1], entry)

	require.Equal(t, len(request.Entries), raft.log.Size())

	require.Equal(t, *raft.committedConfiguration, *raft.configuration)
}

// TestAppendEntriesLeaderStepDownSuccess checks that a raft instance in the leader
// state correctly steps down to the follower state when it receives an AppendEntries
// request with a greater term than its own.
func TestAppendEntriesLeaderStepDownSuccess(t *testing.T) {
	tmpDir := t.TempDir()

	raft, err := makeRaft("1", "127.0.0.0:8080", tmpDir, false, 0)
	require.NoError(t, err)

	raft.currentTerm = 1
	raft.state = Leader
	raft.votedFor = "1"

	request := &AppendEntriesRequest{
		LeaderID: "2",
		Term:     3,
		Entries:  []*LogEntry{},
	}
	response := &AppendEntriesResponse{}

	require.NoError(t, raft.AppendEntries(request, response))
	require.True(t, response.Success)
	require.Equal(t, request.Term, response.Term)

	require.Equal(t, request.Term, raft.currentTerm)
	require.Equal(t, Follower, raft.state)
	require.Empty(t, raft.votedFor)
}

// TestAppendEntriesOutOfDateTermFailure checks that an AppendEntries request
// received that has a term less than the term of server that received it is rejected.
func TestAppendEntriesOutOfDateTermFailure(t *testing.T) {
	tmpDir := t.TempDir()

	raft, err := makeRaft("1", "127.0.0.0:8080", tmpDir, false, 0)
	require.NoError(t, err)

	raft.state = Follower
	raft.currentTerm = 2

	request := &AppendEntriesRequest{
		LeaderID: "2",
		Term:     1,
		Entries:  []*LogEntry{},
	}
	response := &AppendEntriesResponse{}

	require.NoError(t, raft.AppendEntries(request, response))
	require.False(t, response.Success)
	require.Equal(t, response.Term, raft.currentTerm)
}

// TestAppendEntriesPrevLogIndexFailure checks that an AppendEntries request is rejected
// when the log does not contain an entry at the previous log index whose term match the
// previous log term.
func TestAppendEntriesPrevLogIndexFailure(t *testing.T) {
	tmpDir := t.TempDir()

	raft, err := makeRaft("1", "127.0.0.0:8080", tmpDir, false, 0)
	require.NoError(t, err)

	raft.state = Follower
	raft.currentTerm = 1

	require.NoError(
		t,
		raft.log.AppendEntry(NewLogEntry(1, 1, []byte("operation1"), OperationEntry)),
	)

	request := &AppendEntriesRequest{
		LeaderID:     "2",
		Term:         1,
		Entries:      []*LogEntry{},
		PrevLogTerm:  2,
		PrevLogIndex: 1,
	}
	response := &AppendEntriesResponse{}

	require.NoError(t, raft.AppendEntries(request, response))
	require.False(t, response.Success)
	require.Equal(t, request.Term, response.Term)
}

// TestAppendEntriesShutdownFailure checks that an error is returned when an AppendEntries
// request is received by a server in the shutdown state.
func TestAppendEntriesShutdownFailure(t *testing.T) {
	tmpDir := t.TempDir()

	raft, err := makeRaft("1", "127.0.0.0:8080", tmpDir, false, 0)
	require.NoError(t, err)

	request := &AppendEntriesRequest{
		LeaderID: "2",
		Term:     1,
		Entries:  []*LogEntry{},
	}
	response := &AppendEntriesResponse{}

	require.Error(t, raft.AppendEntries(request, response))
	require.False(t, response.Success)
}

// TestRequestVoteSuccess checks that raft handles a RequestVote request that
// should be successful correctly.
func TestRequestVoteSuccess(t *testing.T) {
	tmpDir := t.TempDir()

	raft, err := makeRaft("1", "127.0.0.0:8080", tmpDir, false, 0)
	require.NoError(t, err)

	raft.currentTerm = 1
	raft.state = Follower

	request := &RequestVoteRequest{
		CandidateID: "2",
		Term:        1,
		Prevote:     false,
	}
	response := &RequestVoteResponse{}

	require.NoError(t, raft.RequestVote(request, response))
	require.True(t, response.VoteGranted)
	require.Equal(t, request.Term, response.Term)

	require.Equal(t, request.CandidateID, raft.votedFor)
}

// TestPrevoteNoVote checks that a RequestVote request that is for
// a prevote does not cause the receiving node to increment its term
// or cast its vote, even if the request is successful.
func TestPrevoteNoVote(t *testing.T) {
	tmpDir := t.TempDir()

	raft, err := makeRaft("1", "127.0.0.0:8080", tmpDir, false, 0)
	require.NoError(t, err)

	raft.currentTerm = 1
	raft.votedFor = "2"
	raft.state = Follower

	request := &RequestVoteRequest{
		CandidateID: "3",
		Term:        2,
		Prevote:     true,
	}
	response := &RequestVoteResponse{}

	require.NoError(t, raft.RequestVote(request, response))
	require.True(t, response.VoteGranted)
	require.Equal(t, raft.currentTerm, response.Term)
	require.Equal(t, "2", raft.votedFor)
	require.EqualValues(t, 1, raft.currentTerm)
}

// TestRequestVoteLeaderStepDownSuccess checks that a raft instance in the
// leader state correctly steps down to the follower state when it receives
// an AppendEntries request with a greater term than its own.
func TestRequestVoteLeaderStepDownSuccess(t *testing.T) {
	tmpDir := t.TempDir()

	raft, err := makeRaft("1", "127.0.0.0:8080", tmpDir, false, 0)
	require.NoError(t, err)

	raft.currentTerm = 1
	raft.state = Leader

	request := &RequestVoteRequest{
		CandidateID: "2",
		Term:        2,
		Prevote:     false,
	}
	response := &RequestVoteResponse{}

	require.NoError(t, raft.RequestVote(request, response))
	require.True(t, response.VoteGranted)
	require.Equal(t, request.Term, response.Term)

	require.Equal(t, request.Term, raft.currentTerm)
	require.Equal(t, Follower, raft.state)
	require.Equal(t, request.CandidateID, raft.votedFor)
}

// TestRequestVoteAlreadyVotedSuccess checks that a raft instance that receives
// a RequestVote request from a server that it already voted for grants it vote again.
func TestRequestVoteAlreadyVotedSuccess(t *testing.T) {
	tmpDir := t.TempDir()

	raft, err := makeRaft("1", "127.0.0.0:8080", tmpDir, false, 0)
	require.NoError(t, err)

	raft.currentTerm = 1
	raft.votedFor = "2"
	raft.state = Leader

	request := &RequestVoteRequest{
		CandidateID: "2",
		Term:        1,
		Prevote:     false,
	}
	response := &RequestVoteResponse{}

	require.NoError(t, raft.RequestVote(request, response))
	require.True(t, response.VoteGranted)
}

// TestRequestVoteAlreadyVotedFailure checks that a raft instance that receives a
// RequestVote request from a different server after it already voted for another
// server refuses to grant its vote.
func TestRequestVoteAlreadyVotedFailure(t *testing.T) {
	tmpDir := t.TempDir()

	raft, err := makeRaft("1", "127.0.0.0:8080", tmpDir, false, 0)
	require.NoError(t, err)

	raft.currentTerm = 1
	raft.votedFor = "2"
	raft.state = Follower

	request := &RequestVoteRequest{
		CandidateID: "3",
		Term:        1,
		Prevote:     false,
	}
	response := &RequestVoteResponse{}

	require.NoError(t, raft.RequestVote(request, response))
	require.False(t, response.VoteGranted)
	require.Equal(t, "2", raft.votedFor)
}

// TestRequestVoteLeaderContactFailure checks that a node that recieves a
// RequestVote request that has recently been contacted by the leader rejects
// the request.
func TestRequestVoteLeaderContactFailuire(t *testing.T) {
	tmpDir := t.TempDir()

	raft, err := makeRaft("1", "127.0.0.0:8080", tmpDir, false, 0)
	require.NoError(t, err)

	raft.currentTerm = 1
	raft.votedFor = "2"
	raft.state = Follower
	raft.lastContact = time.Now()

	request := &RequestVoteRequest{
		CandidateID: "3",
		Term:        1,
		Prevote:     false,
	}
	response := &RequestVoteResponse{}

	require.NoError(t, raft.RequestVote(request, response))
	require.False(t, response.VoteGranted)
	require.Equal(t, "2", raft.votedFor)
}

// TestRequestVoteOutOfDateTermFailure checks that a RequestVote request
// received that has a term less than the term of server that received
// it is rejected.
func TestRequestVoteOutOfDateTermFailure(t *testing.T) {
	tmpDir := t.TempDir()

	raft, err := makeRaft("1", "127.0.0.0:8080", tmpDir, false, 0)
	require.NoError(t, err)

	raft.currentTerm = 2
	raft.votedFor = "2"
	raft.state = Follower

	request := &RequestVoteRequest{
		CandidateID: "3",
		Term:        1,
		Prevote:     false,
	}
	response := &RequestVoteResponse{}

	require.NoError(t, raft.RequestVote(request, response))
	require.False(t, response.VoteGranted)
	require.Equal(t, "2", raft.votedFor)
}

// TestRequestVoteOutOfDateLogFailure checks that a RequestVote request received
// from a server with an out-of-date log is rejected.
func TestRequestVoteOutOfDateLogFailure(t *testing.T) {
	tmpDir := t.TempDir()

	raft, err := makeRaft("1", "127.0.0.1:8080", tmpDir, false, 0)
	require.NoError(t, err)

	raft.currentTerm = 2
	raft.votedFor = "2"
	raft.state = Follower
	require.NoError(
		t,
		raft.log.AppendEntries(
			[]*LogEntry{NewLogEntry(2, 2, []byte("operation1"), OperationEntry)},
		),
	)

	request := &RequestVoteRequest{
		CandidateID:  "3",
		Term:         2,
		LastLogIndex: 2,
		LastLogTerm:  1,
		Prevote:      false,
	}
	response := &RequestVoteResponse{}

	require.NoError(t, raft.RequestVote(request, response))
	require.False(t, response.VoteGranted)
	require.Equal(t, "2", raft.votedFor)
}

// TestRequestVoteShutdownFailure checks that a RequestVote request received
// by a server in the shutdown state is rejected and an error is returned.
func TestRequestVoteShutdownFailure(t *testing.T) {
	tmpDir := t.TempDir()

	raft, err := makeRaft("test", "127.0.0.1:8080", tmpDir, false, 0)
	require.NoError(t, err)

	request := &RequestVoteRequest{
		CandidateID: "candidate",
		Term:        1,
		Prevote:     false,
	}
	response := &RequestVoteResponse{}

	require.Error(t, raft.RequestVote(request, response))
	require.False(t, response.VoteGranted)
}

// TestInstallSnapshotSuccess checks that raft handles a basic InstallSnapshot
// request that should be successful correctly.
func TestInstallSnapshotSuccess(t *testing.T) {
	tmpDir := t.TempDir()

	raft, err := makeRaft("1", "127.0.0.0:8080", tmpDir, false, 0)
	require.NoError(t, err)
	defer func() { raft.transport.Shutdown() }()

	raft.currentTerm = 1
	raft.followers = make(map[string]*follower)
	raft.state = Follower
	raft.configuration = &Configuration{
		Members: map[string]string{"1": "127.0.0.0:8080", "2": "127.0.0.1:8080"},
		IsVoter: map[string]bool{"1": true, "2": true},
		Index:   1,
	}

	snapshotConfiguration := Configuration{
		Members: map[string]string{
			"1": "127.0.0.0:8080",
			"2": "127.0.0.1:8080",
			"3": "127.0.0.2:8080",
		},
		IsVoter: map[string]bool{"1": true, "2": true, "3": false},
		Index:   2,
	}
	configurationData, err := raft.transport.EncodeConfiguration(&snapshotConfiguration)
	require.NoError(t, err)

	operationData, err := encodeOperations(
		[]Operation{{Bytes: []byte("operation1"), LogIndex: 3, LogTerm: 1}},
	)
	require.NoError(t, err)

	chunk1 := operationData[:len(operationData)/2]
	request1 := &InstallSnapshotRequest{
		LeaderID:          "2",
		Term:              1,
		LastIncludedIndex: 3,
		LastIncludedTerm:  1,
		Bytes:             chunk1,
		Configuration:     configurationData,
		Offset:            0,
		Done:              false,
	}
	response := &InstallSnapshotResponse{}

	require.NoError(t, raft.InstallSnapshot(request1, response))
	require.Equal(t, request1.Term, response.Term)
	require.Zero(t, raft.commitIndex)
	require.Zero(t, raft.lastApplied)
	require.Zero(t, raft.lastIncludedIndex)
	require.Zero(t, raft.lastIncludedTerm)

	chunk2 := operationData[len(operationData)/2:]
	request2 := &InstallSnapshotRequest{
		LeaderID:          "2",
		Term:              1,
		LastIncludedIndex: 2,
		LastIncludedTerm:  1,
		Bytes:             chunk2,
		Configuration:     configurationData,
		Offset:            int64(len(chunk1)),
		Done:              true,
	}

	require.NoError(t, raft.InstallSnapshot(request2, response))
	require.Equal(t, request2.Term, response.Term)
	require.Equal(t, request2.LastIncludedIndex, raft.commitIndex)
	require.Equal(t, request2.LastIncludedIndex, raft.lastApplied)
	require.Equal(t, request2.LastIncludedIndex, raft.lastIncludedIndex)
	require.Equal(t, request2.LastIncludedTerm, raft.lastIncludedTerm)
	require.Equal(t, snapshotConfiguration, *raft.configuration)
	require.Equal(t, snapshotConfiguration, *raft.committedConfiguration)

	fsm := raft.fsm.(*stateMachineMock)
	writer := new(bytes.Buffer)
	require.NoError(t, fsm.Snapshot(writer))
	require.Equal(t, operationData, writer.Bytes())
}

// TestInstallSnapshotLeaderStepDownSuccess checks that a raft instance in the leader
// state correctly steps down to the follower state when it receives an InstallSnapshot
// request with a greater term than its own.
func TestInstallSnapshotLeaderStepDownSuccess(t *testing.T) {
	tmpDir := t.TempDir()

	raft, err := makeRaft("1", "127.0.0.0:8080", tmpDir, false, 0)
	require.NoError(t, err)
	defer func() { raft.transport.Shutdown() }()

	raft.currentTerm = 1
	raft.state = Leader
	raft.followers = make(map[string]*follower)
	raft.configuration = &Configuration{
		Members: map[string]string{"1": "127.0.0.0:8080", "2": "127.0.0.1:8080"},
		IsVoter: map[string]bool{"1": true, "2": true},
		Index:   1,
	}

	snapshotConfiguration := Configuration{
		Members: map[string]string{
			"1": "127.0.0.0:8080",
			"2": "127.0.0.1:8080",
			"3": "127.0.0.2:8080",
		},
		IsVoter: map[string]bool{"1": true, "2": true, "3": false},
		Index:   2,
	}
	configurationData, err := raft.transport.EncodeConfiguration(&snapshotConfiguration)
	require.NoError(t, err)

	operationData, err := encodeOperations(
		[]Operation{{Bytes: []byte("operation1"), LogIndex: 3, LogTerm: 1}},
	)
	require.NoError(t, err)

	request := &InstallSnapshotRequest{
		LeaderID:          "2",
		Term:              2,
		LastIncludedIndex: 3,
		LastIncludedTerm:  1,
		Bytes:             operationData,
		Configuration:     configurationData,
		Offset:            int64(0),
		Done:              true,
	}
	response := &InstallSnapshotResponse{}

	require.NoError(t, raft.InstallSnapshot(request, response))
	require.Equal(t, request.Term, response.Term)
	require.Equal(t, Follower, raft.state)
	require.Equal(t, request.Term, raft.currentTerm)
	require.Equal(t, snapshotConfiguration, *raft.configuration)
	require.Equal(t, snapshotConfiguration, *raft.committedConfiguration)
}

// TestInstallSnapshotOutOfDateTermFailure checks that a InstallSnapshot request
// received that has a term less tham the term of server that received it is rejected.
func TestInstallSnapshotOutOfDateTermFailure(t *testing.T) {
	tmpDir := t.TempDir()

	raft, err := makeRaft("1", "127.0.0.0:8080", tmpDir, false, 0)
	require.NoError(t, err)

	raft.currentTerm = 2
	raft.votedFor = "2"
	raft.state = Follower

	request := &InstallSnapshotRequest{
		LeaderID:          "3",
		Term:              1,
		LastIncludedIndex: 1,
		LastIncludedTerm:  1,
		Bytes:             []byte{},
		Configuration:     []byte{},
		Offset:            int64(0),
		Done:              true,
	}
	response := &InstallSnapshotResponse{}

	require.NoError(t, raft.InstallSnapshot(request, response))
	require.Equal(t, raft.currentTerm, response.Term)
	require.Zero(t, raft.commitIndex)
	require.Zero(t, raft.lastApplied)
	require.Zero(t, raft.lastIncludedIndex)
	require.Zero(t, raft.lastIncludedTerm)
}
