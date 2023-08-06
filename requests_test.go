package raft

import (
	pb "github.com/jmsadair/raft/internal/protobuf"
	"github.com/stretchr/testify/require"
	"testing"
)

// TestMakeProtoEntries checks that an array of log entries is correctly converted
// to an array of protobuf log entries.
func TestMakeProtoEntries(t *testing.T) {
	logEntries := []*LogEntry{
		{Index: 1, Term: 2, Data: []byte("entry1")},
		{Index: 2, Term: 3, Data: []byte("entry2")},
	}

	protoEntries := makeProtoEntries(logEntries)

	require.Equal(t, len(logEntries), len(protoEntries))
	for i, entry := range logEntries {
		require.Equal(t, entry.Index, protoEntries[i].GetIndex())
		require.Equal(t, entry.Term, protoEntries[i].GetTerm())
		require.Equal(t, entry.Data, protoEntries[i].GetData())
	}
}

// TestMakeProtoRequestVoteRequest checks that a RequestVoteRequest is correctly converted to a
// protobuf RequestVoteRequest.
func TestMakeProtoRequestVoteRequest(t *testing.T) {
	req := RequestVoteRequest{
		CandidateID:  "candidate",
		Term:         2,
		LastLogIndex: 3,
		LastLogTerm:  4,
	}

	protoReq := makeProtoRequestVoteRequest(req)

	require.Equal(t, req.CandidateID, protoReq.GetCandidateId())
	require.Equal(t, req.Term, protoReq.GetTerm())
	require.Equal(t, req.LastLogIndex, protoReq.GetLastLogIndex())
	require.Equal(t, req.LastLogTerm, protoReq.GetLastLogTerm())
}

// TestMakeRequestVoteResponse checks that a protobuf RequestVoteRequest is correctly converted to a
// RequestVoteRequest.
func TestMakeRequestVoteResponse(t *testing.T) {
	protoResponse := &pb.RequestVoteResponse{
		Term:        1,
		VoteGranted: true,
	}

	response := makeRequestVoteResponse(protoResponse)

	require.Equal(t, protoResponse.GetTerm(), response.Term)
	require.Equal(t, protoResponse.GetVoteGranted(), response.VoteGranted)
}

// TestMakeProtoAppendEntriesRequest checks that a AppendEntriesRequest is correctly converted to a
// protobuf AppendEntriesRequest.
func TestMakeProtoAppendEntriesRequest(t *testing.T) {
	req := AppendEntriesRequest{
		LeaderID:     "leader",
		Term:         2,
		LeaderCommit: 3,
		PrevLogIndex: 4,
		PrevLogTerm:  5,
		Entries:      []*LogEntry{{Index: 1, Term: 2, Data: []byte("entry1")}},
	}

	protoReq := makeProtoAppendEntriesRequest(req)

	require.Equal(t, req.LeaderID, protoReq.GetLeaderId())
	require.Equal(t, req.Term, protoReq.GetTerm())
	require.Equal(t, req.LeaderCommit, protoReq.GetLeaderCommit())
	require.Equal(t, req.PrevLogIndex, protoReq.GetPrevLogIndex())
	require.Equal(t, req.PrevLogTerm, protoReq.GetPrevLogTerm())
	require.Equal(t, makeProtoEntries(req.Entries), protoReq.GetEntries())
}

// TestMakeAppendEntriesResponse checks that a protobuf AppendEntriesResponse is correctly converted to
// AppendEntriesResponse.
func TestMakeAppendEntriesResponse(t *testing.T) {
	protoResponse := &pb.AppendEntriesResponse{
		Success: true,
		Term:    1,
		Index:   2,
	}

	response := makeAppendEntriesResponse(protoResponse)

	require.Equal(t, protoResponse.GetSuccess(), response.Success)
	require.Equal(t, protoResponse.GetTerm(), response.Term)
	require.Equal(t, protoResponse.GetIndex(), response.Index)
}

// TestMakeProtoInstallSnapshotRequest checks that a InstallSnapshotRequest is correctly converted to a
// protobuf InstallSnapshotRequest.
func TestMakeProtoInstallSnapshotRequest(t *testing.T) {
	req := InstallSnapshotRequest{
		LeaderID:          "leader",
		Term:              2,
		LastIncludedIndex: 3,
		LastIncludedTerm:  4,
		Bytes:             []byte("test"),
	}

	protoReq := makeProtoInstallSnapshotRequest(req)

	require.Equal(t, req.LeaderID, protoReq.GetLeader())
	require.Equal(t, req.Term, protoReq.GetTerm())
	require.Equal(t, req.LastIncludedIndex, protoReq.GetLastIncludedIndex())
	require.Equal(t, req.LastIncludedTerm, protoReq.GetLastIncludedTerm())
	require.Equal(t, req.Bytes, protoReq.GetData())
}

// TestMakeInstallSnapshotResponse checks that a protobuf InstallSnapshotResponse is correctly converted to a
// InstallSnapshotResponse.
func TestMakeInstallSnapshotResponse(t *testing.T) {
	protoResponse := &pb.InstallSnapshotResponse{
		Term: 1,
	}

	response := makeInstallSnapshotResponse(protoResponse)

	require.Equal(t, protoResponse.GetTerm(), response.Term)
}

// TestMakeEntries checks that an array of protobuf log entries is correctly converted to an array
// of log entries.
func TestMakeEntries(t *testing.T) {
	protoEntries := []*pb.LogEntry{
		{Index: 1, Term: 2, Data: []byte("entry1")},
		{Index: 2, Term: 3, Data: []byte("entry2")},
	}

	entries := makeEntries(protoEntries)

	require.Equal(t, len(protoEntries), len(entries))
	for i, protoEntry := range protoEntries {
		require.Equal(t, protoEntry.GetIndex(), entries[i].Index)
		require.Equal(t, protoEntry.GetTerm(), entries[i].Term)
		require.Equal(t, protoEntry.GetData(), entries[i].Data)
	}
}

// TestMakeRequestVoteRequest checks that a RequestVoteRequest is correctly converted to a protobuf
// RequestVoteRequest.
func TestMakeRequestVoteRequest(t *testing.T) {
	protoReq := &pb.RequestVoteRequest{
		CandidateId:  "candidate",
		Term:         2,
		LastLogIndex: 3,
		LastLogTerm:  4,
	}

	req := makeRequestVoteRequest(protoReq)

	require.Equal(t, protoReq.GetCandidateId(), req.CandidateID)
	require.Equal(t, protoReq.GetTerm(), req.Term)
	require.Equal(t, protoReq.GetLastLogIndex(), req.LastLogIndex)
	require.Equal(t, protoReq.GetLastLogTerm(), req.LastLogTerm)
}

// TestMakeProtoRequestVoteResponse checks that a protobuf RequestVoteRequest is correctly converted to a
// RequestVoteRequest.
func TestMakeProtoRequestVoteResponse(t *testing.T) {
	response := RequestVoteResponse{
		Term:        1,
		VoteGranted: true,
	}

	protoResponse := makeProtoRequestVoteResponse(response)

	require.Equal(t, response.Term, protoResponse.GetTerm())
	require.Equal(t, response.VoteGranted, protoResponse.GetVoteGranted())
}

// TestMakeAppendEntriesRequest checks that a protobuf AppendEntriesRequest is correctly converted to a
// AppendEntriesRequest.
func TestMakeAppendEntriesRequest(t *testing.T) {
	protoReq := &pb.AppendEntriesRequest{
		LeaderId:     "leader",
		Term:         2,
		LeaderCommit: 3,
		PrevLogIndex: 4,
		PrevLogTerm:  5,
		Entries:      makeProtoEntries([]*LogEntry{{Index: 1, Term: 2, Data: []byte("entry1")}}),
	}

	req := makeAppendEntriesRequest(protoReq)

	require.Equal(t, protoReq.GetLeaderId(), req.LeaderID)
	require.Equal(t, protoReq.GetTerm(), req.Term)
	require.Equal(t, protoReq.GetLeaderCommit(), req.LeaderCommit)
	require.Equal(t, protoReq.GetPrevLogIndex(), req.PrevLogIndex)
	require.Equal(t, protoReq.GetPrevLogTerm(), req.PrevLogTerm)
	require.Equal(t, makeEntries(protoReq.GetEntries()), req.Entries)
}

// TestMakeProtoAppendEntriesResponse checks that a AppendEntriesResponse is correctly converted to a
// protobuf AppendEntriesResponse.
func TestMakeProtoAppendEntriesResponse(t *testing.T) {
	response := AppendEntriesResponse{
		Success: true,
		Term:    1,
		Index:   2,
	}

	protoResponse := makeProtoAppendEntriesResponse(response)

	require.Equal(t, response.Success, protoResponse.GetSuccess())
	require.Equal(t, response.Term, protoResponse.GetTerm())
	require.Equal(t, response.Index, protoResponse.GetIndex())
}

// TestMakeInstallSnapshotRequest checks that a protobuf InstallSnapshotRequest is correctly converted to a
// InstallSnapshotRequest.
func TestMakeInstallSnapshotRequest(t *testing.T) {
	protoReq := &pb.InstallSnapshotRequest{
		Leader:            "leader",
		Term:              2,
		LastIncludedIndex: 3,
		LastIncludedTerm:  4,
		Data:              []byte("test"),
	}

	req := makeInstallSnapshotRequest(protoReq)

	require.Equal(t, protoReq.GetLeader(), req.LeaderID)
	require.Equal(t, protoReq.GetTerm(), req.Term)
	require.Equal(t, protoReq.GetLastIncludedIndex(), req.LastIncludedIndex)
	require.Equal(t, protoReq.GetLastIncludedTerm(), req.LastIncludedTerm)
	require.Equal(t, protoReq.GetData(), req.Bytes)
}

// TestMakeProtoInstallSnapshotResponse checks that a InstallSnapshotResponse is correctly converted to a
// protobuf InstallSnapshotResponse.
func TestMakeProtoInstallSnapshotResponse(t *testing.T) {
	response := InstallSnapshotResponse{
		Term: 1,
	}

	protoResponse := makeProtoInstallSnapshotResponse(response)

	require.Equal(t, response.Term, protoResponse.GetTerm())
}
