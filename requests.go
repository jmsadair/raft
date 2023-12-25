package raft

import pb "github.com/jmsadair/raft/internal/protobuf"

// AppendEntriesRequest is a request invoked by the leader to replicate log entries and also serves as a heartbeat.
type AppendEntriesRequest struct {
	// The leader's ID. Allows followers to redirect clients.
	LeaderID string

	// The leader's Term.
	Term uint64

	// The leader's commit index.
	LeaderCommit uint64

	// The index of the log entry immediately preceding the new ones.
	PrevLogIndex uint64

	// The term of the log entry immediately preceding the new ones.
	PrevLogTerm uint64

	// Contains the log Entries to store (empty for heartbeat).
	Entries []*LogEntry
}

// AppendEntriesResponse is a response to a request to to replicate log entries.
type AppendEntriesResponse struct {
	// The term of the server that received the request.
	Term uint64

	// Indicates whether the request to append entries was successful.
	Success bool

	// The conflicting Index if there is one.
	Index uint64
}

// RequestVoteRequest is a request invoked by candidates to gather votes.
type RequestVoteRequest struct {
	// The ID of the candidate requesting the vote.
	CandidateID string

	// The candidate's term.
	Term uint64

	// The index of the candidate's last log entry.
	LastLogIndex uint64

	// The term of the candidate's last log entry.
	LastLogTerm uint64
}

// RequestVoteResponse is a response to a request for a vote.
type RequestVoteResponse struct {
	// The term of the server that received the request.
	Term uint64

	// Indicates whether the vote request was successful.
	VoteGranted bool
}

// InstallSnapshotRequest is invoked by the leader to send a snapshot to a follower.
type InstallSnapshotRequest struct {
	// The leader's ID.
	LeaderID string

	// The leader's Term.
	Term uint64

	// The snapshot replaces all entries up to and including
	// this index.
	LastIncludedIndex uint64

	// The term associated with the last included index.
	LastIncludedTerm uint64

	// The state of the state machine in Bytes.
	Bytes []byte
}

// InstallSnapshotResponse is a response to a snapshot installation.
type InstallSnapshotResponse struct {
	// The term of the server that received the request.
	Term uint64
}

// makeProtoEntries converts an array of LogEntry instances to an array of protobuf LogEntry instances.
func makeProtoEntries(entries []*LogEntry) []*pb.LogEntry {
	protoEntries := make([]*pb.LogEntry, len(entries))
	for i, entry := range entries {
		protoEntry := &pb.LogEntry{
			Index:     entry.Index,
			Term:      entry.Term,
			Data:      entry.Data,
			EntryType: pb.LogEntry_LogEntryType(entry.EntryType),
		}
		protoEntries[i] = protoEntry
	}
	return protoEntries
}

// makeProtoRequestVoteRequest converts a RequestVoteRequest instance to a protobuf RequestVoteRequest instance.
func makeProtoRequestVoteRequest(request RequestVoteRequest) *pb.RequestVoteRequest {
	return &pb.RequestVoteRequest{
		CandidateId:  request.CandidateID,
		Term:         request.Term,
		LastLogIndex: request.LastLogIndex,
		LastLogTerm:  request.LastLogTerm,
	}
}

// makeRequestVoteResponse converts a protobuf RequestVoteResponse instance to a RequestVoteResponse instance.
func makeRequestVoteResponse(response *pb.RequestVoteResponse) RequestVoteResponse {
	return RequestVoteResponse{
		Term:        response.GetTerm(),
		VoteGranted: response.GetVoteGranted(),
	}
}

// makeProtoAppendEntriesRequest converts an AppendEntriesRequest instance to a protobuf AppendEntriesRequest instance.
func makeProtoAppendEntriesRequest(request AppendEntriesRequest) *pb.AppendEntriesRequest {
	return &pb.AppendEntriesRequest{
		LeaderId:     request.LeaderID,
		Term:         request.Term,
		LeaderCommit: request.LeaderCommit,
		PrevLogIndex: request.PrevLogIndex,
		PrevLogTerm:  request.PrevLogTerm,
		Entries:      makeProtoEntries(request.Entries),
	}
}

// makeAppendEntriesResponse converts a protobuf AppendEntriesResponse instance to an AppendEntriesResponse instance.
func makeAppendEntriesResponse(response *pb.AppendEntriesResponse) AppendEntriesResponse {
	return AppendEntriesResponse{
		Success: response.GetSuccess(),
		Term:    response.GetTerm(),
		Index:   response.GetIndex(),
	}
}

// makeProtoInstallSnapshotRequest converts an InstallSnapshotRequest instance to a protobuf InstallSnapshotRequest instance.
func makeProtoInstallSnapshotRequest(request InstallSnapshotRequest) *pb.InstallSnapshotRequest {
	return &pb.InstallSnapshotRequest{
		Leader:            request.LeaderID,
		Term:              request.Term,
		LastIncludedIndex: request.LastIncludedIndex,
		LastIncludedTerm:  request.LastIncludedTerm,
		Data:              request.Bytes,
	}
}

// makeInstallSnapshotResponse converts an protobuf InstallSnapshotResponse instance to a InstallSnapshotResponse instance.
func makeInstallSnapshotResponse(response *pb.InstallSnapshotResponse) InstallSnapshotResponse {
	return InstallSnapshotResponse{
		Term: response.GetTerm(),
	}
}

// makeEntries converts an array of protobuf LogEntry instances to an array of LogEntry instances.
func makeEntries(protoEntries []*pb.LogEntry) []*LogEntry {
	entries := make([]*LogEntry, len(protoEntries))
	for i, protoEntry := range protoEntries {
		entry := &LogEntry{
			Index:     protoEntry.GetIndex(),
			Term:      protoEntry.GetTerm(),
			Data:      protoEntry.GetData(),
			EntryType: LogEntryType(protoEntry.EntryType),
		}
		entries[i] = entry
	}
	return entries
}

// makeRequestVoteRequest converts a protobuf RequestVoteRequest instance to a RequestVoteRequest instance.
func makeRequestVoteRequest(request *pb.RequestVoteRequest) RequestVoteRequest {
	return RequestVoteRequest{
		CandidateID:  request.GetCandidateId(),
		Term:         request.GetTerm(),
		LastLogIndex: request.GetLastLogIndex(),
		LastLogTerm:  request.GetLastLogTerm(),
	}
}

// makeProtoRequestVoteResponse converts a RequestVoteResponse instance to a protobuf RequestVoteResponse instance.
func makeProtoRequestVoteResponse(response RequestVoteResponse) *pb.RequestVoteResponse {
	return &pb.RequestVoteResponse{
		Term:        response.Term,
		VoteGranted: response.VoteGranted,
	}
}

// makeAppendEntriesRequest converts a protobuf AppendEntriesRequest instance to an AppendEntriesRequest instance.
func makeAppendEntriesRequest(request *pb.AppendEntriesRequest) AppendEntriesRequest {
	return AppendEntriesRequest{
		LeaderID:     request.GetLeaderId(),
		Term:         request.GetTerm(),
		LeaderCommit: request.GetLeaderCommit(),
		PrevLogIndex: request.GetPrevLogIndex(),
		PrevLogTerm:  request.GetPrevLogTerm(),
		Entries:      makeEntries(request.GetEntries()),
	}
}

// makeProtoAppendEntriesResponse converts an AppendEntriesResponse instance to a protobuf AppendEntriesResponse instance.
func makeProtoAppendEntriesResponse(response AppendEntriesResponse) *pb.AppendEntriesResponse {
	return &pb.AppendEntriesResponse{
		Success: response.Success,
		Term:    response.Term,
		Index:   response.Index,
	}
}

// makeInstallSnapshotRequest converts a protobuf InstallSnapshotRequest instance to a InstallSnapshotRequest instance.
func makeInstallSnapshotRequest(request *pb.InstallSnapshotRequest) InstallSnapshotRequest {
	return InstallSnapshotRequest{
		LeaderID:          request.GetLeader(),
		Term:              request.GetTerm(),
		LastIncludedIndex: request.GetLastIncludedIndex(),
		LastIncludedTerm:  request.GetLastIncludedTerm(),
		Bytes:             request.GetData(),
	}
}

// makeProtoInstallSnapshotResponse converts an InstallSnapshotResponse instance to a protobuf InstallSnapshotResponse instance.
func makeProtoInstallSnapshotResponse(
	response InstallSnapshotResponse,
) *pb.InstallSnapshotResponse {
	return &pb.InstallSnapshotResponse{
		Term: response.Term,
	}
}
