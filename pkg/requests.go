package raft

import pb "github.com/jmsadair/raft/internal/protobuf"

// AppendEntriesRequest is a request invoked by the leader to replicate log entries and also serves as a heartbeat.
type AppendEntriesRequest struct {
	// The leader's ID. Allows followers to redirect clients.
	leaderID string

	// The leader's term.
	term uint64

	// The leader's commit index.
	leaderCommit uint64

	// The index of the log entry immediately preceding the new ones.
	prevLogIndex uint64

	// The term of the log entry immediately preceding the new ones.
	prevLogTerm uint64

	// Contains the log entries to store.
	entries []*LogEntry
}

// AppendEntriesResponse is a response to a request to to replicate log entries.
type AppendEntriesResponse struct {
	// The term of the server that received the request.
	term uint64

	// Indicates whether the request to append entries was successful.
	// True if the request was successful and false otherwise.
	success bool

	// The conflicting index if there is one.
	index uint64
}

// RequestVoteRequest is a request invoked by candidates to gather votes.
type RequestVoteRequest struct {
	// The ID of the candidate requesting the vote.
	candidateID string

	// The candidate's term.
	term uint64

	// The index of the candidate's last log entry.
	lastLogIndex uint64

	// The term of the candidate's last log entry.
	lastLogTerm uint64
}

// RequestVoteResponse is a response to a request for a vote.
type RequestVoteResponse struct {
	// The term of the server that received the request.
	term uint64

	// Indicates whether the vote request was successful. True if
	// the vote has been granted and false otherwise.
	voteGranted bool
}

// InstallSnapshotRequest is invoked by the leader to send a snapshot to a follower.
type InstallSnapshotRequest struct {
	// The leader's ID.
	leaderID string

	// The leader's term.
	term uint64

	// The last index incuded in the snapshot.
	lastIncludedIndex uint64

	// The last term included in the snapshot.
	lastIncludedTerm uint64

	// The state of the state machine in bytes.
	bytes []byte
}

// InstallSnapshotResponse is a response to a snapshot installation.
type InstallSnapshotResponse struct {
	// The term of the server that recieved the request.
	term uint64
}

// Status is the status of a Raft instance.
type Status struct {
	// The ID of the Raft instance.
	ID string

	// The current term.
	Term uint64

	// The current commit index.
	CommitIndex uint64

	// The index of the last log entry applied to the state machine.
	LastApplied uint64

	// The current state of Raft instance.
	State State
}

// makeProtoEntries converts an array of LogEntry instances to an array of protobuf LogEntry instances.
func makeProtoEntries(entries []*LogEntry) []*pb.LogEntry {
	protoEntries := make([]*pb.LogEntry, len(entries))
	for i, entry := range entries {
		protoEntry := &pb.LogEntry{Index: entry.index, Term: entry.term, Data: entry.data}
		protoEntries[i] = protoEntry
	}
	return protoEntries
}

// makeProtoRequestVoteRequest converts a RequestVoteRequest instance to a protobuf RequestVoteRequest instance.
func makeProtoRequestVoteRequest(request RequestVoteRequest) *pb.RequestVoteRequest {
	return &pb.RequestVoteRequest{
		CandidateId:  request.candidateID,
		Term:         request.term,
		LastLogIndex: request.lastLogIndex,
		LastLogTerm:  request.lastLogTerm,
	}
}

// makeRequestVoteResponse converts a protobuf RequestVoteResponse instance to a RequestVoteResponse instance.
func makeRequestVoteResponse(response *pb.RequestVoteResponse) RequestVoteResponse {
	return RequestVoteResponse{
		term:        response.GetTerm(),
		voteGranted: response.GetVoteGranted(),
	}
}

// makeProtoAppendEntriesRequest converts an AppendEntriesRequest instance to a protobuf AppendEntriesRequest instance.
func makeProtoAppendEntriesRequest(request AppendEntriesRequest) *pb.AppendEntriesRequest {
	return &pb.AppendEntriesRequest{
		LeaderId:     request.leaderID,
		Term:         request.term,
		LeaderCommit: request.leaderCommit,
		PrevLogIndex: request.prevLogIndex,
		PrevLogTerm:  request.prevLogTerm,
		Entries:      makeProtoEntries(request.entries),
	}
}

// makeAppendEntriesResponse converts a protobuf AppendEntriesResponse instance to an AppendEntriesResponse instance.
func makeAppendEntriesResponse(response *pb.AppendEntriesResponse) AppendEntriesResponse {
	return AppendEntriesResponse{
		success: response.GetSuccess(),
		term:    response.GetTerm(),
		index:   response.GetIndex(),
	}
}

// makeProtoInstallSnapshotRequest converts an InstallSnapshotRequest instance to a protobuf InstallSnapshotRequest instance.
func makeProtoInstallSnapshotRequest(request InstallSnapshotRequest) *pb.InstallSnapshotRequest {
	return &pb.InstallSnapshotRequest{
		Leader:            request.leaderID,
		Term:              request.term,
		LastIncludedIndex: request.lastIncludedIndex,
		LastIncludedTerm:  request.lastIncludedTerm,
		Data:              request.bytes,
	}
}

// makeInstallSnapshotResponse converts an protobuf InstallSnapshotResponse instance to a InstallSnapshotResponse instance.
func makeInstallSnapshotResponse(response *pb.InstallSnapshotResponse) InstallSnapshotResponse {
	return InstallSnapshotResponse{
		term: response.GetTerm(),
	}
}

// makeEntries converts an array of protobuf LogEntry instances to an array of LogEntry instances.
func makeEntries(protoEntries []*pb.LogEntry) []*LogEntry {
	entries := make([]*LogEntry, len(protoEntries))
	for i, protoEntry := range protoEntries {
		entry := &LogEntry{index: protoEntry.GetIndex(), term: protoEntry.GetTerm(), data: protoEntry.GetData()}
		entries[i] = entry
	}
	return entries
}

// makeRequestVoteRequest converts a protobuf RequestVoteRequest instance to a RequestVoteRequest instance.
func makeRequestVoteRequest(request *pb.RequestVoteRequest) RequestVoteRequest {
	return RequestVoteRequest{
		candidateID:  request.GetCandidateId(),
		term:         request.GetTerm(),
		lastLogIndex: request.GetLastLogIndex(),
		lastLogTerm:  request.GetLastLogTerm(),
	}
}

// makeProtoRequestVoteResponse converts a RequestVoteResponse instance to a protobuf RequestVoteResponse instance.
func makeProtoRequestVoteResponse(response RequestVoteResponse) *pb.RequestVoteResponse {
	return &pb.RequestVoteResponse{
		Term:        response.term,
		VoteGranted: response.voteGranted,
	}
}

// makeAppendEntriesRequest converts a protobuf AppendEntriesRequest instance to an AppendEntriesRequest instance.
func makeAppendEntriesRequest(request *pb.AppendEntriesRequest) AppendEntriesRequest {
	return AppendEntriesRequest{
		leaderID:     request.GetLeaderId(),
		term:         request.GetTerm(),
		leaderCommit: request.GetLeaderCommit(),
		prevLogIndex: request.GetPrevLogIndex(),
		prevLogTerm:  request.GetPrevLogTerm(),
		entries:      makeEntries(request.GetEntries()),
	}
}

// makeProtoAppendEntriesResponse converts an AppendEntriesResponse instance to a protobuf AppendEntriesResponse instance.
func makeProtoAppendEntriesResponse(response AppendEntriesResponse) *pb.AppendEntriesResponse {
	return &pb.AppendEntriesResponse{
		Success: response.success,
		Term:    response.term,
		Index:   response.index,
	}
}

// makeInstallSnapshotRequest converts a protobuf InstallSnapshotRequest instance to a InstallSnapshotRequest instance.
func makeInstallSnapshotRequest(request *pb.InstallSnapshotRequest) InstallSnapshotRequest {
	return InstallSnapshotRequest{
		leaderID:          request.GetLeader(),
		term:              request.GetTerm(),
		lastIncludedIndex: request.GetLastIncludedIndex(),
		lastIncludedTerm:  request.GetLastIncludedTerm(),
		bytes:             request.GetData(),
	}
}

// makeProtoInstallSnapshotResponse converts an InstallSnapshotResponse instance to a protobuf InstallSnapshotResponse instance.
func makeProtoInstallSnapshotResponse(response InstallSnapshotResponse) *pb.InstallSnapshotResponse {
	return &pb.InstallSnapshotResponse{
		Term: response.term,
	}
}
