package raft

import (
	logger "github.com/jmsadair/raft/internal/logger"
	pb "github.com/jmsadair/raft/internal/protobuf"
	"github.com/jmsadair/raft/internal/util"
)

type State uint32

const (
	Leader State = iota
	Follower
	Candidate
)

func (s State) String() string {
	switch s {
	case Leader:
		return "leader"
	case Follower:
		return "follower"
	case Candidate:
		return "candidate"
	default:
		panic("invalid state")
	}
}

type Config struct {
	// The minimum election timeout, in milliseconds.
	minElectionTimeout uint64

	// The maximum election timeout, in milliseconds.
	maxElectionTimeout uint64

	// Time between heartbeats, in milliseconds.
	heartbeat uint64
}

type Raft struct {
	// The ID of this raft server.
	id string

	// The current leader of the raft cluster.
	leader string

	// The state of this raft server: leader, follower, candidate.
	state State

	// Maps ID of peer to peer.
	peers map[string]*Peer

	// Persistent state on all servers.
	log         Log
	commitIndex uint64
	lastApplied uint64

	// Volatile state on all servers
	currentTerm uint64
	votedFor    string

	// Volatile state on server that is leader.
	nextIndex  map[string]uint64
	matchIndex map[string]uint64

	logger logger.Logger
}

func (r *Raft) Submit(command any) error {
	panic("Submit: not implemented")
}

func (r *Raft) appendEntries(request *pb.AppendEntriesRequest) *pb.AppendEntriesResponse {
	response := &pb.AppendEntriesResponse{Term: r.currentTerm, Success: false}

	if request.GetTerm() < r.currentTerm {
		r.logger.Debugf("server %s rejected request to append entries: %d < %d (term < currentTerm)",
			r.id, request.GetTerm(), r.currentTerm)
		return response
	}

	if request.GetTerm() > r.currentTerm {
		r.currentTerm = request.GetTerm()
		response.Term = r.currentTerm
		defer r.becomeFollower()
	}

	r.leader = request.GetLeaderId()

	if !r.log.Contains(request.GetPrevLogIndex()) || r.log.GetEntry(request.GetPrevLogIndex()).Term() != request.GetTerm() {
		r.logger.Debugf("server %s rejected request to append entries: prevLogIndex %d does not have term that matches %d",
			r.id, request.GetPrevLogIndex(), request.GetPrevLogTerm())
		return response
	}

	toAppend := make([]*LogEntry, len(request.GetEntries()))
	for i, entry := range request.GetEntries() {
		toAppend[i] = &LogEntry{entry: entry}
	}
	lastAppendIndex := r.log.AppendEntries(toAppend...)

	if request.GetLeaderCommit() > r.commitIndex {
		r.commitIndex = util.Min(request.GetLeaderCommit(), lastAppendIndex)
	}

	response.Success = true

	return response
}

func (r *Raft) requestVote(request *pb.RequestVoteRequest) *pb.RequestVoteResponse {
	response := &pb.RequestVoteResponse{Term: r.currentTerm, VoteGranted: false}

	if request.GetTerm() < r.currentTerm {
		return response
	}

	if request.GetTerm() > r.currentTerm {
		r.currentTerm = request.GetTerm()
		response.Term = r.currentTerm
		defer r.becomeFollower()
	}

	lastTerm := r.log.LastTerm()
	lastIndex := r.log.LastIndex()

	if r.votedFor != "" && r.votedFor != request.GetCandidateId() {
		return response
	}

	if request.GetTerm() < lastTerm || (request.GetLastLogTerm() == lastTerm && lastIndex > request.GetLastLogIndex()) {
		return response
	}

	r.votedFor = request.GetCandidateId()

	response.VoteGranted = true

	return response
}

func (r *Raft) sendAppendEntries() {
	panic("SendAppendEntries: not implemented")
}

func (r *Raft) leaderLoop() {
	panic("leaderLoop: not implemented")
}

func (r *Raft) followerLoop() {
	panic("followerLoop: not implemented")
}

func (r *Raft) candidateLoop() {
	panic("candidateLoop: not implemented")
}

func (r *Raft) mainLoop() {
	panic("mainLoop: not implemented")
}

func (r *Raft) becomeLeader() {
	r.state = Leader
	r.nextIndex = make(map[string]uint64)
	r.matchIndex = make(map[string]uint64)
}

func (r *Raft) becomeFollower() {
	r.votedFor = ""
	r.state = Follower
}

func (r *Raft) becomeCandidate() {
	r.currentTerm += 1
	r.state = Candidate
}
