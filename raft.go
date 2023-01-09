package raft

import (
	"sync"

	logger "github.com/jmsadair/raft/internal/logger"
	pb "github.com/jmsadair/raft/internal/protobuf"
	"github.com/jmsadair/raft/internal/util"
)

type State uint

const (
	Leader State = iota
	Follower
	Candidate
	Dead
)

func (s State) String() string {
	switch s {
	case Leader:
		return "leader"
	case Follower:
		return "follower"
	case Candidate:
		return "candidate"
	case Dead:
		return "dead"
	default:
		panic("invalid state")
	}
}

type Raft struct {
	// The ID of this raft server.
	id string

	// The state of this raft server: leader, follower, candidate, or dead.
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

	mu sync.Mutex

	logger logger.Logger
}

func (r *Raft) Submit(command any) error {
	panic("Submit: not implemented")
}

func (r *Raft) AppendEntries(request *pb.AppendEntriesRequest) *pb.AppendEntriesResponse {
	if request.GetTerm() < r.currentTerm {
		r.logger.Debugf("server failed to append entries: %d < %d (term < currentTerm)", request.GetTerm(), r.currentTerm)
		return &pb.AppendEntriesResponse{Term: r.currentTerm, Success: false}
	}
	if !r.log.Contains(request.GetPrevLogIndex()) || r.log.GetEntry(request.GetPrevLogIndex()).Term() != request.GetTerm() {
		r.logger.Debugf("server failed to append entries: prevLogIndex %d does not have term that matches %d", request.GetPrevLogIndex(), request.GetPrevLogTerm())
		return &pb.AppendEntriesResponse{Term: r.currentTerm, Success: false}
	}

	toAppend := make([]*LogEntry, len(request.GetEntries()))
	for i, entry := range request.GetEntries() {
		toAppend[i] = &LogEntry{entry: entry}
	}
	lastAppendIndex := r.log.AppendEntries(toAppend...)

	if request.GetLeaderCommit() > r.commitIndex {
		r.commitIndex = util.Min(request.GetLeaderCommit(), lastAppendIndex)
	}

	return &pb.AppendEntriesResponse{Term: r.currentTerm, Success: true}
}

func (r *Raft) SendAppendEntries() {
	panic("SendAppendEntries: not implemented")
}

func (r *Raft) RequestVote(request *pb.RequestVoteRequest) *pb.RequestVoteResponse {
	panic("RequestVote: not implemented")
}

func (r *Raft) SendRequestVote() {
	panic("SendRequestVote: not implemented")
}

func (r *Raft) StartElection() {
	panic("StartElection: not implemented")
}

func (r *Raft) LeaderLoop() {
	panic("LeaderLoop: not implemented")
}

func (r *Raft) FollowerLoop() {
	panic("FollowerLoop: not implemented")
}

func (r *Raft) BecomeLeader() {
	panic("BecomeLeader: not implemented")
}

func (r *Raft) BecomeFollower() {
	panic("BecomeFollower: not implemented")
}

func (r *Raft) BecomeCandidate() {
	panic("BecomeCandidate: not implemented")
}
