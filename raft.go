package raft

import (
	"sync"
	"time"

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

type Raft struct {
	// Timing and logger configuration.
	config *Config

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

	// Time at which server was last contacted.
	lastContact time.Time

	mu sync.Mutex

	logger logger.Logger
}

func (r *Raft) Submit(command any) error {
	panic("Submit: not implemented")
}

func (r *Raft) appendEntries(request *pb.AppendEntriesRequest) *pb.AppendEntriesResponse {
	var lastAppendIndex uint64
	var prevLogEntry *LogEntry
	var err error

	response := &pb.AppendEntriesResponse{Term: r.getCurrentTerm(), Success: false}
	currentTerm := r.getCurrentTerm()

	if request.GetTerm() < currentTerm {
		r.logger.Debugf("rejecting request to append entries: out of date term: term = %d, request term = %d",
			currentTerm, request.GetTerm())
		return response
	}

	if request.GetTerm() > currentTerm {
		r.setCurrentTerm(request.GetTerm())
		response.Term = r.getCurrentTerm()
		defer r.becomeFollower()
	}

	r.leader = request.GetLeaderId()

	if prevLogEntry, err = r.log.GetEntry(request.GetPrevLogIndex()); err != nil {
		r.logger.Debugf("rejecting request to append entries: previous log entry at index %d does not exist: %s",
			request.GetPrevLogIndex(), err.Error())
		return response
	}

	if prevLogEntry.Term() != request.GetPrevLogTerm() {
		r.logger.Debugf("rejecting request to append entries: previous log term does not match: local = %d, remote = %d",
			prevLogEntry.Term(), request.GetPrevLogTerm())
	}

	toAppend := make([]*LogEntry, len(request.GetEntries()))
	for i, entry := range request.GetEntries() {
		toAppend[i] = &LogEntry{entry: entry}
	}

	if lastAppendIndex, err = r.log.AppendEntries(toAppend...); err != nil {
		r.logger.Fatalf("error appending entries to log: %s", err.Error())
	}

	if request.GetLeaderCommit() > r.commitIndex {
		r.commitIndex = util.Min(request.GetLeaderCommit(), lastAppendIndex)
	}

	response.Success = true

	r.setLastContact(time.Now())

	return response
}

func (r *Raft) requestVote(request *pb.RequestVoteRequest) *pb.RequestVoteResponse {
	response := &pb.RequestVoteResponse{Term: r.getCurrentTerm(), VoteGranted: false}

	currentTerm := r.getCurrentTerm()
	votedFor := r.getVotedFor()
	lastTerm := r.log.LastTerm()
	lastIndex := r.log.LastIndex()

	if request.GetTerm() < currentTerm {
		r.logger.Debugf("rejected vote request: out of date term: term = %d, candidate term = %d", currentTerm, request.GetTerm())
		return response
	}

	if request.GetTerm() > currentTerm {
		r.setCurrentTerm(request.GetTerm())
		response.Term = r.getCurrentTerm()
		currentTerm = r.getCurrentTerm()
		defer r.becomeFollower()
	}

	if currentTerm == request.GetTerm() && votedFor != "" && votedFor != request.GetCandidateId() {
		r.logger.Debugf("rejecting vote request: duplicate vote: %s already voted for %s, candidate ID = %s",
			r.id, votedFor, request.GetCandidateId())
		return response
	}

	if request.GetTerm() < lastTerm || (request.GetLastLogTerm() == lastTerm && lastIndex > request.GetLastLogIndex()) {
		r.logger.Debugf("rejecting vote request: out of date log: lastIndex = %d, lastTerm = %d, candidate lastIndex = %d, candidate lastTerm = %d",
			lastIndex, lastTerm, request.GetLastLogIndex(), request.GetLastLogTerm())
		return response
	}

	r.setVotedFor(request.GetCandidateId())
	response.VoteGranted = true

	r.logger.Debugf("request for vote granted: %s voted for %s, term = %d", r.id, request.GetCandidateId(), currentTerm)
	return response
}

func (r *Raft) leaderLoop() {
	panic("leaderLoop: not implemented")
}

func (r *Raft) followerLoop() {
	electionTimeout := util.RandomTimeout(r.config.ElectionTimeout, r.config.ElectionTimeout*2)

	for {
		select {
		case <-electionTimeout:
			electionTimeout = util.RandomTimeout(r.config.ElectionTimeout, r.config.ElectionTimeout*2)
			if time.Since(r.getLastContact()) > r.config.ElectionTimeout {
				r.becomeCandidate()
				return
			}
		default:
			if r.getState() != Follower {
				return
			}
		}
	}
}

func (r *Raft) candidateLoop() {
	votesReceived := 1
	r.setCurrentTerm(r.getCurrentTerm() + 1)

	request := pb.RequestVoteRequest{
		CandidateId:  r.id,
		Term:         r.getCurrentTerm(),
		LastLogIndex: r.log.LastIndex(),
		LastLogTerm:  r.log.LastTerm()}

	responses := make(chan *pb.RequestVoteResponse)
	electionTimeout := util.RandomTimeout(r.config.ElectionTimeout, r.config.ElectionTimeout*2)

	collectPeerVote := func(peer *Peer, responses chan<- *pb.RequestVoteResponse) {
		response, err := peer.RequestVote(&request)
		if err != nil {
			r.logger.Errorf("error requesting vote from peer %s: %s", peer.id, err.Error())
		}
		responses <- response
	}

	for _, peer := range r.peers {
		go collectPeerVote(peer, responses)
	}

	for {
		select {
		case response := <-responses:
			if response.GetTerm() > r.getCurrentTerm() {
				r.setCurrentTerm(response.GetTerm())
				r.becomeFollower()
				return
			}
			if response.VoteGranted {
				votesReceived++
			}
			if votesReceived >= r.quorum() {
				r.becomeLeader()
				return
			}
		case <-electionTimeout:
			return
		default:
			if r.getState() != Candidate {
				return
			}
		}
	}
}

func (r *Raft) mainLoop() {
	for {
		switch r.getState() {
		case Candidate:
			r.candidateLoop()
		case Leader:
			r.leaderLoop()
		case Follower:
			r.followerLoop()
		}
	}
}

func (r *Raft) becomeCandidate() {
	r.setState(Candidate)
	r.logger.Infof("%s has entered the candidate state", r.id)
}

func (r *Raft) becomeLeader() {
	r.setState(Leader)
	r.nextIndex = make(map[string]uint64)
	r.matchIndex = make(map[string]uint64)
	r.logger.Infof("%s has entered the leader state", r.id)
}

func (r *Raft) becomeFollower() {
	r.setVotedFor("")
	r.setState(Follower)
	r.logger.Infof("%s has entered the follower state", r.id)
}

func (r *Raft) quorum() int {
	return len(r.peers)/2 + 1
}

func (r *Raft) setState(state State) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.state = state
}

func (r *Raft) getState() State {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.state
}

func (r *Raft) setCurrentTerm(term uint64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.currentTerm = term
}

func (r *Raft) getCurrentTerm() uint64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.currentTerm
}

func (r *Raft) setVotedFor(votedFor string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.votedFor = votedFor
}

func (r *Raft) getVotedFor() string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.votedFor
}

func (r *Raft) setLastContact(time time.Time) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.lastContact = time
}

func (r *Raft) getLastContact() time.Time {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.lastContact
}
