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
	config      Config
	id          string
	peers       map[string]*Peer
	server      *raftServer
	state       State
	log         Log
	currentTerm uint64
	votedFor    string
	lastContact time.Time
	logger      logger.Logger
	mu          sync.RWMutex
}

func (r *Raft) Id() string {
	return r.id
}

func (r *Raft) State() State {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.state
}

func (r *Raft) LastContact() time.Time {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.lastContact
}

func (r *Raft) CurrentTerm() uint64 {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.currentTerm
}

func (r *Raft) VotedFor() string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.votedFor
}

func (r *Raft) Quorum() int {
	return len(r.peers)/2 + 1
}

func (r *Raft) Submit(command any) error {
	panic("Submit: not implemented")
}

func (r *Raft) appendEntries(request *pb.AppendEntriesRequest) *pb.AppendEntriesResponse {
	var lastAppendIndex uint64
	var prevLogEntry *LogEntry
	var err error

	response := &pb.AppendEntriesResponse{
		Term:    r.CurrentTerm(),
		Success: false,
	}

	if request.GetTerm() < r.CurrentTerm() {
		r.logger.Debugf("rejecting request to append entries: out of date term: term = %d, request term = %d",
			r.CurrentTerm(), request.GetTerm())
		return response
	}

	if request.GetTerm() > r.CurrentTerm() {
		r.setCurrentTerm(request.GetTerm())
		response.Term = r.CurrentTerm()
		defer r.becomeFollower()
	}

	if prevLogEntry, err = r.log.GetEntry(request.GetPrevLogIndex()); err != nil {
		r.logger.Debugf("rejecting request to append entries: previous log entry at index %d does not exist: %s",
			request.GetPrevLogIndex(), err.Error())
		return response
	}

	if prevLogEntry.Term() != request.GetPrevLogTerm() {
		r.logger.Debugf("rejecting request to append entries: previous log term does not match: local = %d, remote = %d",
			prevLogEntry.Term(), request.GetPrevLogTerm())
		return response
	}

	var toAppend []*LogEntry
	entries := make([]*LogEntry, len(request.GetEntries()))

	for i, entry := range request.GetEntries() {
		entries[i] = &LogEntry{entry: entry}
	}

	for i, entry := range entries {
		if r.log.LastIndex() < entry.Index() {
			toAppend = entries[i:]
			break
		}
		existing, _ := r.log.GetEntry(entry.Index())
		if !existing.IsConflict(entry) {
			continue
		}
		if err = r.log.Truncate(entry.Index()); err != nil {
			r.logger.Fatalf("error truncating log: %s", err.Error())
		}
		toAppend = entries[i:]
		break
	}

	if lastAppendIndex, err = r.log.AppendEntries(toAppend...); err != nil {
		r.logger.Fatalf("error appending entries to log: %s", err.Error())
	}

	if request.GetLeaderCommit() > r.log.CommitIndex() {
		r.log.SetCommitIndex(util.Min(request.GetLeaderCommit(), lastAppendIndex))
	}

	response.Success = true
	r.setLastContact(time.Now())

	return response
}

func (r *Raft) sendAppendEntries() {
	for _, peer := range r.peers {
		go func(peer *Peer) {
			nextIndex := peer.getNextIndex()
			prevLogIndex := r.log.LastIndex()
			prevLogTerm := r.log.LastTerm()

			entries := make([]*pb.LogEntry, 0)
			for index := nextIndex; index <= prevLogIndex; index++ {
				entry, err := r.log.GetEntry(index)
				if err != nil {
					r.logger.Fatalf("error getting entry from log: %s", err.Error())
					return
				}
				entries = append(entries, entry.Entry())
			}

			request := &pb.AppendEntriesRequest{
				Term:         r.CurrentTerm(),
				LeaderId:     r.Id(),
				PrevLogIndex: prevLogIndex,
				PrevLogTerm:  prevLogTerm,
				Entries:      entries,
				LeaderCommit: r.log.CommitIndex(),
			}

			response, err := peer.AppendEntries(request)
			if err != nil {
				r.logger.Errorf("error appending entries to peer: %s", err.Error())
				return
			}

			if response.GetTerm() > r.CurrentTerm() {
				r.setCurrentTerm(request.GetTerm())
				response.Term = r.CurrentTerm()
				r.becomeFollower()
				return
			}

			if !response.GetSuccess() {
				peer.setNextIndex(prevLogIndex - 1)
				return
			}

			peer.setNextIndex(prevLogIndex + uint64(len(entries)))
			peer.setMatchIndex(prevLogIndex + uint64(len(entries)) - 1)
		}(peer)
	}
}

func (r *Raft) requestVote(request *pb.RequestVoteRequest) *pb.RequestVoteResponse {
	response := &pb.RequestVoteResponse{
		Term:        r.CurrentTerm(),
		VoteGranted: false,
	}

	if request.GetTerm() < r.CurrentTerm() {
		r.logger.Debugf("rejected vote request: out of date term: term = %d, candidate term = %d",
			r.CurrentTerm(), request.GetTerm())
		return response
	}

	if request.GetTerm() > r.CurrentTerm() {
		r.setCurrentTerm(request.GetTerm())
		response.Term = r.CurrentTerm()
		defer r.becomeFollower()
	}

	if request.GetTerm() < r.log.LastTerm() || (request.GetLastLogTerm() == r.log.LastTerm() && r.log.LastIndex() > request.GetLastLogIndex()) {
		r.logger.Debugf("rejecting vote request: out of date log: lastIndex = %d, lastTerm = %d, candidate lastIndex = %d, candidate lastTerm = %d",
			r.log.LastIndex(), r.log.LastTerm(), request.GetLastLogIndex(), request.GetLastLogTerm())
		return response
	}

	r.setVotedFor(request.GetCandidateId())
	response.VoteGranted = true
	r.logger.Debugf("request for vote granted: %s voted for %s, term = %d", r.id, request.GetCandidateId(), r.CurrentTerm())

	return response
}

func (r *Raft) leaderLoop() {
	heartbeatInterval := r.config.HeartbeatInterval
	heartbeat := time.NewTicker(heartbeatInterval)

	for r.State() == Leader {
		select {
		case <-heartbeat.C:
			r.sendAppendEntries()
			heartbeat.Reset(heartbeatInterval)
		default:
			for i := r.log.CommitIndex(); i < r.log.LastIndex(); i++ {
				if entry, _ := r.log.GetEntry(i); entry == nil || entry.Term() != r.CurrentTerm() {
					break
				}
				matches := 1
				for _, peer := range r.peers {
					if peer.getMatchIndex() >= i {
						matches += 1
					}
				}
				if matches >= r.Quorum() {
					r.log.SetCommitIndex(i)
				}
			}
		}
	}
}

func (r *Raft) followerLoop() {
	electionTimeout := r.config.ElectionTimeout
	electionTimer := util.RandomTimeout(electionTimeout, electionTimeout*2)

	for r.State() == Follower {
		select {
		case <-electionTimer:
			electionTimer = util.RandomTimeout(electionTimeout, electionTimeout*2)
			if time.Since(r.LastContact()) > electionTimeout {
				r.becomeCandidate()
				return
			}
		default:
		}
	}
}

func (r *Raft) candidateLoop() {
	votesReceived := 1
	r.setCurrentTerm(r.CurrentTerm() + 1)

	responses := make(chan *pb.RequestVoteResponse)

	electionTimeout := r.config.ElectionTimeout
	electionTimer := util.RandomTimeout(electionTimeout, electionTimeout*2)

	for _, peer := range r.peers {
		go func(peer *Peer) {
			request := &pb.RequestVoteRequest{
				CandidateId:  r.Id(),
				Term:         r.CurrentTerm(),
				LastLogIndex: r.log.LastIndex(),
				LastLogTerm:  r.log.LastTerm(),
			}

			response, err := peer.RequestVote(request)

			if err != nil {
				r.logger.Errorf("error requesting vote from peer %s: %s", peer.Id(), err.Error())
				return
			}

			responses <- response
		}(peer)
	}

	for r.State() == Candidate {
		select {
		case response := <-responses:
			if response.GetTerm() > r.CurrentTerm() {
				r.setCurrentTerm(response.GetTerm())
				r.becomeFollower()
				return
			}
			if response.VoteGranted {
				votesReceived++
			}
			if votesReceived >= r.Quorum() {
				r.becomeLeader()
				return
			}
		case <-electionTimer:
			return
		default:
		}
	}
}

func (r *Raft) mainLoop() {
	for {
		switch r.State() {
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
	for _, peer := range r.peers {
		peer.setNextIndex(r.log.LastIndex() + 1)
		peer.setMatchIndex(0)
	}
	r.logger.Infof("%s has entered the leader state", r.id)
}

func (r *Raft) becomeFollower() {
	r.setVotedFor("")
	r.setState(Follower)
	r.logger.Infof("%s has entered the follower state", r.id)
}

func (r *Raft) setLastContact(time time.Time) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.lastContact = time
}

func (r *Raft) setState(state State) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.state = state
}

func (r *Raft) setCurrentTerm(term uint64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.currentTerm = term
}

func (r *Raft) setVotedFor(votedFor string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.votedFor = votedFor
}
