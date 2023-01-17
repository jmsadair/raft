package raft

import (
	"sync"
	"time"

	"github.com/jmsadair/raft/internal/errors"
	logger "github.com/jmsadair/raft/internal/logger"
	pb "github.com/jmsadair/raft/internal/protobuf"
	"github.com/jmsadair/raft/internal/util"
)

type Raft struct {
	id          string
	options     options
	peers       map[string]*Peer
	server      *Server
	log         Log
	storage     Storage
	lastContact time.Time
	state       *raftState
	mu          sync.RWMutex
}

func NewRaft(id string, peers []Peer, server Server, log Log, storage Storage, opts ...Option) (*Raft, error) {
	var options options
	for _, opt := range opts {
		if err := opt(&options); err != nil {
			return nil, errors.WrapError(err, "failed to create new raft: %s", err.Error())
		}
	}

	if options.logger == nil {
		logger, err := logger.NewLogger()
		if err != nil {
			return nil, errors.WrapError(err, "failed to create new raft: %s", err.Error())
		}
		options.logger = logger
	}

	raft := &Raft{
		id:      id,
		options: options,
		server:  &server,
		log:     log,
		storage: storage,
		state:   NewRaftState(),
	}
	raft.state.setState(Stopped)

	return raft, nil
}

func (r *Raft) Id() string {
	return r.id
}

func (r *Raft) Quorum() int {
	return len(r.peers)/2 + 1
}

func (r *Raft) Submit(command any) error {
	panic("Submit: not implemented")
}

func (r *Raft) appendEntries(request *pb.AppendEntriesRequest) *pb.AppendEntriesResponse {
	var prevLogEntry *LogEntry
	var err error

	response := &pb.AppendEntriesResponse{
		Term:    r.state.getCurrentTerm(),
		Success: false,
	}

	if request.GetTerm() < r.state.getCurrentTerm() {
		r.options.logger.Debugf("rejecting request to append entries: out of date term: term = %d, request term = %d",
			r.state.getCurrentTerm(), request.GetTerm())
		return response
	}

	if request.GetTerm() > r.state.getCurrentTerm() {
		r.state.setCurrentTerm(request.GetTerm())
		response.Term = r.state.getCurrentTerm()
		defer r.becomeFollower()
	}

	if prevLogEntry, err = r.log.GetEntry(request.GetPrevLogIndex()); err != nil {
		r.options.logger.Debugf("rejecting request to append entries: previous log entry at index %d does not exist: %s",
			request.GetPrevLogIndex(), err.Error())
		return response
	}

	if prevLogEntry.Term() != request.GetPrevLogTerm() {
		r.options.logger.Debugf("rejecting request to append entries: previous log term does not match: local = %d, remote = %d",
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
			r.options.logger.Fatalf("error truncating log: %s", err.Error())
		}
		toAppend = entries[i:]
		break
	}

	if err = r.log.AppendEntries(toAppend); err != nil {
		r.options.logger.Fatalf("error appending entries to log: %s", err.Error())
	}

	if request.GetLeaderCommit() > r.state.getCommitIndex() {
		r.state.setCommitIndex(util.Min(request.GetLeaderCommit(), r.log.LastIndex()))
	}

	response.Success = true

	r.mu.Lock()
	r.lastContact = time.Now()
	r.mu.Unlock()

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
					r.options.logger.Fatalf("error getting entry from log: %s", err.Error())
					return
				}
				entries = append(entries, entry.Entry())
			}

			request := &pb.AppendEntriesRequest{
				Term:         r.state.getCurrentTerm(),
				LeaderId:     r.Id(),
				PrevLogIndex: prevLogIndex,
				PrevLogTerm:  prevLogTerm,
				Entries:      entries,
				LeaderCommit: r.state.getCommitIndex(),
			}

			response, err := peer.appendEntries(request)
			if err != nil {
				r.options.logger.Errorf("error appending entries to peer: %s", err.Error())
				return
			}

			if response.GetTerm() > r.state.getCurrentTerm() {
				r.state.setCurrentTerm(request.GetTerm())
				response.Term = r.state.getCurrentTerm()
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
		Term:        r.state.getCurrentTerm(),
		VoteGranted: false,
	}

	if request.GetTerm() < r.state.getCurrentTerm() {
		r.options.logger.Debugf("rejected vote request: out of date term: term = %d, candidate term = %d",
			r.state.getCurrentTerm(), request.GetTerm())
		return response
	}

	if request.GetTerm() > r.state.getCurrentTerm() {
		r.state.setCurrentTerm(request.GetTerm())
		response.Term = r.state.getCurrentTerm()
		defer r.becomeFollower()
	}

	if request.GetTerm() < r.log.LastTerm() || (request.GetLastLogTerm() == r.log.LastTerm() && r.log.LastIndex() > request.GetLastLogIndex()) {
		r.options.logger.Debugf("rejecting vote request: out of date log: lastIndex = %d, lastTerm = %d, candidate lastIndex = %d, candidate lastTerm = %d",
			r.log.LastIndex(), r.log.LastTerm(), request.GetLastLogIndex(), request.GetLastLogTerm())
		return response
	}

	r.state.setVotedFor(request.GetCandidateId())
	response.VoteGranted = true
	r.options.logger.Debugf("request for vote granted: %s voted for %s, term = %d", r.id, request.GetCandidateId(), r.state.getCurrentTerm())

	return response
}

func (r *Raft) leaderLoop() {
	heartbeatInterval := r.options.heartbeatInterval
	heartbeat := time.NewTicker(heartbeatInterval)

	for r.state.getState() == Leader {
		select {
		case <-heartbeat.C:
			r.sendAppendEntries()
			heartbeat.Reset(heartbeatInterval)
		default:
			for i := r.state.getCommitIndex(); i < r.log.LastIndex(); i++ {
				if entry, _ := r.log.GetEntry(i); entry == nil || entry.Term() != r.state.getCurrentTerm() {
					break
				}
				matches := 1
				for _, peer := range r.peers {
					if peer.getMatchIndex() >= i {
						matches += 1
					}
				}
				if matches >= r.Quorum() {
					r.state.setCommitIndex(i)
				}
			}
		}
	}
}

func (r *Raft) followerLoop() {
	electionTimeout := r.options.electionTimeout
	electionTimer := util.RandomTimeout(electionTimeout, electionTimeout*2)

	for r.state.getState() == Follower {
		select {
		case <-electionTimer:
			electionTimer = util.RandomTimeout(electionTimeout, electionTimeout*2)
			r.mu.Lock()
			if time.Since(r.lastContact) > electionTimeout {
				r.becomeCandidate()
				r.mu.Unlock()
				return
			}
			r.mu.Unlock()
		default:
		}
	}
}

func (r *Raft) candidateLoop() {
	votesReceived := 1
	r.state.setCurrentTerm(r.state.getCurrentTerm() + 1)

	responses := make(chan *pb.RequestVoteResponse)

	electionTimeout := r.options.electionTimeout
	electionTimer := util.RandomTimeout(electionTimeout, electionTimeout*2)

	for _, peer := range r.peers {
		go func(peer *Peer) {
			request := &pb.RequestVoteRequest{
				CandidateId:  r.Id(),
				Term:         r.state.getCurrentTerm(),
				LastLogIndex: r.log.LastIndex(),
				LastLogTerm:  r.log.LastTerm(),
			}

			response, err := peer.requestVote(request)

			if err != nil {
				r.options.logger.Errorf("error requesting vote from peer %s: %s", peer.id, err.Error())
				return
			}

			responses <- response
		}(peer)
	}

	for r.state.getState() == Candidate {
		select {
		case response := <-responses:
			if response.GetTerm() > r.state.getCurrentTerm() {
				r.state.setCurrentTerm(response.GetTerm())
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
		switch r.state.getState() {
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
	r.state.setState(Candidate)
	r.options.logger.Infof("%s has entered the candidate state", r.id)
}

func (r *Raft) becomeLeader() {
	r.state.setState(Leader)
	for _, peer := range r.peers {
		peer.setNextIndex(r.log.LastIndex() + 1)
		peer.setMatchIndex(0)
	}
	r.options.logger.Infof("%s has entered the leader state", r.id)
}

func (r *Raft) becomeFollower() {
	r.state.setVotedFor("")
	r.state.setState(Follower)
	r.options.logger.Infof("%s has entered the follower state", r.id)
}
