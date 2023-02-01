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
	id               string
	options          options
	peers            []*Peer
	log              Log
	storage          Storage
	fsm              StateMachine
	state            State
	commitIndex      uint64
	lastApplied      uint64
	currentTerm      uint64
	votedFor         string
	lastContact      time.Time
	appendEntriesCh  chan interface{}
	applyCh          chan interface{}
	clientResponseCh chan<- ReplicateResponse
	stateCh          chan interface{}
	shutdownCh       chan interface{}
	mu               sync.Mutex
}

type ReplicateResponse struct {
	Term     uint64
	Index    uint64
	Command  []byte
	Response interface{}
}

type AppendEntriesResponse struct {
	peer       *Peer
	numEntries int
	response   *pb.AppendEntriesResponse
}

func NewRaft(id string, peers []*Peer, log Log, storage Storage, fsm StateMachine, responseCh chan<- ReplicateResponse, opts ...Option) (*Raft, error) {
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

	if options.heartbeatInterval == 0 {
		options.heartbeatInterval = defaultHeartbeat
	}

	if options.electionTimeout == 0 {
		options.electionTimeout = defaultElectionTimeout
	}

	currentTermKey := []byte("currentTerm")
	currentTerm, err := storage.GetUint64(currentTermKey)
	if err != nil {
		return nil, errors.WrapError(err, "failed to restore current term from storage: %s", err.Error())
	}

	votedForKey := []byte("votedFor")
	votedForBytes, err := storage.Get(votedForKey)
	if err != nil {
		return nil, errors.WrapError(err, "failed to restore vote from storage: %s", err.Error())
	}
	votedFor := string(votedForBytes)

	raft := &Raft{
		id:               id,
		options:          options,
		peers:            peers,
		log:              log,
		storage:          storage,
		fsm:              fsm,
		state:            Shutdown,
		currentTerm:      currentTerm,
		votedFor:         votedFor,
		clientResponseCh: responseCh,
	}

	return raft, nil
}

func (r *Raft) Start() {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.state != Shutdown {
		return
	}

	for _, peer := range r.peers {
		err := peer.connect()
		if err != nil {
			r.options.logger.Errorf("error connecting to peer: %s", err.Error())
		}
	}

	r.applyCh = make(chan interface{}, 1)
	r.shutdownCh = make(chan interface{})
	r.appendEntriesCh = make(chan interface{}, 1)
	r.stateCh = make(chan interface{}, 1)
	r.lastContact = time.Now()

	r.becomeFollower(0)

	go r.applyLoop()
	go r.mainLoop()

	r.options.logger.Infof("raft server with ID %s started", r.id)
}

func (r *Raft) Stop() {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.state == Shutdown {
		return
	}

	r.state = Shutdown

	close(r.shutdownCh)
	close(r.applyCh)
	close(r.stateCh)
	close(r.appendEntriesCh)

	for _, peer := range r.peers {
		peer.disconnect()
	}

	r.options.logger.Infof("raft server with ID %s stopped", r.id)
}

func (r *Raft) Replicate(command []byte) (uint64, error) {
	r.mu.Lock()
	if r.state != Leader {
		r.mu.Unlock()
		return 0, errors.WrapError(nil, "%s is not the leader", r.id)
	}
	entry := NewLogEntry(r.log.LastIndex()+1, r.currentTerm, command)
	r.log.AppendEntry(entry)
	r.mu.Unlock()

	r.appendEntriesCh <- struct{}{}

	return entry.Index(), nil
}

func (r *Raft) Quorum() int {
	return len(r.peers)/2 + 1
}

func (r *Raft) Status() (id string, term uint64, commitIndex uint64, state State) {
	r.mu.Lock()
	defer r.mu.Unlock()

	return r.id, r.currentTerm, r.commitIndex, r.state
}

func (r *Raft) appendEntries(request *pb.AppendEntriesRequest) *pb.AppendEntriesResponse {
	r.mu.Lock()
	defer r.mu.Unlock()

	var prevLogEntry *LogEntry
	var err error

	response := &pb.AppendEntriesResponse{
		Term:    r.currentTerm,
		Success: false,
	}

	if request.GetTerm() < r.currentTerm {
		r.options.logger.Debugf("%s rejecting request to append entries: out of date term: term = %d, request term = %d",
			r.id, r.currentTerm, request.GetTerm())
		return response
	}

	if request.GetTerm() > r.currentTerm {
		r.becomeFollower(request.GetTerm())
		response.Term = r.currentTerm
		r.stateCh <- struct{}{}
	}

	if request.GetPrevLogIndex() != 0 {
		if prevLogEntry, err = r.log.GetEntry(request.GetPrevLogIndex()); err != nil {
			r.options.logger.Debugf("%s rejecting request to append entries: previous log entry at index %d does not exist: %s",
				r.id, request.GetPrevLogIndex(), err.Error())
			return response
		}

		if prevLogEntry.Term() != request.GetPrevLogTerm() {
			r.options.logger.Debugf("%s rejecting request to append entries: previous log term does not match: local = %d, remote = %d",
				r.id, prevLogEntry.Term(), request.GetPrevLogTerm())
			return response
		}
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

	if request.GetLeaderCommit() > r.commitIndex {
		r.commitIndex = util.Min(request.GetLeaderCommit(), r.log.LastIndex())
		r.applyCh <- struct{}{}
	}

	response.Success = true
	r.lastContact = time.Now()

	return response
}

func (r *Raft) sendAppendEntries(peerResponseCh chan<- AppendEntriesResponse) {
	for _, peer := range r.peers {
		go func(peer *Peer) {
			r.mu.Lock()
			nextIndex := peer.getNextIndex()
			prevLogIndex := nextIndex - 1
			prevLogTerm := uint64(0)

			if prevEntry, err := r.log.GetEntry(prevLogIndex); err == nil {
				prevLogTerm = prevEntry.Term()
			}

			entries := make([]*pb.LogEntry, 0)
			for index := nextIndex; index <= r.log.LastIndex(); index++ {
				entry, err := r.log.GetEntry(index)
				if err != nil {
					r.options.logger.Fatalf("error getting entry from log: %s", err.Error())
				}
				entries = append(entries, entry.Entry())
			}

			request := &pb.AppendEntriesRequest{
				Term:         r.currentTerm,
				LeaderId:     r.id,
				PrevLogIndex: prevLogIndex,
				PrevLogTerm:  prevLogTerm,
				Entries:      entries,
				LeaderCommit: r.commitIndex,
			}
			r.mu.Unlock()

			response, err := peer.appendEntries(request)
			if err != nil {
				r.options.logger.Errorf("error appending entries to peer: %s", err.Error())
				return
			}
			peerResponseCh <- AppendEntriesResponse{peer: peer, numEntries: len(entries), response: response}
		}(peer)
	}
}

func (r *Raft) requestVote(request *pb.RequestVoteRequest) *pb.RequestVoteResponse {
	r.mu.Lock()
	defer r.mu.Unlock()

	response := &pb.RequestVoteResponse{
		Term:        r.currentTerm,
		VoteGranted: false,
	}

	if request.GetTerm() < r.currentTerm {
		r.options.logger.Debugf("%s rejected vote request: out of date term: term = %d, candidate term = %d",
			r.id, r.currentTerm, request.GetTerm())
		return response
	}

	if request.GetTerm() > r.currentTerm {
		r.becomeFollower(request.GetTerm())
		response.Term = r.currentTerm
		r.stateCh <- struct{}{}
	}

	if r.votedFor != "" && r.votedFor != request.GetCandidateId() {
		r.options.logger.Debugf("%s rejected vote request: already voted: votedFor = %s, term = %d",
			r.id, r.votedFor, r.currentTerm)
		return response
	}

	if request.GetTerm() < r.log.LastTerm() || (request.GetLastLogTerm() == r.log.LastTerm() && r.log.LastIndex() > request.GetLastLogIndex()) {
		r.options.logger.Debugf("%s rejecting vote request: out of date log: lastIndex = %d, lastTerm = %d, candidate lastIndex = %d, candidate lastTerm = %d",
			r.id, r.log.LastIndex(), r.log.LastTerm(), request.GetLastLogIndex(), request.GetLastLogTerm())
		return response
	}

	r.setVotedFor(request.GetCandidateId())

	response.VoteGranted = true

	r.options.logger.Debugf("request for vote granted: %s voted for %s, term = %d", r.id, request.GetCandidateId(), r.currentTerm)

	return response
}

func (r *Raft) installSnapshot(request *pb.InstallSnapshotRequest) *pb.InstallSnapshotResponse {
	panic("installSnapshot: not implemented")
}

func (r *Raft) leaderLoop() {
	peerResponseCh := make(chan AppendEntriesResponse, len(r.peers))
	heartbeatInterval := r.options.heartbeatInterval
	r.sendAppendEntries(peerResponseCh)

	for {
		select {
		case <-time.After(heartbeatInterval):
			r.sendAppendEntries(peerResponseCh)
		case <-r.stateCh:
			r.mu.Lock()
			if r.state != Leader {
				r.mu.Unlock()
				return
			}
			r.mu.Unlock()
		case <-r.appendEntriesCh:
			r.sendAppendEntries(peerResponseCh)
		case peerResponse := <-peerResponseCh:
			r.mu.Lock()
			peer := peerResponse.peer
			response := peerResponse.response
			entriesAppended := peerResponse.numEntries

			if response.GetTerm() > r.currentTerm {
				r.becomeFollower(response.GetTerm())
				r.stateCh <- struct{}{}
			}

			if !response.GetSuccess() {
				if peer.getNextIndex() != 1 {
					peer.setNextIndex(peer.getNextIndex() - 1)
				}
				r.mu.Unlock()
				break
			}

			peer.setNextIndex(peer.getNextIndex() + uint64(entriesAppended))
			peer.setMatchIndex(peer.getNextIndex() - 1)

			oldCommitIndex := r.commitIndex
			for index := r.commitIndex + 1; index <= r.log.LastIndex(); index++ {
				if entry, _ := r.log.GetEntry(index); entry.Term() != r.currentTerm {
					continue
				}
				matches := 1
				for _, peer := range r.peers {
					if peer.getMatchIndex() >= index {
						matches += 1
					}
					if matches >= r.Quorum() {
						r.commitIndex = index
						break
					}
				}
			}
			newCommitIndex := r.commitIndex
			r.mu.Unlock()

			if newCommitIndex != oldCommitIndex {
				r.applyCh <- struct{}{}
				r.appendEntriesCh <- struct{}{}
			}
		case <-r.shutdownCh:
			return
		}
	}
}

func (r *Raft) followerLoop() {
	electionTimeout := r.options.electionTimeout
	electionTimer := util.RandomTimeout(electionTimeout, electionTimeout*2)

	for {
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
		case <-r.stateCh:
			r.mu.Lock()
			if r.state != Follower {
				r.mu.Unlock()
				return
			}
			r.mu.Unlock()
		case <-r.appendEntriesCh:
			r.options.logger.Warnf("ignoring request to replicate: %s is not the leader", r.id)
		case <-r.shutdownCh:
			return
		}
	}
}

func (r *Raft) candidateLoop() {
	votesReceived := 1

	r.mu.Lock()
	r.votedFor = r.id
	r.currentTerm++
	r.mu.Unlock()

	peerResponsesCh := make(chan *pb.RequestVoteResponse, len(r.peers))
	electionTimeout := r.options.electionTimeout
	electionTimer := util.RandomTimeout(electionTimeout, electionTimeout*2)

	for _, peer := range r.peers {
		go func(peer *Peer) {
			r.mu.Lock()
			request := &pb.RequestVoteRequest{
				CandidateId:  r.id,
				Term:         r.currentTerm,
				LastLogIndex: r.log.LastIndex(),
				LastLogTerm:  r.log.LastTerm(),
			}
			r.mu.Unlock()

			response, err := peer.requestVote(request)
			if err != nil {
				r.options.logger.Errorf("error requesting vote from peer %s: %s", peer.id, err.Error())
				return
			}

			peerResponsesCh <- response
		}(peer)
	}

	for {
		select {
		case response := <-peerResponsesCh:
			if response.VoteGranted {
				votesReceived++
			}
			r.mu.Lock()
			if response.GetTerm() > r.currentTerm {
				r.becomeFollower(response.GetTerm())
				r.mu.Unlock()
				return
			}
			if votesReceived >= r.Quorum() {
				r.becomeLeader()
				r.mu.Unlock()
				return
			}
			r.mu.Unlock()
		case <-r.stateCh:
			r.mu.Lock()
			if r.state != Candidate {
				r.mu.Unlock()
				return
			}
			r.mu.Unlock()
		case <-r.shutdownCh:
			return
		case <-electionTimer:
			return
		}
	}
}

func (r *Raft) applyLoop() {
	for range r.applyCh {
		r.mu.Lock()
		responses := make([]ReplicateResponse, r.commitIndex-r.lastApplied)
		for index := r.lastApplied + 1; index <= r.commitIndex; index++ {
			entry, err := r.log.GetEntry(index)
			if err != nil {
				r.options.logger.Fatalf("failed to get log entry: %s", err.Error())
			}
			response := ReplicateResponse{
				Term:     entry.Term(),
				Index:    entry.Index(),
				Command:  entry.Data(),
				Response: r.fsm.Apply(entry.Data()),
			}
			responses[index-r.lastApplied-1] = response
		}
		r.lastApplied = r.commitIndex
		r.mu.Unlock()

		for _, response := range responses {
			r.clientResponseCh <- response
		}
	}
}

func (r *Raft) mainLoop() {
	for {
		r.mu.Lock()
		state := r.state
		r.mu.Unlock()

		switch state {
		case Candidate:
			r.candidateLoop()
		case Leader:
			r.leaderLoop()
		case Follower:
			r.followerLoop()
		case Shutdown:
			return
		}
	}
}

func (r *Raft) becomeCandidate() {
	// Expects mutex to be locked.
	r.state = Candidate
	r.options.logger.Infof("%s has entered the candidate state", r.id)
}

func (r *Raft) becomeLeader() {
	// Expects mutex to be locked.
	r.state = Leader
	for _, peer := range r.peers {
		peer.setNextIndex(r.log.LastIndex() + 1)
		peer.setMatchIndex(0)
	}
	r.options.logger.Infof("%s has entered the leader state", r.id)
}

func (r *Raft) becomeFollower(term uint64) {
	// Expects mutex to be locked.
	r.setCurrentTerm(term)
	r.votedFor = ""
	r.state = Follower
	r.options.logger.Infof("%s has entered the follower state", r.id)
}

func (r *Raft) setCurrentTerm(term uint64) {
	// Expects mutex to be locked.
	currentTermKey := []byte("currentTerm")
	if err := r.storage.SetUint64(currentTermKey, term); err != nil {
		r.options.logger.Fatalf("failed to persist current term to storage: %s", err.Error())
	}
	r.currentTerm = term
}

func (r *Raft) setVotedFor(votedFor string) {
	// Expects mutex to be locked.
	votedForKey := []byte("votedFor")
	if err := r.storage.Set(votedForKey, []byte(votedFor)); err != nil {
		r.options.logger.Fatalf("failed to persist vote to storage: %s", err.Error())
	}
	r.votedFor = votedFor
}
