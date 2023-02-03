package raft

import (
	"time"

	"github.com/jmsadair/raft/internal/errors"
	logger "github.com/jmsadair/raft/internal/logger"
	pb "github.com/jmsadair/raft/internal/protobuf"
	"github.com/jmsadair/raft/internal/util"
)

type Command struct {
	Bytes []byte
}

type CommandResponse struct {
	Term     uint64
	Index    uint64
	Command  []byte
	Response interface{}
}

type Status struct {
	Id          string
	Term        uint64
	CommitIndex uint64
	LastApplied uint64
	State       State
}

type Raft struct {
	state             RaftState
	id                string
	options           options
	peers             map[string]*Peer
	log               Log
	storage           Storage
	snapshotStorage   SnapshotStorage
	fsm               StateMachine
	appendEntriesCh   chan AppendEntriesRPC
	requestVoteCh     chan RequestVoteRPC
	installSnapshotCh chan InstallSnapshotRPC
	applyCh           chan interface{}
	commandCh         chan interface{}
	commandResponseCh chan<- CommandResponse
	shutdownCh        chan interface{}
}

func NewRaft(id string, peers []*Peer, log Log, storage Storage, snapshotStorage SnapshotStorage, fsm StateMachine, responseCh chan<- CommandResponse, opts ...Option) (*Raft, error) {
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

	if options.snapshotInterval == 0 {
		options.snapshotInterval = defaultSnapshotInterval
	}

	if options.maxEntriesPerSnapshot == 0 {
		options.maxEntriesPerSnapshot = defaultMaxEntriesPerSnapshot
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

	peerLookup := make(map[string]*Peer)
	for _, peer := range peers {
		peerLookup[peer.Id()] = peer
	}

	raft := &Raft{
		id:                id,
		options:           options,
		peers:             peerLookup,
		log:               log,
		storage:           storage,
		snapshotStorage:   snapshotStorage,
		fsm:               fsm,
		commandResponseCh: responseCh,
	}

	raft.state.setCurrentTerm(currentTerm)
	raft.state.setVotedFor(votedFor)
	raft.state.setState(Shutdown)

	if err := raft.restoreFromSnapshot(); err != nil {
		return nil, errors.WrapError(err, "failed to restore raft from snapshot: %s", err.Error())
	}

	return raft, nil
}

func (r *Raft) Start() {
	if r.state.getState() != Shutdown {
		return
	}

	for _, peer := range r.peers {
		err := peer.connect()
		if err != nil {
			r.options.logger.Errorf("error connecting to peer: %s", err.Error())
		}
	}

	r.applyCh = make(chan interface{}, 1)
	r.commandCh = make(chan interface{}, 1)
	r.appendEntriesCh = make(chan AppendEntriesRPC, 1)
	r.requestVoteCh = make(chan RequestVoteRPC, 1)
	r.installSnapshotCh = make(chan InstallSnapshotRPC, 1)
	r.shutdownCh = make(chan interface{})

	r.state.setLastContact(time.Now())

	r.becomeFollower(0)
	go r.snapshotLoop()
	go r.applyLoop()
	go r.mainLoop()

	r.options.logger.Infof("raft server with ID %s started", r.id)
}

func (r *Raft) Stop() {
	if r.state.getState() == Shutdown {
		return
	}
	r.state.setState(Shutdown)

	close(r.shutdownCh)
	close(r.applyCh)
	close(r.commandCh)
	close(r.appendEntriesCh)
	close(r.requestVoteCh)
	close(r.installSnapshotCh)

	for _, peer := range r.peers {
		var err error
		if peer.isConnected() {
			err = peer.disconnect()
		}
		if err != nil {
			r.options.logger.Errorf("error disconnecting from peer: %s", err.Error())
		}
	}

	r.options.logger.Infof("raft server with ID %s stopped", r.id)
}

func (r *Raft) SubmitCommand(command Command) (uint64, error) {
	if r.state.getState() != Leader {
		return 0, errors.WrapError(nil, "%s is not the leader", r.id)
	}

	entry := NewLogEntry(r.log.LastIndex()+1, r.state.getCurrentTerm(), command.Bytes)
	r.log.AppendEntry(entry)

	r.commandCh <- struct{}{}

	return entry.Index(), nil
}

func (r *Raft) Status() Status {
	return Status{
		Id:          r.id,
		Term:        r.state.getCurrentTerm(),
		CommitIndex: r.state.getCommitIndex(),
		LastApplied: r.state.getLastApplied(),
		State:       r.state.getState(),
	}
}

func (r *Raft) appendEntries(request *pb.AppendEntriesRequest) *pb.AppendEntriesResponse {
	var prevLogEntry *LogEntry
	var err error

	response := &pb.AppendEntriesResponse{
		Term:    r.state.getCurrentTerm(),
		Success: false,
	}

	if request.Term < r.state.getCurrentTerm() {
		return response
	}

	if request.Term > r.state.getCurrentTerm() {
		r.becomeFollower(request.Term)
		response.Term = r.state.getCurrentTerm()
	}

	if request.PrevLogIndex != 0 {
		if prevLogEntry, err = r.log.GetEntry(request.PrevLogIndex); err != nil {
			return response
		}

		if prevLogEntry.Term() != request.PrevLogTerm {
			return response
		}
	}

	entries := make([]*LogEntry, len(request.Entries))
	for i, entry := range request.Entries {
		entries[i] = NewLogEntry(entry.GetIndex(), entry.Term, entry.Data)
	}

	var toAppend []*LogEntry
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

	if request.LeaderCommit > r.state.getCommitIndex() {
		r.state.setCommitIndex(util.Min(request.LeaderCommit, r.log.LastIndex()))
		r.applyCh <- struct{}{}
	}

	response.Success = true
	r.state.setLastContact(time.Now())

	return response
}

func (r *Raft) sendAppendEntries(responseCh chan<- AppendEntriesMessage) {
	for _, peer := range r.peers {
		go func(peer *Peer) {
			nextIndex := peer.getNextIndex()
			prevLogIndex := nextIndex - 1
			prevLogTerm := uint64(0)

			if prevEntry, err := r.log.GetEntry(prevLogIndex); err == nil {
				prevLogTerm = prevEntry.Term()
			}

			entries := make([]*pb.LogEntry, 0)
			for index := nextIndex; index <= r.log.LastIndex() && index >= r.log.FirstIndex(); index++ {
				entry, err := r.log.GetEntry(index)
				if err != nil {
					r.options.logger.Fatalf("error getting entry from log: %s", r.id, err.Error())
				}
				entries = append(entries, &pb.LogEntry{Index: entry.Index(), Term: entry.Term(), Data: entry.Data()})
			}

			request := &pb.AppendEntriesRequest{
				Term:         r.state.getCurrentTerm(),
				LeaderId:     r.id,
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

			responseCh <- AppendEntriesMessage{response: response, peer: peer, numEntriesAppended: uint64(len(entries))}
		}(peer)
	}
}

func (r *Raft) sendInstallSnapshot(peer *Peer, responseCh chan<- InstallSnapshotMessage) {
	go func(peer *Peer) {
		lastSnapshot, err := r.snapshotStorage.LastSnapshot()

		if err != nil {
			return
		}

		request := &pb.InstallSnapshotRequest{
			Term:              r.state.getCurrentTerm(),
			Leader:            r.id,
			LastIncludedIndex: lastSnapshot.LastIncludedIndex,
			LastIncludedTerm:  lastSnapshot.LastIncludedTerm,
			Data:              lastSnapshot.Data,
		}
		response, err := peer.installSnapshot(request)
		if err != nil {
			r.options.logger.Errorf("error sending snapshot to peer: %s", err.Error())
			return
		}

		responseCh <- InstallSnapshotMessage{response: response, peer: peer, lastIncludedIndex: request.LastIncludedIndex}
	}(peer)
}

func (r *Raft) sendRequestVote(responseCh chan<- *pb.RequestVoteResponse) {
	for _, peer := range r.peers {
		go func(peer *Peer) {
			request := &pb.RequestVoteRequest{
				CandidateId:  r.id,
				Term:         r.state.getCurrentTerm(),
				LastLogIndex: r.log.LastIndex(),
				LastLogTerm:  r.log.LastTerm(),
			}

			response, err := peer.requestVote(request)
			if err != nil {
				r.options.logger.Errorf("error requesting vote from peer %s: %s", peer.id, err.Error())
				return
			}

			responseCh <- response
		}(peer)
	}
}

func (r *Raft) requestVote(request *pb.RequestVoteRequest) *pb.RequestVoteResponse {
	response := &pb.RequestVoteResponse{
		Term:        r.state.getCurrentTerm(),
		VoteGranted: false,
	}

	if request.Term < r.state.getCurrentTerm() {
		return response
	}

	if request.Term > r.state.getCurrentTerm() {
		r.becomeFollower(request.Term)
		response.Term = r.state.getCurrentTerm()
	}

	if r.state.getVotedFor() != "" && r.state.getVotedFor() != request.CandidateId {
		return response
	}

	if request.LastLogTerm < r.log.LastTerm() || (request.LastLogTerm == r.log.LastTerm() && r.log.LastIndex() > request.LastLogIndex) {
		return response
	}

	r.updateVotedFor(request.CandidateId)

	response.VoteGranted = true

	return response
}

func (r *Raft) installSnapshot(request *pb.InstallSnapshotRequest) *pb.InstallSnapshotResponse {
	response := &pb.InstallSnapshotResponse{Term: r.state.getCurrentTerm()}

	if request.Term < r.state.getCurrentTerm() {
		return response
	}

	if request.Term > r.state.getCurrentTerm() {
		r.becomeFollower(request.Term)
		response.Term = request.Term
	}

	r.fsm.Restore(request.Data)

	r.state.setLastSnapshotIndex(request.LastIncludedIndex)
	r.state.setLastSnapshotTerm(request.LastIncludedTerm)

	r.log.Compact(request.LastIncludedIndex, request.LastIncludedTerm)

	r.state.setCommitIndex(request.LastIncludedIndex)
	r.state.setLastApplied(request.LastIncludedIndex)

	return response
}

func (r *Raft) takeSnapshot() {
	entry, err := r.log.GetEntry(r.state.getLastApplied())

	if err != nil {
		r.options.logger.Fatalf("%s error getting entry from log for snapshot: %s", err.Error())
	}

	lastSnapshotIndex := r.state.getLastApplied()
	lastSnapshotTerm := entry.Term()

	fsmSnapshot, err := r.fsm.Snapshot()
	if err != nil {
		r.options.logger.Fatalf("error creating snapshot of state machine: %s", err.Error())
	}

	snapshot := NewSnapshot(lastSnapshotIndex, lastSnapshotTerm, fsmSnapshot)
	r.snapshotStorage.SaveSnapshot(snapshot)

	r.state.setLastSnapshotIndex(lastSnapshotIndex)
	r.state.setLastSnapshotTerm(lastSnapshotTerm)

	if err := r.log.Compact(lastSnapshotIndex, lastSnapshotTerm); err != nil {
		r.options.logger.Fatalf("error compacting log: %s", err.Error())
	}
}

func (r *Raft) restoreFromSnapshot() error {
	snapshot, err := r.snapshotStorage.LastSnapshot()

	if err != nil {
		return nil
	}

	if err := r.fsm.Restore(snapshot.Data); err != nil {
		return errors.WrapError(err, "error restoring state machine: %s", err.Error())
	}

	r.state.setLastApplied(snapshot.LastIncludedIndex)
	r.state.setCommitIndex(snapshot.LastIncludedIndex)
	r.state.setLastSnapshotIndex(snapshot.LastIncludedIndex)
	r.state.setLastSnapshotTerm(snapshot.LastIncludedTerm)

	return nil
}

func (r *Raft) leaderLoop() {
	appendEntriesResponses := make(chan AppendEntriesMessage, len(r.peers))
	installSnapshotResponses := make(chan InstallSnapshotMessage, len(r.peers))

	r.sendAppendEntries(appendEntriesResponses)

	for {
		select {
		case <-time.After(r.options.heartbeatInterval):
			r.sendAppendEntries(appendEntriesResponses)
		case <-r.commandCh:
			r.sendAppendEntries(appendEntriesResponses)
		case message := <-appendEntriesResponses:

			if message.response.Term > r.state.getCurrentTerm() {
				r.becomeFollower(message.response.Term)
				return
			}

			if !message.response.GetSuccess() && message.peer.getMatchIndex()+r.options.maxEntriesPerSnapshot < r.log.LastIndex() {
				r.sendInstallSnapshot(message.peer, installSnapshotResponses)
				break
			}

			if !message.response.GetSuccess() && message.peer.getNextIndex() > 1 {
				message.peer.setNextIndex(message.peer.getNextIndex() - 1)
				break
			} else if !message.response.GetSuccess() {
				break
			}

			message.peer.setNextIndex(message.peer.getNextIndex() + message.numEntriesAppended)
			message.peer.setMatchIndex(message.peer.getNextIndex() - 1)

			committed := false
			for index := r.state.getCommitIndex() + 1; index <= r.log.LastIndex(); index++ {
				if entry, _ := r.log.GetEntry(index); entry.Term() != r.state.getCurrentTerm() {
					continue
				}
				matches := 1
				for _, peer := range r.peers {
					if peer.getMatchIndex() >= index {
						matches += 1
					}
					if r.hasQuorum(matches) {
						r.state.setCommitIndex(index)
						committed = true
						break
					}
				}
			}
			if committed {
				r.applyCh <- struct{}{}
				r.commandCh <- struct{}{}
			}
		case message := <-installSnapshotResponses:
			if message.response.Term > r.state.getCurrentTerm() {
				r.becomeFollower(message.response.Term)
				return
			}
			message.peer.setMatchIndex(message.lastIncludedIndex)
			message.peer.setNextIndex(message.lastIncludedIndex + 1)
		case rpc := <-r.appendEntriesCh:
			rpc.responseCh <- r.appendEntries(rpc.request)
			if r.state.getState() != Leader {
				return
			}
		case rpc := <-r.requestVoteCh:
			rpc.responseCh <- r.requestVote(rpc.request)
			if r.state.getState() != Leader {
				return
			}
		case rpc := <-r.installSnapshotCh:
			rpc.responseCh <- r.installSnapshot(rpc.request)
			if r.state.getState() != Leader {
				return
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
			if time.Since(r.state.getLastContact()) > electionTimeout {
				r.becomeCandidate()
				return
			}
		case <-r.commandCh:
			r.options.logger.Warnf("ignoring request to replicate: %s is not the leader", r.id)
		case rpc := <-r.appendEntriesCh:
			rpc.responseCh <- r.appendEntries(rpc.request)
		case rpc := <-r.requestVoteCh:
			rpc.responseCh <- r.requestVote(rpc.request)
		case rpc := <-r.installSnapshotCh:
			rpc.responseCh <- r.installSnapshot(rpc.request)
		case <-r.shutdownCh:
			return
		}
	}
}

func (r *Raft) candidateLoop() {
	votesReceived := 1
	r.updateVotedFor(r.id)
	r.updateCurrentTerm(r.state.getCurrentTerm() + 1)
	requestVoteResponses := make(chan *pb.RequestVoteResponse, len(r.peers))
	electionTimeout := r.options.electionTimeout
	electionTimer := util.RandomTimeout(electionTimeout, electionTimeout*2)

	r.sendRequestVote(requestVoteResponses)

	for {
		select {
		case response := <-requestVoteResponses:
			if response.VoteGranted {
				votesReceived++
			}
			if response.Term > r.state.getCurrentTerm() {
				r.becomeFollower(response.Term)
				return
			}
			if r.hasQuorum(votesReceived) {
				r.becomeLeader()
				return
			}
		case rpc := <-r.appendEntriesCh:
			rpc.responseCh <- r.appendEntries(rpc.request)
			if r.state.getState() != Candidate {
				return
			}
		case rpc := <-r.requestVoteCh:
			rpc.responseCh <- r.requestVote(rpc.request)
			if r.state.getState() != Candidate {
				return
			}
		case rpc := <-r.installSnapshotCh:
			rpc.responseCh <- r.installSnapshot(rpc.request)
			if r.state.getState() != Candidate {
				return
			}
		case <-electionTimer:
			votesReceived = 1
			electionTimer = util.RandomTimeout(electionTimeout, electionTimeout*2)
			r.sendRequestVote(requestVoteResponses)
		case <-r.shutdownCh:
			return
		}
	}
}

func (r *Raft) applyLoop() {
	for range r.applyCh {
		responses := make([]CommandResponse, r.state.getCommitIndex()-r.state.getLastApplied())
		lastApplied := r.state.getLastApplied()
		commitIndex := r.state.getCommitIndex()

		for index := lastApplied + 1; index <= commitIndex; index++ {
			entry, err := r.log.GetEntry(index)
			if err != nil {
				r.options.logger.Fatalf("error getting log entry: %s", err.Error())
			}
			response := CommandResponse{
				Term:     entry.Term(),
				Index:    entry.Index(),
				Command:  entry.Data(),
				Response: r.fsm.Apply(entry.Data()),
			}
			responses[index-lastApplied-1] = response
		}

		r.state.setLastApplied(commitIndex)

		for _, response := range responses {
			r.commandResponseCh <- response
		}
	}
}

func (r *Raft) snapshotLoop() {
	snapshotTimer := util.RandomTimeout(r.options.snapshotInterval, r.options.snapshotInterval*2)

	for {
		select {
		case <-snapshotTimer:
			entriesSinceSnapshot := r.log.LastIndex() - r.state.getLastSnapshotIndex()
			if entriesSinceSnapshot >= uint64(r.options.maxEntriesPerSnapshot) {
				r.takeSnapshot()
			}
			snapshotTimer = util.RandomTimeout(r.options.snapshotInterval, r.options.snapshotInterval*2)
		case <-r.shutdownCh:
			return
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
		case Shutdown:
			return
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

func (r *Raft) becomeFollower(term uint64) {
	r.updateCurrentTerm(term)
	r.state.setVotedFor("")
	r.state.setState(Follower)
	r.options.logger.Infof("%s has entered the follower state", r.id)
}

func (r *Raft) updateCurrentTerm(term uint64) {
	currentTermKey := []byte("currentTerm")
	if err := r.storage.SetUint64(currentTermKey, term); err != nil {
		r.options.logger.Fatalf("failed to persist current term to storage: %s", err.Error())
	}
	r.state.setCurrentTerm(term)
}

func (r *Raft) updateVotedFor(votedFor string) {
	votedForKey := []byte("votedFor")
	if err := r.storage.Set(votedForKey, []byte(votedFor)); err != nil {
		r.options.logger.Fatalf("failed to persist vote to storage: %s", err.Error())
	}
	r.state.setVotedFor(votedFor)
}

func (r *Raft) hasQuorum(count int) bool {
	return count >= (len(r.peers)/2 + 1)
}
