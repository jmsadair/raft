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
	// The ID of this raft server, must be a unique, non-empty string.
	id string

	// The state of this raft server: Leader, Follower, Candidate, or Shutdown.
	state RaftState

	// The configuration options for this raft server: heartbeat interval,
	// election timeout, logger, and others.
	options options

	// The peers of this raft server, maps peer ID to peer.
	peers map[string]*Peer

	// Log is used to persist log entries.
	log Log

	// Storage is used to persist the votedFor and currentTerm
	// fields of RaftState.
	storage Storage

	// SnapshotStorage is used to store and retrieve snapshots.
	snapshotStorage SnapshotStorage

	// The state machine provided by the client that commands will be applied to.
	fsm StateMachine

	// Accepts incoming requests to append entries.
	appendEntriesCh chan AppendEntriesRPC

	// Accepts incoming requests for a vote.
	requestVoteCh chan RequestVoteRPC

	// Accepts incoming requests to install a snapshot.
	installSnapshotCh chan InstallSnapshotRPC

	// Notifies receivers that the commit index has been updated.
	applyCh chan interface{}

	// Notifies leader that a new command has been accepted.
	commandCh chan interface{}

	// Notifies receivers that command has been applied.
	commandResponseCh chan<- CommandResponse

	// Notifies raft server to shutdown.
	shutdownCh chan interface{}
}

func NewRaft(id string, peers []*Peer, log Log, storage Storage, snapshotStorage SnapshotStorage, fsm StateMachine, responseCh chan<- CommandResponse, opts ...Option) (*Raft, error) {
	// Apply provided options.
	var options options
	for _, opt := range opts {
		if err := opt(&options); err != nil {
			return nil, errors.WrapError(err, "failed to create new raft: %s", err.Error())
		}
	}

	// Set default values if option not provided.
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

	// Restore the current term if it has been persisted.
	currentTermKey := []byte("currentTerm")
	currentTerm, err := storage.GetUint64(currentTermKey)
	if err != nil {
		return nil, errors.WrapError(err, "failed to restore current term from storage: %s", err.Error())
	}

	// Restore the prior vote if it has been persisted.
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

	if !options.restoreFromSnapshot {
		return raft, nil
	}

	if err := raft.restoreFromSnapshot(); err != nil {
		return nil, errors.WrapError(err, "failed to restore raft from snapshot: %s", err.Error())
	}

	return raft, nil
}

// Start starts the raft server if it is not already started.
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
	r.commandCh = make(chan interface{}, 256)
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

// Stop stops the raft server if it is not already stopped.
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

// SubmitCommand accepts a command from a client for replication and
// returns the log index assigned to the command. If this raft server
// is not the leader, the command will be rejected and an error will
// be returned.
func (r *Raft) SubmitCommand(command Command) (uint64, error) {
	if r.state.getState() != Leader {
		return 0, errors.WrapError(nil, "%s is not the leader", r.id)
	}

	entry := NewLogEntry(r.log.LastIndex()+1, r.state.getCurrentTerm(), command.Bytes)
	r.log.AppendEntry(entry)

	// Notify leader that a new entry has been appended to the log.
	r.commandCh <- struct{}{}

	return entry.Index(), nil
}

// Status returns the current status of this raft server.
func (r *Raft) Status() Status {
	return Status{
		Id:          r.id,
		Term:        r.state.getCurrentTerm(),
		CommitIndex: r.state.getCommitIndex(),
		LastApplied: r.state.getLastApplied(),
		State:       r.state.getState(),
	}
}

// appendEntries is used to append log entries to the log of this raft server.
func (r *Raft) appendEntries(request *pb.AppendEntriesRequest) *pb.AppendEntriesResponse {
	var prevLogEntry *LogEntry
	var err error

	response := &pb.AppendEntriesResponse{
		Term:    r.state.getCurrentTerm(),
		Success: false,
	}

	// Reject any requests with out-of-date term.
	if request.Term < r.state.getCurrentTerm() {
		return response
	}

	// If the request has a more up-to-date term, update current term and
	// become a follower.
	if request.Term > r.state.getCurrentTerm() {
		r.becomeFollower(request.Term)
		response.Term = r.state.getCurrentTerm()
	}

	// Reject any request that do not have a matching previous log entry (if one exists).
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

// sendAppendEntries will send appendEntries RPCs to the peers of this server concurrently.
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
				//r.options.logger.Errorf("error appending entries to peer: %s", err.Error())
				return
			}

			responseCh <- AppendEntriesMessage{response: response, peer: peer, numEntriesAppended: uint64(len(entries))}
		}(peer)
	}
}

// requestVote is used to request a vote from this server.
func (r *Raft) requestVote(request *pb.RequestVoteRequest) *pb.RequestVoteResponse {
	response := &pb.RequestVoteResponse{
		Term:        r.state.getCurrentTerm(),
		VoteGranted: false,
	}

	// Reject the request if the term is out-of-date.
	if request.Term < r.state.getCurrentTerm() {
		return response
	}

	// If the request has a more up-to-date term, update current term and
	// become a follower.
	if request.Term > r.state.getCurrentTerm() {
		r.becomeFollower(request.Term)
		response.Term = r.state.getCurrentTerm()
	}

	// Reject the request if this server has already voted.
	if r.state.getVotedFor() != "" && r.state.getVotedFor() != request.CandidateId {
		return response
	}

	// Reject any requests with out-date-log.
	// To determine which log is more up-to-date:
	// 1. If the logs have last entries with different terms, than the log with the
	//    greater term is more up-to-date.
	// 2. If the logs end with the same term, the longer log is more up-to-date.
	if request.LastLogTerm < r.log.LastTerm() || (request.LastLogTerm == r.log.LastTerm() && r.log.LastIndex() > request.LastLogIndex) {
		return response
	}

	r.updateVotedFor(request.CandidateId)

	response.VoteGranted = true

	return response
}

// sendRequestVote will send vote requests to the peers of this server concurrently.
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
				//r.options.logger.Errorf("error requesting vote from peer %s: %s", peer.id, err.Error())
				return
			}

			responseCh <- response
		}(peer)
	}
}

// installSnapshot is used to install a snapshot sent by another server on this server.
func (r *Raft) installSnapshot(request *pb.InstallSnapshotRequest) *pb.InstallSnapshotResponse {
	response := &pb.InstallSnapshotResponse{Term: r.state.getCurrentTerm()}

	// Reject any requests with out-of-date term.
	if request.Term < r.state.getCurrentTerm() {
		return response
	}

	// If the request has a more up-to-date term, update current term and
	// become a follower.
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

// sendInstallSnapshot is used to send a snapshot to a peer for installation.
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

// takeSnapshot is used to take a snapshot of this raft server.
func (r *Raft) takeSnapshot() {
	r.options.logger.Infof("%s taking snapshot", r.id)

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

// restoreFromSnapshot is used to restore this raft server from a snapshot.
// This should only be called during initialization.
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

// leaderLoop implements the logic of the raft leader, will not return
// until this server steps down or the server is shutdown.
func (r *Raft) leaderLoop() {
	appendEntriesResponses := make(chan AppendEntriesMessage, len(r.peers))
	installSnapshotResponses := make(chan InstallSnapshotMessage, len(r.peers))

	// Send AppendEntries to peers to establish leadership.
	r.sendAppendEntries(appendEntriesResponses)

	// Start a heartbeat.
	heartbeatInterval := r.options.heartbeatInterval
	stopHeartbeat := make(chan interface{})
	defer close(stopHeartbeat)
	go func() {
		for {
			select {
			case <-time.After(heartbeatInterval):
				r.sendAppendEntries(appendEntriesResponses)
			case <-stopHeartbeat:
				return
			}
		}
	}()

	for {
		select {
		case <-r.commandCh:
			r.sendAppendEntries(appendEntriesResponses)
		case message := <-appendEntriesResponses:
			// Become a follower if a peer has a more up-to-date term.
			if message.response.Term > r.state.getCurrentTerm() {
				r.becomeFollower(message.response.Term)
				return
			}

			// If the follower is lagging, send them an InstallSnapshot RPC.
			if !message.response.Success && message.peer.getMatchIndex()+r.options.maxEntriesPerSnapshot < r.log.LastIndex() {
				r.sendInstallSnapshot(message.peer, installSnapshotResponses)
				break
			}

			// If the AppendEntries RPC was not successful, decrement the next index associated
			// with the peer.
			if !message.response.Success && message.peer.getNextIndex() > 1 {
				message.peer.setNextIndex(message.peer.getNextIndex() - 1)
				break
			} else if !message.response.Success {
				break
			}

			message.peer.setNextIndex(message.peer.getNextIndex() + message.numEntriesAppended)
			message.peer.setMatchIndex(message.peer.getNextIndex() - 1)

			// Check if log entries can be committed.
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
			// Become a follower if a peer has a more up-to-date term.
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

// followerLoop implements the logic of a raft follower, will not return
// until this server becomes a candidate or the server is shutdown.
func (r *Raft) followerLoop() {
	electionTimeout := r.options.electionTimeout
	electionTimer := util.RandomTimeout(electionTimeout, electionTimeout*2)

	for {
		select {
		case <-electionTimer:
			electionTimer = util.RandomTimeout(electionTimeout, electionTimeout*2)
			// If this server has not heard from the leader within the election timeout,
			// become a candidate.
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

// candidateLoop implements the logic of a raft candidate.
func (r *Raft) candidateLoop() {
	// At the start of an election, a server must vote for itself and increment its
	// current term.
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
			// Become a follower if a peer has a more up-to-date term.
			if response.Term > r.state.getCurrentTerm() {
				r.becomeFollower(response.Term)
				return
			}
			// If we have received votes from the majority of peers, become a leader.
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
			return
		case <-r.shutdownCh:
			return
		}
	}
}

// applyLoop is used to apply newly committed log entries to the client state machine
// and send responses to the client. Must be called in a separate go-routine because it
// will block.
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

// snapshotLoop takes a snapshot any time the log has exceeded the client specified
// maximum number of log entries.
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

// becomeCandidate transitions this server into the candidate state.
func (r *Raft) becomeCandidate() {
	r.state.setState(Candidate)
	r.options.logger.Infof("%s has entered the candidate state", r.id)
}

// becomeLeader transitions this server into the leader state.
func (r *Raft) becomeLeader() {
	r.state.setState(Leader)
	for _, peer := range r.peers {
		peer.setNextIndex(r.log.LastIndex() + 1)
		peer.setMatchIndex(0)
	}
	r.options.logger.Infof("%s has entered the leader state", r.id)
}

// becomeFollower transitions this server into the follower state.
func (r *Raft) becomeFollower(term uint64) {
	r.updateCurrentTerm(term)
	r.state.setVotedFor("")
	r.state.setState(Follower)
	r.options.logger.Infof("%s has entered the follower state", r.id)
}

// updateCurrentTerm updates this server's current term and persists it to storage.
func (r *Raft) updateCurrentTerm(term uint64) {
	currentTermKey := []byte("currentTerm")
	if err := r.storage.SetUint64(currentTermKey, term); err != nil {
		r.options.logger.Fatalf("failed to persist current term to storage: %s", err.Error())
	}
	r.state.setCurrentTerm(term)
}

// updateVotedFor update the vote that this server took and persists it to storage.
func (r *Raft) updateVotedFor(votedFor string) {
	votedForKey := []byte("votedFor")
	if err := r.storage.Set(votedForKey, []byte(votedFor)); err != nil {
		r.options.logger.Fatalf("failed to persist vote to storage: %s", err.Error())
	}
	r.state.setVotedFor(votedFor)
}

// hasQuorum returns true if count meets or exceeds
// the number that is the majority of peers and false
// otherwise.
func (r *Raft) hasQuorum(count int) bool {
	return count >= (len(r.peers)/2 + 1)
}
