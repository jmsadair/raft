package raft

import (
	"sync"
	"time"

	"github.com/jmsadair/raft/internal/errors"
	logger "github.com/jmsadair/raft/internal/logger"
	pb "github.com/jmsadair/raft/internal/protobuf"
	"github.com/jmsadair/raft/internal/util"
)

type State uint32

const (
	Leader State = iota
	Follower
	Shutdown
)

func (s State) String() string {
	switch s {
	case Leader:
		return "leader"
	case Follower:
		return "follower"
	case Shutdown:
		return "shutdown"
	default:
		panic("invalid state")
	}
}

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

// TODO: Add wait group to ensure that all go routines have exited.

type Raft struct {
	// The ID of this raft server, must be a unique, non-empty string.
	id string

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

	// Notifies applier that the commit index has been updated.
	applyCond *sync.Cond

	// Notifies committer that new log entries may be ready to be committed.
	commitCond *sync.Cond

	// Notifies receivers that command has been applied.
	commandResponseCh chan<- CommandResponse

	// Indicates the current state of this raft instance: leader, follower, or shutdown.
	state State

	// The index of the last log entry that was committed.
	commitIndex uint64

	// The index of the last log entry that was applied.
	lastApplied uint64

	// The current term of this raft instance. Must be persisted.
	currentTerm uint64

	// The ID of the candidate that this raft instance voted for. Must be persisted.
	votedFor string

	// The last time this raft instance was contacted by the leader.
	lastContact time.Time

	wg sync.WaitGroup

	mu sync.Mutex
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
		currentTerm:       currentTerm,
		votedFor:          votedFor,
		state:             Shutdown,
		commitIndex:       0,
		lastApplied:       0,
	}

	raft.applyCond = sync.NewCond(&raft.mu)
	raft.commitCond = sync.NewCond(&raft.mu)

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
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.state != Shutdown {
		return
	}

	for _, peer := range r.peers {
		if err := peer.connect(); err != nil {
			r.options.logger.Errorf("error connecting to peer: %s", err.Error())
		}
	}

	r.lastContact = time.Now()
	r.becomeFollower(r.currentTerm)

	r.wg.Add(4)
	go r.applyLoop()
	go r.electionLoop()
	go r.heartbeatLoop()
	go r.commitLoop()

	r.options.logger.Infof("raft server with ID %s started", r.id)
}

// Stop stops the raft server if it is not already stopped.
func (r *Raft) Stop() {
	r.mu.Lock()
	if r.state == Shutdown {
		r.mu.Unlock()
		return
	}
	r.state = Shutdown
	r.applyCond.Broadcast()
	r.commitCond.Broadcast()
	r.mu.Unlock()

	r.wg.Wait()

	close(r.commandResponseCh)
	for _, peer := range r.peers {
		if err := peer.disconnect(); err != nil {
			r.options.logger.Errorf("error disconnecting from peer: %s", err.Error())
		}
	}

	r.options.logger.Infof("raft server with ID %s stopped", r.id)
}

// SubmitCommand accepts a command from a client for replication and
// returns the log index and term assigned to the command. If this raft server
// is not the leader, the command will be rejected and an error will
// be returned.
func (r *Raft) SubmitCommand(command Command) (uint64, uint64, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.state != Leader {
		return 0, 0, errors.WrapError(nil, "%s is not the leader", r.id)
	}
	entry := NewLogEntry(r.log.NextIndex(), r.currentTerm, command.Bytes)
	r.log.AppendEntry(entry)
	r.sendAppendEntries()
	r.options.logger.Debugf("server %s submitted command: logEntry = %v", r.id, entry)
	return entry.Index(), entry.Term(), nil
}

// Status returns the current status of this raft server.
func (r *Raft) Status() Status {
	r.mu.Lock()
	defer r.mu.Unlock()

	return Status{
		Id:          r.id,
		Term:        r.currentTerm,
		CommitIndex: r.commitIndex,
		LastApplied: r.lastApplied,
		State:       r.state,
	}
}

// appendEntries is used to append log entries to the log of this raft server.
func (r *Raft) appendEntries(request *pb.AppendEntriesRequest) *pb.AppendEntriesResponse {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.options.logger.Debugf("server %s received AppendEntries RPC: leaderID = %s, leaderCommit = %d, term = %d, prevLogIndex = %d, prevLogTerm = %d",
		r.id, request.GetLeaderId(), request.GetLeaderCommit(), request.GetTerm(), request.GetPrevLogIndex(), request.GetPrevLogTerm())

	response := &pb.AppendEntriesResponse{
		Term:    r.currentTerm,
		Success: false,
	}

	// Reject any requests with out-of-date term.
	if request.GetTerm() < r.currentTerm {
		r.options.logger.Debugf("server %s rejecting AppendEntries RPC: out of date term: %d > %d", r.id, r.currentTerm, request.GetTerm())
		return response
	}

	// Update the time of last contact for this server - note that this should be done
	// even if the request is rejected due to having a non-matching previous log entry.
	r.lastContact = time.Now()

	// If the request has a more up-to-date term, update current term and
	// become a follower.
	if request.GetTerm() > r.currentTerm {
		r.becomeFollower(request.GetTerm())
		response.Term = r.currentTerm
	}

	// TODO: change this after snapshots are added. Need to compare previous log index to last included index.
	if request.GetPrevLogIndex() != 0 {
		// Reject the request if this server does not have the previous log entry.
		if r.log.NextIndex() <= request.GetPrevLogIndex() {
			r.options.logger.Debugf("server %s rejecting AppendEntries RPC: server does not have previous log entry: index = %d",
				r.id, request.GetPrevLogIndex())
			return response
		}

		prevLogEntry, err := r.log.GetEntry(request.GetPrevLogIndex())
		if err != nil {
			r.options.logger.Fatalf("server %s: error getting entry from log: %s", r.id, err.Error())
		}

		// Reject the request if the server has the previous log entry, but its term does not match.
		if prevLogEntry.Term() != request.GetPrevLogTerm() {
			r.options.logger.Debugf("server %s rejecting AppendEntries RPC: previous log entry has different term: index = %d, localTerm = %d, remoteTerm = %d",
				r.id, request.GetPrevLogIndex(), prevLogEntry.Term(), request.GetPrevLogTerm())
			return response
		}
	}

	response.Success = true

	entries := make([]*LogEntry, len(request.GetEntries()))
	for i, entry := range request.GetEntries() {
		entries[i] = NewLogEntry(entry.GetIndex(), entry.GetTerm(), entry.GetData())
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
		if err := r.log.Truncate(entry.Index()); err != nil {
			r.options.logger.Fatalf("error truncating log: %s", err.Error())
		}
		toAppend = entries[i:]
		break
	}

	r.log.AppendEntries(toAppend)

	if request.GetLeaderCommit() > r.commitIndex {
		r.commitIndex = util.Min(request.GetLeaderCommit(), r.log.LastIndex())
		r.applyCond.Broadcast()
	}

	return response
}

// sendAppendEntries will send appendEntries RPCs to the peers of this server concurrently.
func (r *Raft) sendAppendEntries() {
	for _, peer := range r.peers {
		if peer.Id() == r.id {
			continue
		}

		go func(peer *Peer) {
			r.mu.Lock()
			defer r.mu.Unlock()

			if r.state != Leader {
				return
			}

			nextIndex := peer.getNextIndex()
			prevLogIndex := nextIndex - 1
			prevLogTerm := uint64(0)

			// TODO: change this after snapshots are added. Need to compare previous log index to last included index.
			if prevLogIndex != 0 && prevLogIndex < r.log.NextIndex() {
				prevEntry, err := r.log.GetEntry(prevLogIndex)
				if err != nil {
					r.options.logger.Fatalf("server %s: error getting entry from log: %s", r.id, err.Error())
				}
				prevLogTerm = prevEntry.Term()
			}

			entries := make([]*pb.LogEntry, 0, r.log.NextIndex()-nextIndex)
			for index := nextIndex; index < r.log.NextIndex(); index++ {
				entry, err := r.log.GetEntry(index)
				if err != nil {
					r.options.logger.Fatalf("server %s: error getting entry from log: %s", r.id, err.Error())
				}
				entries = append(entries, &pb.LogEntry{Index: entry.Index(), Term: entry.Term(), Data: entry.Data()})
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
			r.mu.Lock()

			if err != nil || r.state != Leader {
				return
			}
			// Become a follower if a peer has a more up-to-date term.
			if response.GetTerm() > r.currentTerm {
				r.becomeFollower(response.GetTerm())
				return
			}
			// If the AppendEntries RPC was not successful, decrement the next index associated with the peer.
			if !response.GetSuccess() && peer.getNextIndex() > 1 {
				peer.setNextIndex(peer.getNextIndex() - 1)
				return
			} else if !response.GetSuccess() {
				return
			}

			if request.GetPrevLogIndex()+uint64(len(entries)) >= peer.getNextIndex() {
				peer.setNextIndex(request.GetPrevLogIndex() + uint64(len(entries)) + 1)
				peer.setMatchIndex(peer.getNextIndex() - 1)
				r.commitCond.Broadcast()
				go r.sendAppendEntries()
			}
		}(peer)
	}
}

// requestVote is used to request a vote from this server.
func (r *Raft) requestVote(request *pb.RequestVoteRequest) *pb.RequestVoteResponse {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.options.logger.Debugf("server %s received RequestVote RPC: candidateID = %s, term = %d, lastLogIndex = %d, lastLogTerm = %d",
		r.id, request.GetCandidateId(), request.GetTerm(), request.GetLastLogIndex(), request.GetLastLogTerm())

	response := &pb.RequestVoteResponse{
		Term:        r.currentTerm,
		VoteGranted: false,
	}

	// Reject the request if the term is out-of-date.
	if request.GetTerm() < r.currentTerm {
		r.options.logger.Debugf("server %s rejecting RequestVote RPC: out of date term: %d > %d", r.id, r.currentTerm, request.GetTerm())
		return response
	}

	// Update this server's time of last contact.
	r.lastContact = time.Now()

	// If the request has a more up-to-date term, update current term and
	// become a follower.
	if request.GetTerm() > r.currentTerm {
		r.becomeFollower(request.GetTerm())
		response.Term = r.currentTerm
	}

	// Reject the request if this server has already voted.
	if r.votedFor != "" && r.votedFor != request.GetCandidateId() {
		r.options.logger.Debugf("server %s rejecting RequestVote RPC: already voted: votedFor = %s", r.id, r.votedFor)
		return response
	}

	// Reject any requests with out-date-log.
	// To determine which log is more up-to-date:
	// 1. If the logs have last entries with different terms, than the log with the
	//    greater term is more up-to-date.
	// 2. If the logs end with the same term, the longer log is more up-to-date.
	if request.GetLastLogTerm() < r.log.LastTerm() ||
		(request.GetLastLogTerm() == r.log.LastTerm() && r.log.LastIndex() > request.GetLastLogIndex()) {
		return response
	}

	r.votedFor = request.GetCandidateId()
	response.VoteGranted = true

	return response
}

// sendRequestVote will send vote requests to the peers of this server concurrently.
func (r *Raft) sendRequestVote(votes *int) {
	for _, peer := range r.peers {
		if peer.Id() == r.id {
			continue
		}

		go func(peer *Peer) {
			r.mu.Lock()
			defer r.mu.Unlock()

			request := &pb.RequestVoteRequest{
				CandidateId:  r.id,
				Term:         r.currentTerm,
				LastLogIndex: r.log.LastIndex(),
				LastLogTerm:  r.log.LastTerm(),
			}

			r.mu.Unlock()
			response, err := peer.requestVote(request)
			r.mu.Lock()

			if err != nil {
				return
			}

			if response.GetVoteGranted() {
				*votes += 1
			}

			// Become a follower if a peer has a more up-to-date term.
			if response.GetTerm() > r.currentTerm {
				r.becomeFollower(response.GetTerm())
				return
			}

			// If we have received votes from the majority of peers, become a leader.
			if r.hasQuorum(*votes) && r.state == Follower {
				r.becomeLeader()
				return
			}
		}(peer)
	}
}

// installSnapshot is used to install a snapshot sent by another server on this server.
func (r *Raft) installSnapshot(request *pb.InstallSnapshotRequest) *pb.InstallSnapshotResponse {
	// TODO: this probably needs to get moved to the apply loop to prevent synchronization issues.
	// TODO: the log compaction should be fixed (e.g. the indexing is not correct).
	panic("installSnapshot not implemented")
}

// sendInstallSnapshot is used to send a snapshot to a peer for installation.
func (r *Raft) sendInstallSnapshot(peer *Peer) {
	panic("sendInstallSnapshot not implemented")
}

// takeSnapshot is used to take a snapshot of this raft server.
func (r *Raft) takeSnapshot() {
	panic("takeSnapshot not implemented")
}

// restoreFromSnapshot is used to restore this raft server from a snapshot.
// This should only be called during initialization.
func (r *Raft) restoreFromSnapshot() error {
	panic("restoreFromSnapshot not implemented")
}

func (r *Raft) heartbeatLoop() {
	defer r.wg.Done()

	for {
		time.Sleep(r.options.heartbeatInterval)
		r.mu.Lock()
		if r.state == Shutdown {
			r.mu.Unlock()
			return
		}
		if r.state == Leader {
			r.mu.Unlock()
			r.sendAppendEntries()
			r.mu.Lock()
		}
		r.mu.Unlock()
	}
}

func (r *Raft) electionLoop() {
	defer r.wg.Done()

	for {
		electionTimer := util.RandomTimeout(r.options.electionTimeout, 2*r.options.electionTimeout)
		time.Sleep(electionTimer * time.Millisecond)
		r.mu.Lock()
		if r.state == Shutdown {
			r.mu.Unlock()
			return
		}
		r.mu.Unlock()
		r.election()
	}
}

func (r *Raft) election() {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.state != Follower || time.Since(r.lastContact) < r.options.electionTimeout {
		return
	}

	votesReceived := 1
	r.becomeCandidate()
	r.sendRequestVote(&votesReceived)
}

func (r *Raft) commitLoop() {
	r.mu.Lock()
	defer r.mu.Unlock()
	defer r.wg.Done()

	for r.state != Shutdown {
		r.commitCond.Wait()
		committed := false
		for index := r.commitIndex + 1; index <= r.log.LastIndex(); index++ {
			if entry, _ := r.log.GetEntry(index); entry.Term() != r.currentTerm {
				continue
			}
			matches := 1
			for _, peer := range r.peers {
				if peer.Id() == r.id {
					continue
				}
				if peer.getMatchIndex() >= index {
					matches += 1
				}
				if r.hasQuorum(matches) {
					r.commitIndex = index
					committed = true
					break
				}
			}
		}

		if committed {
			r.applyCond.Broadcast()
		}
	}
}

// applyLoop is used to apply newly committed log entries to the client state machine
// and send responses to the client. Must be called in a separate go-routine because it
// will block.
func (r *Raft) applyLoop() {
	r.mu.Lock()
	defer r.mu.Unlock()
	defer r.wg.Done()

	for r.state != Shutdown {
		r.applyCond.Wait()
		for index := r.lastApplied + 1; index <= r.commitIndex; index++ {
			entry, err := r.log.GetEntry(index)
			if err != nil {
				r.options.logger.Fatalf("server %s: error getting entry from log: %s", r.id, err.Error())
			}
			response := CommandResponse{
				Index:    entry.index,
				Term:     entry.term,
				Command:  entry.data,
				Response: r.fsm.Apply(entry.data),
			}
			r.lastApplied++
			r.mu.Unlock()
			r.options.logger.Debugf("server %s applied command: response = %v", r.id, response)
			r.commandResponseCh <- response
			r.mu.Lock()
		}
	}
}

// becomeCandidate transitions this server into the candidate state.
func (r *Raft) becomeCandidate() {
	r.currentTerm++
	r.votedFor = r.id
	r.persistTermAndVote()
	r.options.logger.Infof("%s has entered the candidate state: term = %d", r.id, r.currentTerm)
}

// becomeLeader transitions this server into the leader state.
func (r *Raft) becomeLeader() {
	r.state = Leader
	for _, peer := range r.peers {
		peer.setNextIndex(r.log.LastIndex() + 1)
		peer.setMatchIndex(0)
	}
	r.sendAppendEntries()
	r.options.logger.Infof("%s has entered the leader state", r.id)
}

// becomeFollower transitions this server into the follower state.
func (r *Raft) becomeFollower(term uint64) {
	r.state = Follower
	r.currentTerm = term
	r.votedFor = ""
	r.persistTermAndVote()
	r.options.logger.Infof("%s has entered the follower state", r.id)
}

// hasQuorum returns true if count meets or exceeds
// the number that is the majority of peers and false
// otherwise.
func (r *Raft) hasQuorum(count int) bool {
	return count > len(r.peers)/2
}

func (r *Raft) persistTermAndVote() {
	// TODO: Make atomic?
	currentTermKey := []byte("currentTerm")
	r.storage.SetUint64(currentTermKey, r.currentTerm)
	votedForKey := []byte("votedFor")
	r.storage.Set(votedForKey, []byte(r.votedFor))
}
