package raft

import (
	"sync"
	"time"

	"github.com/jmsadair/raft/internal/errors"
	logger "github.com/jmsadair/raft/internal/logger"
	pb "github.com/jmsadair/raft/internal/protobuf"
	"github.com/jmsadair/raft/internal/util"
)

// The state of a Raft instance.
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

// Command is an operation that will be applied to the state machine.
type Command struct {
	// The bytes of the operation.
	Bytes []byte
}

// CommandResponse is the response that is generated after applying
// a command to the state machine.
type CommandResponse struct {
	// The term of the log entry containing the applied command.
	Term uint64

	// The index of the log entry containing the applied command.
	Index uint64

	// The bytes of the operation applied to the state machine.
	Command []byte

	// The response returned by the state machine after applying the command.
	Response interface{}
}

// AppendEntriesRequest is a request invoked by the leader to replicate
// log entries and also serves as a heartbeat.
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

	// Contains the log entries to store (may be empty for heartbeat).
	entries []*LogEntry
}

// AppendEntriesResponse is a response to a request to to replicate log
// entries.
type AppendEntriesResponse struct {
	// The term of the server that received the request.
	term uint64

	// Indicates whether the request to append entries was successful.
	// True if the request was successful and false otherwise.
	success bool
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

// Raft is the consensus module in the replicated state machine architecture.
type Raft struct {
	// The ID of this Raft instance.
	id string

	// The configuration options for this Raft instance.
	options options

	// The peers of this Raft instance.
	peers map[string]Peer

	// A durable log for storing log entries.
	log Log

	// Durable storage for persistent state (outside of the log).
	storage Storage

	// Stores and retrieves snapshots.
	snapshotStorage SnapshotStorage

	// State machine provided by the client that commands will be applied to.
	fsm StateMachine

	// Notifies applier that the commit index has been updated.
	applyCond *sync.Cond

	// Notifies committer that new log entries may be ready to be committed.
	commitCond *sync.Cond

	// Notifies receivers that command has been applied.
	commandResponseCh chan<- CommandResponse

	// The current state of this raft instance: leader, follower, or shutdown.
	state State

	// Index of the last log entry that was committed.
	commitIndex uint64

	// Index of the last log entry that was applied.
	lastApplied uint64

	// The current term of this Raft instance. Must be persisted.
	currentTerm uint64

	// ID of the candidate that this Raft instance voted for. Must be persisted.
	votedFor string

	// Time of last contact by the leader.
	lastContact time.Time

	wg sync.WaitGroup

	mu sync.Mutex
}

func NewRaft(id string, peers []Peer, log Log, storage Storage, snapshotStorage SnapshotStorage, fsm StateMachine, responseCh chan<- CommandResponse, opts ...Option) (*Raft, error) {
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

	// Open the storage to recover persisted state.
	if err := storage.Open(); err != nil {
		return nil, errors.WrapError(err, "failed to open storage: %s", err.Error())
	}

	// Restore the current term and vote if they have been persisted.
	persistentState, err := storage.GetState()
	if err != nil {
		return nil, errors.WrapError(err, "failed to recover storage: %s", err.Error())
	}
	currentTerm := persistentState.term
	votedFor := persistentState.votedFor

	// Open the log for new operations.
	if err := log.Open(); err != nil {
		return nil, errors.WrapError(err, "failed to open log: %s", err.Error())
	}

	peerLookup := make(map[string]Peer)
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

	// Restore the state machine from the snapshot.
	if err := raft.restoreFromSnapshot(); err != nil {
		return nil, errors.WrapError(err, "failed to restore raft from snapshot: %s", err.Error())
	}

	return raft, nil
}

// Start starts this Raft instance if it is not already started.
func (r *Raft) Start() {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.state != Shutdown {
		return
	}

	for _, peer := range r.peers {
		if err := peer.Connect(); err != nil {
			r.options.logger.Errorf("error connecting to peer: %s", err.Error())
		}
	}

	r.lastContact = time.Now()
	r.state = Follower

	r.wg.Add(4)
	go r.applyLoop()
	go r.electionLoop()
	go r.heartbeatLoop()
	go r.commitLoop()

	r.options.logger.Infof("raft server with ID %s started", r.id)
}

// Stop stops this Raft instance if it is not already stopped.
func (r *Raft) Stop() {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.state == Shutdown {
		return
	}

	// Indicate shutdown state and notify commit loop and apply loop
	// so that they can return.
	r.state = Shutdown
	r.applyCond.Broadcast()
	r.commitCond.Broadcast()

	r.mu.Unlock()
	r.wg.Wait()
	r.mu.Lock()

	close(r.commandResponseCh)

	for _, peer := range r.peers {
		if err := peer.Disconnect(); err != nil {
			r.options.logger.Errorf("error disconnecting from peer: %s", err.Error())
		}
	}

	r.log.Close()
	r.storage.Close()

	r.options.logger.Infof("raft server with ID %s stopped", r.id)
}

// SubmitCommand accepts a command from a client for replication and
// returns the log index assigned to the command, the term assigned to the
// command, and an error if this Raft instance is not the leader.
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

	return entry.index, entry.term, nil
}

// Status returns the current status of this Raft instance.
func (r *Raft) Status() Status {
	r.mu.Lock()
	defer r.mu.Unlock()

	return Status{
		ID:          r.id,
		Term:        r.currentTerm,
		CommitIndex: r.commitIndex,
		LastApplied: r.lastApplied,
		State:       r.state,
	}
}

func (r *Raft) appendEntries(request AppendEntriesRequest) AppendEntriesResponse {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.options.logger.Debugf("server %s received AppendEntries RPC: leaderID = %s, leaderCommit = %d, term = %d, prevLogIndex = %d, prevLogTerm = %d",
		r.id, request.leaderID, request.leaderCommit, request.term, request.prevLogIndex, request.prevLogTerm)

	response := AppendEntriesResponse{
		term:    r.currentTerm,
		success: false,
	}

	// Reject any requests with an out-of-date term.
	if request.term < r.currentTerm {
		r.options.logger.Debugf("server %s rejecting AppendEntries RPC: out of date term: %d > %d", r.id, r.currentTerm, request.term)
		return response
	}

	// Update the time of last contact - note that this should be done even
	// if the request is rejected due to having a non-matching previous log entry.
	r.lastContact = time.Now()

	// If the request has a more up-to-date term, update current term and
	// become a follower.
	if request.term > r.currentTerm {
		r.becomeFollower(request.term)
		response.term = r.currentTerm
	}

	// TODO: change this after snapshots are added. Need to compare previous log index to last included index.
	if request.prevLogIndex != 0 {
		// Reject the request if the log does not have the previous log entry.
		if r.log.NextIndex() <= request.prevLogIndex {
			r.options.logger.Debugf("server %s rejecting AppendEntries RPC: server does not have previous log entry: index = %d",
				r.id, request.prevLogIndex)
			return response
		}

		prevLogEntry, err := r.log.GetEntry(request.prevLogIndex)
		if err != nil {
			r.options.logger.Fatalf("server %s failed to get entry from log: %s", r.id, err.Error())
		}

		// Reject the request if the log has the previous log entry, but its term does not match.
		if prevLogEntry.term != request.prevLogTerm {
			r.options.logger.Debugf("server %s rejecting AppendEntries RPC: previous log entry has different term: index = %d, localTerm = %d, remoteTerm = %d",
				r.id, request.prevLogIndex, prevLogEntry.term, request.prevLogTerm)
			return response
		}
	}

	response.success = true

	var toAppend []*LogEntry
	for i, entry := range request.entries {
		if r.log.LastIndex() < entry.index {
			toAppend = request.entries[i:]
			break
		}

		existing, _ := r.log.GetEntry(entry.index)
		if !existing.IsConflict(entry) {
			continue
		}

		r.options.logger.Infof("server %s truncating log: index = %d", r.id, entry.index)
		if err := r.log.Truncate(entry.index); err != nil {
			r.options.logger.Fatalf("server %s failed truncating log: %s", err.Error())
		}

		toAppend = request.entries[i:]
		break
	}

	r.log.AppendEntries(toAppend)

	if request.leaderCommit > r.commitIndex {
		r.commitIndex = util.Min(request.leaderCommit, r.log.LastIndex())
		r.applyCond.Broadcast()
	}

	return response
}

func (r *Raft) sendAppendEntries() {
	for _, peer := range r.peers {
		if peer.Id() == r.id {
			continue
		}

		go func(peer Peer) {
			r.mu.Lock()
			defer r.mu.Unlock()

			if r.state != Leader {
				return
			}

			nextIndex := peer.NextIndex()
			prevLogIndex := nextIndex - 1
			prevLogTerm := uint64(0)

			// TODO: change this after snapshots are added. Need to compare previous log index to last included index.
			if prevLogIndex != 0 && prevLogIndex < r.log.NextIndex() {
				prevEntry, err := r.log.GetEntry(prevLogIndex)
				if err != nil {
					r.options.logger.Fatalf("server %s failed getting entry from log: %s", r.id, err.Error())
				}
				prevLogTerm = prevEntry.term
			}

			entries := make([]*LogEntry, 0, r.log.NextIndex()-nextIndex)
			for index := nextIndex; index < r.log.NextIndex(); index++ {
				entry, err := r.log.GetEntry(index)
				if err != nil {
					r.options.logger.Fatalf("server %s failed getting entry from log: %s", r.id, err.Error())
				}
				entries = append(entries, entry)
			}

			request := AppendEntriesRequest{
				term:         r.currentTerm,
				leaderID:     r.id,
				prevLogIndex: prevLogIndex,
				prevLogTerm:  prevLogTerm,
				entries:      entries,
				leaderCommit: r.commitIndex,
			}

			r.mu.Unlock()
			response, err := peer.AppendEntries(request)
			r.mu.Lock()

			// Ensure that we have not transitioned out of the leader state.
			if err != nil || r.state != Leader {
				return
			}

			// Become a follower if a peer has a more up-to-date term.
			if response.term > r.currentTerm {
				r.becomeFollower(response.term)
				return
			}

			// If the peer rejected the request, decrement the next index associated with the peer.
			if !response.success && peer.NextIndex() > 1 {
				peer.SetNextIndex(peer.NextIndex() - 1)
				return
			} else if !response.success {
				return
			}

			if request.prevLogIndex+uint64(len(entries)) >= peer.NextIndex() {
				peer.SetNextIndex(request.prevLogIndex + uint64(len(entries)) + 1)
				peer.SetMatchIndex(peer.NextIndex() - 1)
				r.commitCond.Broadcast()
				go r.sendAppendEntries()
			}
		}(peer)
	}
}

func (r *Raft) requestVote(request RequestVoteRequest) RequestVoteResponse {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.options.logger.Debugf("server %s received RequestVote RPC: candidateID = %s, term = %d, lastLogIndex = %d, lastLogTerm = %d",
		r.id, request.candidateID, request.term, request.lastLogIndex, request.lastLogTerm)

	response := RequestVoteResponse{
		term:        r.currentTerm,
		voteGranted: false,
	}

	// Reject the request if the term is out-of-date.
	if request.term < r.currentTerm {
		r.options.logger.Debugf("server %s rejecting RequestVote RPC: out of date term: %d > %d", r.id, r.currentTerm, request.term)
		return response
	}

	// If the request has a more up-to-date term, update current term and
	// become a follower.
	if request.term > r.currentTerm {
		r.becomeFollower(request.term)
		response.term = r.currentTerm
	}

	// Reject the request if this server has already voted.
	if r.votedFor != "" && r.votedFor != request.candidateID {
		r.options.logger.Debugf("server %s rejecting RequestVote RPC: already voted: votedFor = %s", r.id, r.votedFor)
		return response
	}

	// Reject any requests with out-date-log.
	// To determine which log is more up-to-date:
	// 1. If the logs have last entries with different terms, than the log with the
	//    greater term is more up-to-date.
	// 2. If the logs end with the same term, the longer log is more up-to-date.
	if request.lastLogTerm < r.log.LastTerm() ||
		(request.lastLogTerm == r.log.LastTerm() && r.log.LastIndex() > request.lastLogIndex) {
		return response
	}

	r.lastContact = time.Now()
	r.votedFor = request.candidateID
	response.voteGranted = true
	r.persistTermAndVote()

	return response
}

func (r *Raft) sendRequestVote(votes *int) {
	for _, peer := range r.peers {
		if peer.Id() == r.id {
			continue
		}

		go func(peer Peer) {
			r.mu.Lock()
			defer r.mu.Unlock()

			request := RequestVoteRequest{
				candidateID:  r.id,
				term:         r.currentTerm,
				lastLogIndex: r.log.LastIndex(),
				lastLogTerm:  r.log.LastTerm(),
			}

			r.mu.Unlock()
			response, err := peer.RequestVote(request)
			r.mu.Lock()

			if err != nil {
				return
			}

			// Ensure this response is not stale. It is possible that this
			// server has started another election.
			if r.currentTerm != request.term {
				return
			}

			// Increment vote count if vote is granted.
			if response.voteGranted {
				*votes += 1
			}

			// Become a follower if a peer has a more up-to-date term.
			if response.term > r.currentTerm {
				r.becomeFollower(response.term)
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

func (r *Raft) installSnapshot(request *pb.InstallSnapshotRequest) *pb.InstallSnapshotResponse {
	panic("installSnapshot not implemented")
}

func (r *Raft) sendInstallSnapshot(peer *ProtobufPeer) {
	panic("sendInstallSnapshot not implemented")
}

func (r *Raft) takeSnapshot() {
	panic("takeSnapshot not implemented")
}

func (r *Raft) restoreFromSnapshot() error {
	panic("restoreFromSnapshot not implemented")
}

func (r *Raft) heartbeatLoop() {
	defer r.wg.Done()

	// If this server is the leader, broadcast heartbeat messages to peers
	// once every heartbeat interval (the default heartbeat interval is 50ms).
	for {
		time.Sleep(r.options.heartbeatInterval)

		r.mu.Lock()
		if r.state == Shutdown {
			r.mu.Unlock()
			return
		}

		// Only the leader sends heartbeats.
		if r.state == Follower {
			r.mu.Unlock()
			continue
		}

		r.sendAppendEntries()
		r.mu.Unlock()
	}
}

func (r *Raft) electionLoop() {
	defer r.wg.Done()

	for {
		// A random timeout between the specified election timeout (by default 200 ms) and twice the
		// election timeout is chosen to sleep for in order to prevent multiple servers from becoming
		// candidates at the same time.
		timeout := util.RandomTimeout(r.options.electionTimeout, 2*r.options.electionTimeout)
		time.Sleep(timeout * time.Millisecond)

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

	// If we have already been elected the leaser or we have been contacted by the leader
	// since the last election timeout, an election is not needed.
	if r.state != Follower || time.Since(r.lastContact) < r.options.electionTimeout {
		return
	}

	// This server votes for itself and then requests votes from all of its peers.
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

		// Followers may not commit log entries.
		if r.state != Leader {
			continue
		}

		// Indicates whether log entries have been committed and are ready to be applied.
		committed := false

		for index := r.commitIndex + 1; index <= r.log.LastIndex(); index++ {
			// It is NOT safe for the leader to commit an entry with a term
			// different than the current term. It is possible for a log entry
			// to be agreed upon by the majority of servers in the cluster, but
			// be overwritten by a future leader.
			if entry, err := r.log.GetEntry(index); err != nil {
				r.options.logger.Fatalf("server %s failed getting entry from log: %s", r.id, err.Error())
			} else if entry.term != r.currentTerm {
				continue
			}

			// Check whether the majority of servers in the cluster agree on the entry.
			// If they do, it is safe to commit.
			matches := 1
			for _, peer := range r.peers {
				if peer.Id() == r.id {
					continue
				}
				if peer.MatchIndex() >= index {
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
			r.sendAppendEntries()
		}
	}
}

func (r *Raft) applyLoop() {
	r.mu.Lock()
	defer r.mu.Unlock()
	defer r.wg.Done()

	for r.state != Shutdown {
		r.applyCond.Wait()

		// Scan the log starting at the entry following the last applied entry
		// and apply any entries that have been committed.
		for index := r.lastApplied + 1; index <= r.commitIndex; index++ {
			entry, err := r.log.GetEntry(index)
			if err != nil {
				r.options.logger.Fatalf("server %s failed getting entry from log: %s", r.id, err.Error())
			}

			response := CommandResponse{
				Index:    entry.index,
				Term:     entry.term,
				Command:  entry.data,
				Response: r.fsm.Apply(entry.data),
			}

			r.lastApplied++

			// Warning: do not hold locks when sending on the response channel. May deadlock
			// if the client is not listening.
			r.mu.Unlock()
			r.commandResponseCh <- response
			r.mu.Lock()

			r.options.logger.Debugf("server %s applied command: response = %v", r.id, response)
		}
	}
}

func (r *Raft) becomeCandidate() {
	r.currentTerm++
	r.votedFor = r.id
	r.persistTermAndVote()
	r.options.logger.Infof("server %s has entered the candidate state: term = %d", r.id, r.currentTerm)
}

func (r *Raft) becomeLeader() {
	r.state = Leader
	for _, peer := range r.peers {
		peer.SetNextIndex(r.log.LastIndex() + 1)
		peer.SetMatchIndex(0)
	}
	r.sendAppendEntries()
	r.options.logger.Infof("server %s has entered the leader state: term = %d", r.id, r.currentTerm)
}

func (r *Raft) becomeFollower(term uint64) {
	r.state = Follower
	r.currentTerm = term
	r.votedFor = ""
	r.persistTermAndVote()
	r.options.logger.Infof("server %s has entered the follower state: term = %d", r.id, r.currentTerm)
}

func (r *Raft) hasQuorum(count int) bool {
	return count > len(r.peers)/2
}

func (r *Raft) persistTermAndVote() {
	persistentState := &PersistentState{term: r.currentTerm, votedFor: r.votedFor}
	if err := r.storage.SetState(persistentState); err != nil {
		r.options.logger.Fatalf("server %s failed persisting term and vote: %s", err.Error())
	}
}
