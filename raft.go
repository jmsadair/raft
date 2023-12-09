package raft

import (
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/jmsadair/raft/internal/errors"
	"github.com/jmsadair/raft/internal/logger"
	"github.com/jmsadair/raft/internal/util"
)

var (
	errShutdown = errors.New("server is shutdown")
)

// NotLeaderError is an error returned when an operation is submitted to a
// server, and it is not the leader. Only the leader may submit operations.
type NotLeaderError struct {
	// The ID of the server the operation was submitted to.
	ServerID string

	// The ID of the server that this server recognizes as the leader. Note that this may not always be accurate.
	KnownLeader string
}

// This function implements the error interface for the NotLeaderError type.
// It formats and returns an error message indicating that the server with
// the ID e.ServerID is not the leader, and the known leader is e.KnownLeader.
func (e NotLeaderError) Error() string {
	return fmt.Sprintf("server %s is not the leader: knownLeader = %s", e.ServerID, e.KnownLeader)
}

// InvalidLeaseError is returned when a read-only operation is submitted but the lease expires or the server
// loses leadership before it can be applied to the state machine.
type InvalidLeaseError struct {
	// The ID of the server that the operation was submitted to.
	ServerID string
}

// This function implements the error interface for the InvalidLeaseError type.
// It formats and returns an error message indicating that the server with
// the ID e.ServerID does not have a valid lease.
func (e InvalidLeaseError) Error() string {
	return fmt.Sprintf("server %s does not have a valid lease", e.ServerID)
}

// State represents the current state of a Raft server.
// A Raft server may either be shutdown, the leader, or a follower.
type State uint32

const (
	// Leader is a state indicating that a server is responsible for replicating and
	// committing log entries. Typically, only one server in a cluster will be the leader.
	// However, if there are partitions or other failures, it is possible there is more than
	// one leader.
	Leader State = iota

	// Follower is a state indicating that a server is responsible for accepting log entries replicated
	// by the leader.
	// A server in the follower state may not accept operations for replication.
	Follower

	// Shutdown is a state indicating that the server is currently offline.
	Shutdown
)

// String converts a State into a string.
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

// Status is the status of a Raft instance.
type Status struct {
	// The ID of the Raft instance.
	ID string

	// The Address of the Raft instance.
	Address net.Addr

	// The current term.
	Term uint64

	// The current commit index.
	CommitIndex uint64

	// The index of the last log entry applied to the state machine.
	LastApplied uint64

	// The current state of Raft instance: leader, follower, shutdown.
	State State
}

// Protocol represents an abstraction of the raft consensus protocol.
// The raft protocol is a distributed consensus algorithm designed for fault-tolerant systems.
// It provides functions to start and stop the protocol, submit operations, check the status and manage snapshots.
type Protocol interface {
	// Start initializes the consensus protocol and prepares it to receive commands.
	// It may involve establishing network connections, allocating resources etc.
	// Returns error if the initialization fails.
	Start() error

	// Stop shuts down the consensus protocol safely. It may involve releasing resources,
	// closing network connections etc. Returns error if the shutdown fails.
	Stop() error

	// SubmitOperation takes a byte array representing an operation and adds it to the
	// protocol's log. It returns an OperationResponseFuture with the provided timeout,
	// representing a future response to applying the operation to the state machine.
	SubmitOperation(operation []byte, timeout time.Duration) *OperationResponseFuture

	// SubmitReadOnlyOperation takes a byte array representing a read-only operation and applies
	// it to the state machine without adding it to the protocol's log. It returns an OperationResponseFuture
	// with the provided timeout, representing a future response to applying the operation to the state machine.
	SubmitReadOnlyOperation(operation []byte, timeout time.Duration) *OperationResponseFuture

	// Status returns the current status of the protocol. The returned status includes information
	// like the current term, whether the protocol is a leader, follower or candidate, and more.
	Status() Status

	// RequestVote handles vote requests from other nodes during elections. It takes a vote request
	// and fills the response with the result of the vote. It returns an error if the vote request
	// fails to be processed.
	RequestVote(request *RequestVoteRequest, response *RequestVoteResponse) error

	// AppendEntries handles log replication requests from the leader. It takes a request to append
	// entries and fills the response with the result of the append operation. It returns an error
	// if the append operation fails.
	AppendEntries(request *AppendEntriesRequest, response *AppendEntriesResponse) error

	// InstallSnapshot handles snapshot installation requests from the leader. It takes a request to
	// install a snapshot and fills the response with the result of the installation. It returns an
	// error if the snapshot installation process fails.
	InstallSnapshot(request *InstallSnapshotRequest, response *InstallSnapshotResponse) error

	// ListSnapshots returns a list of all snapshots currently stored by the protocol.
	// Snapshots are used for log compaction and to bring new servers up to date.
	ListSnapshots() []Snapshot
}

// Raft implements the Protocol interface. This implementation of Raft should be utilized as the internal
// logic for an actual server, as it solely encapsulates the core functionality of Raft and cannot operate
// as a standalone server.
type Raft struct {
	// The ID of this Raft instance.
	id string

	// The ID that Raft instance believes is the leader. Used to redirect clients.
	leaderId string

	// The configuration options for this Raft instance.
	options options

	// The peers of this Raft instance.
	peers map[string]Peer

	// This contains the next log index to send to each server.
	nextIndex map[string]uint64

	// This contains the highest log entry known to be replicated on each sever.
	matchIndex map[string]uint64

	// Outstanding futures that have yet to be filled. Maps log index to the corresponding future.
	// Does not include futures for read-only operations.
	pendingFutures map[uint64]*OperationResponseFuture

	// A lease for the leader to serve read-only operations, must be nil if the server is not the leader.
	lease *lease

	// This stores and retrieves log entries in a durable manner.
	log Log

	// This stores and retrieves the vote and term in a durable manner.
	stateStorage StateStorage

	// This stores and retrieves snapshots in a durable manner.
	snapshotStorage SnapshotStorage

	// The state machine provided by the client that operations will be applied to.
	fsm StateMachine

	// A channel for notifying the apply loop that the commit index has been updated.
	applyCond *sync.Cond

	// A channel for notifying the commit loop that new log entries may be ready to be committed.
	commitCond *sync.Cond

	// A channel for transferring applied log entries from the apply loop to the fsm loop for
	// actual application to the state machine.
	fsmCh chan *Operation

	// The current state of this raft instance: leader, follower, or shutdown.
	state State

	// Index of the last log entry that was committed.
	commitIndex uint64

	// Index of the last log entry that was applied.
	lastApplied uint64

	// The current term of this Raft instance. Must be persisted.
	currentTerm uint64

	// The last included index of the most recent snapshot.
	lastIncludedIndex uint64

	// The last included term of the most recent snapshot.
	lastIncludedTerm uint64

	// ID of the candidate that this Raft instance voted for. Must be persisted.
	votedFor string

	// The timestamp representing the time of the last contact by the leader.
	lastContact time.Time

	// Ensures go routines have exited upon stopping.
	wg sync.WaitGroup

	// A mutex used for synchronization within the Raft struct
	mu sync.Mutex
}

// NewRaft creates a new instance of Raft that is configured with the provided options. Responses from
// applying operations to the state machine will be sent over the provided response channel. If the log,
// state storage, or snapshot storage contain any persisted state, it will be read into memory and Raft
// will be initialized with that state.
func NewRaft(
	id string,
	peers map[string]Peer,
	log Log,
	stateStorage StateStorage,
	snapshotStorage SnapshotStorage,
	fsm StateMachine,
	opts ...Option,
) (*Raft, error) {
	// Apply provided options.
	var options options
	for _, opt := range opts {
		if err := opt(&options); err != nil {
			return nil, errors.WrapError(err, "failed to apply raft option")
		}
	}

	// Set default values if option not provided.
	if options.logger == nil {
		defaultLogger, err := logger.NewLogger()
		if err != nil {
			return nil, errors.WrapError(err, "failed to create default raft logger")
		}
		options.logger = defaultLogger
	}
	if options.heartbeatInterval == 0 {
		options.heartbeatInterval = defaultHeartbeat
	}
	if options.electionTimeout == 0 {
		options.electionTimeout = defaultElectionTimeout
	}
	if options.maxEntriesPerRPC == 0 {
		options.maxEntriesPerRPC = defaultMaxEntriesPerRPC
	}
	if options.leaseDuration == 0 {
		options.leaseDuration = defaultLeaseDuration
	}

	// Open the state storage to recover persisted state.
	if err := stateStorage.Open(); err != nil {
		return nil, errors.WrapError(err, "failed to open state storage")
	}

	// Replay the persisted state into memory.
	if err := stateStorage.Replay(); err != nil {
		return nil, errors.WrapError(err, "failed to replay state storage")
	}

	// Restore the current term and vote if they have been persisted.
	currentTerm, votedFor, err := stateStorage.State()
	if err != nil {
		return nil, errors.WrapError(err, "failed to retrieve state from state storage")
	}

	// Open the log for new operations.
	if err := log.Open(); err != nil {
		return nil, errors.WrapError(err, "failed to open log")
	}

	// Replay the persisted state of the log into memory.
	if err := log.Replay(); err != nil {
		return nil, errors.WrapError(err, "failed to replay log")
	}

	// Open the snapshot stateStorage for new operations.
	if err := snapshotStorage.Open(); err != nil {
		return nil, errors.WrapError(err, "failed to open snapshot storage")
	}

	// Replay the persisted snapshots into memory.
	if err := snapshotStorage.Replay(); err != nil {
		return nil, errors.WrapError(err, "failed to replay snapshot storage")
	}

	nextIndex := make(map[string]uint64)
	matchIndex := make(map[string]uint64)
	for _, peer := range peers {
		nextIndex[peer.ID()] = 0
		matchIndex[peer.ID()] = 0
	}

	raft := &Raft{
		id:              id,
		options:         options,
		peers:           peers,
		nextIndex:       nextIndex,
		matchIndex:      matchIndex,
		pendingFutures:  make(map[uint64]*OperationResponseFuture),
		log:             log,
		stateStorage:    stateStorage,
		snapshotStorage: snapshotStorage,
		fsm:             fsm,
		fsmCh:           make(chan *Operation, 100),
		currentTerm:     currentTerm,
		votedFor:        votedFor,
		state:           Shutdown,
		commitIndex:     0,
		lastApplied:     0,
	}

	raft.applyCond = sync.NewCond(&raft.mu)
	raft.commitCond = sync.NewCond(&raft.mu)

	// Restore the state machine from the most recent snapshot if there was one.
	snapshot, ok := snapshotStorage.LastSnapshot()
	if ok {
		raft.lastIncludedIndex = snapshot.LastIncludedIndex
		raft.lastIncludedTerm = snapshot.LastIncludedTerm
		raft.commitIndex = snapshot.LastIncludedIndex
		raft.lastApplied = snapshot.LastIncludedIndex

		if err := raft.fsm.Restore(&snapshot); err != nil {
			return nil, errors.WrapError(err, "failed to restore state machine")
		}
	}

	return raft, nil
}

// Start starts the Raft instance if it is not already started. Once started,
// the Raft instance transitions to the follower state and is ready to start
// sending and receiving RPCs.
func (r *Raft) Start() error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.state != Shutdown {
		return nil
	}

	r.lastContact = time.Now()
	r.state = Follower

	for _, peer := range r.peers {
		if err := peer.Connect(); err != nil {
			r.options.logger.Errorf("error connecting to peer: %s", err.Error())
		}
	}

	r.wg.Add(4)
	go r.applyLoop()
	go r.electionLoop()
	go r.heartbeatLoop()
	go r.commitLoop()

	// Do not increment the wait group for this go routine. This go routine is terminated
	// independently after all other go routines are shut down to avoid a race where read-only
	// operations are written to the fsm channel after is closed.
	go r.fsmLoop()

	r.options.logger.Infof(
		"server %s started: electionTimeout = %v, heartbeatInterval = %v, leaseDuration = %v, maxLogEntriesPerRPC = %v",
		r.id,
		r.options.electionTimeout,
		r.options.heartbeatInterval,
		r.options.leaseDuration,
		r.options.maxEntriesPerRPC,
	)

	return nil
}

// Stop stops the Raft instance if is not already stopped.
func (r *Raft) Stop() error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.state == Shutdown {
		return nil
	}

	// Indicate shutdown state and notify commit loop and apply loop so that they can return.
	r.state = Shutdown
	r.applyCond.Broadcast()
	r.commitCond.Broadcast()

	r.mu.Unlock()
	r.wg.Wait()
	r.mu.Lock()

	close(r.fsmCh)

	for _, peer := range r.peers {
		if err := peer.Disconnect(); err != nil {
			r.options.logger.Errorf(
				"server %s failed to disconnect from peer: %s",
				r.id,
				err.Error(),
			)
		}
	}

	if err := r.log.Close(); err != nil {
		r.options.logger.Errorf("server %s failed to close log: %s", r.id, err.Error())
	}

	if err := r.stateStorage.Close(); err != nil {
		r.options.logger.Errorf("server %s failed to close stateStorage: %s", r.id, err.Error())
	}

	if err := r.snapshotStorage.Close(); err != nil {
		r.options.logger.Errorf(
			"server %s failed to close snapshot stateStorage: %s",
			r.id,
			err.Error(),
		)
	}

	r.options.logger.Infof("server %s stopped", r.id)

	return nil
}

// SubmitOperation accepts an operation (as bytes) from a client for replication and
// returns a future for the response to the operation. Note that submitting an operation
// for replication does not guarantee replication if there are failures. Once the operation
// has been replicated and applied to the state machine, the future will be populated with the
// response.
func (r *Raft) SubmitOperation(operation []byte, timeout time.Duration) *OperationResponseFuture {
	r.mu.Lock()
	defer r.mu.Unlock()

	future := NewOperationResponseFuture(operation, timeout)

	if r.state != Leader {
		future.responseCh <- OperationResponse{Err: NotLeaderError{ServerID: r.id, KnownLeader: r.leaderId}}
		return future
	}

	entry := NewLogEntry(r.log.NextIndex(), r.currentTerm, operation)
	if err := r.log.AppendEntry(entry); err != nil {
		r.options.logger.Fatalf("server %s failed to append entry to log: %s", err.Error())
	}

	r.pendingFutures[entry.Index] = future

	r.sendAppendEntriesToPeers()

	r.options.logger.Debugf("server %s submitted operation: logEntry = %v", r.id, entry)

	return future
}

// SubmitReadOnlyOperation accepts an operation (as bytes) from a client and immediately
// applies it to the state machine without replication if this server has a valid lease.
// Consequently, read-only operations will generally be significantly more performant than
// replicated operations. However, read-only operations may read stale or incorrect data
// under certain conditions.
func (r *Raft) SubmitReadOnlyOperation(
	operation []byte,
	timeout time.Duration,
) *OperationResponseFuture {
	r.mu.Lock()
	defer r.mu.Unlock()

	future := NewOperationResponseFuture(operation, timeout)

	// It is necessary to increment the wait group to avoid a race where an operation is written
	// to the fsm channel after it is closed during the shutdown process.
	r.wg.Add(1)
	defer r.wg.Done()

	if r.state != Leader {
		future.responseCh <- OperationResponse{Err: NotLeaderError{ServerID: r.id, KnownLeader: r.leaderId}}
		return future
	}

	readOnlyOperation := &Operation{
		Bytes:      operation,
		IsReadOnly: true,
		ResponseCh: future.responseCh,
	}

	r.options.logger.Debugf(
		"server %s submitted read-only operation: operation = %v",
		r.id,
		operation,
	)

	r.mu.Unlock()
	r.fsmCh <- readOnlyOperation
	r.mu.Lock()

	return future
}

// Status returns the status of the Raft instance. The status includes
// the ID, address, term, commit index, last applied index, and state.
func (r *Raft) Status() Status {
	r.mu.Lock()
	defer r.mu.Unlock()

	return Status{
		ID:          r.id,
		Address:     r.peers[r.id].Address(),
		Term:        r.currentTerm,
		CommitIndex: r.commitIndex,
		LastApplied: r.lastApplied,
		State:       r.state,
	}
}

// RequestVote is invoked by the candidate server to gather a vote from this server.
func (r *Raft) RequestVote(request *RequestVoteRequest, response *RequestVoteResponse) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.state == Shutdown {
		return errShutdown
	}

	r.options.logger.Debugf(
		"server %s received RequestVote RPC: candidateID = %s, term = %d, lastLogIndex = %d, lastLogTerm = %d",
		r.id,
		request.CandidateID,
		request.Term,
		request.LastLogIndex,
		request.LastLogTerm,
	)

	response.Term = r.currentTerm
	response.VoteGranted = false

	// Reject the request if the term is out-of-date.
	if request.Term < r.currentTerm {
		r.options.logger.Debugf(
			"server %s rejecting RequestVote RPC: out of date term: %d > %d",
			r.id,
			r.currentTerm,
			request.Term,
		)
		return nil
	}

	// If the request has a more up-to-date term, update current term and become a follower.
	if request.Term > r.currentTerm {
		r.becomeFollower(request.CandidateID, request.Term)
		response.Term = r.currentTerm
	}

	// Reject the request if this server has already voted.
	if r.votedFor != "" && r.votedFor != request.CandidateID {
		r.options.logger.Debugf(
			"server %s rejecting RequestVote RPC: already voted: votedFor = %s",
			r.id,
			r.votedFor,
		)
		return nil
	}

	// Reject any requests with an out-of-date log.
	// To determine which log is more up-to-date:
	// 1. If the logs have last entries with different terms, then the log with the
	//    greater term is more up-to-date.
	// 2. If the logs end with the same term, the longer log is more up-to-date.
	if request.LastLogTerm < r.log.LastTerm() ||
		(request.LastLogTerm == r.log.LastTerm() && r.log.LastIndex() > request.LastLogIndex) {
		return nil
	}

	r.lastContact = time.Now()
	response.VoteGranted = true

	// Update this server's vote and write it to disk.
	r.votedFor = request.CandidateID
	r.persistTermAndVote()

	return nil
}

// AppendEntries is invoked by the leader to replicate log entries.
func (r *Raft) AppendEntries(request *AppendEntriesRequest, response *AppendEntriesResponse) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.state == Shutdown {
		return errShutdown
	}

	r.options.logger.Debugf(
		"server %s received AppendEntries RPC: leaderID = %s, leaderCommit = %d, term = %d, prevLogIndex = %d, prevLogTerm = %d",
		r.id,
		request.LeaderID,
		request.LeaderCommit,
		request.Term,
		request.PrevLogIndex,
		request.PrevLogTerm,
	)

	response.Term = r.currentTerm
	response.Success = false

	// Reject any requests with an out-of-date term.
	if request.Term < r.currentTerm {
		r.options.logger.Debugf(
			"server %s rejecting AppendEntries RPC: out of date term: %d > %d",
			r.id,
			r.currentTerm,
			request.Term,
		)
		return nil
	}

	// Update the time of last contact - note that this should be done even
	// if the request is rejected due to having a non-matching previous log entry.
	r.lastContact = time.Now()

	// Update the ID of the server that this server recognizes as the leader.
	r.leaderId = request.LeaderID

	// If the request has a more up-to-date term, update current term and
	// become a follower.
	if request.Term > r.currentTerm {
		r.becomeFollower(request.LeaderID, request.Term)
		response.Term = r.currentTerm
	}

	// Reject the request if the log has been compacted and no longer contains the previous log entry.
	if r.lastIncludedIndex > request.PrevLogIndex {
		r.options.logger.Debugf(
			"server %s rejecting AppendEntries RPC: server does not have previous log entry: index = %d",
			r.id,
			request.PrevLogIndex,
		)
		response.Index = r.lastIncludedIndex + 1
		return nil
	}

	// Reject the request if the log is too short to contain the previous log entry.
	if r.log.NextIndex() <= request.PrevLogIndex {
		r.options.logger.Debugf(
			"server %s rejecting AppendEntries RPC: server does not have previous log entry: index = %d",
			r.id,
			request.PrevLogIndex,
		)
		response.Index = r.log.NextIndex()
		return nil
	}

	// Reject the request if the previous log index matches the last included log index, but the previous log term does
	// not match the last included term.
	if r.lastIncludedIndex == request.PrevLogIndex && r.lastIncludedTerm != request.PrevLogTerm {
		r.options.logger.Debugf(
			"server %s rejecting AppendEntries RPC: previous log entry has different term: index = %d, localTerm = %d, remoteTerm = %d",
			r.id,
			request.PrevLogIndex,
			r.lastIncludedTerm,
			request.PrevLogTerm,
		)
		response.Index = r.lastIncludedIndex
		return nil

	}

	if r.lastIncludedIndex < request.PrevLogIndex {
		prevLogEntry, err := r.log.GetEntry(request.PrevLogIndex)
		if err != nil {
			r.options.logger.Fatalf("server %s failed to get entry from log: %s", r.id, err.Error())
		}

		// Reject the request if the log has the previous log entry, but its term does not match.
		if prevLogEntry.Term != request.PrevLogTerm {
			r.options.logger.Debugf(
				"server %s rejecting AppendEntries RPC: previous log entry has different term: index = %d, localTerm = %d, remoteTerm = %d",
				r.id,
				request.PrevLogIndex,
				prevLogEntry.Term,
				request.PrevLogTerm,
			)

			// Find the first index of the conflicting term.
			var index uint64
			for index = request.PrevLogIndex - 1; index > r.lastIncludedIndex; index-- {
				entry, err := r.log.GetEntry(index)
				if err != nil {
					r.options.logger.Fatalf(
						"server %s failed to get entry from log: %s",
						r.id,
						err.Error(),
					)
				}
				if entry.Term != prevLogEntry.Term {
					break
				}
			}
			response.Index = index + 1
			return nil
		}
	}

	response.Success = true

	var toAppend []*LogEntry
	for i, entry := range request.Entries {
		if r.log.LastIndex() < entry.Index {
			toAppend = request.Entries[i:]
			break
		}

		existing, _ := r.log.GetEntry(entry.Index)
		if !existing.IsConflict(entry) {
			continue
		}

		r.options.logger.Warnf("server %s truncating log: index = %d", r.id, entry.Index)

		if err := r.log.Truncate(entry.Index); err != nil {
			r.options.logger.Fatalf("server %s failed truncating log: %s", err.Error())
		}

		toAppend = request.Entries[i:]
		break
	}

	if err := r.log.AppendEntries(toAppend); err != nil {
		r.options.logger.Fatalf("server %s failed to append entries to log: %s", r.id, err.Error())
	}

	// Update the commit index.
	if request.LeaderCommit > r.commitIndex {
		r.commitIndex = util.Min(request.LeaderCommit, r.log.LastIndex())
		r.applyCond.Broadcast()
	}

	return nil
}

// InstallSnapshot is invoked by the leader to send a snapshot to a follower.
func (r *Raft) InstallSnapshot(
	request *InstallSnapshotRequest,
	response *InstallSnapshotResponse,
) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.state == Shutdown {
		return errShutdown
	}

	r.options.logger.Debugf(
		"server %s received InstallSnapshot request: leaderID = %s, term = %d, lastIncludedIndex = %d, lastIncludedTerm = %d",
		r.id,
		request.LeaderID,
		request.Term,
		request.LastIncludedIndex,
		request.LastIncludedTerm,
	)

	response.Term = r.currentTerm

	// Reject the request if the term is out-of-date.
	if r.currentTerm > request.Term {
		r.options.logger.Debugf(
			"server %s rejecting InstallSnapshot request: out of date term: %d > %d",
			r.id,
			r.currentTerm,
			request.Term,
		)
		return nil
	}

	// If the request has a more up-to-date term, update current term and become a follower.
	if r.currentTerm < request.Term {
		r.becomeFollower(request.LeaderID, request.Term)
		response.Term = request.Term
	}

	r.lastContact = time.Now()

	// The snapshot does not contain any new information.
	if r.lastIncludedIndex >= request.LastIncludedIndex ||
		r.commitIndex >= request.LastIncludedIndex {
		return nil
	}

	var entry *LogEntry
	if r.log.Contains(request.LastIncludedIndex) {
		entry, _ = r.log.GetEntry(request.LastIncludedIndex)
	}

	// Persist the snapshot.
	snapshot := NewSnapshot(request.LastIncludedIndex, request.LastIncludedTerm, request.Bytes)
	if err := r.snapshotStorage.SaveSnapshot(snapshot); err != nil {
		r.options.logger.Fatalf("server %s failed to save snapshot: %s", r.id, err.Error())
	}

	r.commitIndex = request.LastIncludedIndex
	r.lastApplied = request.LastIncludedIndex
	r.lastIncludedIndex = request.LastIncludedIndex
	r.lastIncludedTerm = request.LastIncludedTerm

	// If the log either does not have an entry at the last included index or the log has an
	// entry at the last included index but its term does not match the last included term, then
	// discard the log and reset the state machine with the data from the snapshot.
	if entry == nil || entry.Term != request.LastIncludedTerm {
		if err := r.log.DiscardEntries(r.lastIncludedIndex, r.lastIncludedTerm); err != nil {
			r.options.logger.Fatalf(
				"server %s failed to discard log entries: %s",
				r.id,
				err.Error(),
			)
		}
		if err := r.fsm.Restore(snapshot); err != nil {
			r.options.logger.Fatalf(
				"server %s failed to reset state machine with snapshot: %s",
				r.id,
				err.Error(),
			)
		}
		return nil
	}

	// Otherwise, if the log has an entry at last included index with a term that matches the last included
	// term, then compact the log up to and including that entry.
	if err := r.log.Compact(request.LastIncludedIndex); err != nil {
		r.options.logger.Fatalf("server %s failed to compact log: %s", r.id, err.Error())
	}

	return nil
}

// ListSnapshots returns an array of all the snapshots that have been taken.
func (r *Raft) ListSnapshots() []Snapshot {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.snapshotStorage.ListSnapshots()
}

// sendAppendEntriesToPeers sends an AppendEntries RPC to all peers concurrently.
// Expects lock to be held.
func (r *Raft) sendAppendEntriesToPeers() {
	numResponses := 1
	for _, peer := range r.peers {
		go r.sendAppendEntries(peer, &numResponses)
	}
}

// sendAppendEntries sends an AppendEntries RPC to the provided peer.
func (r *Raft) sendAppendEntries(peer Peer, numResponses *int) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.state != Leader {
		return
	}

	// Handle the single server case.
	if peer.ID() == r.id {
		if len(r.peers) == 1 {
			if r.log.LastIndex() > r.commitIndex {
				r.commitCond.Broadcast()
			}
			r.lease.renew()
		}
		return
	}

	nextIndex := r.nextIndex[peer.ID()]
	prevLogIndex := util.Max(nextIndex-1, r.lastIncludedIndex)
	prevLogTerm := r.lastIncludedTerm

	if prevLogIndex > r.lastIncludedIndex && prevLogIndex < r.log.NextIndex() {
		prevEntry, err := r.log.GetEntry(prevLogIndex)
		if err != nil {
			r.options.logger.Fatalf(
				"server %s failed getting entry from log: %s",
				r.id,
				err.Error(),
			)
		}
		prevLogTerm = prevEntry.Term
	}

	// Retrieve the log entries that need to be replicated to the peer.
	entries := make([]*LogEntry, 0, r.log.NextIndex()-nextIndex)
	for index := nextIndex; index < r.log.NextIndex(); index++ {
		// Make sure that the index is in bounds since the log may have been compacted.
		// Make sure the max entries per RPC is not exceeded.
		if index <= r.lastIncludedIndex || len(entries) >= r.options.maxEntriesPerRPC {
			break
		}

		entry, err := r.log.GetEntry(index)
		if err != nil {
			r.options.logger.Fatalf(
				"server %s failed getting entry from log: %s",
				r.id,
				err.Error(),
			)
		}

		entries = append(entries, entry)
	}

	request := AppendEntriesRequest{
		Term:         r.currentTerm,
		LeaderID:     r.id,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
		Entries:      entries,
		LeaderCommit: r.commitIndex,
	}

	r.mu.Unlock()
	response, err := peer.AppendEntries(request)
	r.mu.Lock()

	// Ensure that we did not transition out of the leader state after
	// releasing the lock.
	if err != nil || r.state != Leader {
		return
	}

	// Become a follower if a peer has a more up-to-date term.
	if response.Term > r.currentTerm {
		r.becomeFollower(peer.ID(), response.Term)
		return
	}

	// Renew the lease if the majority of peers have responded.
	// Set the response counter to nil to prevent additional renewals during
	// this round of heartbeats.
	if numResponses != nil {
		*numResponses += 1
		if r.hasQuorum(*numResponses) {
			r.lease.renew()
			numResponses = nil
		}
	}

	if !response.Success {
		r.nextIndex[peer.ID()] = util.Max(1, util.Min(r.log.NextIndex(), response.Index))

		// The log has been compacted and no longer contains the entries the peer needs.
		// Send the peer a snapshot to catch them up.
		if r.nextIndex[peer.ID()] <= r.lastIncludedIndex {
			go r.sendInstallSnapshot(peer)
		}

		return
	}

	// Update the next and match index of the peer.
	if request.PrevLogIndex+uint64(len(entries)) >= r.nextIndex[peer.ID()] {
		r.nextIndex[peer.ID()] = request.PrevLogIndex + uint64(len(entries)) + 1
		r.matchIndex[peer.ID()] = request.PrevLogIndex + uint64(len(entries))
		r.commitCond.Broadcast()
	}
}

// sendRequestVoteToPeers sends a RequestVote RPC to all peers concurrent. Expects
// lock to be held.
func (r *Raft) sendRequestVoteToPeers(votes *int) {
	for _, peer := range r.peers {
		go r.sendRequestVote(peer, votes)
	}
}

// sendRequestVote sends a RequestVote RPC to the provided peer.
func (r *Raft) sendRequestVote(peer Peer, votes *int) {
	r.mu.Lock()
	defer r.mu.Unlock()

	// The candidate will vote for itself.
	if peer.ID() == r.id {
		*votes++

		// Handle the single server case.
		if r.hasQuorum(*votes) {
			r.becomeLeader()
		}

		return
	}

	request := RequestVoteRequest{
		CandidateID:  r.id,
		Term:         r.currentTerm,
		LastLogIndex: r.log.LastIndex(),
		LastLogTerm:  r.log.LastTerm(),
	}

	r.mu.Unlock()
	response, err := peer.RequestVote(request)
	r.mu.Lock()

	// Ensure this response is not stale. It is possible that this
	// server has started another election.
	if err != nil || r.currentTerm != request.Term {
		return
	}

	// Increment vote count if vote is granted.
	if response.VoteGranted {
		*votes++
	}

	// Become a follower if a peer has a more up-to-date term.
	if response.Term > r.currentTerm {
		r.becomeFollower(peer.ID(), response.Term)
		return
	}

	// If we have received votes from the majority of peers, become a leader.
	if r.hasQuorum(*votes) && r.state == Follower {
		r.becomeLeader()
	}
}

// takeSnapshot takes a snapshot of the current state of the state machine,
// persists the snapshot, and compacts the log up to and including the
// last included index of the snapshot.
func (r *Raft) takeSnapshot(lastIncludedIndex, lastIncludedTerm uint64) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if lastIncludedIndex >= r.log.LastIndex() {
		return
	}

	// Retrieve current state of state machine.
	snapshotBytes, err := r.fsm.Snapshot()
	if err != nil {
		r.options.logger.Fatalf(
			"server %s failed to take snapshot of state machine: %s",
			r.id,
			err.Error(),
		)
	}

	r.lastIncludedIndex = lastIncludedIndex
	r.lastIncludedTerm = lastIncludedTerm
	snapshot := NewSnapshot(r.lastIncludedIndex, r.lastIncludedTerm, snapshotBytes)

	// Persist the snapshot.
	if err := r.snapshotStorage.SaveSnapshot(snapshot); err != nil {
		r.options.logger.Fatalf("server %s failed to save snapshot: %s", r.id, err.Error())
	}

	r.options.logger.Warnf("server %s compacting log: index = %d", r.id, r.lastIncludedIndex)

	// Compact the log up to the last log entry that was applied to the state machine.
	if err := r.log.Compact(r.lastIncludedIndex); err != nil {
		r.options.logger.Fatalf("server %s failed compacting log: %s", r.id, err.Error())
	}

	r.options.logger.Infof("server %s took snapshot: lastIncludedIndex = %d, lastIncludedTerm = %d",
		r.id, r.lastIncludedIndex, r.lastIncludedTerm)
}

// sendInstallSnapshot sends an InstallSnapshot RPC to the provided peer.
func (r *Raft) sendInstallSnapshot(peer Peer) {
	r.mu.Lock()
	defer r.mu.Unlock()

	// Only leaders may send snapshots. If the leader has the log entry corresponding to the next
	// index of a peer, a snapshot is not necessary.
	if r.state != Leader || r.nextIndex[peer.ID()] > r.lastIncludedIndex {
		return
	}

	snapshot, ok := r.snapshotStorage.LastSnapshot()
	if !ok {
		return
	}

	request := InstallSnapshotRequest{
		LeaderID:          r.id,
		Term:              r.currentTerm,
		LastIncludedIndex: snapshot.LastIncludedIndex,
		LastIncludedTerm:  snapshot.LastIncludedTerm,
		Bytes:             snapshot.Data,
	}

	r.mu.Unlock()
	response, err := peer.InstallSnapshot(request)
	r.mu.Lock()

	if err != nil {
		return
	}

	// If the peer has a more up-to-date term, transition to the follower state.
	if response.Term > r.currentTerm {
		r.becomeFollower(peer.ID(), response.Term)
		return
	}

	r.nextIndex[peer.ID()] = request.LastIncludedIndex + 1
	r.matchIndex[peer.ID()] = request.LastIncludedIndex
}

// heartbeatLoop is a long-running loop that periodically sends AppendEntries RPCs
// to all the peers if this server is the leader. This function should be called
// when the Raft instance is started and should be run as a separate go routine.
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

		r.sendAppendEntriesToPeers()
		r.mu.Unlock()
	}
}

// electionLoop is a long-running loop that kicks off an election if this server
// has not heard from the leader within the election timeout. This should be called
// when the Raft instance is started and should be run as a separate go routine.
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

// election makes this server a candidate and sends RequestVote RPCs to all peers.
func (r *Raft) election() {
	r.mu.Lock()
	defer r.mu.Unlock()

	// If we have already been elected the leaser, or we have been contacted by the leader
	// since the last election timeout, an election is not needed.
	if r.state != Follower || time.Since(r.lastContact) < r.options.electionTimeout {
		return
	}

	var votesReceived int
	r.becomeCandidate()
	r.sendRequestVoteToPeers(&votesReceived)
}

// commitLoop is a long-running loop that updates the commit index when signaled. If the commit
// index has been updated, this loop will signal to the apply loop that entries may be applied
// to the state machine. This should be called when the Raft instance is started and should be
// run as a separate go routine.
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
			// different from the current term. It is possible for a log entry
			// to be agreed upon by the majority of servers in the cluster, but
			// be overwritten by a future leader.
			if entry, err := r.log.GetEntry(index); err != nil {
				r.options.logger.Fatalf(
					"server %s failed getting entry from log: %s",
					r.id,
					err.Error(),
				)
			} else if entry.Term != r.currentTerm {
				continue
			}

			// Check whether the majority of servers in the cluster agree on the entry.
			// If they do, it is safe to commit.
			matches := 1
			for id, matchIndex := range r.matchIndex {
				if id == r.id {
					continue
				}
				if matchIndex >= index {
					matches++
				}
			}

			if r.hasQuorum(matches) {
				r.commitIndex = index
				committed = true
			}
		}

		if committed {
			r.applyCond.Broadcast()
			r.sendAppendEntriesToPeers()
		}
	}
}

// applyLoop is a long-running loop that sends log entries to the state machine loop to applied when
// signaled. This should be called when the Raft instance is started and ran as a separate go routine.
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
				r.options.logger.Fatalf(
					"server %s failed getting entry from log: %s",
					r.id,
					err.Error(),
				)
			}

			var responseCh chan OperationResponse
			if future, ok := r.pendingFutures[entry.Index]; ok {
				responseCh = future.responseCh
			}

			operation := &Operation{
				Bytes:      entry.Data,
				IsReadOnly: false,
				LogIndex:   entry.Index,
				LogTerm:    entry.Term,
				ResponseCh: responseCh,
			}

			// Clean up the pending future.
			delete(r.pendingFutures, entry.Index)

			// Warning: do not hold locks when sending on the response channel. May deadlock
			// if the client is not listening.
			r.mu.Unlock()
			r.fsmCh <- operation
			r.mu.Lock()

			r.lastApplied++
		}
	}
}

// fsmLoop is a long-running loop that applies operations to the state machine and sends responses to
// operations over the response channel. If auto snapshotting is enabled, it will take a snapshot once
// enough entries have been applied. This should be called when the Raft instance is started and ran
// as a separate go routine.
func (r *Raft) fsmLoop() {
	for operation := range r.fsmCh {
		response := OperationResponse{Operation: *operation}

		r.mu.Lock()
		logSizeInBytes, err := r.log.SizeInBytes()
		if err != nil {
			r.options.logger.Fatalf("failed to get log size: %s", err.Error())
		}

		// Ensure that the lease is valid before performing read-only operation.
		// This check should be as close as possible to where the operation is
		// actually applied to the state machine to reduce the risk of reading stale
		// data.
		if operation.IsReadOnly {
			if r.lease == nil {
				r.options.logger.Debugf(
					"server %s rejecting read-only operation: NotLeaderError",
					r.id,
				)
				response.Err = NotLeaderError{ServerID: r.id, KnownLeader: r.leaderId}
			} else if !r.lease.isValid() {
				r.options.logger.Debugf("server %s rejecting read-only operation: InvalidLeaseError", r.id)
				response.Err = InvalidLeaseError{ServerID: r.id}
			}
			r.mu.Unlock()
			r.sendResponseWithoutBlocking(operation.ResponseCh, response)
			continue
		}
		r.mu.Unlock()

		response.Response = r.fsm.Apply(operation)

		r.options.logger.Debugf(
			"server %s applied operation: read-only = %v, index = %d, term = %d",
			r.id,
			operation.IsReadOnly,
			operation.LogIndex,
			operation.LogTerm,
		)

		r.sendResponseWithoutBlocking(operation.ResponseCh, response)

		// Take a snapshot of the state machine if necessary.
		if !operation.IsReadOnly && r.fsm.NeedSnapshot(logSizeInBytes) {
			r.takeSnapshot(operation.LogIndex, operation.LogTerm)
		}
	}
}

// becomeCandidate transitions this server to the candidate state. It increments the term and sets this
// server's vote as itself. Expects lock to be held.
func (r *Raft) becomeCandidate() {
	r.currentTerm++
	r.votedFor = r.id
	r.persistTermAndVote()
	r.options.logger.Infof(
		"server %s has entered the candidate state: term = %d",
		r.id,
		r.currentTerm,
	)
}

// becomeLeader transitions this server to the leader state. It sets the next index for all peers to the index
// following the last log index and sets the match index for all peers to zero. Expects lock to be held.
func (r *Raft) becomeLeader() {
	r.state = Leader
	for _, peer := range r.peers {
		r.nextIndex[peer.ID()] = r.log.LastIndex() + 1
		r.matchIndex[peer.ID()] = 0
	}
	r.lease = newLease(r.options.leaseDuration)
	r.sendAppendEntriesToPeers()
	r.options.logger.Infof("server %s has entered the leader state: term = %d", r.id, r.currentTerm)
}

// becomeFollower transitions this server to the follower state. It clears the vote, sets the term to
// the provided term, and sets the leader ID to the provided ID. Additionally, it responds to any pending futures
// with a NotLeaderError. Expects lock to be held.
func (r *Raft) becomeFollower(leaderID string, term uint64) {
	r.state = Follower
	r.currentTerm = term
	r.leaderId = leaderID
	r.votedFor = ""
	r.lease = nil
	r.persistTermAndVote()
	r.options.logger.Infof(
		"server %s has entered the follower state: term = %d",
		r.id,
		r.currentTerm,
	)

	// Clean up the pending futures.
	response := OperationResponse{Err: NotLeaderError{ServerID: r.id, KnownLeader: r.leaderId}}
	for _, pendingFuture := range r.pendingFutures {
		r.sendResponseWithoutBlocking(pendingFuture.responseCh, response)
	}
	r.pendingFutures = make(map[uint64]*OperationResponseFuture)
}

// hasQuorum indicates whether the provided count constitutes a majority
// of the cluster. Expects lock to be held.
func (r *Raft) hasQuorum(count int) bool {
	return count > len(r.peers)/2
}

// persistTermAndVote writes the term and vote to the provided state stateStorage mechanism.
// Expects lock to be held.
func (r *Raft) persistTermAndVote() {
	if err := r.stateStorage.SetState(r.currentTerm, r.votedFor); err != nil {
		r.options.logger.Fatalf("server %s failed persisting term and vote: %s", r.id, err.Error())
	}
}

// disconnectPeer disconnects the peer with the provided ID from this server. This
// is primarily for testing purposes.
func (r *Raft) disconnectPeer(id string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if err := r.peers[id].Disconnect(); err != nil {
		return errors.WrapError(err, "server failed to disconnect peer: %s", err.Error())
	}
	return nil
}

// connectPeer connects the peer with the provided ID from this server. This
// is primarily for testing purposes.
func (r *Raft) connectPeer(id string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if err := r.peers[id].Connect(); err != nil {
		return errors.WrapError(err, "server failed to connect peer: %s", err.Error())
	}
	return nil
}

// sendResponseWithoutBlocking sends the provided response over the provided channel without blocking.
func (r *Raft) sendResponseWithoutBlocking(
	responseCh chan OperationResponse,
	response OperationResponse,
) {
	select {
	case responseCh <- response:
	default:
	}
}
