package raft

import (
	"bytes"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/jmsadair/raft/internal/errors"
	"github.com/jmsadair/raft/internal/logger"
	"github.com/jmsadair/raft/internal/util"
)

var errShutdown = errors.New("server is shutdown")

// NotLeaderError is an error returned when an operation is submitted to a
// server, and it is not the leader. Only the leader may submit operations.
type NotLeaderError struct {
	// The ID of the server the operation was submitted to.
	ServerID string

	// The ID of the server that this server recognizes as the leader. Note that this may not always be accurate.
	KnownLeader string
}

// Error formats and returns an error message indicating that the server with
// the ID e.ServerID is not the leader, and the known leader is e.KnownLeader.
func (e NotLeaderError) Error() string {
	return fmt.Sprintf("server %s is not the leader: knownLeader = %s", e.ServerID, e.KnownLeader)
}

// InvalidLeaseError is returned when a lease-based read-only operation is
// submitted but the lease expires or the server loses leadership before
// it can be applied to the state machine.
type InvalidLeaseError struct {
	// The ID of the server that the operation was submitted to.
	ServerID string
}

// Error formats and returns an error message indicating that the server with
// the ID e.ServerID does not have a valid lease.
func (e InvalidLeaseError) Error() string {
	return fmt.Sprintf("server %s does not have a valid lease", e.ServerID)
}

// InvalidOperationTypeError is returned when an operation type is submitted that is
// not supported.
type InvalidOperationTypeError struct {
	// The operation type that is invalid.
	OperationType OperationType
}

// Error formats and returns an error message indicating that operation type
// e.OperationType is not valid.
func (e InvalidOperationTypeError) Error() string {
	return fmt.Sprintf("operation type '%s' is not a supported operation type", e.OperationType)
}

// State represents the current state of a raft node.
// A raft node may either be shutdown, the leader, or a followerState.
type State uint32

const (
	// Leader is a state indicating that the raft node is responsible for replicating and
	// committing log entries. Typically, only one raft node in a cluster will be the leader.
	// However, if there are partitions or other failures, it is possible there is more than
	// one leader.
	Leader State = iota

	// Follower is a state indicating that a raft node  is responsible for accepting log entries replicated
	// by the leader. A node in the followerState state may not accept operations for replication.
	Follower

	// Shutdown is a state indicating that the raft node is currently offline.
	Shutdown
)

// String converts a State into a string.
func (s State) String() string {
	switch s {
	case Leader:
		return "leader"
	case Follower:
		return "followerState"
	case Shutdown:
		return "shutdown"
	default:
		panic("invalid state")
	}
}

// Status is the status of a raft node.
type Status struct {
	// The unique identifier of this node.
	ID string

	// The address of the node.
	Address string

	// The current term.
	Term uint64

	// The current commit index.
	CommitIndex uint64

	// The index of the last log entry applied to the state machine.
	LastApplied uint64

	// The current state of the raft node: leader, followerState, shutdown.
	State State
}

// Protocol represents an abstraction of the raft consensus protocol.
// The raft protocol is a distributed consensus algorithm designed for fault-tolerant systems.
// It provides functions to start and stop the protocol, submit operations, check the status and manage snapshots.
type Protocol interface {
	// Start initializes the consensus protocol and prepares it to receive client operations.
	Start()

	// Stop shuts down the consensus protocol.
	Stop()

	// SubmitOperation takes a byte array representing an operation and adds it to the
	// protocol's log. It returns an OperationResponseFuture with the provided timeout,
	// representing a future response to applying the operation to the state machine.
	SubmitOperation(
		operation []byte,
		operationType OperationType,
		timeout time.Duration,
	) *OperationResponseFuture

	// Status returns the current status of the protocol. The returned status includes information
	// like the current term, whether the protocol is a leader, followerState or candidate, and more.
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
}

// peerState contains all state associated with peers.
type peerState struct {
	// The next log index that should be sent to this node.
	nextIndex uint64

	// The highest log index known to be replicated on this node.
	matchIndex uint64

	// The snapshot file to read when sending a snapshot to this node.
	snapshot SnapshotFile
}

// Raft implements the Protocol interface. This implementation of Raft should be utilized as the internal
// logic for an actual server, as it solely encapsulates the core functionality of Raft and cannot operate
// as a standalone server.
type Raft struct {
	// The ID of this raft node.
	id string

	// The ID that this raft node believes is the leader. Used to redirect clients.
	leaderId string

	// The configuration options for this raft node.
	options options

	// Maps ID to peer.
	peers map[string]Peer

	// Maps ID to state of peers which are followerStates.
	followerState map[string]*peerState

	// Manages both read-only and replicated operations.
	operationManager *operationManager

	// This stores and retrieves persisted log entries.
	log Log

	// This stores and retrieves persisted vote and term.
	stateStorage StateStorage

	// This stores and retrieves persisted snapshots.
	snapshotStorage SnapshotStorage

	// A writer for a snapshot file if one is being installed.
	snapshot SnapshotFile

	// The state machine provided by the client that operations will be applied to.
	fsm StateMachine

	// Notifies the apply loop that the commit index has been updated and that
	// replicated operation may be applied to the state machine.
	applyCond *sync.Cond

	// Notifies the commit loop that new log entries may be ready to be committed.
	commitCond *sync.Cond

	// Notifies read-only loop that read-only operations may be able to be applied
	// to the state machine.
	readOnlyCond *sync.Cond

	// The current state of this raft node: leader, followerState, or shutdown.
	state State

	// Index of the last log entry that was committed.
	commitIndex uint64

	// Index of the last log entry that was applied.
	lastApplied uint64

	// The current term of this raft node. Must be persisted.
	currentTerm uint64

	// The last included index of the most recent snapshot.
	lastIncludedIndex uint64

	// The last included term of the most recent snapshot.
	lastIncludedTerm uint64

	// ID of the candidate that this raft node voted for. Must be persisted.
	votedFor string

	// The timestamp representing the time of the last contact by the leader.
	lastContact time.Time

	wg sync.WaitGroup

	mu sync.Mutex
}

// NewRaft creates a new instance of Raft that is configured with the provided options.
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
	if options.leaseDuration == 0 {
		options.leaseDuration = defaultLeaseDuration
	}

	followerState := make(map[string]*peerState, len(peers))
	for id := range peers {
		followerState[id] = &peerState{}
	}

	raft := &Raft{
		id:               id,
		options:          options,
		peers:            peers,
		followerState:    followerState,
		operationManager: newOperationManager(options.leaseDuration),
		log:              log,
		stateStorage:     stateStorage,
		snapshotStorage:  snapshotStorage,
		fsm:              fsm,
		state:            Shutdown,
	}

	raft.applyCond = sync.NewCond(&raft.mu)
	raft.commitCond = sync.NewCond(&raft.mu)
	raft.readOnlyCond = sync.NewCond(&raft.mu)

	return raft, nil
}

// Start starts the Raft instance if it is not already started. Once started,
// the Raft instance transitions to the followerState state and is ready to start
// sending and receiving RPCs.
func (r *Raft) Start() {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.state != Shutdown {
		return
	}

	// Restore the current term and vote if they have been persisted.
	currentTerm, votedFor, err := r.stateStorage.State()
	if err != nil {
		r.options.logger.Fatalf("server %s failed to recover state: %s", r.id, err.Error())
	}
	r.currentTerm = currentTerm
	r.votedFor = votedFor

	// Open the log for new operations.
	if err := r.log.Open(); err != nil {
		r.options.logger.Debugf("server %s failed to open log: %s", r.id, err.Error())
	}

	// Replay the persisted state of the log into memory.
	if err := r.log.Replay(); err != nil {
		r.options.logger.Fatalf("server %s failed to replay log: %s", r.id, err.Error())
	}

	// Restore the state machine from the most recent snapshot if there was one.
	file, err := r.snapshotStorage.SnapshotFile()
	if err != nil {
		r.options.logger.Fatalf("server %s failed to retrieve snapshot file: %s", r.id, err.Error())
	}
	if file != nil {
		metadata := file.Metadata()
		r.lastIncludedIndex = metadata.LastIncludedIndex
		r.lastIncludedTerm = metadata.LastIncludedTerm
		r.commitIndex = metadata.LastIncludedIndex
		r.lastApplied = metadata.LastIncludedIndex
		if err := r.fsm.Restore(file); err != nil {
			r.options.logger.Fatalf(
				"server %s failed to restore state machine with snapshot: %s",
				r.id,
				err.Error(),
			)
		}
		if err := file.Close(); err != nil {
			r.options.logger.Errorf(
				"server %s failed to close snapshot file: %s",
				r.id,
				err.Error(),
			)
		}
	}

	r.lastContact = time.Now()
	r.state = Follower

	for id, peer := range r.peers {
		if id == r.id {
			continue
		}
		if err := peer.Connect(); err != nil {
			r.options.logger.Errorf("error connecting to followerState: %s", err.Error())
		}
	}

	r.wg.Add(5)
	go r.readOnlyLoop()
	go r.applyLoop()
	go r.electionLoop()
	go r.heartbeatLoop()
	go r.commitLoop()

	r.options.logger.Infof(
		"server %s started: electionTimeout = %v, heartbeatInterval = %v, leaseDuration = %v",
		r.id,
		r.options.electionTimeout,
		r.options.heartbeatInterval,
		r.options.leaseDuration,
	)
}

// Stop stops the Raft instance if is not already stopped.
func (r *Raft) Stop() {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.state == Shutdown {
		return
	}

	r.state = Shutdown
	r.applyCond.Broadcast()
	r.commitCond.Broadcast()
	r.readOnlyCond.Broadcast()

	r.mu.Unlock()
	r.wg.Wait()
	r.mu.Lock()

	for id, peer := range r.peers {
		if id == r.id {
			continue
		}
		if err := peer.Disconnect(); err != nil {
			r.options.logger.Errorf(
				"server %s failed to disconnect peer: %s",
				r.id,
				err.Error(),
			)
		}
	}

	if err := r.log.Close(); err != nil {
		r.options.logger.Errorf("server %s failed to close log: %s", r.id, err.Error())
	}

	r.resetSnapshotFiles()

	r.options.logger.Infof("server %s stopped", r.id)
}

// SubmitOperation accepts an operation from a client for replication andcreturns a future
// for the response to the operation. Note that submitting an operation for replication does
// not guarantee replication if there are failures. Once the operation has been applied to
// the state machine, the future will be populated with the response.
func (r *Raft) SubmitOperation(
	operation []byte,
	operationType OperationType,
	timeout time.Duration,
) *OperationResponseFuture {
	switch operationType {
	case Replicated:
		return r.submitReplicatedOperation(operation, timeout)
	case LeaseBasedReadOnly, LinearizableReadOnly:
		return r.submitReadOnlyOperation(operation, operationType, timeout)
	default:
		future := NewOperationResponseFuture(operation, timeout)
		future.responseCh <- OperationResponse{Err: InvalidOperationTypeError{OperationType: operationType}}
		return future
	}
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

	// If the request has a more up-to-date term, update current term and become a followerState.
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
	// become a followerState.
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
			r.options.logger.Fatalf(
				"server %s failed to get previous entry from log: %s",
				r.id,
				err.Error(),
			)
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
			index := request.PrevLogIndex - 1
			for ; index > r.lastIncludedIndex; index-- {
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

	if request.LeaderCommit > r.commitIndex {
		r.commitIndex = util.Min(request.LeaderCommit, r.log.LastIndex())
		r.applyCond.Broadcast()
	}

	return nil
}

// InstallSnapshot is invoked by the leader to send a snapshot to a followerState.
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
		"server %s received InstallSnapshot request: leaderID = %s, term = %d, lastIncludedIndex = %d, lastIncludedTerm = %d, offset = %d, done = %v",
		r.id,
		request.LeaderID,
		request.Term,
		request.LastIncludedIndex,
		request.LastIncludedTerm,
		request.Offset,
		request.Done,
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

	// The recieved snapshot does not contain anything new.
	if r.lastIncludedIndex >= request.LastIncludedIndex ||
		r.lastApplied >= request.LastIncludedIndex {
		return nil
	}

	// Discard an incomplete snapshot if this request has a greater last index.
	if r.snapshot != nil {
		metadata := r.snapshot.Metadata()
		if metadata.LastIncludedIndex < request.LastIncludedIndex {
			if err := r.snapshot.Discard(); err != nil {
				r.options.logger.Errorf(
					"server %s failed to discard snapshot: %s",
					r.id,
					err.Error(),
				)
			}
			r.snapshot = nil
		}
	}

	// Create a new snapshot file if one has not already been created.
	if r.snapshot == nil {
		snapshot, err := r.snapshotStorage.NewSnapshotFile(
			request.LastIncludedIndex,
			request.LastIncludedTerm,
		)
		if err != nil {
			r.options.logger.Fatalf(
				"server %s failed to create snapshot file: %s",
				r.id,
				err.Error(),
			)
		}
		r.snapshot = snapshot
	}

	// Ensure the offset in the request is the expected offset.
	offset, err := r.snapshot.Seek(0, io.SeekCurrent)
	if err != nil {
		r.options.logger.Fatalf("server %s failed to seek snapshot file: %s", err.Error())
	}
	if request.Offset != offset {
		r.options.logger.Warnf(
			"server %s rejecting InstallSnapshot request: incorrect offset: expected %d, got %d",
			r.id,
			offset,
			request.Offset,
		)
		return nil
	}

	// Write the snapshot chunk to disk.
	reader := bytes.NewReader(request.Bytes)
	if _, err := io.Copy(r.snapshot, reader); err != nil {
		r.options.logger.Fatalf("server %s failed to write to snapshot file: %s", r.id, err.Error())
	}

	// Wait for more chunks before finalizing the snapshot.
	if !request.Done {
		return nil
	}

	if err := r.snapshot.Close(); err != nil {
		r.options.logger.Errorf("server %s failed to close snapshot file: %s", r.id)
	}
	r.snapshot = nil

	r.lastIncludedIndex = request.LastIncludedIndex
	r.lastIncludedTerm = request.LastIncludedTerm

	// If an existing log entry has the same index and term as the last index
	// and last term, discard the log through the last index and reply.
	if entry, _ := r.log.GetEntry(request.LastIncludedIndex); entry != nil &&
		entry.Term == request.LastIncludedTerm {
		// Wait for all operations up to last included index have been applied before compacting the log.
		// This is necessary since compacting the log may remove log entries that have yet to be applied.
		for r.lastApplied < request.LastIncludedIndex {
			r.applyCond.Wait()
		}

		// It's possible that a snapshot was taken and the log was compacted while the lock was released.
		if r.lastIncludedIndex > request.LastIncludedIndex {
			return nil
		}

		r.options.logger.Warnf(
			"server %s compacting log: index = %d",
			r.id,
			request.LastIncludedIndex,
		)
		if err := r.log.Compact(request.LastIncludedIndex); err != nil {
			r.options.logger.Fatalf("server %s failed to compact log: %s", r.id, err.Error())
		}
		return nil
	}

	// Discard the entire log and restore the state machine with the snapshot.
	snapshot, err := r.snapshotStorage.SnapshotFile()
	if err != nil {
		r.options.logger.Fatalf("server %s failed to retreive snapshot file: %s", r.id, err.Error())
	}

	r.mu.Unlock()
	r.options.logger.Warnf("server %s restoring state machine", r.id)
	if err := r.fsm.Restore(snapshot); err != nil {
		r.options.logger.Fatalf(
			"server %s failed to reset state machine with snapshot: %s",
			r.id,
			err.Error(),
		)
	}
	if err := snapshot.Close(); err != nil {
		r.options.logger.Errorf("server %s failed to close snapshot file: %s", err.Error())
	}
	r.mu.Lock()

	r.options.logger.Warnf("server %s discarding log", r.id)
	if err := r.log.DiscardEntries(request.LastIncludedIndex, request.LastIncludedTerm); err != nil {
		r.options.logger.Fatalf(
			"server %s failed to discard log entries: %s",
			r.id,
			err.Error(),
		)
	}

	r.lastApplied = request.LastIncludedIndex
	r.commitIndex = request.LastIncludedIndex

	return nil
}

func (r *Raft) submitReplicatedOperation(
	operationBytes []byte,
	timeout time.Duration,
) *OperationResponseFuture {
	r.mu.Lock()
	defer r.mu.Unlock()

	future := NewOperationResponseFuture(operationBytes, timeout)

	if r.state != Leader {
		future.responseCh <- OperationResponse{Err: NotLeaderError{ServerID: r.id, KnownLeader: r.leaderId}}
		return future
	}

	entry := NewLogEntry(r.log.NextIndex(), r.currentTerm, operationBytes, OperationEntry)
	if err := r.log.AppendEntry(entry); err != nil {
		r.options.logger.Fatalf("server %s failed to append entry to log: %s", err.Error())
	}

	r.operationManager.pendingReplicated[entry.Index] = future.responseCh

	r.sendAppendEntriesToPeers()

	r.options.logger.Debugf("server %s submitted operation: logEntry = %v", r.id, entry)

	return future
}

func (r *Raft) submitReadOnlyOperation(
	operationBytes []byte,
	readOnlyType OperationType,
	timeout time.Duration,
) *OperationResponseFuture {
	r.mu.Lock()
	defer r.mu.Unlock()

	future := NewOperationResponseFuture(operationBytes, timeout)

	if r.state != Leader {
		future.responseCh <- OperationResponse{Err: NotLeaderError{ServerID: r.id, KnownLeader: r.leaderId}}
		return future
	}

	operation := &Operation{
		Bytes:         operationBytes,
		OperationType: LeaseBasedReadOnly,
		readIndex:     r.commitIndex,
		responseCh:    future.responseCh,
	}
	r.operationManager.pendingReadOnly[operation] = true
	if readOnlyType == LeaseBasedReadOnly && operation.readIndex <= r.lastApplied {
		r.readOnlyCond.Broadcast()
	}
	if readOnlyType == LinearizableReadOnly && r.operationManager.shouldVerifyQuorum {
		r.sendAppendEntriesToPeers()
		r.operationManager.shouldVerifyQuorum = false
	}

	return future
}

func (r *Raft) sendAppendEntriesToPeers() {
	numResponses := 1
	for _, peer := range r.peers {
		go r.sendAppendEntries(peer, &numResponses)
	}
}

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
			r.tryApplyReadOnlyOperations()
		}
		return
	}

	followerState := r.followerState[peer.ID()]

	// Send a snapshot instead if this server no longer has the previous log entry.
	if followerState.nextIndex <= r.lastIncludedIndex {
		r.sendInstallSnapshot(peer)
		return
	}

	nextIndex := followerState.nextIndex
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

	entries := make([]*LogEntry, 0, r.log.NextIndex()-nextIndex)
	for index := nextIndex; index < r.log.NextIndex(); index++ {
		// Make sure that the index is in bounds since the log may have been compacted.
		if index <= r.lastIncludedIndex {
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

	if err != nil || r.state != Leader {
		return
	}

	// Become a followerState if a followerState has a more up-to-date term.
	if response.Term > r.currentTerm {
		r.becomeFollower(peer.ID(), response.Term)
		return
	}

	// Renew the lease if the majority of peers have responded.
	// Update any pending read-only operations to indicate that the quorum was just verified.
	if numResponses != nil {
		*numResponses += 1
		if r.hasQuorum(*numResponses) {
			r.tryApplyReadOnlyOperations()
			numResponses = nil
		}
	}

	if !response.Success {
		followerState.nextIndex = response.Index
		// Send a snapshot to the follower if the log no longer
		// contains the previous entry.
		if followerState.nextIndex <= r.lastIncludedIndex {
			r.sendInstallSnapshot(peer)
		}
		return
	}

	// Update the next and match index of the followerState.
	if request.PrevLogIndex+uint64(len(entries)) > followerState.matchIndex {
		followerState.nextIndex = util.Max(
			followerState.nextIndex,
			request.PrevLogIndex+uint64(len(entries))+1,
		)
		followerState.matchIndex = request.PrevLogIndex + uint64(len(entries))
		if followerState.matchIndex > r.commitIndex {
			r.commitCond.Broadcast()
		}
	}
}

func (r *Raft) sendRequestVoteToPeers(votes *int) {
	for _, peer := range r.peers {
		go r.sendRequestVote(peer, votes)
	}
}

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

	// Become a followerState if a followerState has a more up-to-date term.
	if response.Term > r.currentTerm {
		r.becomeFollower(peer.ID(), response.Term)
		return
	}

	// If we have received votes from the majority of peers, become a leader.
	if r.hasQuorum(*votes) && r.state == Follower {
		r.becomeLeader()
	}
}

func (r *Raft) takeSnapshot() {
	if r.lastApplied <= r.lastIncludedIndex {
		return
	}

	lastAppliedEntry, err := r.log.GetEntry(r.lastApplied)
	if err != nil {
		r.options.logger.Fatalf("server %s failed getting entry from log: %s", r.id, err.Error())
	}

	// Create a new snapshot file.
	snapshot, err := r.snapshotStorage.NewSnapshotFile(
		lastAppliedEntry.Index,
		lastAppliedEntry.Term,
	)
	if err != nil {
		r.options.logger.Fatalf("server %s failed to create snapshot file: %s", err.Error())
	}

	// Take a snapshot of the state machine.
	r.mu.Unlock()
	if err := r.fsm.Snapshot(snapshot); err != nil {
		r.options.logger.Fatalf(
			"server %s failed while taking snapshot of state machine: %s",
			r.id,
			err.Error(),
		)
	}
	if err := snapshot.Close(); err != nil {
		r.options.logger.Fatalf("server %s failed to close snapshot file: %s", err.Error())
	}
	r.mu.Lock()

	// It's possible a snapshot was installed and the log has already been compacted.
	if lastAppliedEntry.Index <= r.lastIncludedIndex {
		return
	}

	// Compact the log.
	r.lastIncludedIndex = lastAppliedEntry.Index
	r.lastIncludedTerm = lastAppliedEntry.Term
	r.options.logger.Warnf("server %s compacting log: index = %d", r.id, r.lastIncludedIndex)
	if err := r.log.Compact(r.lastIncludedIndex); err != nil {
		r.options.logger.Fatalf("server %s failed while taking snapshot: %s", r.id, err.Error())
	}
	r.resetSnapshotFiles()

	r.options.logger.Infof("server %s took snapshot: lastIncludedIndex = %d, lastIncludedTerm = %d",
		r.id, r.lastIncludedIndex, r.lastIncludedTerm)
}

func (r *Raft) sendInstallSnapshot(peer Peer) {
	if r.state != Leader {
		return
	}

	followerState := r.followerState[peer.ID()]

	if followerState.snapshot == nil {
		snapshot, err := r.snapshotStorage.SnapshotFile()
		if err != nil {
			r.options.logger.Fatalf(
				"server %s failed to retrieve snapshot file: %s",
				r.id,
				err.Error(),
			)
		}
		if snapshot == nil {
			r.options.logger.Warnf(
				"server %s tried to send snapshot but has not taken a snapshot",
				r.id,
			)
			return
		}
		followerState.snapshot = snapshot
	}

	metadata := followerState.snapshot.Metadata()
	offset, err := followerState.snapshot.Seek(0, io.SeekCurrent)
	if err != nil {
		r.options.logger.Fatalf("server %s failed to seek snapshot file: %s", r.id, err.Error())
	}

	request := InstallSnapshotRequest{
		LeaderID:          r.id,
		Term:              r.currentTerm,
		LastIncludedIndex: metadata.LastIncludedIndex,
		LastIncludedTerm:  metadata.LastIncludedTerm,
		Offset:            offset,
	}

	var buf bytes.Buffer
	n, err := io.Copy(&buf, followerState.snapshot)
	if err != nil {
		r.options.logger.Errorf("server %s failed to read snapshot file: %s", r.id, err.Error())
		followerState.snapshot.Close()
	}
	request.Bytes = buf.Bytes()
	request.Done = n < 32*1024

	r.mu.Unlock()
	response, err := peer.InstallSnapshot(request)
	r.mu.Lock()

	if followerState.snapshot == nil {
		return
	}

	if err != nil {
		followerState.snapshot.Seek(-n, io.SeekCurrent)
		return
	}

	// If the followerState has a more up-to-date term, transition to the followerState state.
	if response.Term > r.currentTerm {
		r.becomeFollower(peer.ID(), response.Term)
		return
	}

	if !request.Done {
		return
	}

	if err := followerState.snapshot.Close(); err != nil {
		r.options.logger.Errorf("server %s failed to close snapshot file: %s", err.Error())
	}
	followerState.snapshot = nil
	followerState.matchIndex = request.LastIncludedIndex
	followerState.nextIndex = request.LastIncludedIndex + 1
}

func (r *Raft) heartbeatLoop() {
	defer r.wg.Done()

	// If this server is the leader, broadcast heartbeat messages to peers
	// once every heartbeat interval.
	for {
		time.Sleep(r.options.heartbeatInterval)

		r.mu.Lock()
		if r.state == Shutdown {
			r.mu.Unlock()
			return
		}
		if r.state == Follower {
			r.mu.Unlock()
			continue
		}
		r.sendAppendEntriesToPeers()
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

	// If we have already been elected the leader, or we have been contacted by the leader
	// since the last election timeout, an election is not needed.
	if r.state != Follower || time.Since(r.lastContact) < r.options.electionTimeout {
		return
	}

	var votesReceived int
	r.becomeCandidate()
	r.sendRequestVoteToPeers(&votesReceived)
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
			for id, followerState := range r.followerState {
				if id == r.id {
					continue
				}
				if followerState.matchIndex >= index {
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

func (r *Raft) applyLoop() {
	r.mu.Lock()
	defer r.mu.Unlock()
	defer r.wg.Done()

	for r.state != Shutdown {
		r.applyCond.Wait()

		// Scan the log starting at the entry following the last applied entry
		// and apply any entries that have been committed.
		for r.lastApplied < r.commitIndex {
			entry, err := r.log.GetEntry(r.lastApplied + 1)
			if err != nil {
				r.options.logger.Fatalf(
					"server %s failed getting committed entry from log: %s",
					r.id,
					err.Error(),
				)
			}

			if entry.EntryType == NoOpEntry {
				r.lastApplied++
				continue
			}

			responseCh, ok := r.operationManager.pendingReplicated[entry.Index]
			if ok {
				delete(r.operationManager.pendingReplicated, entry.Index)
			}

			operation := Operation{
				LogIndex:      entry.Index,
				LogTerm:       entry.Term,
				Bytes:         entry.Data,
				OperationType: Replicated,
				responseCh:    responseCh,
			}
			response := OperationResponse{Operation: operation}

			lastApplied := r.lastApplied

			r.mu.Unlock()
			response.Response = r.fsm.Apply(&operation)
			r.sendResponseWithoutBlocking(operation.responseCh, response)
			r.options.logger.Debugf(
				"server %s applied operation: index = %d, term = %d",
				r.id,
				operation.LogIndex,
				operation.LogTerm,
			)
			r.mu.Lock()

			if r.lastApplied != lastApplied {
				continue
			}

			r.lastApplied++

			if r.fsm.NeedSnapshot(r.log.Size()) {
				r.takeSnapshot()
			}
		}

		if r.state == Leader {
			r.readOnlyCond.Broadcast()
		}
	}
}

func (r *Raft) readOnlyLoop() {
	r.mu.Lock()
	defer r.mu.Unlock()
	defer r.wg.Done()

	for r.state != Shutdown {
		r.readOnlyCond.Wait()
		// Only the leader may apply read-only operations and it is
		// only safe to apply them once the leader has committed
		// atleast one log entry.
		if r.state != Leader || r.log.LastTerm() != r.currentTerm {
			continue
		}

		appliableOperations := r.operationManager.appliableReadOnlyOperations(r.lastApplied)
		for _, operation := range appliableOperations {
			response := OperationResponse{Operation: *operation}

			if operation.OperationType == LeaseBasedReadOnly &&
				!r.operationManager.leaderLease.isValid() {
				response.Err = InvalidLeaseError{ServerID: r.id}
				r.sendResponseWithoutBlocking(operation.responseCh, response)
				continue
			}

			r.mu.Unlock()
			response.Response = r.fsm.Apply(operation)
			r.sendResponseWithoutBlocking(
				operation.responseCh,
				response,
			)
			r.options.logger.Debugf(
				"server %s applied read-only operation: readIndex = %d",
				r.id,
				operation.readIndex,
			)
			r.mu.Lock()

			if r.state != Leader {
				break
			}
		}
	}
}

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

func (r *Raft) becomeLeader() {
	r.state = Leader
	r.resetSnapshotFiles()
	for _, followerState := range r.followerState {
		followerState.nextIndex = r.log.LastIndex() + 1
		followerState.matchIndex = 0
	}

	r.operationManager = newOperationManager(r.options.leaseDuration)

	entry := NewLogEntry(r.log.NextIndex(), r.currentTerm, make([]byte, 0), NoOpEntry)
	if err := r.log.AppendEntry(entry); err != nil {
		r.options.logger.Fatal("server %s failed to append entry to log: err = %s", r.id, err)
	}

	r.sendAppendEntriesToPeers()

	r.options.logger.Infof("server %s has entered the leader state: term = %d", r.id, r.currentTerm)
}

func (r *Raft) becomeFollower(leaderID string, term uint64) {
	r.state = Follower
	r.currentTerm = term
	r.leaderId = leaderID
	r.votedFor = ""
	r.persistTermAndVote()
	r.resetSnapshotFiles()

	r.options.logger.Infof(
		"server %s has entered the followerState state: term = %d",
		r.id,
		r.currentTerm,
	)

	// Cancel any pending operations.
	r.operationManager.notifyLostLeaderShip(r.id, r.leaderId)
	r.operationManager = newOperationManager(r.options.leaseDuration)
}

func (r *Raft) tryApplyReadOnlyOperations() {
	r.operationManager.markAsVerified()
	r.operationManager.leaderLease.renew()
	r.operationManager.shouldVerifyQuorum = true
	r.readOnlyCond.Broadcast()
}

func (r *Raft) resetSnapshotFiles() {
	for _, followerState := range r.followerState {
		if followerState.snapshot != nil {
			if err := followerState.snapshot.Close(); err != nil {
				r.options.logger.Errorf("server %s failed to close snapshot file: %s", err.Error())
			}
			followerState.snapshot = nil
		}
	}
	if r.snapshot != nil {
		if err := r.snapshot.Discard(); err != nil {
			r.options.logger.Errorf("server %s failed to discard snapshot file: %s", err.Error())
		}
		r.snapshot = nil
	}
}

func (r *Raft) hasQuorum(count int) bool {
	return count > len(r.peers)/2
}

func (r *Raft) persistTermAndVote() {
	if err := r.stateStorage.SetState(r.currentTerm, r.votedFor); err != nil {
		r.options.logger.Fatalf("server %s failed persisting term and vote: %s", r.id, err.Error())
	}
}

func (r *Raft) disconnectPeer(id string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if id == r.id {
		return nil
	}
	if err := r.peers[id].Disconnect(); err != nil {
		return errors.WrapError(err, "server failed to disconnect followerState: %s", err.Error())
	}
	return nil
}

func (r *Raft) connectPeer(id string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if id == r.id {
		return nil
	}
	if err := r.peers[id].Connect(); err != nil {
		return errors.WrapError(err, "server failed to connect followerState: %s", err.Error())
	}
	return nil
}

func (r *Raft) sendResponseWithoutBlocking(
	responseCh chan OperationResponse,
	response OperationResponse,
) {
	select {
	case responseCh <- response:
	default:
	}
}
