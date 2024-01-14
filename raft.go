package raft

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/jmsadair/raft/internal/logger"
	"github.com/jmsadair/raft/internal/util"
)

var (
	// ErrNotLeader is returned when an operation or configuration change is
	// submitted to a node that is not a leader.
	ErrNotLeader = errors.New("this node is not the leader")

	// ErrInvalidLease is returned when a lease-based read-only operation is
	// rejected due to the leader's lease having expired.
	ErrInvalidLease = errors.New("this node does not have a valid lease")

	// ErrPendingConfiguration is returned when a membership change is requested and their
	// is a pending membership change that has not yet been committed.
	ErrPendingConfiguration = errors.New("only one membership change may be committed at a time")
)

// The default chunk size for InstallSnapshot RPCs.
const snapshotChunkSize = 32 * 1024

// State represents the current state of a raft node.
// A raft node may either be shutdown, the leader, or a followers.
type State uint32

const (
	// Leader is a state indicating that the raft node is responsible for replicating and
	// committing log entries. Typically, only one raft node in a cluster will be the leader.
	// However, if there are partitions or other failures, it is possible there is more than
	// one leader.
	Leader State = iota

	// Follower is a state indicating that a raft node  is responsible for accepting log entries replicated
	// by the leader. A node in the followers state may not accept operations for replication.
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
		return "followers"
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

	// The current state of the raft node: leader, followers, shutdown.
	State State
}

// follower contains all state associated with followers.
type follower struct {
	// The next log index that should be sent to this node.
	nextIndex uint64

	// The highest log index known to be replicated on this node.
	matchIndex uint64

	// The snapshot file to read when sending a snapshot to this node.
	snapshot SnapshotFile
}

// Raft implements the raft consensus protocol.
type Raft struct {
	// The ID of this node.
	id string

	// The address of this node.
	address string

	// The ID that this raft node believes is the leader. Used to redirect clients.
	leaderId string

	// The configuration options for this raft node.
	options options

	// The network transport for sending and recieving RPCs.
	transport Transport

	// The latest configuration of the cluster.
	// This may or may not be committed.
	configuration *Configuration

	// The most recently committed configuration of the cluster.
	committedConfiguration *Configuration

	// A channel used to respond to membership change requests.
	configurationResponseCh chan Result[Configuration]

	// Maps ID to the the state of the other nodes in the cluster.
	// Maintained by the leader.
	followers map[string]*follower

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

	// The current state of this raft node: leader, followers, or shutdown.
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

// NewRaft creates a new instance of Raft with the provided ID and address.
// The datapath is the top level directory where state for this node will be persisted.
func NewRaft(
	id string,
	address string,
	fsm StateMachine,
	dataPath string,
	opts ...Option,
) (*Raft, error) {
	// Apply provided options.
	var options options
	for _, opt := range opts {
		if err := opt(&options); err != nil {
			return nil, err
		}
	}

	raft := &Raft{id: id, address: address, state: Shutdown, fsm: fsm}

	// Set default values if option not provided.
	if options.logger == nil {
		defaultLogger, err := logger.NewLogger()
		if err != nil {
			return nil, err
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
	if options.log == nil {
		log, err := NewLog(dataPath)
		if err != nil {
			return nil, fmt.Errorf("could not create log instance: %w", err)
		}
		raft.log = log
	}
	if options.stateStorage == nil {
		stateStore, err := NewStateStorage(dataPath)
		if err != nil {
			return nil, fmt.Errorf("could not create state storage instance: %w", err)
		}
		raft.stateStorage = stateStore
	}
	if options.snapshotStorage == nil {
		snapshotStore, err := NewSnapshotStorage(dataPath)
		if err != nil {
			return nil, fmt.Errorf("could not create snapshbot storage instance: %w", err)
		}
		raft.snapshotStorage = snapshotStore
	}
	if options.transport == nil {
		transport, err := NewTransport(address)
		if err != nil {
			return nil, fmt.Errorf("could not create transport instance: %w", err)
		}
		raft.transport = transport
	}

	raft.options = options
	raft.operationManager = newOperationManager(options.leaseDuration)
	raft.applyCond = sync.NewCond(&raft.mu)
	raft.commitCond = sync.NewCond(&raft.mu)
	raft.readOnlyCond = sync.NewCond(&raft.mu)

	if err := raft.restore(); err != nil {
		return nil, fmt.Errorf("could not restore node: %w", err)
	}

	return raft, nil
}

// restore will recover any persisted state and initialize the node with it.
func (r *Raft) restore() error {
	if err := r.log.Open(); err != nil {
		return fmt.Errorf("could not open log: %w", err)
	}
	if err := r.log.Replay(); err != nil {
		return fmt.Errorf("could not replay log: %w", err)
	}

	// Restore the current term and vote.
	currentTerm, votedFor, err := r.stateStorage.State()
	if err != nil {
		return fmt.Errorf("could not recover state from storage: %w", err)
	}
	r.currentTerm = currentTerm
	r.votedFor = votedFor

	// Restore the state machine from the most recent snapshot.
	file, err := r.snapshotStorage.SnapshotFile()
	if err != nil {
		return fmt.Errorf("could not get snapshot file: %w", err)
	}
	if file != nil {
		metadata := file.Metadata()
		r.lastIncludedIndex = metadata.LastIncludedIndex
		r.lastIncludedTerm = metadata.LastIncludedTerm
		r.commitIndex = metadata.LastIncludedIndex
		r.lastApplied = metadata.LastIncludedIndex
		if err := r.fsm.Restore(file); err != nil {
			return fmt.Errorf("could not restore state machine with snapshot: %w", err)
		}
		configuration, err := r.transport.DecodeConfiguration(metadata.Configuration)
		if err != nil {
			return fmt.Errorf("could not decode snapshot configuration: %w", err)
		}
		r.configuration = &configuration
		r.committedConfiguration = &configuration
		if err := file.Close(); err != nil {
			return fmt.Errorf("could not close snapshot file: %w", err)
		}
	}

	// Use the most recent configuration from the log.
	for index := r.log.LastIndex(); index > r.lastIncludedIndex; index-- {
		entry, err := r.log.GetEntry(index)
		if err != nil {
			return fmt.Errorf("could not get entry from log: %w", err)
		}
		if entry.EntryType != ConfigurationEntry {
			continue
		}
		configuration, err := r.transport.DecodeConfiguration(entry.Data)
		if err != nil {
			return fmt.Errorf("could not decode log entry configuration: %w", err)
		}
		r.configuration = &configuration
		break
	}

	return nil
}

// Bootstrap initializes this node with a cluster configuration.
// The configuration must contain the ID and address of all nodes
// in the cluster including this one and all nodes must agree on
// this configuration. This should only be called when starting a
// cluster for the first time and there is no existing configuration.
// All nodes in the configuration will have voter status.
func (r *Raft) Bootstrap(configuration map[string]string) error {
	if address, ok := configuration[r.id]; !ok || r.address != address {
		return errors.New("configuration must contain this node")
	}
	if r.configuration != nil {
		return fmt.Errorf(
			"node already has an existing configuration: Bootstrap should only be called for a cluster starting for the first time: configuration = %s",
			r.configuration.String(),
		)
	}
	if r.log.LastIndex() > 0 {
		return errors.New(
			"node already has existing state: Bootstrap should only be called for a clustrer starting for the first time",
		)
	}

	index := uint64(1)
	term := uint64(1)
	r.configuration = NewConfiguration(index, configuration)

	// Append the configuration to the log.
	data, err := r.transport.EncodeConfiguration(r.configuration)
	if err != nil {
		return fmt.Errorf("could not encode configuration: %w", err)
	}
	entry := NewLogEntry(index, term, data, ConfigurationEntry)
	if err := r.log.AppendEntry(entry); err != nil {
		return fmt.Errorf("could not append configuration to log: %w", err)
	}

	r.options.logger.Infof("node bootstrapped: configuration = %s", r.configuration.String())

	return nil
}

// Start starts the node. If the node does not have an existing configuration,
// a configuration will be created that only includes this node as a non-voter.
// If the node has been started before, Restart should be called instead.
func (r *Raft) Start() error {
	return r.start(false)
}

// Restart starts a node that has been started and stopped before. The difference
// between Restart and Start is that Restart restores the state of the node from
// non-volatile whereas Start does not.
func (r *Raft) Restart() error {
	return r.start(true)
}

// start will start this node if it is not already started. If restore is true,
// then any persisted state will be restored.
func (r *Raft) start(restore bool) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.state != Shutdown {
		return nil
	}

	if restore {
		if err := r.restore(); err != nil {
			return fmt.Errorf("could not restore state: %w", err)
		}
	}

	if r.configuration == nil {
		r.configuration = &Configuration{}
		r.options.logger.Infof("node has no configuration, will start as non-voter")
	}

	// Register the functions for handling RPCs.
	r.transport.RegisterAppendEntriesHandler(r.AppendEntries)
	r.transport.RegisterRequestVoteHandler(r.RequestVote)
	r.transport.RegsiterInstallSnapshotHandler(r.InstallSnapshot)

	// Initialize the follower state.
	r.followers = make(map[string]*follower)
	for id := range r.configuration.Members {
		r.followers[id] = new(follower)
	}

	// Connect to the other nodes in the cluster.
	for id, address := range r.configuration.Members {
		if id == r.id {
			continue
		}
		if err := r.transport.Connect(address); err != nil {
			r.options.logger.Errorf(
				"failed to connect to node: id = %s, address = %s, error = %v",
				id,
				address,
				err,
			)
		}
	}

	r.lastContact = time.Now()
	r.state = Follower

	r.wg.Add(5)
	go r.readOnlyLoop()
	go r.applyLoop()
	go r.electionLoop()
	go r.heartbeatLoop()
	go r.commitLoop()

	// Start serving incoming RPCs.
	if err := r.transport.Run(); err != nil {
		return fmt.Errorf("could not run transport: %w", err)
	}

	r.options.logger.Infof(
		"node started: address = %s electionTimeout = %v, heartbeatInterval = %v, leaseDuration = %v",
		r.address,
		r.options.electionTimeout,
		r.options.heartbeatInterval,
		r.options.leaseDuration,
	)

	return nil
}

// Stop stops the raft consensus protocol if is not already stopped.
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

	// Close any connections to other nodes in the cluster and stop accepting RPCs.
	r.transport.CloseAll()
	r.transport.Shutdown()

	if err := r.log.Close(); err != nil {
		r.options.logger.Errorf("failed to close log: %v", err)
	}

	// Close or discard of any snapshot files.
	r.resetSnapshotFiles()

	r.options.logger.Info("node stopped")
}

// Status returns the status of this node. The status includes
// the ID, address, term, commit index, last applied index, and
// state of this node.
func (r *Raft) Status() Status {
	r.mu.Lock()
	defer r.mu.Unlock()

	return Status{
		ID:          r.id,
		Address:     r.transport.Address(),
		Term:        r.currentTerm,
		CommitIndex: r.commitIndex,
		LastApplied: r.lastApplied,
		State:       r.state,
	}
}

// Configuration returns the most current configuration of this node.
// This configuration may or may not have been committed yet.
func (r *Raft) Configuration() Configuration {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.configuration == nil {
		return Configuration{}
	}
	return r.configuration.Clone()
}

// AddServer adds a node with the provided ID and address to the cluster.
// If voter is true, the node will be added as a voting member. Otherwise,
// if voter is false, the node will be added as a non-voting member. It is
// recommended that a node be added as a non-voter first so that it has
// a chance to sync with the leader. Once the operation is successful, it can
// be added as a voting member.
func (r *Raft) AddServer(
	id string,
	address string,
	isVoter bool,
	timeout time.Duration,
) Future[Configuration] {
	r.mu.Lock()
	defer r.mu.Unlock()

	configurationFuture := newFuture[Configuration](timeout)

	// Only the leader can make membership changes.
	if r.state != Leader {
		configurationFuture.responseCh <- newResult[Configuration](Configuration{}, ErrNotLeader)
		return configurationFuture
	}

	// The membership change that adds the provided node is still pending - return a new future for the response.
	if _, ok := r.configuration.Members[id]; ok && r.commitIndex < r.configuration.Index {
		r.configurationResponseCh = configurationFuture.responseCh
		return configurationFuture
	}

	// The membership change is still pending - a different node cannot be added at this time.
	if r.commitIndex < r.configuration.Index {
		configurationFuture.responseCh <- newResult[Configuration](Configuration{}, ErrPendingConfiguration)
		return configurationFuture
	}

	// The provided node is already a part of the cluster.
	if memberAddress, ok := r.configuration.Members[id]; ok &&
		r.configuration.IsVoter[id] == isVoter &&
		memberAddress == address {
		configurationFuture.responseCh <- newResult[Configuration](*r.configuration, nil)
		return configurationFuture
	}

	// Create the configuration that includes the new node as a non-voter.
	configuration := r.configuration.Clone()
	configuration.Members[id] = address
	configuration.IsVoter[id] = isVoter

	// Add the configuration to the log.
	r.appendConfiguration(&configuration)

	r.configuration = &configuration
	r.followers[id] = &follower{nextIndex: 1}
	if err := r.transport.Connect(address); err != nil {
		r.options.logger.Errorf(
			"failed to connect to node: id = %s, address = %s, error = %v",
			id,
			address,
			err,
		)
	}

	r.sendAppendEntriesToPeers()

	r.options.logger.Debugf(
		"request to add node submitted: id = %s, address = %s, voter = %t, logIndex = %d",
		id,
		address,
		isVoter,
		configuration.Index,
	)

	return configurationFuture
}

// RemoveServer will remove the node with the provided ID from the cluster.
// Once the node is removed, it can safely be shutdown.
func (r *Raft) RemoveServer(id string, timeout time.Duration) Future[Configuration] {
	r.mu.Lock()
	defer r.mu.Unlock()

	configurationFuture := newFuture[Configuration](timeout)

	// Only the leader can make membership changes.
	if r.state != Leader {
		configurationFuture.responseCh <- newResult[Configuration](Configuration{}, ErrNotLeader)
		return configurationFuture
	}

	// The membership change that removes the node is still pending - return a new future for the response.
	// Note that if the node being removed is the leader, it will remain in the configurtion until applied.
	if _, ok := r.configuration.Members[id]; (!ok || id == r.id) &&
		r.commitIndex < r.configuration.Index {
		r.configurationResponseCh = configurationFuture.responseCh
		return configurationFuture
	}

	// The membership change is still pending - a different node cannot be removed at this time.
	if r.commitIndex < r.configuration.Index {
		configurationFuture.responseCh <- newResult[Configuration](Configuration{}, ErrPendingConfiguration)
		return configurationFuture
	}

	// The provided node is already removed from the cluster.
	if _, ok := r.configuration.Members[id]; !ok {
		configurationFuture.responseCh <- newResult[Configuration](*r.configuration, nil)
		return configurationFuture
	}

	// Create the configuration without the node.
	configuration := r.configuration.Clone()
	delete(configuration.Members, id)
	delete(configuration.IsVoter, id)

	// Add the configuration to the log.
	r.appendConfiguration(&configuration)

	r.sendAppendEntriesToPeers()

	r.options.logger.Debugf(
		"request to remove node submitted: id = %s, logIndex = %d",
		id,
		configuration.Index,
	)

	return configurationFuture
}

// SubmitOperation accepts an operation from a client for replication and returns a future
// for the response to the operation. Note that submitting an operation for replication does
// not guarantee replication if there are failures. Once the operation has been applied to
// the state machine, the future will be populated with the response.
func (r *Raft) SubmitOperation(
	operation []byte,
	operationType OperationType,
	timeout time.Duration,
) Future[OperationResponse] {
	switch operationType {
	case Replicated:
		return r.submitReplicatedOperation(operation, timeout)
	case LeaseBasedReadOnly, LinearizableReadOnly:
		return r.submitReadOnlyOperation(operation, operationType, timeout)
	default:
		operationFuture := newFuture[OperationResponse](timeout)
		response := newResult(OperationResponse{}, errors.New("operation type is not valid"))
		operationFuture.responseCh <- response
		return operationFuture
	}
}

// submitReplicatedOperation submits a replicated operation to be applied to the state machine.
func (r *Raft) submitReplicatedOperation(
	operationBytes []byte,
	timeout time.Duration,
) Future[OperationResponse] {
	r.mu.Lock()
	defer r.mu.Unlock()

	operationFuture := newFuture[OperationResponse](timeout)

	if r.state != Leader {
		response := newResult[OperationResponse](OperationResponse{}, ErrNotLeader)
		operationFuture.responseCh <- response
		return operationFuture
	}

	entry := NewLogEntry(r.log.NextIndex(), r.currentTerm, operationBytes, OperationEntry)
	if err := r.log.AppendEntry(entry); err != nil {
		r.options.logger.Fatalf("failed to append entry to log: error = %v", err)
	}

	r.operationManager.pendingReplicated[entry.Index] = operationFuture.responseCh

	r.sendAppendEntriesToPeers()

	r.options.logger.Debugf(
		"operation submitted: logIndex = %d, logTerm = %d, type = %s",
		entry.Index,
		entry.Term,
		Replicated.String(),
	)

	return operationFuture
}

// submitReadOnlyOperation submits a read-only operation to be applied to the state machine.
func (r *Raft) submitReadOnlyOperation(
	operationBytes []byte,
	readOnlyType OperationType,
	timeout time.Duration,
) Future[OperationResponse] {
	r.mu.Lock()
	defer r.mu.Unlock()

	operationFuture := newFuture[OperationResponse](timeout)

	if r.state != Leader {
		response := newResult[OperationResponse](OperationResponse{}, ErrNotLeader)
		operationFuture.responseCh <- response
		return operationFuture
	}

	operation := &Operation{
		Bytes:         operationBytes,
		OperationType: LeaseBasedReadOnly,
		readIndex:     r.commitIndex,
	}
	r.operationManager.pendingReadOnly[operation] = operationFuture.responseCh
	if readOnlyType == LeaseBasedReadOnly && operation.readIndex <= r.lastApplied {
		r.readOnlyCond.Broadcast()
	}
	if readOnlyType == LinearizableReadOnly && r.operationManager.shouldVerifyQuorum {
		r.sendAppendEntriesToPeers()
		r.operationManager.shouldVerifyQuorum = false
	}

	r.options.logger.Debugf(
		"operation submitted: readIndex = %d, type = %s",
		operation.readIndex,
		operation.OperationType.String(),
	)

	return operationFuture
}

// AppendEntries handles log replication requests from the leader. It takes a request to append
// entries and fills the response with the result of the append operation.
func (r *Raft) AppendEntries(request *AppendEntriesRequest, response *AppendEntriesResponse) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.state == Shutdown {
		return fmt.Errorf("could not execute RequestVote RPC: %s is shutdown", r.id)
	}

	r.options.logger.Debugf(
		"AppendEntries RPC recieved: leaderID = %s, leaderCommit = %d, term = %d, prevLogIndex = %d, prevLogTerm = %d",
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
			"AppendEntries RPC rejected: reason = out of date term, localTerm = %d, remoteTerm = %d",
			r.currentTerm,
			request.Term,
		)
		return nil
	}

	// Update the time of last contact - note that this should be done even
	// if the request is rejected due to having a non-matching previous log entry.
	r.lastContact = time.Now()

	// Update the ID of the node that this node recognizes as the leader.
	r.leaderId = request.LeaderID

	// If the request has a more up-to-date term, update current term and
	// become a followers.
	if request.Term > r.currentTerm {
		r.becomeFollower(request.LeaderID, request.Term)
		response.Term = r.currentTerm
	}

	// Reject the request if the log has been compacted and no longer contains the previous log entry.
	if r.lastIncludedIndex > request.PrevLogIndex {
		r.options.logger.Debugf(
			"AppendEntries RPC rejected: reason = log no longer contains previous entry, logIndex = %d, lastIncludedIndex = %d",
			request.PrevLogIndex,
			r.lastIncludedIndex,
		)
		response.Index = r.lastIncludedIndex + 1
		return nil
	}

	// Reject the request if the log is too short to contain the previous log entry.
	if r.log.NextIndex() <= request.PrevLogIndex {
		r.options.logger.Debugf(
			"AppendEntries RPC rejected: reason = log does not contain previous entry, logIndex = %d, lastLogIndex = %d",
			request.PrevLogIndex,
			r.log.LastIndex(),
		)
		response.Index = r.log.NextIndex()
		return nil
	}

	// Reject the request if the previous log index matches the last included log index, but the previous log term does
	// not match the last included term.
	if r.lastIncludedIndex == request.PrevLogIndex && r.lastIncludedTerm != request.PrevLogTerm {
		r.options.logger.Debugf(
			"AppendEntries RPC rejected: reason = previous log entry has conflicting term, logIndex = %d, localTerm = %d, remoteTerm = %d",
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
			r.options.logger.Fatalf("failed to get entry from log: error = %v", err)
		}

		// Reject the request if the log has the previous log entry, but its term does not match.
		if prevLogEntry.Term != request.PrevLogTerm {
			r.options.logger.Debugf(
				"AppendEntries RPC rejected: reason = previous log entry has conflicting term, index = %d, localTerm = %d, remoteTerm = %d",
				request.PrevLogIndex,
				prevLogEntry.Term,
				request.PrevLogTerm,
			)

			// Find the first index of the conflicting term.
			index := request.PrevLogIndex - 1
			for ; index > r.lastIncludedIndex; index-- {
				entry, err := r.log.GetEntry(index)
				if err != nil {
					r.options.logger.Fatalf("failed to get entry from log: error = %v", err)
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

		existing, err := r.log.GetEntry(entry.Index)
		if err != nil {
			r.options.logger.Fatalf("failed to get entry from log: error = %v", err)
		}
		if !existing.IsConflict(entry) {
			continue
		}

		r.options.logger.Warnf("truncating log: index = %d", entry.Index)
		if err := r.log.Truncate(entry.Index); err != nil {
			r.options.logger.Fatalf("failed to truncate log: %v", err)
		}

		// Fall back to the committed configuration if the current one is
		// truncated. This is necessary since a partitioned leader may have
		// recieved a membership change request.
		if entry.Index <= r.configuration.Index {
			r.nextConfiguration(r.committedConfiguration)
		}

		toAppend = request.Entries[i:]
		break
	}

	if err := r.log.AppendEntries(toAppend); err != nil {
		r.options.logger.Fatalf("failed to append entries to log: %v", err)
	}

	if request.LeaderCommit > r.commitIndex {
		r.commitIndex = util.Min(request.LeaderCommit, r.log.LastIndex())
		r.applyCond.Broadcast()
	}

	return nil
}

// sendAppendEntriesToPeers sends an AppendEntries RPC to all nodes.
func (r *Raft) sendAppendEntriesToPeers() {
	numResponses := 1
	for id, address := range r.configuration.Members {
		go r.sendAppendEntries(id, address, &numResponses)
	}
}

// sendAppendEntries sends an AppendEntries RPC to a node with the provided ID
// and address.
func (r *Raft) sendAppendEntries(id string, address string, numResponses *int) {
	r.mu.Lock()
	defer r.mu.Unlock()

	// Only leader may send AppendEntries RPCs.
	if r.state != Leader {
		return
	}

	// It's possible that this node was removed from the cluster while the lock
	// was released.
	follower, ok := r.followers[id]
	if !ok {
		return
	}

	// Handle the single node case.
	if id == r.id {
		if len(r.configuration.Members) == 1 {
			if r.log.LastIndex() > r.commitIndex {
				r.commitCond.Broadcast()
			}
			r.tryApplyReadOnlyOperations()
		}
		return
	}

	// Send a snapshot instead if the follower log no longer contains the previous log entry.
	if follower.nextIndex <= r.lastIncludedIndex {
		r.sendInstallSnapshot(id, address)
		return
	}

	nextIndex := follower.nextIndex
	prevLogIndex := util.Max(nextIndex-1, r.lastIncludedIndex)
	prevLogTerm := r.lastIncludedTerm

	if prevLogIndex > r.lastIncludedIndex && prevLogIndex < r.log.NextIndex() {
		prevEntry, err := r.log.GetEntry(prevLogIndex)
		if err != nil {
			r.options.logger.Fatalf("failed getting entry from log: error = %v", err)
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
			r.options.logger.Fatalf("failed getting entry from log: error = %v", err)
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
	response, err := r.transport.SendAppendEntries(address, request)
	r.mu.Lock()

	// Quit if RPC failed, leadership was lost, or the node was removed from the cluster.
	if _, ok := r.followers[id]; !ok || err != nil || r.state != Leader {
		return
	}

	// Become a follower if a follower has a more up-to-date term.
	if response.Term > r.currentTerm {
		r.becomeFollower(id, response.Term)
		return
	}

	// Recieved ack from majority of cluster - this node is a legitimate leader.
	// Try to apply pending read-only operations.
	if numResponses != nil {
		*numResponses += 1
		if r.hasQuorum(*numResponses) {
			r.tryApplyReadOnlyOperations()
			numResponses = nil
		}
	}

	if !response.Success {
		follower.nextIndex = response.Index
		// Send a snapshot to the follower if the log no longer contains the previous entry.
		if follower.nextIndex <= r.lastIncludedIndex {
			r.sendInstallSnapshot(id, address)
		}
		return
	}

	// Update the next and match index of the followers.
	if request.PrevLogIndex+uint64(len(entries)) > follower.matchIndex {
		follower.nextIndex = util.Max(
			follower.nextIndex,
			request.PrevLogIndex+uint64(len(entries))+1,
		)
		follower.matchIndex = request.PrevLogIndex + uint64(len(entries))
		if follower.matchIndex > r.commitIndex {
			r.commitCond.Broadcast()
		}
	}
}

// RequestVote handles vote requests from other nodes during elections. It takes a vote request
// and fills the response with the result of the vote.
func (r *Raft) RequestVote(request *RequestVoteRequest, response *RequestVoteResponse) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.state == Shutdown {
		return fmt.Errorf("could not execute RequestVote RPC: %s is shutdown", r.id)
	}

	r.options.logger.Debugf(
		"RequestVote RPC received: candidateID = %s, term = %d, lastLogIndex = %d, lastLogTerm = %d",
		request.CandidateID,
		request.Term,
		request.LastLogIndex,
		request.LastLogTerm,
	)

	response.Term = r.currentTerm
	response.VoteGranted = false

	// Unless this node is a leader that needs to step down, ignore any requests for
	// a vote if this node has heard from the leader within an election timeout.
	if r.state != Leader && time.Since(r.lastContact) < r.options.electionTimeout {
		return nil
	}

	// Reject the request if the term is out-of-date.
	if request.Term < r.currentTerm {
		r.options.logger.Debugf(
			"RequestVote RPC rejected: reason = out of date term, localTerm =  %d, remoteTerm = %d",
			r.currentTerm,
			request.Term,
		)
		return nil
	}

	// If the request has a more up-to-date term, update current term and become a followers.
	if request.Term > r.currentTerm {
		r.becomeFollower(request.CandidateID, request.Term)
		response.Term = r.currentTerm
	}

	// Reject the request if this node has already voted.
	if r.votedFor != "" && r.votedFor != request.CandidateID {
		r.options.logger.Debugf(
			"RequestVote RPC rejected: reason = already voted, votedFor = %s",
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
		r.options.logger.Debugf(
			"RequestVote RPC rejected: reason = out-of-date log, localLastLogIndex = %d, localLastLogTerm = %d, remoteLastLogTerm = %d, remoteLastLogIndex = %d",
			r.log.LastIndex(),
			r.log.LastTerm(),
			request.LastLogIndex,
			request.LastLogTerm,
		)
		return nil
	}

	r.lastContact = time.Now()
	response.VoteGranted = true
	r.votedFor = request.CandidateID
	r.persistTermAndVote()

	r.options.logger.Infof(
		"RequestVote RPC successful: votedFor = %s, term = %d",
		request.CandidateID,
		r.currentTerm,
	)

	return nil
}

// electionLoop is a long running loop that will kick off election when this node
// has not beem contacted by the leader within the election timeout period.
func (r *Raft) electionLoop() {
	defer r.wg.Done()

	for {
		// A random timeout between the specified election timeout (by default 200 ms) and twice the
		// election timeout is chosen to sleep for in order to prevent multiple nodes from becoming
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

// election will kick off an election if this node is a voting member and
// it is not been contacted by the leader within the election timeout period.
func (r *Raft) election() {
	r.mu.Lock()
	defer r.mu.Unlock()

	// If we have already been elected the leader, or we have been contacted by the leader
	// since the last election timeout, an election is not needed.
	if r.state != Follower || time.Since(r.lastContact) < r.options.electionTimeout {
		return
	}

	// If this node is not a voter, an election is not necessary.
	if !r.configuration.IsVoter[r.id] {
		return
	}

	var votesReceived int
	r.becomeCandidate()
	r.sendRequestVoteToPeers(&votesReceived)
}

// sendRequestVoteToPeers sends a RequestVoteRPC to all nodes in the
// cluster, excluding those that are non-voters.
func (r *Raft) sendRequestVoteToPeers(votes *int) {
	for id, address := range r.configuration.Members {
		go r.sendRequestVote(id, address, votes)
	}
}

// sendRequestVote sends a RequestVote RPC to the node with the provided
// ID and address if it is a voting member.
func (r *Raft) sendRequestVote(id string, address string, votes *int) {
	r.mu.Lock()
	defer r.mu.Unlock()

	// The candidate will vote for itself.
	if id == r.id {
		*votes++
		// Handle the single node case.
		if r.hasQuorum(*votes) {
			r.becomeLeader()
		}
		return
	}

	// Do not send request to any non-voting members.
	if !r.configuration.IsVoter[id] {
		return
	}

	request := RequestVoteRequest{
		CandidateID:  r.id,
		Term:         r.currentTerm,
		LastLogIndex: r.log.LastIndex(),
		LastLogTerm:  r.log.LastTerm(),
	}

	r.mu.Unlock()
	response, err := r.transport.SendRequestVote(address, request)
	r.mu.Lock()

	// Ensure this response is not stale. It is possible that this
	// node has started another election.
	if err != nil || r.currentTerm != request.Term {
		return
	}

	// Increment vote count if vote is granted.
	if response.VoteGranted {
		*votes++
	}

	// Become a followers if a followers has a more up-to-date term.
	if response.Term > r.currentTerm {
		r.becomeFollower(id, response.Term)
		return
	}

	// If we have received votes from the majority of followers, become a leader.
	if r.hasQuorum(*votes) && r.state == Follower {
		r.becomeLeader()
	}
}

// InstallSnapshot handles snaopshot installation requests from the leader. It takes a request to
// install a snapshot and fills the response with the result of the installation.
func (r *Raft) InstallSnapshot(
	request *InstallSnapshotRequest,
	response *InstallSnapshotResponse,
) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.state == Shutdown {
		return fmt.Errorf("could not execute InstallSnapshot RPC: %s is shutdown", r.id)
	}

	r.options.logger.Debugf(
		"InstallSnapshot RPC received: leaderID = %s, term = %d, lastIndex = %d, lastTerm = %d, offset = %d, done = %v",
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
			"InstallSnapshot RPC rejected: reason = out of date term, localTerm = %d, remoteTerm = %d",
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
				r.options.logger.Fatalf("failed to discard snapshot: error = %v", err)
			}
			r.snapshot = nil
		}
	}

	// Create a new snapshot file if one has not already been created.
	if r.snapshot == nil {
		snapshot, err := r.snapshotStorage.NewSnapshotFile(
			request.LastIncludedIndex,
			request.LastIncludedTerm,
			request.Configuration,
		)
		if err != nil {
			r.options.logger.Fatalf("failed to create snapshot file: error = %v", err)
		}
		r.snapshot = snapshot
	}

	// Ensure the offset in the request is the expected offset.
	offset, err := r.snapshot.Seek(0, io.SeekCurrent)
	response.BytesWritten = offset
	if err != nil {
		r.options.logger.Fatalf("failed to seek snapshot file: error = %v", err)
	}
	if request.Offset != offset {
		r.options.logger.Warnf(
			"InstallSnapshot RPC contains incorrect offset: expectedOffset = %d, receivedOffset = %d",
			offset,
			request.Offset,
		)
		return nil
	}

	// Write the snapshot chunk to the file.
	reader := bytes.NewReader(request.Bytes)
	n, err := io.Copy(r.snapshot, reader)
	if err != nil {
		r.options.logger.Fatalf("failed to write snapshot file: error = %v", err)
	}
	response.BytesWritten += n

	if !request.Done {
		return nil
	}

	if err := r.snapshot.Close(); err != nil {
		r.options.logger.Fatalf("failed to close snapshot file: error = %v", err)
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

		r.options.logger.Warnf("compacting log: logIndex = %d", request.LastIncludedIndex)
		if err := r.log.Compact(request.LastIncludedIndex); err != nil {
			r.options.logger.Fatalf("failed to compact log: error = %v", err)
		}
		return nil
	}

	// Restore the state machine with the snapshot.
	snapshot, err := r.snapshotStorage.SnapshotFile()
	if err != nil {
		r.options.logger.Fatalf("failed to get snapshot file: error = %v", err)
	}
	r.mu.Unlock()
	r.options.logger.Warnf(
		"restoring state machine with snapshot: lastIndex = %d, lastTerm = %d",
		request.LastIncludedIndex,
		request.LastIncludedTerm,
	)
	if err := r.fsm.Restore(snapshot); err != nil {
		r.options.logger.Fatalf("failed to restore state machine with snapshot: error = %v", err)
	}
	if err := snapshot.Close(); err != nil {
		r.options.logger.Fatalf("failed to close snapshot file: error = %v", err)
	}
	r.mu.Lock()
	r.lastApplied = request.LastIncludedIndex
	r.commitIndex = request.LastIncludedIndex

	// Discard the entire log.
	r.options.logger.Warnf(
		"discarding log: lastIndex = %d, lastTerm = %d",
		request.LastIncludedIndex,
		request.LastIncludedTerm,
	)
	if err := r.log.DiscardEntries(request.LastIncludedIndex, request.LastIncludedTerm); err != nil {
		r.options.logger.Fatalf("failed to discard log entries: error = %v", err)
	}

	// Update the configuration.
	configuration := r.decodeConfiguration(request.Configuration)
	r.nextConfiguration(&configuration)
	r.committedConfiguration = &configuration

	r.options.logger.Infof(
		"snapshot installation completed successfully: lastIndex = %d, lastTerm = %d",
		request.LastIncludedIndex,
		request.LastIncludedTerm,
	)

	return nil
}

// takeSnapshot takes a snapshot of the state machine. A snapshot will
// only be taken if there is new state since the previous snapshot and there
// is not a pending configuration change.
func (r *Raft) takeSnapshot() {
	// There is nothing new to snapshot.
	if r.lastApplied <= r.lastIncludedIndex {
		return
	}

	// Put off snapshots if there is an outstanding configuration change.
	if r.committedConfiguration == nil || r.committedConfiguration.Index > r.lastApplied {
		return
	}

	lastAppliedEntry, err := r.log.GetEntry(r.lastApplied)
	if err != nil {
		r.options.logger.Fatalf("failed to get entry from log: error = %v", err)
	}

	r.options.logger.Infof(
		"starting to take snapshot: lastIndex = %d, lastTerm = %d",
		lastAppliedEntry.Index,
		lastAppliedEntry.Term,
	)

	// Create a new snapshot file.
	configurationData := r.encodeConfiguration(r.committedConfiguration)
	snapshot, err := r.snapshotStorage.NewSnapshotFile(
		lastAppliedEntry.Index,
		lastAppliedEntry.Term,
		configurationData,
	)
	if err != nil {
		r.options.logger.Fatalf("failed to create snapshot file: error = %v", err)
	}

	// Take a snapshot of the state machine.
	// It's best that the lock is not held here since this might take a while.
	r.mu.Unlock()
	if err := r.fsm.Snapshot(snapshot); err != nil {
		r.options.logger.Fatalf("failed to take snapshot of state machine: error = %v", err)
	}
	if err := snapshot.Close(); err != nil {
		r.options.logger.Fatalf("failed to close snapshot file: error = %v", err)
	}
	r.mu.Lock()

	// It's possible a snapshot was installed and the log was compacted while the lock was released.
	if lastAppliedEntry.Index <= r.lastIncludedIndex {
		return
	}

	// Compact the log.
	r.lastIncludedIndex = lastAppliedEntry.Index
	r.lastIncludedTerm = lastAppliedEntry.Term
	r.options.logger.Warnf("compacting log: logIndex = %d", r.lastIncludedIndex)
	if err := r.log.Compact(r.lastIncludedIndex); err != nil {
		r.options.logger.Fatalf("failed to compact log: error = %v", err)
	}
	r.resetSnapshotFiles()

	r.options.logger.Infof(
		"snapshot taken successfully: lastIndex = %d, lastTerm = %d",
		r.lastIncludedIndex,
		r.lastIncludedTerm,
	)
}

// sendInstallSnapshot sends a snapshot to the node with the provided ID and address.
func (r *Raft) sendInstallSnapshot(id, address string) {
	// Only the leader may send snapshots.
	if r.state != Leader {
		return
	}

	if r.lastIncludedIndex == 0 {
		r.options.logger.Warn("cannot send snapshot to follower because one has not been taken")
		return
	}

	follower := r.followers[id]

	// Retrieve the most recent snapshot file to send to the follower if one is not already open.
	if follower.snapshot == nil {
		snapshot, err := r.snapshotStorage.SnapshotFile()
		if err != nil {
			r.options.logger.Fatalf("failed to get snapshot file: error = %v", err)
		}
		follower.snapshot = snapshot
	}

	metadata := follower.snapshot.Metadata()
	offset, err := follower.snapshot.Seek(0, io.SeekCurrent)
	if err != nil {
		r.options.logger.Fatalf("failed to seek snapshot file: error = %v", err)
	}

	request := InstallSnapshotRequest{
		LeaderID:          r.id,
		Term:              r.currentTerm,
		LastIncludedIndex: metadata.LastIncludedIndex,
		LastIncludedTerm:  metadata.LastIncludedTerm,
		Configuration:     metadata.Configuration,
		Offset:            offset,
	}

	// Read a chunk of the snapshot from the file.
	var buf bytes.Buffer
	n, err := io.Copy(&buf, follower.snapshot)
	if err != nil {
		if err := follower.snapshot.Close(); err != nil {
			r.options.logger.Errorf("failed to close snapshot file: error = %v", err)
		}
		r.options.logger.Fatalf("failed to read snapshot file: error = %v", err)
	}
	request.Bytes = buf.Bytes()
	request.Done = n < snapshotChunkSize

	r.mu.Unlock()
	response, err := r.transport.SendInstallSnapshot(address, request)
	r.mu.Lock()

	if follower.snapshot == nil || err != nil {
		return
	}

	// If the follower has a more up-to-date term, transition to the follower state.
	if response.Term > r.currentTerm {
		r.becomeFollower(id, response.Term)
		return
	}

	// The follower is either missing part of the snapshot or already has this part.
	// Reset to the follower's offset.
	if response.BytesWritten != offset {
		if _, err := follower.snapshot.Seek(response.BytesWritten, io.SeekStart); err != nil {
			r.options.logger.Fatalf("failed to seek snapshot file: error = %v", err)
		}
		return
	}

	if !request.Done {
		return
	}

	if err := follower.snapshot.Close(); err != nil {
		r.options.logger.Fatalf("failed to close snapshot file: error = %v", err)
	}
	follower.snapshot = nil
	follower.matchIndex = request.LastIncludedIndex
	follower.nextIndex = request.LastIncludedIndex + 1
}

// heartbeatLoop is a long running loop that periodically sends heartbeats to
// followers if this node is the leader.
func (r *Raft) heartbeatLoop() {
	defer r.wg.Done()

	// If this node is the leader, broadcast heartbeat messages to followers
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

// commitLoop is a long running loop that verifies quorum has been achieved
// for log entries and updates the commit index of the leader.
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
			// It is not safe for the leader to commit an entry with a term
			// different from the current term. It is possible for a log entry
			// to be agreed upon by the majority of node in the cluster, but
			// be overwritten by a future leader.
			if entry, err := r.log.GetEntry(index); err != nil {
				r.options.logger.Fatalf("failed to get  entry from log: error = %v", err)
			} else if entry.Term != r.currentTerm {
				continue
			}

			// Check whether the majority of nodes in the cluster agree on the entry.
			// If they do, it is safe to commit.
			matches := 1
			for id, follower := range r.followers {
				// Ignore this node and any nodes which are not voting members.
				if id == r.id || !r.configuration.IsVoter[id] {
					continue
				}
				if follower.matchIndex >= index {
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

// applyLoop is a long running loop that applies replicated operations to the state machine
// and takes snapshots when necessary.
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
				r.options.logger.Fatalf("failed to get entry from log: error = %v", err)
			}

			if entry.EntryType == NoOpEntry {
				r.lastApplied++
				continue
			}

			if entry.EntryType == ConfigurationEntry {
				r.applyConfigurationEntry(entry)
				r.lastApplied++
				response := newResult[Configuration](*r.configuration, nil)
				select {
				case r.configurationResponseCh <- response:
				default:
				}
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
			}
			lastApplied := r.lastApplied

			r.mu.Unlock()
			response := OperationResponse{
				Operation:           operation,
				ApplicationResponse: r.fsm.Apply(&operation),
			}
			if ok {
				responseCh <- newResult[OperationResponse](response, nil)
			}
			r.options.logger.Debugf(
				"applied operation to state machine: logIndex = %d, logTerm = %d, type = %s",
				operation.LogIndex,
				operation.LogTerm,
				operation.OperationType.String(),
			)
			r.mu.Lock()

			// It's possible a snapshot was installed while the lock was released.
			// It's not safe to increment the last applied index if it has changed.
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

// applyConfigurationEntry applies a log entry containing a configuration to this node.
func (r *Raft) applyConfigurationEntry(entry *LogEntry) {
	configuration := r.decodeConfiguration(entry.Data)
	r.nextConfiguration(&configuration)
	r.committedConfiguration = &configuration
}

// readOnlyLoop is a long running loop that applies read-only operations to the state machine.
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
		for operation, responseCh := range appliableOperations {
			if operation.OperationType == LeaseBasedReadOnly &&
				!r.operationManager.leaderLease.isValid() {
				responseCh <- newResult[OperationResponse](OperationResponse{}, ErrInvalidLease)
				continue
			}

			r.mu.Unlock()
			response := OperationResponse{
				Operation:           *operation,
				ApplicationResponse: r.fsm.Apply(operation),
			}
			responseCh <- newResult[OperationResponse](response, nil)
			r.options.logger.Debugf(
				"applied operation to state machine: readIndex = %d, type = %s",
				operation.readIndex,
				operation.OperationType.String(),
			)
			r.mu.Lock()

			if r.state != Leader {
				break
			}
		}
	}
}

// becomeCandidate transitions this node to the candidate state.
func (r *Raft) becomeCandidate() {
	r.currentTerm++
	r.votedFor = r.id
	r.persistTermAndVote()
	r.options.logger.Infof("entered the candidate state: term = %d", r.currentTerm)
}

// becomeLeader transitions this node to the leader state.
func (r *Raft) becomeLeader() {
	r.state = Leader
	r.operationManager = newOperationManager(r.options.leaseDuration)
	for _, follower := range r.followers {
		follower.nextIndex = r.log.LastIndex() + 1
		follower.matchIndex = 0
	}
	r.resetSnapshotFiles()

	// Append a new log entry for this term.
	entry := NewLogEntry(r.log.NextIndex(), r.currentTerm, []byte{}, NoOpEntry)
	if err := r.log.AppendEntry(entry); err != nil {
		r.options.logger.Fatal("failed to append entry to log: error = %v", err)
	}
	r.sendAppendEntriesToPeers()

	r.options.logger.Infof("entered the leader state: term = %d", r.currentTerm)
}

// becomeFollower transitions this node to the follower state.
func (r *Raft) becomeFollower(leaderID string, term uint64) {
	r.state = Follower
	r.currentTerm = term
	r.leaderId = leaderID
	r.votedFor = ""
	r.persistTermAndVote()
	r.resetSnapshotFiles()

	// Cancel any pending operations.
	r.operationManager.notifyLostLeaderShip(r.id, r.leaderId)
	r.operationManager = newOperationManager(r.options.leaseDuration)

	r.options.logger.Infof("entered the follower state: term = %d", r.currentTerm)
}

// tryApplyReadOnlyOperations renews the lease and notifies the read-only
// loop that it may be possible to apply some read-only operations.
func (r *Raft) tryApplyReadOnlyOperations() {
	r.operationManager.markAsVerified()
	r.operationManager.leaderLease.renew()
	r.operationManager.shouldVerifyQuorum = true
	r.readOnlyCond.Broadcast()
}

// resetSnapshotFiles closes or discards any open snapshot files on this node.
// If the file is being written, it is discarded. If it is being read, it is closed.
func (r *Raft) resetSnapshotFiles() {
	for _, follower := range r.followers {
		if follower.snapshot != nil {
			if err := follower.snapshot.Close(); err != nil {
				r.options.logger.Fatalf("failed to close snapshot file: error = %v", err)
			}
			follower.snapshot = nil
		}
	}
	if r.snapshot != nil {
		if err := r.snapshot.Discard(); err != nil {
			r.options.logger.Fatalf("failed to discard snapshot file: error = %v", err)
		}
		r.snapshot = nil
	}
}

// hasQuorum returns true if the provided count makes constitutes
// quorum for the cluster and false otherwise. Non-voting members
// of the cluster are not considered for quorum.
func (r *Raft) hasQuorum(count int) bool {
	voters := 0
	for _, isVoter := range r.configuration.IsVoter {
		if isVoter {
			voters++
		}
	}
	return count > voters/2
}

// persistTermAndVote writes the term and vote for this node to non-volatile storage.
func (r *Raft) persistTermAndVote() {
	if err := r.stateStorage.SetState(r.currentTerm, r.votedFor); err != nil {
		r.options.logger.Fatalf("failed to persist term and vote: error = %v", err)
	}
}

// nextConfiguration transitions this node from its current configuration to
// to the provided configuration. Any unused connections will be torn down
// and any new connections will be estabslished. If this node is not a member
// of the next configuration, it will transition to a configuration consisting
// of only itself with non-voter status.
func (r *Raft) nextConfiguration(next *Configuration) {
	r.options.logger.Infof("transitioning to new configuration: configuration = %s", next.String())

	// Step down if this node is being removed.
	if _, ok := next.Members[r.id]; !ok {
		r.resetSnapshotFiles()
		r.state = Follower
	}

	// Close any connections with removed nodes.
	for id, address := range r.configuration.Members {
		if _, ok := next.Members[id]; !ok {
			delete(r.followers, id)
			if id == r.id {
				continue
			}
			if err := r.transport.Close(address); err != nil {
				r.options.logger.Errorf(
					"failed to close connection to node: id = %s, address = %s, error = %v",
					id,
					address,
					err,
				)
			}
		}
	}

	// Establish connection with any new nodes.
	for id, address := range next.Members {
		if _, ok := r.configuration.Members[id]; !ok {
			r.followers[id] = new(follower)
			if id == r.id {
				continue
			}
			if err := r.transport.Connect(address); err != nil {
				r.options.logger.Errorf(
					"failed to connect to peer: id = %s, address = %s, error = %v",
					id,
					address,
					err,
				)
			}
		}
	}

	r.configuration = next
}

// encodeConfiguration encodes the provided configuration.
func (r *Raft) encodeConfiguration(configuration *Configuration) []byte {
	data, err := r.transport.EncodeConfiguration(configuration)
	if err != nil {
		r.options.logger.Fatalf("failed to encode configuration: error = %v", err)
	}
	return data
}

// decodeConfiguration decodes the provided bytes into a configuration.
func (r *Raft) decodeConfiguration(data []byte) Configuration {
	configuration, err := r.transport.DecodeConfiguration(data)
	if err != nil {
		r.options.logger.Fatalf("failed to decode configuration: error = %v", err)
	}
	return configuration
}

// appendConfiguration sets the log index associated with the
// configuration and appends it to the log.
func (r *Raft) appendConfiguration(configuration *Configuration) {
	configuration.Index = r.log.NextIndex()
	data := r.encodeConfiguration(configuration)
	entry := NewLogEntry(configuration.Index, r.currentTerm, data, ConfigurationEntry)
	if err := r.log.AppendEntry(entry); err != nil {
		r.options.logger.Fatalf("failed to append entry to log: error = %v", err)
	}
}
