package raft

import (
	"time"
)

// OperationType is the type of the operation that is being submitted to
// raft.
type OperationType uint32

const (
	// Replicated indicates that the provided operation will be written to the
	// log and guarantees linearizable semantics.
	Replicated OperationType = iota

	// LinearizableReadOnly indicates that the provided operation will not be written
	// to the log and requires that the recieving server verify its leadership through
	// a round  of heartbeats to its peers. Guarantees linearizable semantics.
	LinearizableReadOnly

	// LeaseBasedReadOnly indicates that the provided operation will not be written
	// to the log and requires that the server verify its leadership via its lease.
	// This operation type does not guarantee linearizable semantics.
	LeaseBasedReadOnly
)

// String converts an OperationType to a string.
func (o OperationType) String() string {
	switch o {
	case Replicated:
		return "replicated"
	case LinearizableReadOnly:
		return "linearizableReadOnly"
	case LeaseBasedReadOnly:
		return "leaseBasedReadOnly"
	default:
		panic("invalid operation type")
	}
}

// OperationTimeoutError represents an error that occurs when an operation submitted to
// raft times out.
type OperationTimeoutError struct {
	// The operation that was submitted to raft.
	Operation []byte
}

// Implements the Error interface for the OperationTimeoutError type.
func (e OperationTimeoutError) Error() string {
	return "The operation timed out while waiting for a response. This could be due to loss of server " +
		"leadership, a partitioned leader, prolonged processing, or a different reason. Try submitting the " +
		"operation to this server again or another server."
}

// Operation is an operation that will be applied to the state machine.
// An operation must be deterministic.
type Operation struct {
	// The operation as bytes. The provided state machine should be capable
	// of decoding these bytes.
	Bytes []byte

	// The type of the operation.
	OperationType OperationType

	// The log entry index associated with the operation.
	// Valid only if this is a replicated operation and the operation was successful.
	LogIndex uint64

	// The log entry term associated with the operation.
	// Valid only if this is a replicated operation and the operation was successful.
	LogTerm uint64

	// Indicates whether leadership was verified via a round of hearbeats after this
	// operation was submitted. Only applicable to linearizable read-only operations.
	quorumVerified bool

	// The commit index at the time the operation was submitted. Only applicable to
	// linearizable and lease-based read-only operations.
	readIndex uint64

	// The channel that the result of the operation will be sent over.
	responseCh chan OperationResponse
}

// OperationResponse is the response that is generated after applying
// an operation to the state machine.
type OperationResponse struct {
	// The operation applied to the state machine.
	Operation Operation

	// The response returned by the state machine after applying the operation.
	Response interface{}

	// An error encountered during the processing of the response, if any.
	Err error
}

// OperationResponseFuture represents a future response for an operation.
type OperationResponseFuture struct {
	// The operation associated with the future response.
	operation []byte

	// The maximum time to wait for a response.
	timeout time.Duration

	// A buffered channel for receiving the response.
	responseCh chan OperationResponse
}

// NewOperationResponseFuture creates a new OperationResponseFuture instance.
func NewOperationResponseFuture(operation []byte, timeout time.Duration) *OperationResponseFuture {
	return &OperationResponseFuture{
		operation:  operation,
		timeout:    timeout,
		responseCh: make(chan OperationResponse, 1),
	}
}

// Await waits for the response associated with the future operation.
// Note that the returned response may contain an error which should always be
// checked before consuming the content of the response. The content is not valid
// if the error is not nil.
func (o *OperationResponseFuture) Await() OperationResponse {
	for {
		select {
		case response := <-o.responseCh:
			return response
		case <-time.After(o.timeout):
			return OperationResponse{Err: OperationTimeoutError{Operation: o.operation}}
		}
	}
}

type operationManager struct {
	// Contains read-only operations waiting to be applied.
	pendingReadOnly map[*Operation]bool

	// Maps log index associated with the operation to its response channel.
	pendingReplicated map[uint64]chan OperationResponse

	// A flag that indicates whether a round of heartbeats should be sent to peers to confirm leadership.
	shouldVerifyQuorum bool

	// The lease for lease-based reads.
	leaderLease *lease
}

func newOperationManager(leaseDuration time.Duration) *operationManager {
	return &operationManager{
		pendingReadOnly:    make(map[*Operation]bool),
		pendingReplicated:  make(map[uint64]chan OperationResponse),
		leaderLease:        newLease(leaseDuration),
		shouldVerifyQuorum: true,
	}
}

func (r *operationManager) markAsVerified() {
	for operation := range r.pendingReadOnly {
		operation.quorumVerified = true
	}
	r.shouldVerifyQuorum = true
}

func (r *operationManager) appliableReadOnlyOperations(applyIndex uint64) []*Operation {
	operations := make([]*Operation, 0)
	for operation := range r.pendingReadOnly {
		if (operation.OperationType == LinearizableReadOnly && operation.quorumVerified && operation.readIndex <= applyIndex) ||
			(operation.OperationType == LeaseBasedReadOnly && operation.readIndex <= applyIndex) {
			operations = append(operations, operation)
			delete(r.pendingReadOnly, operation)
		}
	}
	return operations
}

func (r *operationManager) notifyLostLeaderShip(id string, knownLeader string) {
	response := OperationResponse{Err: NotLeaderError{ServerID: id, KnownLeader: knownLeader}}
	for operation := range r.pendingReadOnly {
		select {
		case operation.responseCh <- response:
		default:
		}
	}
	for _, responseCh := range r.pendingReplicated {
		select {
		case responseCh <- response:
		default:
		}
	}
	r.pendingReadOnly = make(map[*Operation]bool)
	r.pendingReplicated = make(map[uint64]chan OperationResponse)
}
