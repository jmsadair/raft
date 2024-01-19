package raft

import (
	"time"
)

// OperationType is the type of the operation that is being submitted to raft.
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

// OperationResponse is the response that is generated after applying
// an operation to the state machine.
type OperationResponse struct {
	// The operation applied to the state machine.
	Operation Operation

	// The response returned by the state machine after applying the operation.
	ApplicationResponse interface{}
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
}

type operationManager struct {
	// Contains read-only operations waiting to be applied.
	pendingReadOnly map[*Operation]chan Result[OperationResponse]

	// Maps log index associated with the operation to its response channel.
	pendingReplicated map[uint64]chan Result[OperationResponse]

	// A flag that indicates whether a round of heartbeats should be sent to peers to confirm leadership.
	shouldVerifyQuorum bool

	// The lease for lease-based reads.
	leaderLease *lease
}

func newOperationManager(leaseDuration time.Duration) *operationManager {
	return &operationManager{
		pendingReadOnly:    make(map[*Operation]chan Result[OperationResponse]),
		pendingReplicated:  make(map[uint64]chan Result[OperationResponse]),
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

func (r *operationManager) appliableReadOnlyOperations(
	applyIndex uint64,
) map[*Operation]chan Result[OperationResponse] {
	appliableOperations := make(map[*Operation]chan Result[OperationResponse])
	for operation, responseCh := range r.pendingReadOnly {
		if (operation.OperationType == LinearizableReadOnly && operation.quorumVerified && operation.readIndex <= applyIndex) ||
			(operation.OperationType == LeaseBasedReadOnly && operation.readIndex <= applyIndex) {
			appliableOperations[operation] = responseCh
			delete(r.pendingReadOnly, operation)
		}
	}
	return appliableOperations
}

func (r *operationManager) notifyLostLeaderShip(id string, knownLeader string) {
	for _, responseCh := range r.pendingReadOnly {
		respond(responseCh, OperationResponse{}, ErrNotLeader)
	}
	for _, responseCh := range r.pendingReplicated {
		respond(responseCh, OperationResponse{}, ErrNotLeader)
	}
	r.pendingReadOnly = make(map[*Operation]chan Result[OperationResponse])
	r.pendingReplicated = make(map[uint64]chan Result[OperationResponse])
}
