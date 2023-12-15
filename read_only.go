package raft

type readOnlyManager struct {
	// Contains read-only operations waiting to execute.
	pendingOperations map[*Operation]bool

	// A flag that indicates whether a round of heartbeats should
	// be sent to peers to confirm leadership.
	shouldVerifyQuorum bool

	// The lease for lease-based reads.
	leaderLease lease
}

func (r *readOnlyManager) enqueueOperation(operation *Operation) {
	r.pendingOperations[operation] = true
	r.shouldVerifyQuorum = true
}

func (r *readOnlyManager) markAsVerified() {
	for operation := range r.pendingOperations {
		operation.QuorumVerified = true
	}
	r.shouldVerifyQuorum = false
}

func (r *readOnlyManager) appliableOperations(applyIndex uint64) []*Operation {
	operations := make([]*Operation, 0)
	for operation := range r.pendingOperations {
		if (operation.OperationType == LinearizableReadOnly && operation.QuorumVerified && operation.ReadIndex <= applyIndex) ||
			(operation.OperationType == LeaseBasedReadOnly && operation.ReadIndex <= applyIndex) {
			operations = append(operations, operation)
			delete(r.pendingOperations, operation)
		}
	}
	return operations
}

func (r *readOnlyManager) notifyLostLeaderShip(id string, knownLeader string) {
	for operation := range r.pendingOperations {
		select {
		case operation.ResponseCh <- OperationResponse{Err: NotLeaderError{ServerID: id, KnownLeader: knownLeader}}:
		default:
		}
	}
}
