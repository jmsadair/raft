package raft

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestNewOperationManager checks that operation manager is
// correctly initialized.
func TestNewOperationManager(t *testing.T) {
	leaseDuration := time.Duration(200 * time.Millisecond)
	operationManager := newOperationManager(leaseDuration)
	require.NotNil(t, operationManager.pendingReadOnly)
	require.NotNil(t, operationManager.pendingReplicated)
	require.NotNil(t, operationManager.leaderLease)
	require.True(t, operationManager.shouldVerifyQuorum)
	require.Equal(t, leaseDuration, operationManager.leaderLease.duration)
}

// TestMarkAsVerified checks that when all pending read-only
// operations have their quorum verified flag updated after
// markAsVerified is called.
func TestMarkAsVerified(t *testing.T) {
	leaseDuration := time.Duration(200 * time.Millisecond)
	operationManager := newOperationManager(leaseDuration)

	operation1 := &Operation{}
	operation2 := &Operation{}
	operationManager.pendingReadOnly[operation1] = nil
	operationManager.pendingReadOnly[operation2] = nil
	operationManager.markAsVerified()

	require.True(t, operation1.quorumVerified)
	require.True(t, operation2.quorumVerified)
	require.True(t, operationManager.shouldVerifyQuorum)
}

// TestAppliableReadOnlyOperations checks that all
// pending read-only options that can be applied
// are returned and deleted. Operations that can
// be applied should either be:
//  1. A lease-based read-only operation.
//  2. A linearizable read-only operation for which
//     quorum has been verified.
//  3. The applied index should be atleast as large
//     as the read index of the operation.
func TestAppliableReadOnlyOperations(t *testing.T) {
	leaseDuration := time.Duration(200 * time.Millisecond)
	operationManager := newOperationManager(leaseDuration)

	applyIndex := uint64(1)

	// This operation is appliable.
	operation1 := &Operation{
		OperationType:  LinearizableReadOnly,
		quorumVerified: true,
		readIndex:      1,
	}

	// This operation is not appliable - quorum verified flag has not been set.
	operation2 := &Operation{OperationType: LinearizableReadOnly, readIndex: 1}

	// This operation is appliable.
	operation3 := &Operation{OperationType: LeaseBasedReadOnly, readIndex: 1}

	// This operation is not appliable - the read index exceeds the
	// last applied index.
	operation4 := &Operation{OperationType: LeaseBasedReadOnly, readIndex: 2}

	operationManager.pendingReadOnly[operation1] = nil
	operationManager.pendingReadOnly[operation2] = nil
	operationManager.pendingReadOnly[operation3] = nil
	operationManager.pendingReadOnly[operation4] = nil

	appliableOperations := operationManager.appliableReadOnlyOperations(applyIndex)
	require.Len(t, appliableOperations, 2)
	require.Contains(t, appliableOperations, operation1)
	require.Contains(t, appliableOperations, operation3)

	require.NotContains(t, operationManager.pendingReadOnly, operation1)
	require.NotContains(t, operationManager.pendingReadOnly, operation3)
	require.Contains(t, operationManager.pendingReadOnly, operation2)
	require.Contains(t, operationManager.pendingReadOnly, operation4)
}
