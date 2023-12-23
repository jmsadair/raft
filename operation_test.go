package raft

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestOperationFutureAwaitSuccess checks that OperationResponseFuture.Await does not return
// an error when a response is received with the specified timeout.
func TestOperationResponseFutureAwaitSuccess(t *testing.T) {
	timeout := time.Millisecond * 1000
	operation := []byte("test")

	future := NewOperationResponseFuture(operation, timeout)

	go func() {
		time.Sleep(100 * time.Millisecond)
		future.responseCh <- OperationResponse{}
	}()

	response := future.Await()
	require.NoError(t, response.Err)
}

// TestOperationFutureAwaitSuccess checks that OperationResponseFuture.Await times out and
// returns an error when the specified timeout has elapsed with no response.
func TestOperationResponseFutureAwaitTimeout(t *testing.T) {
	timeout := time.Millisecond * 200
	operation := []byte("test")

	future := NewOperationResponseFuture(operation, timeout)

	response := future.Await()
	require.Error(t, response.Err)
}

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
	operationManager.pendingReadOnly[operation1] = true
	operationManager.pendingReadOnly[operation2] = true
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

	operationManager.pendingReadOnly[operation1] = true
	operationManager.pendingReadOnly[operation2] = true
	operationManager.pendingReadOnly[operation3] = true
	operationManager.pendingReadOnly[operation4] = true

	appliableOperations := operationManager.appliableReadOnlyOperations(applyIndex)
	require.Len(t, appliableOperations, 2)
	require.Contains(t, appliableOperations, operation1)
	require.Contains(t, appliableOperations, operation3)

	require.NotContains(t, operationManager.pendingReadOnly, operation1)
	require.NotContains(t, operationManager.pendingReadOnly, operation3)
	require.Contains(t, operationManager.pendingReadOnly, operation2)
	require.Contains(t, operationManager.pendingReadOnly, operation4)
}
