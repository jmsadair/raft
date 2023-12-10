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
