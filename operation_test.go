package raft

import (
	"github.com/stretchr/testify/require"
	"testing"
	"time"
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

	_, err := future.Await()
	require.NoError(t, err)
}

// TestOperationFutureAwaitSuccess checks that OperationResponseFuture.Await times out and
// returns an error when the specified timeout has elapsed with no response.
func TestOperationResponseFutureAwaitTimeout(t *testing.T) {
	timeout := time.Millisecond * 200
	operation := []byte("test")

	future := NewOperationResponseFuture(operation, timeout)

	_, err := future.Await()
	require.Error(t, err)
}
