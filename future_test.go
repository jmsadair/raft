package raft

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestFutureAwaitSuccess checks that a future that completes successfully
// does not return an error and returns the expected response.
func TestFutureAwaitSuccess(t *testing.T) {
	timeout := time.Millisecond * 500
	operationFuture := newFuture[OperationResponse](timeout)
	operation := Operation{LogIndex: 1, LogTerm: 1, Bytes: []byte("operation")}
	operationResponse := OperationResponse{Operation: operation, ApplicationResponse: "response"}

	go func() {
		time.Sleep(200 * time.Millisecond)
		operationFuture.responseCh <- &result[OperationResponse]{success: operationResponse}
	}()

	response := operationFuture.Await()

	// The future should not have timed out.
	require.NoError(t, response.Error())

	// Make sure calling Error again returns the result.
	require.NoError(t, response.Error())

	// Make sure it is the expected response.
	require.Equal(t, operationResponse, response.Success())

	// Make sure calling Success again returns the same result.
	require.Equal(t, operationResponse, response.Success())
}

// TestFutureAwaitError checks that a future which has not completed before the
// specified timeout returns an error.
func TestFutureAwaitError(t *testing.T) {
	timeout := time.Millisecond * 100
	operationFuture := newFuture[OperationResponse](timeout)

	go func() {
		time.Sleep(200 * time.Millisecond)
		operationFuture.responseCh <- &result[OperationResponse]{success: OperationResponse{}}
	}()

	response := operationFuture.Await()

	// The future should have timed out.
	require.Error(t, response.Error())

	// Make sure that calling Error again returns the same result.
	require.Error(t, response.Error())
}
