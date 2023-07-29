package raft

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestNewLease checks that a new lease is not valid.
// A lease should not be valid until renewed.
func TestNewLease(t *testing.T) {
	testLease := newLease(defaultElectionTimeout)
	require.False(t, testLease.isValid())
}

// TestRenew checks that a lease is valid after being renewed.
func TestRenew(t *testing.T) {
	testLease := newLease(time.Duration(1 * time.Second))
	testLease.renew()
	require.True(t, testLease.isValid())
}

// TestExpires checks that a lease is no longer valid after
// it has not been renewed.
func TestExpires(t *testing.T) {
	testLease := newLease(defaultElectionTimeout)
	time.Sleep(defaultElectionTimeout)
	require.False(t, testLease.isValid())
}
