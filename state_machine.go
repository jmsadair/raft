package raft

import "io"

// StateMachine is an interface representing a replicated state machine.
// The implementation must be concurrent safe.
type StateMachine interface {
	// Apply applies the given operation to the state machine.
	Apply(operation *Operation) interface{}

	// Snapshot returns a snapshot of the current state of the state machine
	// using the provided writer.
	Snapshot(snapshotWriter io.WriteCloser) error

	// Restore recovers the state of the state machine given a reader for a previously
	// created snapshot.
	Restore(snapshotReader io.ReadCloser) error

	// NeedSnapshot returns true if a snapshot should be taken of the state machine and false
	// otherwise. The provided log size is the number of entries currently in the log.
	NeedSnapshot(logSize int) bool
}
