package raft

// StateMachine is an interface representing a replicated state machine.
// The implementation must be concurrent safe.
type StateMachine interface {
	// Apply applies the given Operation to the state machine.
	Apply(operation *Operation) interface{}

	// Snapshot returns a snapshot of the current state of the state machine.
	// The bytes contained in the snapshot must be serialized in a way that
	// the Restore function can understand.
	Snapshot() (Snapshot, error)

	// Restore recovers the state of the state machine given a snapshot that was produced
	// by Snapshot.
	Restore(snapshot *Snapshot) error

	// NeedSnapshot returns true if a snapshot should be taken of the state machine and false
	// otherwise.
	NeedSnapshot() bool
}
