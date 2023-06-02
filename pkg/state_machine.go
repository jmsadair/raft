package raft

// StateMachine is an interface representing a replicated state machine.
type StateMachine interface {
	// Apply applies a command with the associated index and term
	// to the state machine and returns the result.
	Apply(command []byte, index uint64, term uint64) interface{}

	// Snapshot returns a snapshot of the current state of the state machine.
	// The returned bytes must be serialized in a way that the Restore function can understand.
	Snapshot() ([]byte, error)

	// Restore recovers the state of the state machine given a snapshot that was produced
	// by Snapshot.
	Restore(snapshot []byte) error
}
