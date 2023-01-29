package raft

type StateMachine interface {
	// Apply applies a command to the state machine.
	Apply(command []byte) interface{}

	// Snapshot returns a snapshot of the current state of the state
	// machine. The returned bytes must be serialized in a way that
	// the Restore function can understand.
	Snapshot() ([]byte, error)

	// Restore recovers the state of the state machine given a snapshot.
	Restore(snapshot []byte) error
}
