package raft

// StateMachine is an interface representing a replicated state machine.
type StateMachine interface {
	// Apply applies a command to the state machine and returns the result.
	//
	// Parameters:
	//     - command: The command to apply to the state machine.
	//
	// Returns:
	//     - interface{}: The result of applying the command to the state machine.
	Apply(command []byte) interface{}

	// Snapshot returns a snapshot of the current state of the state machine.
	// The returned bytes must be serialized in a way that the Restore function can understand.
	//
	// Returns:
	//     - []byte: The serialized snapshot of the current state.
	//     - error: An error if creating the snapshot fails, or nil otherwise.
	Snapshot() ([]byte, error)

	// Restore recovers the state of the state machine given a snapshot.
	//
	// Parameters:
	//     - snapshot: The serialized snapshot representing the state of the state machine.
	//
	// Returns:
	//     - error: An error if restoring the state fails, or nil otherwise.
	Restore(snapshot []byte) error
}
