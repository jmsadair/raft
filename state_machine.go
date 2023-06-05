package raft

// StateMachine is an interface representing a replicated state machine.
type StateMachine interface {
	// Apply applies the given log entry to the state machine.
	Apply(entry *LogEntry) interface{}

	// Snapshot returns a snapshot of the current state of the state machine.
	// The bytes contained in the snapshot must be serialized in a way that
	// the Restore function can understand.
	Snapshot() (Snapshot, error)

	// Restore recovers the state of the state machine given a snapshot that was produced
	// by Snapshot.
	Restore(snapshot *Snapshot) error
}
