package raft

// Snapshot represents a snapshot of the replicated state machine.
type Snapshot struct {
	// Last index included in snapshot
	LastIncludedIndex uint64
	// Last term included in snapshot.
	LastIncludedTerm uint64
	// State of replicated state machine.
	Data []byte
}

// NewSnapshot creates a new Snapshot instance with the provided values.
// Parameters:
//   - lastIncludedIndex: The last index included in the snapshot.
//   - lastIncludedTerm: The last term included in the snapshot.
//   - data: The state data of the replicated state machine.
//
// Returns:
//   - *Snapshot: A pointer to the created Snapshot instance.
func NewSnapshot(lastIncludedIndex uint64, lastIncludedTerm uint64, data []byte) *Snapshot {
	return &Snapshot{LastIncludedIndex: lastIncludedIndex, LastIncludedTerm: lastIncludedTerm, Data: data}
}

// SnapshotStorage is an interface representing a component responsible for managing snapshots.
type SnapshotStorage interface {
	// LastSnapshot gets the most recently saved snapshot, if it exists.
	// Returns:
	//     - Snapshot: The most recently saved snapshot.
	//     - error: An error if retrieving the snapshot fails, or nil otherwise.
	LastSnapshot() (Snapshot, error)

	// SaveSnapshot saves the provided snapshot to durable storage.
	// Parameters:
	//     - snapshot: The snapshot to be saved.
	// Returns:
	//     - error: An error if saving the snapshot fails, or nil otherwise.
	SaveSnapshot(snapshot *Snapshot) error

	// ListSnapshots returns an array of the snapshots that have been saved.
	// Returns:
	//     - []Snapshot: An array of the saved snapshots.
	//     - error: An error if retrieving the snapshots fails, or nil otherwise.
	ListSnapshots() ([]Snapshot, error)
}
