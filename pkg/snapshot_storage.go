package raft

type Snapshot struct {
	// Last index included in snapshot
	LastIncludedIndex uint64
	// Last term included in snapshot.
	LastIncludedTerm uint64
	// State of replicated state machine.
	Data []byte
}

type SnapshotStorage interface {
	// LastSnapshot gets the most recently saved snapshot, if it exists.
	LastSnapshot() (Snapshot, error)

	// SaveSnapshot saves the provided snapshot to durable storage.
	SaveSnapshot(snapshot *Snapshot) error

	// ListSnapshots returns an array of the the snapshots that have been saved.
	ListSnapshots() ([]Snapshot, error)
}
