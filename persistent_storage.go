package raft

import "errors"

var (
	PersistentStorageNotOpen = errors.New("the storage is not open")
)

// PersistentStorage represents a resource that is persisted
// in non-volatile memory.
type PersistentStorage interface {
	// Open opens the storage and makes it ready for reads and writes.
	Open() error

	// Close closes the storage and performs any necessary cleanup. Once closed,
	// any attempts to read and write to the storage will result in an error.
	Close() error

	// Replay replays the persisted data from non-volatile memory into volatile
	// memory. If the storage is not open, an error will be returned.
	Replay() error
}
