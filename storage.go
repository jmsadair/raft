package raft

import (
	"io"
	"os"

	"github.com/jmsadair/raft/internal/errors"
)

var (
	errStorageNotOpen = errors.New("storage is not open")
)

// Storage is an interface representing the internal component of Raft that is responsible
// for durably storing the vote and term.
type Storage interface {
	// Open opens the storage for reading and writing persisting state.
	Open() error

	// Close closes the storage.
	Close() error

	// SetState persists the provided state in the storage. Storage must be open.
	SetState(persistentState *PersistentState) error

	// GetState recovers the state from the storage. Storage must be open.
	GetState() (PersistentState, error)

	// SizeInBytes returns the size of the storage in bytes.
	SizeInBytes() (int64, error)
}

// PersistentState is the state that must be persisted in Raft.
type PersistentState struct {
	// The term of the associated Raft instance.
	Term uint64

	// The vote of the associated Raft instance.
	VotedFor string
}

// persistentStorage is an implementation of the Storage interface that manages durable storage of the persistent
// state associated with a Raft instance. This implementation is not concurrent safe and should only be used
// within the Raft implementation.
type persistentStorage struct {
	// The path to the file where the storage is persisted.
	path string

	// The file associated with the storage, nil if storage is closed.
	file *os.File
}

// NewStorage creates a new instance of Storage at the provided path.
func NewStorage(path string) Storage {
	return &persistentStorage{path: path}
}

func (p *persistentStorage) Open() error {
	file, err := os.OpenFile(p.path, os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		return errors.WrapError(err, "failed to open storage file: path = %s", p.path)
	}
	p.file = file
	return nil
}

func (p *persistentStorage) Close() error {
	if p.file == nil {
		return nil
	}
	if err := p.file.Close(); err != nil {
		return errors.WrapError(err, "failed to close storage file: path = %s", p.path)
	}
	return nil
}

func (p *persistentStorage) SetState(persistentState *PersistentState) error {
	if p.file == nil {
		return errStorageNotOpen
	}

	// Create a temporary file that will replace the file currently associated with storage.
	tmpFile, err := os.Create(p.path + ".tmp")
	if err != nil {
		return errors.WrapError(err, "failed to create temporary storage file")
	}

	// Write the new state to the temporary file.
	if err := encodePersistentState(tmpFile, persistentState); err != nil {
		return errors.WrapError(err, "failed to encode persistent state")
	}
	if err := tmpFile.Sync(); err != nil {
		return errors.WrapError(err, "failed to sync storage file")
	}

	// Perform atomic rename to swap the newly persisted state with the old.
	oldFile := p.file
	if err := os.Rename(tmpFile.Name(), oldFile.Name()); err != nil {
		return errors.WrapError(err, "failed to rename temporary storage file")
	}

	p.file = tmpFile

	// Close the previous file.
	if err := oldFile.Close(); err != nil {
		return errors.WrapError(err, "failed to close storage file")
	}

	return nil
}

func (p *persistentStorage) GetState() (PersistentState, error) {
	if p.file == nil {
		return PersistentState{}, errStorageNotOpen
	}

	// Read the contents of the file associated with the storage.
	reader := io.Reader(p.file)
	persistentState, err := decodePersistentState(reader)

	if err != nil && err != io.EOF {
		return persistentState, errors.WrapError(err, "failed to decode persistent state")
	}

	return persistentState, nil
}

func (p *persistentStorage) SizeInBytes() (int64, error) {
	return p.file.Seek(0, io.SeekCurrent)
}
