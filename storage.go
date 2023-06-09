package raft

import (
	"io"
	"os"

	"github.com/jmsadair/raft/internal/errors"
)

const (
	errStorageNotOpen              = "storage is not open: path = %s"
	errFailedStorageOpen           = "failed to open storage file: path = %s, err = %s"
	errFailedStorageClose          = "failed to close storage file: path = %s, err = %s"
	errFailedStorageSync           = "failed to sync storage file: %s"
	errFailedStorageFlush          = "failed flushing data from storage file writer: %s"
	errFailedStorageCreateTempFile = "failed to create temporary storage file: %s"
	errFailedStorageRename         = "failed to rename temporary storage file: %s"
	errFailedPersistentStateEncode = "storage failed to encode persistent state: %s"
	errFailedPeristentStateDecode  = "storage failed to decode persistent state: %s"
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
}

// PersistentState is the state that must be persisted in Raft.
type PersistentState struct {
	// Term is the term of the associated with Raft instance.
	Term uint64

	// votedFor is the vote associated with a Raft instance.
	VotedFor string
}

// persistentStorage is an implementation of the Storage interface that manages durable storage of the persitent
// state associated with a Raft instance. This implementation is not concurrent safe and should only be used
// within the Raft implementation.
type persistentStorage struct {
	// The path to the file where the storage is persisted.
	path string

	// The file associated with the storage, nil if storage is closed.
	file *os.File
}

// newPersistentStorage creates a new instance of persistentStorage with the provided path.
func newPersistentStorage(path string) *persistentStorage {
	return &persistentStorage{path: path}
}

func (p *persistentStorage) Open() error {
	file, err := os.OpenFile(p.path, os.O_RDWR|os.O_CREATE, 0777)
	if err != nil {
		return errors.WrapError(err, errFailedStorageOpen, p.path, err.Error())
	}
	p.file = file
	return nil
}

func (p *persistentStorage) Close() error {
	if p.file == nil {
		return nil
	}
	if err := p.file.Close(); err != nil {
		return errors.WrapError(err, errFailedStorageClose, p.path, err.Error())
	}
	return nil
}

func (p *persistentStorage) SetState(persistentState *PersistentState) error {
	if p.file == nil {
		return errors.WrapError(nil, errStorageNotOpen, p.path)
	}

	// Create a temporary file that will replace the file currently associated with storage.
	tmpFile, err := os.CreateTemp("", "raft-storage-tmp")
	if err != nil {
		return errors.WrapError(err, errFailedStorageCreateTempFile, err.Error())
	}

	// Write the new state to the temporary file.
	storageEncoder := storageEncoder{}
	if err := storageEncoder.encode(tmpFile, persistentState); err != nil {
		return errors.WrapError(err, errFailedPersistentStateEncode, err.Error())
	}
	if err := tmpFile.Sync(); err != nil {
		return errors.WrapError(err, errFailedStorageSync, err.Error())
	}

	// Perform atomic rename to swap the newly persisted state with the old.
	oldFile := p.file
	if err := os.Rename(tmpFile.Name(), oldFile.Name()); err != nil {
		return errors.WrapError(err, errFailedStorageRename, err.Error())
	}

	p.file = tmpFile
	// Close the previous file.
	oldFile.Close()

	return nil
}

func (p *persistentStorage) GetState() (PersistentState, error) {
	if p.file == nil {
		return PersistentState{}, errors.WrapError(nil, errStorageNotOpen, p.path)
	}

	// Read the contents of the file associated with the storage.
	reader := io.Reader(p.file)
	storageDecoder := storageDecoder{}
	persistentState, err := storageDecoder.decode(reader)

	if err != nil && err != io.EOF {
		return persistentState, errors.WrapError(err, errFailedPeristentStateDecode, err.Error())
	}

	return persistentState, nil
}
