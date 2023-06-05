package raft

import (
	"io"
	"os"

	"github.com/jmsadair/raft/internal/errors"
)

const (
	errStorageNotOpen              = "storage is not open: path = %s"
	errFailedStorageOpen           = "failed to open storage file: path = %s, err = %s"
	errFailedPersistentStateEncode = "storage failed to encode persistent state: %s"
	errFailedPeristentStateDecode  = "storage failed to decode persistent state: %s"
	errFailedStorageSync           = "failed to sync storage file: %s"
	errFailedStorageFlush          = "failed flushing data from storage file writer: %s"
	errFailedStorageClose          = "failed to close storage file: path = %s, err = %s"
	errFailedStorageCreateTempFile = "failed to create temporary storage file: %s"
	errFailedStorageRename         = "failed to rename temporary storage file: %s"
)

// Storage is an interface representing a storage object that can persist state.
type Storage interface {
	// Open opens the storage for reading and writing persisting state.
	Open() error

	// Close closes the storage. After Close is called, SetState and GetState should not be called.
	Close() error

	// SetState persists the provided state in the storage. Storage must be open.
	SetState(persistentState *PersistentState) error

	// GetState recovers the state from the storage. Storage must be open.
	GetState() (PersistentState, error)
}

// PersistentState contains the state that must be persisted
// in the Raft protocol that is not a part of the log.
type PersistentState struct {
	Term     uint64
	VotedFor string
}

// persistentStorage is a type that implements the Storage interface and persists
// state to a file using an encoder and decoder. Not concurrent safe.
type persistentStorage struct {
	// The path to the file where the storage is persisted.
	path string

	// The file associated with the storage, nil if storage is closed.
	file *os.File
}

// newPersistentStorage creates a new instance of PersistentStorage with the provided path, storage encoder,
// and storage decoder.
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
	// Create a temporary file that will replace the file currently associated with storage.
	tmpFile, err := os.CreateTemp("", "persistent-storage")
	if err != nil {
		return errors.WrapError(err, errFailedStorageCreateTempFile, err.Error())
	}

	// Write the new state to the temporary file.
	writer := io.Writer(tmpFile)
	storageEncoder := storageEncoder{}
	if err := storageEncoder.encode(writer, persistentState); err != nil {
		return errors.WrapError(err, errFailedPersistentStateEncode, err.Error())
	}
	if err := tmpFile.Sync(); err != nil {
		return errors.WrapError(err, errFailedStorageSync, err.Error())
	}

	// Perform atomic rename to swap the newly persisted state with the old.
	oldFile := p.file
	if err := os.Rename(tmpFile.Name(), p.path); err != nil {
		return errors.WrapError(err, errFailedStorageRename, err.Error())
	}

	p.file = tmpFile

	// Close the previous file.
	oldFile.Close()

	return nil
}

func (p *persistentStorage) GetState() (PersistentState, error) {
	// Read the contents of the file associated with the storage.
	reader := io.Reader(p.file)
	storageDecoder := storageDecoder{}
	persistentState, err := storageDecoder.decode(reader)

	if err != nil && err != io.EOF {
		return persistentState, errors.WrapError(err, errFailedPeristentStateDecode, err.Error())
	}

	return persistentState, nil
}
