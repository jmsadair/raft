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

// PersistentStorage is a type that implements the Storage interface and persists
// state to a file using an encoder and decoder.
type PersistentStorage struct {
	// The encoder used to encode the persistent state associated with the storage.
	storageEncoder StorageEncoder

	// The decoder used to decode the persistent state associated with the storage.
	storageDecoder StorageDecoder

	// The path to the file where the storage is persisted.
	path string

	// The file associated with the storage, nil if storage is closed.
	file *os.File
}

// PersistentState is a simple struct that contains the persisted state.
type PersistentState struct {
	term     uint64
	votedFor string
}

// NewPersistentStorage creates a new instance of PersistentStorage with the provided path, storage encoder,
// and storage decoder.
func NewPersistentStorage(path string, storageEncoder StorageEncoder, storageDecoder StorageDecoder) *PersistentStorage {
	return &PersistentStorage{path: path, storageEncoder: storageEncoder, storageDecoder: storageDecoder}
}

func (p *PersistentStorage) Open() error {
	file, err := os.OpenFile(p.path, os.O_RDWR|os.O_CREATE, 0777)
	if err != nil {
		return errors.WrapError(err, errFailedStorageOpen, p.path, err.Error())
	}
	p.file = file
	return nil
}

func (p *PersistentStorage) Close() error {
	if p.file == nil {
		return nil
	}
	if err := p.file.Close(); err != nil {
		return errors.WrapError(err, errFailedStorageClose, p.path, err.Error())
	}
	return nil
}

func (p *PersistentStorage) SetState(persistentState *PersistentState) error {
	// Create a temporary file that will replace the file currently associated with storage.
	tmpFile, err := os.CreateTemp("", "persistent-storage")
	if err != nil {
		return errors.WrapError(err, errFailedStorageCreateTempFile, err.Error())
	}

	// Write the new state to the temporary file.
	writer := io.Writer(tmpFile)
	if err := p.storageEncoder.Encode(writer, persistentState); err != nil {
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

func (p *PersistentStorage) GetState() (PersistentState, error) {
	// Read the contents of the file associated with the storage.
	reader := io.Reader(p.file)
	persistentState, err := p.storageDecoder.Decode(reader)

	if err != nil && err != io.EOF {
		return persistentState, errors.WrapError(err, errFailedPeristentStateDecode, err.Error())
	}

	return persistentState, nil
}
