package raft

import (
	"errors"
	"io"
	"io/ioutil"
	"os"
)

// Storage defines the interface for a storage object that can persist state.
type Storage interface {
	// Open opens the storage for persisting state. Open must be called
	// before SetState or GetState are called.
	Open() error

	// Close closes the storage. After Close is called, SetState and GetState
	// should not be called.
	Close() error

	// SetState persists the provided state in storage.
	SetState(persistentState *PersistentState) error

	// GetState recovers the state from storage.
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

func NewPersistentStorage(path string, storageEncoder StorageEncoder, storageDecoder StorageDecoder) *PersistentStorage {
	return &PersistentStorage{path: path, storageEncoder: storageEncoder, storageDecoder: storageDecoder}
}

func (p *PersistentStorage) Open() error {
	file, err := os.OpenFile(p.path, os.O_RDWR|os.O_CREATE, 0777)
	if err != nil {
		return err
	}
	p.file = file
	return nil
}

func (p *PersistentStorage) Close() error {
	if p.file == nil {
		return errors.New("storage already closed")
	}
	return p.file.Close()
}

func (p *PersistentStorage) SetState(persistentState *PersistentState) error {
	// Create a temporary file that will replace the file currently associated with storage.
	tmpFile, err := ioutil.TempFile("", "persistent-storage")
	if err != nil {
		return err
	}

	// Write the new state to the temporary file.
	writer := io.Writer(tmpFile)
	if err := p.storageEncoder.Encode(writer, persistentState); err != nil {
		return err
	}
	tmpFile.Sync()

	// Perform atomic rename to swap the newly persisted state with the old.
	oldFile := p.file
	if err := os.Rename(tmpFile.Name(), p.path); err != nil {
		return err
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
		return persistentState, err
	}

	return persistentState, nil
}
