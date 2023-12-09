package raft

import (
	"io"
	"os"

	"github.com/jmsadair/raft/internal/errors"
)

var (
	errStateStorageNotOpen = errors.New("state storage is not open")
)

// StateStorage represents the component of Raft responsible for persistently storing
// term and vote.
type StateStorage interface {
	PersistentStorage

	// SetState persists the provided state. The storage must be open otherwise an
	// error is returned.
	SetState(term uint64, vote string) error

	// State returns the most recently persisted state in the storage. If there is
	// no pre-existing state or the storage is closed, zero and an empty string
	// will be returned.
	State() (uint64, string)
}

// persistentState is the state that must be persisted in Raft.
type persistentState struct {
	// The term of the associated Raft instance.
	term uint64

	// The vote of the associated Raft instance.
	votedFor string
}

// persistentStateStorage implements the StateStorage interface.
// This implementation is not concurrent safe.
type persistentStateStorage struct {
	// The path to the file where the storage is persisted.
	path string

	// The file associated with the storage, nil if storage is closed.
	file *os.File

	// The most recently persisted state.
	state persistentState
}

// NewStateStorage creates a new StateStorage at the provided path.
func NewStateStorage(path string) StateStorage {
	return &persistentStateStorage{path: path}
}

func (p *persistentStateStorage) Open() error {
	file, err := os.OpenFile(p.path, os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		return errors.WrapError(err, "failed to open storage file: path = %s", p.path)
	}
	p.file = file
	return nil
}

func (p *persistentStateStorage) Close() error {
	if p.file == nil {
		return nil
	}
	if err := p.file.Close(); err != nil {
		return errors.WrapError(err, "failed to close storage file: path = %s", p.path)
	}
	p.state = persistentState{}
	return nil
}

func (p *persistentStateStorage) Replay() error {
	if p.file == nil {
		return errStateStorageNotOpen
	}

	// Read the contents of the file associated with the storage.
	reader := io.Reader(p.file)
	state, err := decodePersistentState(reader)

	if err != nil && err != io.EOF {
		return errors.WrapError(err, "failed to decode persistent state")
	}

	p.state = state

	return nil
}

func (p *persistentStateStorage) SetState(term uint64, vote string) error {
	if p.file == nil {
		return errStateStorageNotOpen
	}

	// Create a temporary file that will replace the file currently associated with storage.
	// Note that it is NOT safe to truncate the file and then write the new state - it must
	// be atomic.
	tmpFile, err := os.Create(p.path + ".tmp")
	if err != nil {
		return errors.WrapError(err, "failed to create temporary storage file")
	}

	// Write the new state to the temporary file.
	p.state = persistentState{term: term, votedFor: vote}
	if err := encodePersistentState(tmpFile, &p.state); err != nil {
		return errors.WrapError(err, "failed to encode state")
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

func (p *persistentStateStorage) State() (uint64, string) {
	return p.state.term, p.state.votedFor
}
