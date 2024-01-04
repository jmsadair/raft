package raft

import (
	"io"
	"os"
	"path/filepath"

	"github.com/jmsadair/raft/internal/errors"
)

// StateStorage represents the component of Raft responsible for persistently storing
// term and vote.
type StateStorage interface {
	// Open opens the state storage and prepares it for reads and writes.
	Open() error

	// Close closes the state storage.
	Close() error

	// SetState persists the provided state. The storage must be open otherwise an
	// error is returned.
	SetState(term uint64, vote string) error

	// State returns the most recently persisted state in the storage. If there is
	// no pre-existing state or the storage is closed, zero and an empty string
	// will be returned. If the state storage is not open, an error will be returned.
	State() (uint64, string, error)
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
	// The directory where the state will be persisted.
	stateDir string

	// The file associated with the storage, nil if storage is closed.
	file *os.File

	// The most recently persisted state.
	state *persistentState
}

// NewStateStorage creates a new state storage.
// The file containing the state will be located at path/state/state.bin.
func NewStateStorage(path string) (StateStorage, error) {
	stateDir := filepath.Join(path, "state")
	if err := os.MkdirAll(stateDir, os.ModePerm); err != nil {
		return nil, err
	}
	return &persistentStateStorage{stateDir: stateDir}, nil
}

func (p *persistentStateStorage) Open() error {
	stateFilename := filepath.Join(p.stateDir, "state.bin")
	stateFile, err := os.OpenFile(stateFilename, os.O_RDWR|os.O_CREATE, 0o666)
	if err != nil {
		return errors.WrapError(err, "failed to open state storage")
	}
	p.file = stateFile
	return nil
}

func (p *persistentStateStorage) Close() error {
	if p.file == nil {
		return nil
	}
	if err := p.file.Close(); err != nil {
		return errors.WrapError(err, "failed to close state storage")
	}
	p.state = nil
	return nil
}

func (p *persistentStateStorage) SetState(term uint64, votedFor string) error {
	if p.file == nil {
		return errors.New("failed to write state, state storage is not open")
	}

	tmpFile, err := os.CreateTemp(p.stateDir, "tmp-")
	if err != nil {
		return errors.WrapError(err, "failed to write state")
	}

	p.state = &persistentState{term: term, votedFor: votedFor}
	if err := encodePersistentState(tmpFile, p.state); err != nil {
		return errors.WrapError(err, "failed to write state")
	}
	if err := tmpFile.Sync(); err != nil {
		return errors.WrapError(err, "failed to write state")
	}

	if err := tmpFile.Close(); err != nil {
		return errors.WrapError(err, "failed to write state")
	}
	if err := p.file.Close(); err != nil {
		return errors.WrapError(err, "failed to write state")
	}
	if err := os.Rename(tmpFile.Name(), p.file.Name()); err != nil {
		return errors.WrapError(err, "failed to write state")
	}

	return nil
}

func (p *persistentStateStorage) State() (uint64, string, error) {
	if p.file == nil {
		return 0, "", errors.New("failed to read state, state storage is not open")
	}

	if p.state == nil {
		reader := io.Reader(p.file)
		state, err := decodePersistentState(reader)
		if err != nil && err != io.EOF {
			return 0, "", errors.WrapError(err, "failed to read state")
		}
		p.state = &state
	}

	return p.state.term, p.state.votedFor, nil
}
