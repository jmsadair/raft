package raft

import (
	"bytes"
	"io"
	"os"
	"path/filepath"

	"github.com/jmsadair/raft/internal/errors"
)

// StateStorage represents the component of Raft responsible for persistently storing
// term and vote.
type StateStorage interface {
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

func (p *persistentStateStorage) SetState(term uint64, votedFor string) error {
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

	filename := filepath.Join(p.stateDir, "state.bin")
	if err := os.Rename(tmpFile.Name(), filename); err != nil {
		return errors.WrapError(err, "failed to write state")
	}

	return nil
}

func (p *persistentStateStorage) State() (uint64, string, error) {
	if p.state == nil {
		filename := filepath.Join(p.stateDir, "state.bin")
		if _, err := os.Stat(filename); err == nil {
			data, err := os.ReadFile(filename)
			if err != nil {
				return 0, "", errors.WrapError(err, "failed to read state")
			}
			reader := bytes.NewReader(data)
			state, err := decodePersistentState(reader)
			if err != nil && err != io.EOF {
				return 0, "", errors.WrapError(err, "failed to decode state")
			}
			p.state = &state
		} else if os.IsNotExist(err) {
			return 0, "", nil
		} else {
			return 0, "", errors.WrapError(err, "failed to stat state file")
		}
	}

	return p.state.term, p.state.votedFor, nil
}
