package raft

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/jmsadair/raft/internal/fileutil"
	pb "github.com/jmsadair/raft/internal/protobuf"
	"google.golang.org/protobuf/proto"
)

const (
	stateBase    = "state.bin"
	stateDirBase = "state"
)

// StateStorage represents the component of Raft responsible for persistently storing term and vote.
type StateStorage interface {
	// SetState persists the provided term and vote.
	SetState(term uint64, vote string) error

	// State returns the most recently persisted term and vote in the storage.
	// If there is no pre-existing state, zero and an empty string will be returned.
	State() (uint64, string, error)
}

// persistentState is the state that must be persisted in Raft.
type persistentState struct {
	// The term of the associated Raft instance.
	term uint64

	// The vote of the associated Raft instance.
	votedFor string
}

func encodePersistentState(w io.Writer, state *persistentState) error {
	pbState := &pb.StorageState{Term: state.term, VotedFor: state.votedFor}
	buf, err := proto.Marshal(pbState)
	if err != nil {
		return fmt.Errorf("could not marshal protobuf message: %w", err)
	}
	size := int32(len(buf))
	if err := binary.Write(w, binary.BigEndian, size); err != nil {
		return fmt.Errorf("could not write length of protobuf message: %w", err)
	}
	if _, err := w.Write(buf); err != nil {
		return fmt.Errorf("could not write protobuf message: %w", err)
	}
	return nil
}

func decodePersistentState(r io.Reader) (persistentState, error) {
	var size int32
	if err := binary.Read(r, binary.BigEndian, &size); err != nil {
		return persistentState{}, fmt.Errorf("could not read length of protobuf message: %w", err)
	}

	buf := make([]byte, size)
	if _, err := io.ReadFull(r, buf); err != nil {
		return persistentState{}, fmt.Errorf("could not read protobuf message: %w", err)
	}

	pbState := &pb.StorageState{}
	if err := proto.Unmarshal(buf, pbState); err != nil {
		return persistentState{}, fmt.Errorf("could not unmarshal protobuf message: %w", err)
	}

	state := persistentState{
		term:     pbState.GetTerm(),
		votedFor: pbState.GetVotedFor(),
	}

	return state, nil
}

// persistentStateStorage implements the StateStorage interface.
// This implementation is not concurrent safe.
type persistentStateStorage struct {
	// The directory where the state will be persisted.
	stateDir string

	// The most recently persisted state.
	state *persistentState
}

// NewStateStorage creates a new instance of a StateStorage.
//
// The file containing the state will be located at path/state/state.bin.
// Any directories on path that do not exist will be created.
func NewStateStorage(path string) (StateStorage, error) {
	stateDir := filepath.Join(path, stateDirBase)
	if err := os.MkdirAll(stateDir, os.ModePerm); err != nil {
		return nil, fmt.Errorf("could not create state directory: %w", err)
	}

	// Delete any temporary files or directories that may have been partially written before a crash.
	if err := fileutil.RemoveTmpFiles(stateDir); err != nil {
		return nil, fmt.Errorf("could not remove temporary files: %w", err)
	}

	return &persistentStateStorage{stateDir: stateDir}, nil
}

func (p *persistentStateStorage) SetState(term uint64, votedFor string) error {
	tmpFile, err := os.CreateTemp(p.stateDir, "tmp-state")
	if err != nil {
		return fmt.Errorf("could not create temporary file: %w", err)
	}

	// Remove the temporary file if the rename is not successful.
	success := false
	defer func() {
		if !success {
			_ = os.Remove(tmpFile.Name())
		}
	}()

	// Write the state to the temporary file and perform the rename.
	p.state = &persistentState{term: term, votedFor: votedFor}
	if err := encodePersistentState(tmpFile, p.state); err != nil {
		return fmt.Errorf("could not encode state: %w", err)
	}
	filename := filepath.Join(p.stateDir, stateBase)
	if err := os.Rename(tmpFile.Name(), filename); err != nil {
		return fmt.Errorf("could not rename temporary file: %w", err)
	}

	success = true

	return nil
}

func (p *persistentStateStorage) State() (uint64, string, error) {
	if p.state == nil {
		filename := filepath.Join(p.stateDir, stateBase)
		if _, err := os.Stat(filename); err == nil {
			data, err := os.ReadFile(filename)
			if err != nil {
				return 0, "", fmt.Errorf("could not read state file: %w", err)
			}
			reader := bytes.NewReader(data)
			state, err := decodePersistentState(reader)
			if err != nil && err != io.EOF {
				return 0, "", fmt.Errorf("could not decode state: %w", err)
			}
			p.state = &state
		} else if errors.Is(err, os.ErrNotExist) {
			return 0, "", nil
		} else {
			return 0, "", fmt.Errorf("could not stat state file: %w", err)
		}
	}

	return p.state.term, p.state.votedFor, nil
}
