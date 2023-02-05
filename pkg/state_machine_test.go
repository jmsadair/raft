package raft

import (
	"bytes"
	"encoding/gob"
	"sync"
	"testing"

	"github.com/jmsadair/raft/internal/errors"
	"github.com/stretchr/testify/assert"
)

type StateMachineMock struct {
	commands [][]byte
	mu       sync.Mutex
}

func NewStateMachineMock() *StateMachineMock {
	return &StateMachineMock{commands: make([][]byte, 0)}
}

func (s *StateMachineMock) Apply(command []byte) interface{} {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.commands = append(s.commands, command)
	return len(s.commands)
}

func (s *StateMachineMock) Snapshot() ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var data bytes.Buffer
	enc := gob.NewEncoder(&data)
	if err := enc.Encode(s.commands); err != nil {
		return nil, errors.WrapError(err, "error saving snapshot: %s", err.Error())
	}
	return data.Bytes(), nil
}

func (s *StateMachineMock) Restore(snapshot []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	var commands [][]byte
	data := bytes.NewBuffer(snapshot)
	dec := gob.NewDecoder(data)
	if err := dec.Decode(&commands); err != nil {
		return errors.WrapError(err, "error restoring from snapshot: %s", err.Error())
	}
	s.commands = commands
	return nil
}

func TestApply(t *testing.T) {
	command := []byte("command")
	fsm := NewStateMachineMock()
	assert.Equal(t, fsm.Apply(command), 1)
	assert.Equal(t, fsm.Apply(command), 2)
	assert.Equal(t, fsm.Apply(command), 3)
}

func TestSnapShotRestore(t *testing.T) {
	command1 := []byte("command1")
	command2 := []byte("command2")
	command3 := []byte("command3")
	fsm := NewStateMachineMock()
	fsm.Apply(command1)
	fsm.Apply(command2)
	fsm.Apply(command3)

	snapshot, err := fsm.Snapshot()
	assert.NoError(t, err)

	err = fsm.Restore(snapshot)
	assert.NoError(t, err)

	assert.Equal(t, len(fsm.commands), 3)
	assert.Equal(t, fsm.commands[0], command1)
	assert.Equal(t, fsm.commands[1], command2)
	assert.Equal(t, fsm.commands[2], command3)
}
