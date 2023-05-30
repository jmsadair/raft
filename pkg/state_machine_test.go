package raft

import (
	"bytes"
	"encoding/gob"
	"sync"
	"testing"

	"github.com/jmsadair/raft/internal/errors"
	"github.com/stretchr/testify/assert"
)

type AppliedCommand struct {
	Command []byte
	Index   uint64
	Term    uint64
}

type StateMachineMock struct {
	commands []AppliedCommand
	mu       sync.Mutex
}

func NewStateMachineMock() *StateMachineMock {
	gob.Register(AppliedCommand{})
	return &StateMachineMock{commands: make([]AppliedCommand, 0)}
}

func (s *StateMachineMock) Apply(command []byte, index uint64, term uint64) interface{} {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.commands = append(s.commands, AppliedCommand{Command: command, Index: index, Term: term})
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
	var commands []AppliedCommand
	data := bytes.NewBuffer(snapshot)
	dec := gob.NewDecoder(data)
	if err := dec.Decode(&commands); err != nil {
		return errors.WrapError(err, "error restoring from snapshot: %s", err.Error())
	}
	s.commands = commands
	return nil
}

func validateCommand(t *testing.T, expected *AppliedCommand, actual *AppliedCommand) {
	assert.Equal(t, expected.Index, actual.Index, "index does not match")
	assert.Equal(t, expected.Term, actual.Term, "term does not match")
	assert.Equal(t, expected.Command, actual.Command, "command does not match")
}

func TestApply(t *testing.T) {
	command := []byte("command")
	fsm := NewStateMachineMock()
	assert.Equal(t, fsm.Apply(command, 1, 1), 1)
	assert.Equal(t, fsm.Apply(command, 2, 2), 2)
	assert.Equal(t, fsm.Apply(command, 3, 3), 3)
}

func TestSnapShotRestore(t *testing.T) {
	command1 := []byte("command1")
	command2 := []byte("command2")
	command3 := []byte("command3")
	fsm := NewStateMachineMock()
	fsm.Apply(command1, 1, 1)
	fsm.Apply(command2, 2, 1)
	fsm.Apply(command3, 3, 1)

	snapshot, err := fsm.Snapshot()
	assert.NoError(t, err)

	err = fsm.Restore(snapshot)
	assert.NoError(t, err)

	assert.Equal(t, len(fsm.commands), 3)
	validateCommand(t, &AppliedCommand{Command: command1, Index: 1, Term: 1}, &fsm.commands[0])
	validateCommand(t, &AppliedCommand{Command: command2, Index: 2, Term: 1}, &fsm.commands[1])
	validateCommand(t, &AppliedCommand{Command: command3, Index: 3, Term: 1}, &fsm.commands[2])
}
