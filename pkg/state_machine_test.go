package raft

import (
	"bytes"
	"encoding/gob"
	"sync"

	"github.com/jmsadair/raft/internal/errors"
)

type StateMachineMock struct {
	commands []*LogEntry
	mu       sync.Mutex
}

func NewStateMachineMock() *StateMachineMock {
	gob.Register(LogEntry{})
	return &StateMachineMock{commands: make([]*LogEntry, 0)}
}

func (s *StateMachineMock) Apply(entry *LogEntry) interface{} {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.commands = append(s.commands, entry)
	return len(s.commands)
}

func (s *StateMachineMock) Snapshot() (Snapshot, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	var data bytes.Buffer
	enc := gob.NewEncoder(&data)
	if err := enc.Encode(s.commands); err != nil {
		return Snapshot{}, errors.WrapError(err, "error saving snapshot: %s", err.Error())
	}

	var lastIncludedIndex uint64
	var lastIncludedTerm uint64
	if len(s.commands) == 0 {
		lastIncludedIndex = 0
		lastIncludedTerm = 0
	} else {
		lastIncludedIndex = s.commands[len(s.commands)-1].Index
		lastIncludedTerm = s.commands[len(s.commands)-1].Term
	}

	return Snapshot{LastIncludedIndex: lastIncludedIndex, LastIncludedTerm: lastIncludedTerm, Data: data.Bytes()}, nil
}

func (s *StateMachineMock) Restore(snapshot *Snapshot) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	var commands []*LogEntry
	data := bytes.NewBuffer(snapshot.Data)
	dec := gob.NewDecoder(data)
	if err := dec.Decode(&commands); err != nil {
		return errors.WrapError(err, "error restoring from snapshot: %s", err.Error())
	}

	s.commands = commands

	return nil
}
