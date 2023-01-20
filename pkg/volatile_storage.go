package raft

import (
	"bytes"
	"encoding/gob"
	"sync"

	"github.com/jmsadair/raft/internal/errors"
)

// VolatileStorage implements the Storage Interface.
// It is completely in-memory and should only be used
// for testing purposes.
type VolatileStorage struct {
	store map[string][]byte
	mu    sync.Mutex
}

func NewVolatileStorage() *VolatileStorage {
	return &VolatileStorage{store: make(map[string][]byte)}
}

func (vs *VolatileStorage) Set(key, value []byte) error {
	vs.mu.Lock()
	defer vs.mu.Unlock()

	vs.store[string(key)] = value
	return nil
}

func (vs *VolatileStorage) Get(key []byte) ([]byte, error) {
	vs.mu.Lock()
	defer vs.mu.Unlock()

	value, ok := vs.store[string(key)]
	if !ok {
		return []byte{}, nil
	}
	return value, nil
}

func (vs *VolatileStorage) SetUint64(key []byte, value uint64) error {
	vs.mu.Lock()
	defer vs.mu.Unlock()

	var data bytes.Buffer
	enc := gob.NewEncoder(&data)
	if err := enc.Encode(value); err != nil {
		return errors.WrapError(err, "error setting uint64 value: %s", err.Error())
	}
	vs.store[string(key)] = data.Bytes()
	return nil
}

func (vs *VolatileStorage) GetUint64(key []byte) (uint64, error) {
	vs.mu.Lock()
	defer vs.mu.Unlock()

	encodedValue, ok := vs.store[string(key)]
	if !ok {
		return 0, nil
	}
	var value uint64
	data := bytes.NewBuffer(encodedValue)
	dec := gob.NewDecoder(data)
	if err := dec.Decode(&value); err != nil {
		return 0, errors.WrapError(err, "error getting uint64 value: %s", err.Error())
	}
	return value, nil
}
