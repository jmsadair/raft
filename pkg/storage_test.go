package raft

import (
	"bytes"
	"encoding/gob"
	"sync"
	"testing"

	"github.com/jmsadair/raft/internal/errors"
	"github.com/stretchr/testify/assert"
)

// StorageMock implements the Storage Interface.
// It is completely in-memory and should only be used
// for testing purposes.
type StorageMock struct {
	store map[string][]byte
	mu    sync.Mutex
}

func NewStorageMock() *StorageMock {
	return &StorageMock{store: make(map[string][]byte)}
}

func (vs *StorageMock) Set(key, value []byte) error {
	vs.mu.Lock()
	defer vs.mu.Unlock()

	vs.store[string(key)] = value
	return nil
}

func (vs *StorageMock) Get(key []byte) ([]byte, error) {
	vs.mu.Lock()
	defer vs.mu.Unlock()

	value, ok := vs.store[string(key)]
	if !ok {
		return []byte{}, nil
	}
	return value, nil
}

func (vs *StorageMock) SetUint64(key []byte, value uint64) error {
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

func (vs *StorageMock) GetUint64(key []byte) (uint64, error) {
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

func TestSetAndGet(t *testing.T) {
	storage := NewStorageMock()

	key := "votedFor"
	value := "candidateId"
	err := storage.Set([]byte(key), []byte(value))
	assert.NoError(t, err)

	valueBytes, err := storage.Get([]byte(key))
	assert.NoError(t, err)
	assert.Equal(t, string(valueBytes), value)
}
