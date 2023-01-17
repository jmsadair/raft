package raft

import (
	"fmt"
	"sync"
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
		return nil, fmt.Errorf("failed to get value: key does not exist")
	}
	return value, nil
}
