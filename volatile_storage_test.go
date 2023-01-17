package raft

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSetAndGet(t *testing.T) {
	storage := NewVolatileStorage()

	key := "votedFor"
	value := "candidateId"
	err := storage.Set([]byte(key), []byte(value))
	assert.NoError(t, err)

	valueBytes, err := storage.Get([]byte(key))
	assert.NoError(t, err)
	assert.Equal(t, string(valueBytes), value)
}
