package raft

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestProtoLogEncoderDecoder(t *testing.T) {
	entry := NewLogEntry(1, 1, []byte("test"))
	buf := new(bytes.Buffer)

	encoder := NewProtoLogEncoder()
	if err := encoder.Encode(buf, entry); err != nil {
		t.Fatalf("failed to encode log entry: %s", err.Error())
	}

	decoder := NewProtoLogDecoder()
	decodedEntry, err := decoder.Decode(buf)
	if err != nil {
		t.Fatalf("failed to decode log entry: %s", err.Error())
	}

	assert.Equal(t, entry.index, decodedEntry.index, "decoded log entry has incorrect index")
	assert.Equal(t, entry.term, decodedEntry.term, "decoded log entry has incorrect term")
	assert.Equal(t, entry.data, decodedEntry.data, "decoded log entry has incorrect data")
}

func TestProtoStorageEncoderDecoder(t *testing.T) {
	persistentState := PersistentState{term: 1, votedFor: "test"}
	buf := new(bytes.Buffer)

	encoder := NewProtoStorageEncoder()
	if err := encoder.Encode(buf, &persistentState); err != nil {
		t.Fatalf("failed to encode persistent state : %s", err.Error())
	}

	decoder := NewProtoStorageDecoder()
	decodedState, err := decoder.Decode(buf)
	if err != nil {
		t.Fatalf("failed to decode persistent state: %s", err.Error())
	}

	assert.Equal(t, persistentState.term, decodedState.term, "decoded state has incorrect term")
	assert.Equal(t, persistentState.votedFor, decodedState.votedFor, "decoded state has incorrect votedFor")
}
