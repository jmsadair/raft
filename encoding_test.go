package raft

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestProtoLogEncoderDecoder(t *testing.T) {
	entry := NewLogEntry(1, 1, []byte("test"))
	buf := new(bytes.Buffer)

	encoder := new(logEncoder)
	if err := encoder.encode(buf, entry); err != nil {
		t.Fatalf("failed to encode log entry: %s", err.Error())
	}

	decoder := new(logDecoder)
	decodedEntry, err := decoder.decode(buf)
	if err != nil {
		t.Fatalf("failed to decode log entry: %s", err.Error())
	}

	assert.Equal(t, entry.Index, decodedEntry.Index, "decoded log entry has incorrect index")
	assert.Equal(t, entry.Term, decodedEntry.Term, "decoded log entry has incorrect term")
	assert.Equal(t, entry.Data, decodedEntry.Data, "decoded log entry has incorrect data")
}

func TestProtoStorageEncoderDecoder(t *testing.T) {
	persistentState := PersistentState{Term: 1, VotedFor: "test"}
	buf := new(bytes.Buffer)

	encoder := new(storageEncoder)
	if err := encoder.encode(buf, &persistentState); err != nil {
		t.Fatalf("failed to encode persistent state : %s", err.Error())
	}

	decoder := new(storageDecoder)
	decodedState, err := decoder.decode(buf)
	if err != nil {
		t.Fatalf("failed to decode persistent state: %s", err.Error())
	}

	assert.Equal(t, persistentState.Term, decodedState.Term, "decoded state has incorrect term")
	assert.Equal(t, persistentState.VotedFor, decodedState.VotedFor, "decoded state has incorrect votedFor")
}

func TestProtoSnapshotEncoderDecoder(t *testing.T) {
	snapshot := Snapshot{LastIncludedIndex: 1, LastIncludedTerm: 1, Data: []byte("test")}
	buf := new(bytes.Buffer)

	encoder := new(snapshotEncoder)
	if err := encoder.encode(buf, &snapshot); err != nil {
		t.Fatalf("failed to encode persistent state : %s", err.Error())
	}

	decoder := new(snapshotDecoder)
	decodedSnapshot, err := decoder.decode(buf)
	if err != nil {
		t.Fatalf("failed to decode persistent state: %s", err.Error())
	}

	assert.Equal(t, snapshot.LastIncludedIndex, decodedSnapshot.LastIncludedIndex, "decoded snapshot has incorrect last included term")
	assert.Equal(t, snapshot.LastIncludedTerm, decodedSnapshot.LastIncludedTerm, "decoded snapshot has incorrect last included index")
	assert.Equal(t, snapshot.Data, decodedSnapshot.Data, "decoded snapshot has incorrect data")
}
