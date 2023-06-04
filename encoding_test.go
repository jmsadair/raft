package raft

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestProtoLogEncoderDecoder(t *testing.T) {
	entry := NewLogEntry(1, 1, []byte("test"))
	buf := new(bytes.Buffer)

	encoder := new(ProtoLogEncoder)
	if err := encoder.Encode(buf, entry); err != nil {
		t.Fatalf("failed to encode log entry: %s", err.Error())
	}

	decoder := new(ProtoLogDecoder)
	decodedEntry, err := decoder.Decode(buf)
	if err != nil {
		t.Fatalf("failed to decode log entry: %s", err.Error())
	}

	assert.Equal(t, entry.Index, decodedEntry.Index, "decoded log entry has incorrect index")
	assert.Equal(t, entry.Term, decodedEntry.Term, "decoded log entry has incorrect term")
	assert.Equal(t, entry.Data, decodedEntry.Data, "decoded log entry has incorrect data")
}

func TestProtoStorageEncoderDecoder(t *testing.T) {
	persistentState := PersistentState{term: 1, votedFor: "test"}
	buf := new(bytes.Buffer)

	encoder := new(ProtoStorageEncoder)
	if err := encoder.Encode(buf, &persistentState); err != nil {
		t.Fatalf("failed to encode persistent state : %s", err.Error())
	}

	decoder := new(ProtoStorageDecoder)
	decodedState, err := decoder.Decode(buf)
	if err != nil {
		t.Fatalf("failed to decode persistent state: %s", err.Error())
	}

	assert.Equal(t, persistentState.term, decodedState.term, "decoded state has incorrect term")
	assert.Equal(t, persistentState.votedFor, decodedState.votedFor, "decoded state has incorrect votedFor")
}

func TestProtoSnapshotEncoderDecoder(t *testing.T) {
	snapshot := Snapshot{LastIncludedIndex: 1, LastIncludedTerm: 1, Data: []byte("test")}
	buf := new(bytes.Buffer)

	encoder := new(ProtoSnapshotEncoder)
	if err := encoder.Encode(buf, &snapshot); err != nil {
		t.Fatalf("failed to encode persistent state : %s", err.Error())
	}

	decoder := new(ProtoSnapshotDecoder)
	decodedSnapshot, err := decoder.Decode(buf)
	if err != nil {
		t.Fatalf("failed to decode persistent state: %s", err.Error())
	}

	assert.Equal(t, snapshot.LastIncludedIndex, decodedSnapshot.LastIncludedIndex, "decoded snapshot has incorrect last included term")
	assert.Equal(t, snapshot.LastIncludedTerm, decodedSnapshot.LastIncludedTerm, "decoded snapshot has incorrect last included index")
	assert.Equal(t, snapshot.Data, decodedSnapshot.Data, "decoded snapshot has incorrect data")
}
