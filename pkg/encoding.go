package raft

import (
	"encoding/binary"
	"io"

	pb "github.com/jmsadair/raft/internal/protobuf"
	"google.golang.org/protobuf/proto"
)

// StorageEncoder is an interface representing a component responsible for encoding a PersistentState
// instance into a binary format that can be stored in a file or transmitted over a network.
type StorageEncoder interface {
	// Encode encodes a PersistentState instance into a binary format and writes it to the provided io.Writer.
	//
	// Parameters:
	//     - w: The io.Writer to write the encoded data to.
	//     - persistentState: The PersistentState instance to encode.
	//
	// Returns:
	//     - error: An error if encoding fails, or nil otherwise.
	Encode(w io.Writer, persistentState *PersistentState) error
}

// ProtoStorageEncoder is an implementation of StorageEncoder that encodes a PersistentState instance
// into a binary format using Protocol Buffers.
type ProtoStorageEncoder struct{}

func (p *ProtoStorageEncoder) Encode(w io.Writer, persistentState *PersistentState) error {
	pbState := &pb.StorageState{Term: persistentState.term, VotedFor: persistentState.votedFor}
	buf, err := proto.Marshal(pbState)
	if err != nil {
		return err
	}
	size := int32(len(buf))
	if err := binary.Write(w, binary.BigEndian, size); err != nil {
		return err
	}
	if _, err := w.Write(buf); err != nil {
		return err
	}
	return nil
}

// StorageDecoder is an interface representing a component responsible for decoding a binary
// representation of a PersistentState instance into an actual PersistentState instance.
type StorageDecoder interface {
	// Decode reads a binary representation of a PersistentState instance from the provided io.Reader,
	// decodes it, and returns the decoded PersistentState instance.
	//
	// Parameters:
	//     - r: The io.Reader to read the binary representation from.
	//
	// Returns:
	//     - PersistentState: The decoded PersistentState instance.
	//     - error: An error if decoding fails, or nil otherwise.
	Decode(r io.Reader) (PersistentState, error)
}

// ProtoStorageDecoder is an implementation of StorageDecoder that decodes a binary representation
// of a PersistentState instance encoded using Protocol Buffers into an actual PersistentState instance.
type ProtoStorageDecoder struct{}

func (p *ProtoStorageDecoder) Decode(r io.Reader) (PersistentState, error) {
	var size int32
	if err := binary.Read(r, binary.BigEndian, &size); err != nil {
		return PersistentState{}, err
	}

	buf := make([]byte, size)
	if _, err := io.ReadFull(r, buf); err != nil {
		return PersistentState{}, err
	}

	pbPersistentState := &pb.StorageState{}
	if err := proto.Unmarshal(buf, pbPersistentState); err != nil {
		return PersistentState{}, err
	}

	persistentState := PersistentState{
		term:     pbPersistentState.GetTerm(),
		votedFor: pbPersistentState.GetVotedFor(),
	}

	return persistentState, nil
}

// LogEncoder is an interface representing a component responsible for encoding a LogEntry
// instance into a binary format that can be stored in a file or transmitted over a network.
type LogEncoder interface {
	// Encode encodes a LogEntry instance into a binary format and writes it to the provided io.Writer.
	//
	// Parameters:
	//     - w: The io.Writer to write the encoded data to.
	//     - entry: The LogEntry instance to encode.
	//
	// Returns:
	//     - error: An error if encoding fails, or nil otherwise.
	Encode(w io.Writer, entry *LogEntry) error
}

// ProtoLogEncoder is an implementation of StorageEncoder that encodes a LogEntry instance
// into a binary format using Protocol Buffers.
type ProtoLogEncoder struct{}

func (p *ProtoLogEncoder) Encode(w io.Writer, entry *LogEntry) error {
	pbEntry := &pb.LogEntry{
		Index:  entry.index,
		Term:   entry.term,
		Data:   entry.data,
		Offset: entry.offset,
	}

	buf, err := proto.Marshal(pbEntry)
	if err != nil {
		return err
	}

	size := int32(len(buf))
	if err := binary.Write(w, binary.BigEndian, size); err != nil {
		return err
	}

	if _, err := w.Write(buf); err != nil {
		return err
	}

	return nil
}

// LogDecoder is an interface representing a component responsible for decoding a binary
// representation of a LogEntry instance into an actual LogEntry instance.
type LogDecoder interface {
	// Decode reads a binary representation of a LogEntry instance from the provided io.Reader,
	// decodes it, and returns the decoded LogEntry instance.
	//
	// Parameters:
	//     - r: The io.Reader to read the binary representation from.
	//
	// Returns:
	//     - LogEntry: The decoded LogEntry instance.
	//     - error: An error if decoding fails, or nil otherwise.
	Decode(r io.Reader) (LogEntry, error)
}

// ProtoLogDecoder is an implementation of LogDecoder that decodes a binary representation
// of a LogEntry instance encoded using Protocol Buffers into an actual LogEntry instance.
type ProtoLogDecoder struct{}

func (p *ProtoLogDecoder) Decode(r io.Reader) (LogEntry, error) {
	var size int32
	if err := binary.Read(r, binary.BigEndian, &size); err != nil {
		return LogEntry{}, err
	}

	buf := make([]byte, size)
	if _, err := io.ReadFull(r, buf); err != nil {
		return LogEntry{}, err
	}

	pbEntry := &pb.LogEntry{}
	if err := proto.Unmarshal(buf, pbEntry); err != nil {
		return LogEntry{}, err
	}

	entry := LogEntry{
		index:  pbEntry.GetIndex(),
		term:   pbEntry.GetTerm(),
		data:   pbEntry.GetData(),
		offset: pbEntry.GetOffset(),
	}

	return entry, nil
}
