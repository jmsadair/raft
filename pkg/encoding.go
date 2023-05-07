package raft

import (
	"encoding/binary"
	"io"

	pb "github.com/jmsadair/raft/internal/protobuf"
	"google.golang.org/protobuf/proto"
)

type StorageEncoder interface {
	Encode(w io.Writer, persistentState *PersistentState) error
}

type ProtoStorageEncoder struct{}

func NewProtoStorageEncoder() *ProtoStorageEncoder {
	return &ProtoStorageEncoder{}
}

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

type StorageDecoder interface {
	Decode(r io.Reader) (PersistentState, error)
}

type ProtoStorageDecoder struct{}

func NewProtoStorageDecoder() *ProtoStorageDecoder {
	return &ProtoStorageDecoder{}
}

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

// LogEncoder supports the encoding of log entries.
type LogEncoder interface {
	// Encode encodes the given log entry using the provided io.Writer.
	// The encoded data is written to the writer in a format that is
	// specific to the implementation of the LogEncoder.
	Encode(w io.Writer, entry *LogEntry) error
}

// ProtoLogEncoder implements the LogEncoder interface and supports
// encoding log entries with protobuf.
type ProtoLogEncoder struct{}

// NewProtoLogEncoder returns a new ProtoLogEncoder.
func NewProtoLogEncoder() *ProtoLogEncoder {
	return &ProtoLogEncoder{}
}

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

// LogDecoder supports the decoding of log entries.
type LogDecoder interface {
	// Decode decodes the given data into a LogEntry object using the provided io.Reader.
	// The format of the data that is expected by the decoder is specific to the implementation
	// of the LogDecoder.
	Decode(r io.Reader) (LogEntry, error)
}

// ProtoLogDecoder implements the LogDecoder interface and supports
// decoding log entries with protobuf.
type ProtoLogDecoder struct{}

// NewProtoLogDecoder returns a new ProtoLogDecoder.
func NewProtoLogDecoder() *ProtoLogDecoder {
	return &ProtoLogDecoder{}
}

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
