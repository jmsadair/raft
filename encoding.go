package raft

import (
	"encoding/binary"
	"io"

	pb "github.com/jmsadair/raft/internal/protobuf"
	"google.golang.org/protobuf/proto"
)

func encodePersistentState(w io.Writer, state *persistentState) error {
	pbState := &pb.StorageState{Term: state.term, VotedFor: state.votedFor}
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

func decodePersistentState(r io.Reader) (persistentState, error) {
	var size int32
	if err := binary.Read(r, binary.BigEndian, &size); err != nil {
		return persistentState{}, err
	}

	buf := make([]byte, size)
	if _, err := io.ReadFull(r, buf); err != nil {
		return persistentState{}, err
	}

	pbState := &pb.StorageState{}
	if err := proto.Unmarshal(buf, pbState); err != nil {
		return persistentState{}, err
	}

	state := persistentState{
		term:     pbState.GetTerm(),
		votedFor: pbState.GetVotedFor(),
	}

	return state, nil
}

func encodeLogEntry(w io.Writer, entry *LogEntry) error {
	pbEntry := &pb.LogEntry{
		Index:     entry.Index,
		Term:      entry.Term,
		Data:      entry.Data,
		Offset:    entry.Offset,
		EntryType: pb.LogEntry_LogEntryType(entry.EntryType),
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

func decodeLogEntry(r io.Reader) (LogEntry, error) {
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
		Index:     pbEntry.GetIndex(),
		Term:      pbEntry.GetTerm(),
		Data:      pbEntry.GetData(),
		Offset:    pbEntry.GetOffset(),
		EntryType: LogEntryType(pbEntry.EntryType),
	}

	return entry, nil
}
