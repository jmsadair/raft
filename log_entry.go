package raft

import (
	pb "github.com/jmsadair/raft/internal/protobuf"
)

type LogEntry struct {
	entry *pb.LogEntry
}

func NewLogEntry(index uint64, term uint64, data []byte) *LogEntry {
	return &LogEntry{entry: &pb.LogEntry{Term: term, Index: index, Data: data}}
}

func (e *LogEntry) Term() uint64 {
	return e.entry.GetTerm()
}

func (e *LogEntry) Index() uint64 {
	return e.entry.GetIndex()
}

func (e *LogEntry) Data() []byte {
	return e.entry.GetData()
}

func (e *LogEntry) Entry() *pb.LogEntry {
	return e.entry
}

func (e *LogEntry) IsConflict(other *LogEntry) bool {
	return e.entry.GetIndex() == other.entry.GetIndex() && e.entry.GetTerm() != other.entry.GetTerm()
}
