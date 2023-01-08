package raft

import "fmt"

const invalidIndexErrorFormat = "invalid index: log does not contain index %d"

type VolatileLog struct {
	entries []*LogEntry
}

func NewVolatileLog() *VolatileLog {
	return &VolatileLog{entries: make([]*LogEntry, 0)}
}

func (l *VolatileLog) Size() int {
	return len(l.entries)
}

func (l *VolatileLog) FirstIndex() uint64 {
	if len(l.entries) == 0 {
		return 0
	}
	return l.entries[0].Index()
}

func (l *VolatileLog) LastIndex() uint64 {
	if len(l.entries) == 0 {
		return 0
	}
	return l.entries[len(l.entries)-1].Index()
}

func (l *VolatileLog) LastTerm() uint64 {
	if len(l.entries) == 0 {
		return 0
	}
	return l.entries[len(l.entries)-1].Term()
}

func (l *VolatileLog) AppendEntries(entries ...*LogEntry) {
	l.entries = append(l.entries, entries...)
}

func (l *VolatileLog) GetEntry(index uint64) (*LogEntry, error) {
	if !l.Contains(index) {
		return nil, fmt.Errorf(invalidIndexErrorFormat, index)
	}
	return l.entries[index-l.entries[0].Index()], nil
}

func (l *VolatileLog) Truncate(from uint64) error {
	if !l.Contains(from) {
		return fmt.Errorf("invalid index: log does not contain %d", from)
	}
	l.entries = l.entries[:from-l.entries[0].Index()]
	return nil
}

func (l *VolatileLog) Clear() {
	l.entries = make([]*LogEntry, 0)
}

func (l *VolatileLog) Contains(index uint64) bool {
	if len(l.entries) == 0 {
		return false
	}
	return l.entries[0].Index() <= index && index <= l.entries[len(l.entries)-1].Index()
}
