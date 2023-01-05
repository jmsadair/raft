package raft

import (
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
)

// TODO: Reconsider AppendEntries parameters? Need to check prevLogIndex and prevLogTerm provided by leader.
// TODO: Need to make concurrent safe. Add mutexes.

type Log struct {
	path        string
	file        *os.File
	entries     []*LogEntry
	commitIndex uint64
	lastApplied uint64
}

func NewLog(path string) *Log {
	return &Log{path: filepath.Join(path, "log"), entries: make([]*LogEntry, 0)}
}

func (l *Log) Path() string {
	return l.path
}

func (l *Log) File() *os.File {
	return l.file
}

func (l *Log) Size() int {
	return len(l.entries)
}

func (l *Log) CommitIndex() uint64 {
	return l.commitIndex
}

func (l *Log) LastApplied() uint64 {
	return l.lastApplied
}

func (l *Log) Open() error {
	file, err := os.OpenFile(l.path, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0666)
	if err != nil {
		log.Fatalf("failed to open log file: %s", err.Error())
	}
	l.file = file

	for {
		entry := NewLogEntry(0, 0, nil)
		if _, err := entry.Decode(file); err != nil {
			if err == io.EOF {
				break
			}
			return err
		}
		l.entries = append(l.entries, entry)
	}

	return nil
}

func (l *Log) Close() {
	l.file.Close()
	l.entries = make([]*LogEntry, 0)
}

func (l *Log) AppendEntries(entries []*LogEntry) error {
	panic(fmt.Errorf("AppendEntries: not implemented"))
}

func (l *Log) AppendEntry(entry *LogEntry) error {
	// Must persist entry before adding to in-memory log.
	if _, err := entry.Encode(l.file); err != nil {
		return err
	}
	l.entries = append(l.entries, entry)
	return nil
}

func (l *Log) LastIndex() uint64 {
	if len(l.entries) == 0 {
		return 0
	}
	return l.entries[len(l.entries)-1].Index()
}

func (l *Log) LastTerm() uint64 {
	if len(l.entries) == 0 {
		return 0
	}
	return l.entries[len(l.entries)-1].Term()
}
