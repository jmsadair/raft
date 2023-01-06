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
// TODO: Add function to truncate log when there is conflicting entries.

type Log struct {
	path        string
	file        *os.File
	entries     []*LogEntry
	startIndex  uint64
	commitIndex uint64
	lastApplied uint64
}

func NewLog(path string) *Log {
	return &Log{path: filepath.Join(path, "log"), entries: make([]*LogEntry, 0)}
}

func (l *Log) Open() {
	if l.file != nil {
		log.Fatal("error opening log: log already open")
	}

	file, err := os.OpenFile(l.path, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0666)
	if err != nil {
		log.Fatalf("error opening log: %s", err.Error())
	}
	l.file = file

	for {
		var err error
		entry := NewLogEntry(0, 0, nil)

		// Set the file offset of the entry.
		if entry.offset, err = l.file.Seek(0, os.SEEK_CUR); err != nil {
			log.Fatalf("error opening log: %s", err.Error())
		}

		// Write the entry to the file.
		if _, err = entry.Decode(file); err != nil {
			if err == io.EOF {
				break
			}
			log.Fatalf("error opening log: %s", err.Error())
		}

		l.entries = append(l.entries, entry)
	}

	if len(l.entries) != 0 {
		l.startIndex = l.entries[0].Index()
	}
}

func (l *Log) Close() error {
	if l.file == nil {
		log.Fatal("error closing log: log not open")
	}
	l.file.Close()
	l.file = nil
	l.entries = make([]*LogEntry, 0)
	return nil
}

func (l *Log) IsOpen() bool {
	return !(l.file == nil)
}

func (l *Log) GetEntry(index uint64) (*LogEntry, bool) {
	if l.file == nil {
		log.Fatal("error getting log entry: log not open")
	}

	if index < l.StartIndex() || index > l.LastIndex() {
		return nil, false
	}

	return l.entries[index-l.StartIndex()], true
}

func (l *Log) Contains(index uint64) bool {
	return index >= l.StartIndex() && index <= l.LastIndex()
}

func (l *Log) AppendEntry(entry *LogEntry) error {
	if l.file == nil {
		log.Fatal("error getting log entry: log not open")
	}

	var err error

	// Entries coming from a different log may have a different offset.
	entry.offset, err = l.file.Seek(0, os.SEEK_CUR)
	if err != nil {
		return err
	}

	// Must persist entry before adding to in-memory log.
	if _, err = entry.Encode(l.file); err != nil {
		return err
	}

	if l.startIndex == 0 {
		l.startIndex = entry.Index()
	}
	l.entries = append(l.entries, entry)

	return nil
}

func (l *Log) AppendEntries(entries []*LogEntry) error {
	if l.file == nil {
		log.Fatal("error getting log entry: log not open")
	}

	for _, entry := range entries {
		if err := l.AppendEntry(entry); err != nil {
			return err
		}
	}

	return nil
}

func (l *Log) Truncate(index uint64) error {
	if l.file == nil {
		log.Fatal("error getting log entry: log not open")
	}

	entry, ok := l.GetEntry(index)
	if !ok {
		return fmt.Errorf("error truncating log: invalid index %d", index)
	}
	l.entries = l.entries[:entry.Index()-l.StartIndex()]

	return l.file.Truncate(entry.offset)
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

func (l *Log) LastTerm() uint64 {
	if len(l.entries) == 0 {
		return 0
	} else {
		return l.entries[len(l.entries)-1].Term()
	}
}

func (l *Log) StartIndex() uint64 {
	return l.startIndex
}

func (l *Log) LastIndex() uint64 {
	if len(l.entries) == 0 {
		return 0
	} else {
		return l.entries[len(l.entries)-1].Index()
	}
}
