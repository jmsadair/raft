package raft

import (
	"fmt"
	"io"
	"os"
	"path/filepath"

	log "github.com/jmsadair/raft/logger"
)

// TODO: Reconsider AppendEntries parameters? Need to check prevLogIndex and prevLogTerm provided by leader.
// TODO: Need to make concurrent safe. Add mutexes.
// TODO: Add function to truncate log when there is conflicting entries.

const (
	openErrorFormat          = "failed to open log: %s"
	closeErrorFormat         = "failed to close log: %s"
	GetEntryErrorFormat      = "failed to get entry: %s"
	AppendEntriesErrorFormat = "failed to append entries: %s"
	TruncateErrorFormat      = "failed to truncate: %s"
)

type Log struct {
	path        string
	file        *os.File
	entries     []*LogEntry
	startIndex  uint64
	commitIndex uint64
	lastApplied uint64
	logger      log.Logger
}

func NewLog(path string) *Log {
	return &Log{path: filepath.Join(path, "log"), entries: make([]*LogEntry, 0), logger: log.GetLogger()}
}

func (l *Log) Open() {
	if l.file != nil {
		l.logger.Fatalf(openErrorFormat, "log already open")
	}

	file, err := os.OpenFile(l.path, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0666)
	if err != nil {
		l.logger.Fatalf(openErrorFormat, err.Error())
	}
	l.file = file

	for {
		var err error
		entry := NewLogEntry(0, 0, nil)

		if entry.offset, err = l.file.Seek(0, os.SEEK_CUR); err != nil {
			l.logger.Fatalf(openErrorFormat, err.Error())
		}

		if _, err = entry.Decode(file); err != nil {
			if err == io.EOF {
				break
			}
			l.logger.Fatalf(openErrorFormat, err.Error())
		}

		l.entries = append(l.entries, entry)
	}

	if len(l.entries) != 0 {
		l.startIndex = l.entries[0].Index()
	}

	l.logger.Debugf("opened log %s", l.path)
}

func (l *Log) Close() error {
	if l.file == nil {
		l.logger.Fatalf(openErrorFormat, "log is not open")
	}
	l.file.Close()
	l.file = nil
	l.entries = make([]*LogEntry, 0)
	l.logger.Debugf("closed log %s", l.path)
	return nil
}

func (l *Log) IsOpen() bool {
	return !(l.file == nil)
}

func (l *Log) GetEntry(index uint64) *LogEntry {
	if l.file == nil {
		l.logger.Fatalf(GetEntryErrorFormat, "log is not open")
	}

	if index < l.entries[0].Index() || index > l.entries[len(l.entries)-1].Index() {
		l.logger.Fatalf(GetEntryErrorFormat, fmt.Sprintf("invalid index %d", index))
	}

	return l.entries[index-l.entries[0].Index()]
}

func (l *Log) Contains(index uint64) bool {
	return index >= l.entries[0].Index() && index <= l.entries[len(l.entries)-1].Index()
}

func (l *Log) AppendEntries(entries ...*LogEntry) {
	if l.file == nil {
		l.logger.Fatalf(AppendEntriesErrorFormat, "log is not open")
	}

	for _, entry := range entries {
		var err error

		if len(l.entries) != 0 && entry.Index() <= l.entries[len(l.entries)-1].Index() {
			existing := l.entries[entry.Index()-l.entries[0].Index()]
			if existing.Term() == entry.Term() {
				continue
			}
			l.logger.Debugf("found conflicting entries at index %d: %d != %d (existing term != provided term)",
				entry.Index(), existing.Term(), entry.Term())
			l.Truncate(entry.Index())
		}

		if entry.offset, err = l.file.Seek(0, os.SEEK_CUR); err != nil {
			l.logger.Fatalf(AppendEntriesErrorFormat, err)
		}

		if _, err = entry.Encode(l.file); err != nil {
			l.logger.Fatalf(AppendEntriesErrorFormat, err)
		}

		if len(l.entries) == 0 {
			l.startIndex = entry.Index()
		}

		l.entries = append(l.entries, entry)
	}
}

func (l *Log) Truncate(index uint64) {
	if l.file == nil {
		l.logger.Fatalf(TruncateErrorFormat, "log is not open")
	}
	entry := l.entries[index-l.startIndex]
	last := l.entries[len(l.entries)-1]
	l.entries = l.entries[:entry.Index()-l.entries[0].Index()]
	if err := l.file.Truncate(entry.offset); err != nil {
		l.logger.Fatalf(TruncateErrorFormat, err.Error())
	}
	l.logger.Debugf("truncated log from index %d to index %d", index, last.Index())
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

func (l *Log) LastIndex() uint64 {
	if len(l.entries) == 0 {
		return 0
	} else {
		return l.entries[len(l.entries)-1].Index()
	}
}
