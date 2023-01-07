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

const (
	openErrorFormat          = "failed to open log: %s"
	closeErrorFormat         = "failed to close log: %s"
	getEntryErrorFormat      = "failed to get entry: %s"
	appendEntriesErrorFormat = "failed to append entries: %s"
	truncateErrorFormat      = "failed to truncate: %s"
)

type Log struct {
	path   string
	file   *os.File
	log    *VolatileLog
	logger log.Logger
}

func newLog(path string) *Log {
	return &Log{path: filepath.Join(path, "log"), log: newVolatileLog(), logger: log.GetLogger()}
}

func (l *Log) open() {
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
		entry := newLogEntry(0, 0, nil)

		if entry.offset, err = l.file.Seek(0, os.SEEK_CUR); err != nil {
			l.logger.Fatalf(openErrorFormat, err.Error())
		}

		if _, err = entry.decode(file); err != nil {
			if err == io.EOF {
				break
			}
			l.logger.Fatalf(openErrorFormat, err.Error())
		}

		l.log.appendEntries(entry)
	}

	l.logger.Debugf("opened log %s", l.path)
}

func (l *Log) close() error {
	if l.file == nil {
		l.logger.Fatalf(openErrorFormat, "log is not open")
	}
	l.file.Close()
	l.file = nil
	l.log.clear()
	l.logger.Debugf("closed log %s", l.path)
	return nil
}

func (l *Log) isOpen() bool {
	return !(l.file == nil)
}

func (l *Log) getEntry(index uint64) *LogEntry {
	if l.file == nil {
		l.logger.Fatalf(getEntryErrorFormat, "log is not open")
	}
	entry, err := l.log.getEntry(index)
	if err != nil {
		l.logger.Fatalf(getEntryErrorFormat, fmt.Sprintf("invalid index %d", index))
	}
	return entry
}

func (l *Log) persistEntries(entries ...*LogEntry) {
	for _, entry := range entries {
		var err error
		if entry.offset, err = l.file.Seek(0, os.SEEK_CUR); err != nil {
			l.logger.Fatalf(appendEntriesErrorFormat, err)
		}
		if _, err = entry.encode(l.file); err != nil {
			l.logger.Fatalf(appendEntriesErrorFormat, err)
		}
	}
}

func (l *Log) appendEntries(entries ...*LogEntry) {
	if l.file == nil {
		l.logger.Fatalf(appendEntriesErrorFormat, "log is not open")
	}

	var toAppend []*LogEntry

	for i, entry := range entries {
		var err error
		var existing *LogEntry

		if l.log.lastIndex() < entry.index() {
			toAppend = entries[i:]
			break
		}

		existing, err = l.log.getEntry(entry.index())

		if err == nil && existing.isConflict(entry) {
			l.logger.Debugf("found conflicting entries at index %d: %d != %d (existing term != provided term)",
				entry.index(), existing.term(), entry.term())
			l.truncate(entry.index())
			toAppend = entries[i:]
			break
		}

		if err == nil {
			continue
		}

		l.logger.Fatalf(appendEntriesErrorFormat, err.Error())
	}

	l.persistEntries(toAppend...)
	l.log.appendEntries(toAppend...)
}

func (l *Log) truncate(index uint64) {
	if l.file == nil {
		l.logger.Fatalf(truncateErrorFormat, "log is not open")
	}

	lastIndex := l.log.lastIndex()
	entry, err := l.log.getEntry(index)

	if err != nil {
		l.logger.Fatalf(truncateErrorFormat, err)
	}
	if err := l.log.truncate(index); err != nil {
		l.logger.Fatalf(truncateErrorFormat, err.Error())
	}
	if err := l.file.Truncate(entry.offset); err != nil {
		l.logger.Fatalf(truncateErrorFormat, err.Error())
	}

	l.logger.Debugf("truncated log from index %d to index %d", index, lastIndex)
}

func (l *Log) logPath() string {
	return l.path
}

func (l *Log) logFile() *os.File {
	return l.file
}

func (l *Log) size() int {
	return l.log.size()
}

func (l *Log) lastTerm() uint64 {
	return l.log.lastTerm()
}

func (l *Log) lastIndex() uint64 {
	return l.log.lastIndex()
}
