package raft

import (
	"fmt"
	"io"
	"os"
	"path/filepath"

	logger "github.com/jmsadair/raft/internal/logger"
)

const (
	openErrorFormat          = "failed to open log: %s"
	closeErrorFormat         = "failed to close log: %s"
	getEntryErrorFormat      = "failed to get entry: %s"
	appendEntriesErrorFormat = "failed to append entries: %s"
	truncateErrorFormat      = "failed to truncate: %s"
)

// PersistentLog implements the Log interface.
type PersistentLog struct {
	path   string
	file   *os.File
	vlog   *VolatileLog
	logger logger.Logger
}

func NewPersistentLog(path string) *PersistentLog {
	return &PersistentLog{path: filepath.Join(path, "log"), vlog: NewVolatileLog(), logger: logger.GetLogger()}
}

func (l *PersistentLog) Open() {
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

		l.vlog.AppendEntries(entry)
	}

	l.logger.Debugf("opened log %s", l.path)
}

func (l *PersistentLog) Close() {
	if l.file == nil {
		l.logger.Fatalf(openErrorFormat, "log is not open")
	}
	l.file.Close()
	l.file = nil
	l.vlog.Clear()
	l.logger.Debugf("closed log %s", l.path)
}

func (l *PersistentLog) IsOpen() bool {
	return !(l.file == nil)
}

func (l *PersistentLog) GetEntry(index uint64) *LogEntry {
	if l.file == nil {
		l.logger.Fatalf(getEntryErrorFormat, "log is not open")
	}
	entry, err := l.vlog.GetEntry(index)
	if err != nil {
		l.logger.Fatalf(getEntryErrorFormat, fmt.Sprintf("invalid index %d", index))
	}
	return entry
}

func (l *PersistentLog) Contains(index uint64) bool {
	return !l.vlog.Contains(index)
}

func (l *PersistentLog) AppendEntries(entries ...*LogEntry) uint64 {
	if l.file == nil {
		l.logger.Fatalf(appendEntriesErrorFormat, "log is not open")
	}

	var toAppend []*LogEntry

	for i, entry := range entries {
		var err error
		var existing *LogEntry

		if l.vlog.LastIndex() < entry.Index() {
			toAppend = entries[i:]
			break
		}

		existing, err = l.vlog.GetEntry(entry.Index())

		if err == nil && existing.IsConflict(entry) {
			l.logger.Debugf("found conflicting entries at index %d: %d != %d (existing term != provided term)",
				entry.Index(), existing.Term(), entry.Term())
			l.Truncate(entry.Index())
			toAppend = entries[i:]
			break
		}

		if err == nil {
			continue
		}

		l.logger.Fatalf(appendEntriesErrorFormat, err.Error())
	}

	l.PersistEntries(toAppend...)
	l.vlog.AppendEntries(toAppend...)

	if len(toAppend) != 0 {
		return toAppend[len(toAppend)-1].Index()
	}
	return 0
}

func (l *PersistentLog) Truncate(index uint64) {
	if l.file == nil {
		l.logger.Fatalf(truncateErrorFormat, "log is not open")
	}

	lastIndex := l.vlog.LastIndex()
	entry, err := l.vlog.GetEntry(index)

	if err != nil {
		l.logger.Fatalf(truncateErrorFormat, err)
	}
	if err := l.file.Truncate(entry.offset); err != nil {
		l.logger.Fatalf(truncateErrorFormat, err.Error())
	}
	if err := l.vlog.Truncate(index); err != nil {
		l.logger.Fatalf(truncateErrorFormat, err.Error())
	}

	l.logger.Debugf("truncated log from index %d to index %d", index, lastIndex)
}

func (l *PersistentLog) LastTerm() uint64 {
	return l.vlog.LastTerm()
}

func (l *PersistentLog) FirstIndex() uint64 {
	return l.vlog.FirstIndex()
}

func (l *PersistentLog) LastIndex() uint64 {
	return l.vlog.LastIndex()
}

func (l *PersistentLog) Path() string {
	return l.path
}

func (l *PersistentLog) File() *os.File {
	return l.file
}

func (l *PersistentLog) Size() int {
	return l.vlog.Size()
}

func (l *PersistentLog) PersistEntries(entries ...*LogEntry) {
	for _, entry := range entries {
		var err error
		if entry.offset, err = l.file.Seek(0, os.SEEK_CUR); err != nil {
			l.logger.Fatalf(appendEntriesErrorFormat, err)
		}
		if _, err = entry.Encode(l.file); err != nil {
			l.logger.Fatalf(appendEntriesErrorFormat, err)
		}
	}
}
