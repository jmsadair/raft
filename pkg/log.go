package raft

import (
	"bufio"
	"io"
	"os"

	"github.com/jmsadair/raft/internal/errors"
)

const (
	errIndexDoesNotExist       = "index %d does not exist"
	errLogNotOpen              = "log is not open: path = %s"
	errFailedLogCreateTempFile = "failed to create temporary log file: %s"
	errFailedLogSync           = "failed to sync log file: %s"
	errFailedLogFlush          = "failed flushing data from log file writer: %s"
	errFailedLogEncode         = "failed to encode log entry: %s"
	errFailedLogDecode         = "failed to decode log entry: %s"
	errFailedLogOpen           = "failed to open log file: path = %s"
	errFailedLogSeek           = "failed to seek to offset in log file: offset = %d, err = %s"
	errFailedLogTruncate       = "failed to truncate log file: size = %d, err = %s"
	errFailedLogRename         = "failed to rename temporary log file: %s"
)

// Log supports appending and retrieving log entries in a durable manner.
type Log interface {
	// Open opens the log for reading and writing.
	Open() error

	// Replay reads the persisted state of the log into
	// memory. Log must be open.
	Replay() error

	// Close closes the log.
	Close() error

	// GetEntry returns the log entry located at the specified index.
	GetEntry(index uint64) (*LogEntry, error)

	// AppendEntry appends a log entry to the log.
	AppendEntry(entry *LogEntry) error

	// AppendEntries appends multiple log entries to the log.
	AppendEntries(entries []*LogEntry) error

	// Truncate deletes all log entries with index greater than
	// or equal to the provided index.
	Truncate(index uint64) error

	// DiscardEntries deletes all in-memory and persistent data in the
	// log. The provided term and index indicate at what term and index
	// the now empty log will start at. Primarily intended to be used
	// for snapshotting.
	DiscardEntries(index uint64, term uint64) error

	// Compact deletes all log entries with index less than
	// or equal to the provided index.
	Compact(index uint64) error

	// Contains checks if the log contains an entry at the specified index.
	Contains(index uint64) bool

	// LastIndex returns the largest index that exists in the log and zero
	// if the log is empty.
	LastIndex() uint64

	// LastTerm returns the largest term in the log and zero if the log
	// is empty.
	LastTerm() uint64

	// NextIndex returns the next index to append to the log.
	NextIndex() uint64
}

// LogEntry represents a log entry in the log.
type LogEntry struct {
	// The index of the log entry.
	index uint64

	// The term of the log entry.
	term uint64

	// The offset of the log entry.
	offset int64

	// The data of the log entry.
	data []byte
}

// NewLogEntry creates a new instance of LogEntry with the provided
// index, term, and data.
func NewLogEntry(index uint64, term uint64, data []byte) *LogEntry {
	return &LogEntry{index: index, term: term, data: data}
}

// IsConflict checks whether the current log entry conflicts with another log entry.
// Two log entries are considered conflicting if they have the same index but different terms.
func (e *LogEntry) IsConflict(other *LogEntry) bool {
	return e.index == other.index && e.term != other.term
}

// PersistentLog implements the Log interface.
type PersistentLog struct {
	// The in-memory log entries of the log.
	entries []*LogEntry

	// The file that the log is written to.
	file *os.File

	// The path to the file that the log is written to.
	path string

	// An encoder for encoding log entries.
	logEncoder LogEncoder

	// A decoder for decoding log entries.
	logDecoder LogDecoder
}

// NewPersistentLog creates a new instance of PersistentLog at the provided path.
// Log entries will be encoded using the provided LogEncoder, and decoded using
// the provided LogDecoder.
func NewPersistentLog(path string, logEncoder LogEncoder, logDecoder LogDecoder) *PersistentLog {
	return &PersistentLog{path: path, logEncoder: logEncoder, logDecoder: logDecoder}
}

func (l *PersistentLog) Open() error {
	file, err := os.OpenFile(l.path, os.O_RDWR|os.O_CREATE, 0777)
	if err != nil {
		return errors.WrapError(err, errFailedLogOpen, l.path)
	}
	l.file = file
	l.entries = make([]*LogEntry, 0)
	return nil
}

func (l *PersistentLog) Replay() error {
	reader := bufio.NewReader(l.file)

	for {
		entry, err := l.logDecoder.Decode(reader)
		if err == io.EOF {
			break
		}
		if err != nil {
			return errors.WrapError(err, errFailedLogDecode, err.Error())
		}
		l.entries = append(l.entries, &entry)
	}

	// The log must always contain atleast one entry.
	// The first entry is a dummy entry used for indexing into the log.
	if len(l.entries) == 0 {
		entry := NewLogEntry(0, 0, nil)
		writer := bufio.NewWriter(l.file)
		if err := l.logEncoder.Encode(writer, entry); err != nil {
			return errors.WrapError(err, errFailedLogEncode, err.Error())
		}
		if err := writer.Flush(); err != nil {
			return errors.WrapError(err, errFailedLogFlush, err.Error())
		}
		if err := l.file.Sync(); err != nil {
			return errors.WrapError(err, errFailedLogSync, err.Error())
		}
		l.entries = append(l.entries, entry)
	}

	return nil
}

func (l *PersistentLog) Close() error {
	if l.file == nil {
		return nil
	}
	l.file.Close()
	l.entries = nil
	l.file = nil
	return nil
}

func (l *PersistentLog) GetEntry(index uint64) (*LogEntry, error) {
	if l.file == nil {
		return nil, errors.WrapError(nil, errLogNotOpen, l.path)
	}

	logIndex := index - l.entries[0].index
	lastIndex := l.entries[len(l.entries)-1].index
	if logIndex <= 0 || logIndex > lastIndex {
		return nil, errors.WrapError(nil, errIndexDoesNotExist, index)
	}

	entry := l.entries[logIndex]

	return entry, nil
}

func (l *PersistentLog) Contains(index uint64) bool {
	logIndex := index - l.entries[0].index
	lastIndex := l.entries[len(l.entries)-1].index
	return !(logIndex <= 0 || logIndex > lastIndex)
}

func (l *PersistentLog) AppendEntry(entry *LogEntry) error {
	return l.AppendEntries([]*LogEntry{entry})
}

func (l *PersistentLog) AppendEntries(entries []*LogEntry) error {
	if l.file == nil {
		return errors.WrapError(nil, errLogNotOpen, l.path)
	}

	writer := bufio.NewWriter(l.file)
	for _, entry := range entries {
		offset, err := l.file.Seek(0, io.SeekCurrent)
		if err != nil {
			return errors.WrapError(err, errFailedLogSeek, offset, err.Error())
		}
		entry.offset = offset
		if err := l.logEncoder.Encode(writer, entry); err != nil {
			return errors.WrapError(err, errFailedLogEncode, err.Error())
		}
		if err := writer.Flush(); err != nil {
			return errors.WrapError(err, errFailedLogFlush, err.Error())
		}
	}

	if err := l.file.Sync(); err != nil {
		return errors.WrapError(err, errFailedLogSync, err.Error())
	}

	l.entries = append(l.entries, entries...)

	return nil
}

func (l *PersistentLog) Truncate(index uint64) error {
	if l.file == nil {
		return errors.WrapError(nil, errLogNotOpen, l.path)
	}

	logIndex := index - l.entries[0].index
	lastIndex := l.entries[len(l.entries)-1].index
	if logIndex <= 0 || logIndex > lastIndex {
		return errors.WrapError(nil, errIndexDoesNotExist, index)
	}

	// The offset of the entry at the provided index is the
	// new size of the file.
	size := l.entries[logIndex].offset

	if err := l.file.Truncate(size); err != nil {
		return errors.WrapError(err, errFailedLogTruncate, size, err.Error())
	}

	// Update to I/O offset to the new size.
	if _, err := l.file.Seek(size, io.SeekStart); err != nil {
		return errors.WrapError(err, errFailedLogSeek, size, err.Error())
	}

	l.entries = l.entries[:logIndex]

	return nil
}

func (l *PersistentLog) Compact(index uint64) error {
	if l.file == nil {
		return errors.WrapError(nil, errLogNotOpen, l.path)
	}

	logIndex := index - l.entries[0].index
	lastIndex := l.entries[len(l.entries)-1].index
	if logIndex <= 0 || logIndex > lastIndex {
		return errors.WrapError(nil, errIndexDoesNotExist, index)
	}

	newEntries := make([]*LogEntry, len(l.entries)-int(logIndex))
	copy(newEntries[:], l.entries[logIndex:])

	// Create a temporary file to write the compacted log to.
	compactedFile, err := os.CreateTemp("", "raft-log")
	if err != nil {
		return errors.WrapError(err, errFailedLogCreateTempFile, err.Error())
	}

	// Write the entries contained in the compacted log to the
	// temporary file.
	writer := bufio.NewWriter(compactedFile)
	for _, entry := range newEntries {
		offset, err := compactedFile.Seek(0, io.SeekCurrent)
		if err != nil {
			return errors.WrapError(err, errFailedLogSeek, offset, err.Error())
		}
		entry.offset = offset
		if err := l.logEncoder.Encode(writer, entry); err != nil {
			return errors.WrapError(err, errFailedLogEncode, err.Error())
		}
	}

	if err := writer.Flush(); err != nil {
		return errors.WrapError(err, errFailedLogFlush, err.Error())
	}

	if err := compactedFile.Sync(); err != nil {
		return errors.WrapError(err, errFailedLogSync, err.Error())
	}

	// Atomically rename the temporary file to the actual file.
	if err := os.Rename(compactedFile.Name(), l.path); err != nil {
		return err
	}

	l.file = compactedFile
	l.entries = newEntries

	return nil
}

func (l *PersistentLog) DiscardEntries(index uint64, term uint64) error {
	if l.file == nil {
		return errors.WrapError(nil, errLogNotOpen, l.path)
	}

	// Create a temporary file for the new log.
	newLogFile, err := os.CreateTemp("", "raft-log")
	if err != nil {
		return errors.WrapError(err, errFailedLogCreateTempFile, err.Error())
	}

	// Write a dummy entry to the temporary file with the provided
	// term and index.
	writer := bufio.NewWriter(newLogFile)
	entry := &LogEntry{index: index, term: term}
	if err := l.logEncoder.Encode(writer, entry); err != nil {
		return errors.WrapError(err, errFailedLogEncode, err.Error())
	}
	if err := writer.Flush(); err != nil {
		return errors.WrapError(err, errFailedLogFlush, err.Error())
	}
	if err := newLogFile.Sync(); err != nil {
		return errors.WrapError(err, errFailedLogSync, err.Error())
	}

	// Atomically rename the temporary file to the actual file.
	if err := os.Rename(newLogFile.Name(), l.path); err != nil {
		return errors.WrapError(err, errFailedLogRename, err.Error())
	}

	l.file = newLogFile
	l.entries = []*LogEntry{entry}

	return nil
}

func (l *PersistentLog) LastTerm() uint64 {
	return l.entries[len(l.entries)-1].term
}

func (l *PersistentLog) LastIndex() uint64 {
	return l.entries[len(l.entries)-1].index
}

func (l *PersistentLog) NextIndex() uint64 {
	return l.entries[len(l.entries)-1].index + 1
}
