package raft

import (
	"bufio"
	"fmt"
	"io"
	"os"

	"github.com/jmsadair/raft/internal/errors"
)

const (
	errIndexDoesNotExist = "index %d does not exist"
)

// Log supports appending and retrieving log entries in
// a durable manner.
type Log interface {
	// Open opens the log for reading and writing.
	// Returns:
	//     - error: An error if opening the log fails.
	Open() error

	// Close closes the log.
	// Returns:
	//     - error: An error if closing the log fails.
	Close() error

	// GetEntry returns the log entry located at the specified index.
	// Parameters:
	//     - index: The index of the log entry to retrieve.
	// Returns:
	//     - *LogEntry: The log entry located at the index.
	//     - error: An error if retrieving the log entry fails.
	GetEntry(index uint64) (*LogEntry, error)

	// AppendEntry appends a log entry to the log.
	// Parameters:
	//     - entry: The log entry to append.
	// Returns:
	//     - error: An error if appending the log entry fails.
	AppendEntry(entry *LogEntry) error

	// AppendEntries appends multiple log entries to the log.
	// Parameters:
	//     - entries: The log entries to append.
	// Returns:
	//     - error: An error if appending the log entries fails.
	AppendEntries(entries []*LogEntry) error

	// Truncate deletes all log entries with index greater than
	// or equal to the provided index.
	// Parameters:
	//     - index: The index from which to truncate the log.
	// Returns:
	//     - error: An error if truncating the log fails.
	Truncate(index uint64) error

	// Compact deletes all log entries with index less than
	// or equal to the provided index.
	// Parameters:
	//     - index: The index to which the log should be compacted.
	// Returns:
	//     - error: An error if compacting the log fails.
	Compact(index uint64) error

	// Contains checks if the log contains an entry at the specified index.
	// Parameters:
	//     - index: The index to check.
	// Returns:
	//     - bool: True if the index exists in the log, false otherwise.
	Contains(index uint64) bool

	// LastIndex returns the largest index that exists in the log and zero
	// if the log is empty.
	// Returns:
	//     - uint64: The largest index in the log.
	LastIndex() uint64

	// LastTerm returns the largest term in the log and zero if the log
	// is empty.
	// Returns:
	//     - uint64: The largest term in the log.
	LastTerm() uint64

	// NextIndex returns the next index to append to the log.
	// Returns:
	//     - uint64: The next index to append to the log.
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

// NewLogEntry creates a new instance of LogEntry.
// Parameters:
//   - index: The index of the log entry.
//   - term: The term of the log entry.
//   - data: The data of the log entry.
//
// Returns:
//   - *LogEntry: A new instance of LogEntry.
func NewLogEntry(index uint64, term uint64, data []byte) *LogEntry {
	return &LogEntry{index: index, term: term, data: data}
}

// IsConflict checks whether the current log entry conflicts with another log entry.
// Two log entries are considered conflicting if they have the same index but different terms.
// Parameters:
//   - other: The other LogEntry to compare against.
//
// Returns:
//   - bool: True if the log entries conflict, false otherwise.
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

// NewPersistentLog creates a new instance of PersistentLog.
// Parameters:
//   - path: The path to the persistent log file.
//   - logEncoder: The log encoder used to encode log entries.
//   - logDecoder: The log decoder used to decode log entries.
//
// Returns:
//   - *PersistentLog: A new instance of PersistentLog.
func NewPersistentLog(path string, logEncoder LogEncoder, logDecoder LogDecoder) *PersistentLog {
	return &PersistentLog{path: path, logEncoder: logEncoder, logDecoder: logDecoder}
}

func (l *PersistentLog) Open() error {
	file, err := os.OpenFile(l.path, os.O_RDWR|os.O_CREATE, 0777)
	if err != nil {
		return err
	}
	l.file = file

	reader := bufio.NewReader(l.file)
	entries := make([]*LogEntry, 0)
	for {
		entry, err := l.logDecoder.Decode(reader)
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		entries = append(entries, &entry)
	}

	if len(entries) == 0 {
		entry := NewLogEntry(0, 0, nil)
		writer := bufio.NewWriter(l.file)
		if err := l.logEncoder.Encode(writer, entry); err != nil {
			return err
		}
		if err := writer.Flush(); err != nil {
			return err
		}
		l.file.Sync()
		entries = append(entries, entry)
	}

	l.entries = entries
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
		return nil, fmt.Errorf("log is not open")
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
		return fmt.Errorf("log is not open")
	}

	writer := bufio.NewWriter(l.file)
	for _, entry := range entries {
		offset, err := l.file.Seek(0, io.SeekCurrent)
		if err != nil {
			return err
		}
		entry.offset = offset
		if err := l.logEncoder.Encode(writer, entry); err != nil {
			return err
		}
		if err := writer.Flush(); err != nil {
			return err
		}
	}

	if err := l.file.Sync(); err != nil {
		return err
	}

	l.entries = append(l.entries, entries...)

	return nil
}

func (l *PersistentLog) Truncate(index uint64) error {
	if l.file == nil {
		return fmt.Errorf("log is not open")
	}

	logIndex := index - l.entries[0].index
	lastIndex := l.entries[len(l.entries)-1].index
	if logIndex <= 0 || logIndex > lastIndex {
		return errors.WrapError(nil, errIndexDoesNotExist, index)
	}
	size := l.entries[logIndex].offset
	if err := l.file.Truncate(size); err != nil {
		return err
	}
	if _, err := l.file.Seek(size, io.SeekStart); err != nil {
		return err
	}
	l.entries = l.entries[:logIndex]
	return nil
}

func (l *PersistentLog) Compact(index uint64) error {
	if l.file == nil {
		return fmt.Errorf("log is not open")
	}

	logIndex := index - l.entries[0].index
	lastIndex := l.entries[len(l.entries)-1].index
	if logIndex <= 0 || logIndex > lastIndex {
		return errors.WrapError(nil, errIndexDoesNotExist, index)
	}

	newEntries := make([]*LogEntry, len(l.entries)-int(logIndex))
	copy(newEntries[:], l.entries[logIndex:])

	compactedFile, err := os.CreateTemp("", "raft-log")
	if err != nil {
		return err
	}

	writer := bufio.NewWriter(compactedFile)
	for _, entry := range newEntries {
		offset, err := compactedFile.Seek(0, io.SeekCurrent)
		if err != nil {
			return err
		}
		entry.offset = offset
		if err := l.logEncoder.Encode(writer, entry); err != nil {
			return err
		}
	}

	if err := writer.Flush(); err != nil {
		return err
	}

	compactedFile.Sync()

	if err := os.Rename(compactedFile.Name(), l.path); err != nil {
		return err
	}

	l.file = compactedFile
	l.entries = newEntries

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
