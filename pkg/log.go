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

// Log supports appending and retrieving log entries in a durable manner.
type Log interface {
	// Open opens the log for reading and writing.
	//
	// Returns:
	//     - error: An error if opening the log fails.
	Open() error

	// Replay reads the persisted state of the log into
	// memory. Log must be open.
	//
	// Returns:
	//     - error: An error if replaying the log fails.
	Replay() error

	// Close closes the log.
	//
	// Returns:
	//     - error: An error if closing the log fails.
	Close() error

	// GetEntry returns the log entry located at the specified index.
	//
	// Parameters:
	//     - index: The index of the log entry to retrieve.
	//
	// Returns:
	//     - *LogEntry: The log entry located at the index.
	//     - error: An error if retrieving the log entry fails.
	GetEntry(index uint64) (*LogEntry, error)

	// AppendEntry appends a log entry to the log.
	//
	// Parameters:
	//     - entry: The log entry to append.
	//
	// Returns:
	//     - error: An error if appending the log entry fails.
	AppendEntry(entry *LogEntry) error

	// AppendEntries appends multiple log entries to the log.
	//
	// Parameters:
	//     - entries: The log entries to append.
	//
	// Returns:
	//     - error: An error if appending the log entries fails.
	AppendEntries(entries []*LogEntry) error

	// Truncate deletes all log entries with index greater than
	// or equal to the provided index.
	//
	// Parameters:
	//     - index: The index from which to truncate the log.
	//
	// Returns:
	//     - error: An error if truncating the log fails.
	Truncate(index uint64) error

	// Discard deletes all in-memory and persistent data in the
	// log and returns a new empty log where the starting index
	// is the provided index and the starting term is the provided
	// term. The new log is located at the same path and is ready for
	// operations immediantly. Primarily intended to be used for snapshotting.
	//
	// Paramaters:
	//     - index: the index that the log will start at.
	//     - term: the term the log will start at.
	//
	// Returns:
	//     - Log: A new, empty log that starts at the provided index and term.
	//	   - error: An error if discarding the log fails.
	Discard(index uint64, term uint64) (Log, error)

	// Compact deletes all log entries with index less than
	// or equal to the provided index.
	//
	// Parameters:
	//     - index: The index to which the log should be compacted.
	//
	// Returns:
	//     - error: An error if compacting the log fails.
	Compact(index uint64) error

	// Contains checks if the log contains an entry at the specified index.
	//
	// Parameters:
	//     - index: The index to check.
	//
	// Returns:
	//     - bool: True if the index exists in the log, false otherwise.
	Contains(index uint64) bool

	// LastIndex returns the largest index that exists in the log and zero
	// if the log is empty.
	//
	// Returns:
	//     - uint64: The largest index in the log.
	LastIndex() uint64

	// LastTerm returns the largest term in the log and zero if the log
	// is empty.
	//
	// Returns:
	//     - uint64: The largest term in the log.
	LastTerm() uint64

	// NextIndex returns the next index to append to the log.
	//
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
//
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
//
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
//
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
			return err
		}
		l.entries = append(l.entries, &entry)
	}

	// The log must always contain atleast one entry.
	// The first entry is a dummy entry used for indexing into the log.
	if len(l.entries) == 0 {
		entry := NewLogEntry(0, 0, nil)
		writer := bufio.NewWriter(l.file)
		if err := l.logEncoder.Encode(writer, entry); err != nil {
			return err
		}
		if err := writer.Flush(); err != nil {
			return err
		}
		l.file.Sync()
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

	// The offset of the entry at the provided index is the
	// new size of the file.
	size := l.entries[logIndex].offset

	if err := l.file.Truncate(size); err != nil {
		return err
	}

	// Update to I/O offset to the new size.
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

	// Create a temporary file to write the compacted log to.
	compactedFile, err := os.CreateTemp("", "raft-log")
	if err != nil {
		return err
	}

	// Write the entries contained in the compacted log to the
	// temporary file.
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

	// Atomically rename the temporary file to the actual file.
	if err := os.Rename(compactedFile.Name(), l.path); err != nil {
		return err
	}

	l.file = compactedFile
	l.entries = newEntries

	return nil
}

func (l *PersistentLog) Discard(index uint64, term uint64) (Log, error) {
	if l.file == nil {
		return nil, fmt.Errorf("log is not open")
	}

	// Create a temporary file for the new log.
	newLogFile, err := os.CreateTemp("", "raft-log")
	if err != nil {
		return nil, err
	}

	// Write a dummy entry to the temporary file with the provided
	// term and index.
	writer := bufio.NewWriter(newLogFile)
	entry := &LogEntry{index: index, term: term}
	if err := l.logEncoder.Encode(writer, entry); err != nil {
		return nil, err
	}
	if err := writer.Flush(); err != nil {
		return nil, err
	}

	newLogFile.Sync()

	// Atomically rename the temporary file to the actual file.
	if err := os.Rename(newLogFile.Name(), l.path); err != nil {
		return nil, err
	}

	l.file = newLogFile
	l.entries = []*LogEntry{entry}

	return l, nil
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
