package raft

import (
	"bufio"
	"io"
	"os"

	"github.com/jmsadair/raft/internal/errors"
)

var (
	errIndexDoesNotExist = errors.New("index does not exist")
	errLogNotOpen        = errors.New("log is not open")
)

// Log is an interface representing the internal component of RaftCore that is responsible
// for durably storing and retrieving log entries.
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
	// The Index of the log entry.
	Index uint64

	// The Term of the log entry.
	Term uint64

	// The Offset of the log entry.
	Offset int64

	// The Data of the log entry.
	Data []byte

	// Used to transmit responses to operations.
	ResponseCh chan OperationResponse
}

// NewLogEntry creates a new instance of LogEntry with the provided
// index, term, and data.
func NewLogEntry(index uint64, term uint64, data []byte, responseCh chan OperationResponse) *LogEntry {
	return &LogEntry{Index: index, Term: term, Data: data, ResponseCh: responseCh}
}

// IsConflict checks whether the current log entry conflicts with another log entry.
// Two log entries are considered conflicting if they have the same index but different terms.
func (e *LogEntry) IsConflict(other *LogEntry) bool {
	return e.Index == other.Index && e.Term != other.Term
}

// persistentLog implements the Log interface. Not concurrent safe.
type persistentLog struct {
	// The in-memory log entries of the log.
	entries []*LogEntry

	// The file that the log is written to.
	file *os.File

	// The path to the file that the log is written to.
	path string
}

// NewLog creates a new instance of Log at the provided path.
func NewLog(path string) Log {
	return &persistentLog{path: path}
}

func (l *persistentLog) Open() error {
	file, err := os.OpenFile(l.path, os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		return errors.WrapError(err, "failed to open log file: path = %s", l.path)
	}
	l.file = file
	l.entries = make([]*LogEntry, 0)
	return nil
}

func (l *persistentLog) Replay() error {
	reader := bufio.NewReader(l.file)

	for {
		entry, err := decodeLogEntry(reader)
		if err == io.EOF {
			break
		}
		if err != nil {
			return errors.WrapError(err, "failed to decode log entry")
		}
		l.entries = append(l.entries, &entry)
	}

	// The log must always contain at least one entry.
	// The first entry is a placeholder entry used for indexing into the log.
	if len(l.entries) == 0 {
		entry := NewLogEntry(0, 0, nil, nil)
		if err := encodeLogEntry(l.file, entry); err != nil {
			return errors.WrapError(err, "failed to encode log entry")
		}
		if err := l.file.Sync(); err != nil {
			return errors.WrapError(err, "failed to sync log file")
		}
		l.entries = append(l.entries, entry)
	}

	return nil
}

func (l *persistentLog) Close() error {
	if l.file == nil {
		return nil
	}
	if err := l.file.Close(); err != nil {
		return errors.WrapError(err, "failed to close log")
	}
	l.entries = nil
	l.file = nil
	return nil
}

func (l *persistentLog) GetEntry(index uint64) (*LogEntry, error) {
	if l.file == nil {
		return nil, errLogNotOpen
	}

	logIndex := index - l.entries[0].Index
	lastIndex := l.entries[len(l.entries)-1].Index
	if logIndex <= 0 || logIndex > lastIndex {
		return nil, errIndexDoesNotExist
	}

	entry := l.entries[logIndex]

	return entry, nil
}

func (l *persistentLog) Contains(index uint64) bool {
	logIndex := index - l.entries[0].Index
	return !(logIndex <= 0 || logIndex >= uint64(len(l.entries)))
}

func (l *persistentLog) AppendEntry(entry *LogEntry) error {
	return l.AppendEntries([]*LogEntry{entry})
}

func (l *persistentLog) AppendEntries(entries []*LogEntry) error {
	if l.file == nil {
		return errLogNotOpen
	}

	for _, entry := range entries {
		offset, err := l.file.Seek(0, io.SeekCurrent)
		if err != nil {
			return errors.WrapError(err, "failed to seek to offset in log file: offset = %d", offset)
		}
		entry.Offset = offset
		if err := encodeLogEntry(l.file, entry); err != nil {
			return errors.WrapError(err, "failed to encode log entry")
		}
	}

	if err := l.file.Sync(); err != nil {
		return errors.WrapError(err, "failed to sync log file")
	}

	l.entries = append(l.entries, entries...)

	return nil
}

func (l *persistentLog) Truncate(index uint64) error {
	if l.file == nil {
		return errLogNotOpen
	}

	logIndex := index - l.entries[0].Index
	if logIndex <= 0 || logIndex >= uint64(len(l.entries)) {
		return errIndexDoesNotExist
	}

	// The offset of the entry at the provided index is the
	// new size of the file.
	size := l.entries[logIndex].Offset

	if err := l.file.Truncate(size); err != nil {
		return errors.WrapError(err, "failed to truncate log file: size = %d", size)
	}

	// Update to I/O offset to the new size.
	if _, err := l.file.Seek(size, io.SeekStart); err != nil {
		return errors.WrapError(err, "failed to seek to offset in log file: offset = %d", size)
	}

	l.entries = l.entries[:logIndex]

	return nil
}

func (l *persistentLog) Compact(index uint64) error {
	if l.file == nil {
		return errLogNotOpen
	}

	logIndex := index - l.entries[0].Index
	if logIndex <= 0 || logIndex >= uint64(len(l.entries)) {
		return errIndexDoesNotExist
	}

	newEntries := make([]*LogEntry, uint64(len(l.entries))-logIndex)
	copy(newEntries[:], l.entries[logIndex:])

	// Create a temporary file to write the compacted log to.
	compactedFile, err := os.Create(l.path + ".bin")
	if err != nil {
		return errors.WrapError(err, "failed to create temporary log file")
	}

	// Write the entries contained in the compacted log to the
	// temporary file.
	for _, entry := range newEntries {
		offset, err := compactedFile.Seek(0, io.SeekCurrent)
		if err != nil {
			return errors.WrapError(err, "failed to seek to offset in log file: offset = %d", offset)
		}
		entry.Offset = offset
		if err := encodeLogEntry(compactedFile, entry); err != nil {
			return errors.WrapError(err, "failed to encode log entry")
		}
	}

	if err := compactedFile.Sync(); err != nil {
		return errors.WrapError(err, "failed to sync log file")
	}

	// Atomically rename the temporary file to the actual file.
	if err := os.Rename(compactedFile.Name(), l.path); err != nil {
		return errors.WrapError(err, "failed to rename temporary log file")
	}

	l.file = compactedFile
	l.entries = newEntries

	return nil
}

func (l *persistentLog) DiscardEntries(index uint64, term uint64) error {
	if l.file == nil {
		return errLogNotOpen
	}

	// Create a temporary file for the new log.
	newLogFile, err := os.Create(l.path + ".tmp")
	if err != nil {
		return errors.WrapError(err, "failed to create temporary log file")
	}

	// Write a placeholder entry to the temporary file with the provided term and index.
	entry := &LogEntry{Index: index, Term: term}
	if err := encodeLogEntry(newLogFile, entry); err != nil {
		return errors.WrapError(err, "failed to encode log entry")
	}
	if err := newLogFile.Sync(); err != nil {
		return errors.WrapError(err, "failed to sync log file")
	}

	// Atomically rename the temporary file to the actual file.
	if err := os.Rename(newLogFile.Name(), l.path); err != nil {
		return errors.WrapError(err, "failed to rename temporary log file")
	}

	l.file = newLogFile
	l.entries = []*LogEntry{entry}

	return nil
}

func (l *persistentLog) LastTerm() uint64 {
	return l.entries[len(l.entries)-1].Term
}

func (l *persistentLog) LastIndex() uint64 {
	return l.entries[len(l.entries)-1].Index
}

func (l *persistentLog) NextIndex() uint64 {
	return l.entries[len(l.entries)-1].Index + 1
}
