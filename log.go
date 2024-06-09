package raft

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"

	"github.com/jmsadair/raft/internal/fileutil"
	pb "github.com/jmsadair/raft/internal/protobuf"
	"google.golang.org/protobuf/proto"
)

// Log represents the internal component of Raft that is responsible
// for persistently storing and retrieving log entries.
type Log interface {
	// Open opens the log and prepares it for reads and writes.
	Open() error

	// Replay replays the content of the log on disk into memory.
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

	// Size returns the number of entries in the log.
	Size() int
}

// LogEntryType is the type of the log entry.
type LogEntryType uint32

const (
	// NoOpEntry log entries are those that do not contain an operation.
	NoOpEntry LogEntryType = iota

	// OperationEntry log entries are those that do contain an operation that
	// will be applied to the state machine.
	OperationEntry

	// ConfigurationEntry are log entries which contain a cluster configuration.
	ConfigurationEntry
)

// LogEntry is a log entry in the log.
type LogEntry struct {
	// The index of the log entry.
	Index uint64

	// The term of the log entry.
	Term uint64

	// The offset of the log entry.
	Offset int64

	// The data of the log entry.
	Data []byte

	// The type of the log entry.
	EntryType LogEntryType
}

// NewLogEntry creates a new instance of a LogEntry with the provided index, term, data, and type.
func NewLogEntry(index uint64, term uint64, data []byte, entryType LogEntryType) *LogEntry {
	return &LogEntry{Index: index, Term: term, Data: data, EntryType: entryType}
}

// IsConflict checks whether the current log entry conflicts with another log entry.
// Two log entries are considered conflicting if they have the same index but different terms.
func (e *LogEntry) IsConflict(other *LogEntry) bool {
	return e.Index == other.Index && e.Term != other.Term
}

func encodeLogEntry(w io.Writer, entry *LogEntry) error {
	pbEntry := &pb.LogEntry{
		Index:     entry.Index,
		Term:      entry.Term,
		Data:      entry.Data,
		Offset:    entry.Offset,
		EntryType: pb.LogEntry_LogEntryType(entry.EntryType),
	}

	buf, err := proto.Marshal(pbEntry)
	if err != nil {
		return fmt.Errorf("could not marshal protobuf message: %w", err)
	}

	size := int32(len(buf))
	if err := binary.Write(w, binary.BigEndian, size); err != nil {
		return fmt.Errorf("could not write length of protobuf message: %w", err)
	}

	if _, err := w.Write(buf); err != nil {
		return fmt.Errorf("could not write protobuf message: %w", err)
	}

	return nil
}

func decodeLogEntry(r io.Reader) (LogEntry, error) {
	var size int32
	if err := binary.Read(r, binary.BigEndian, &size); err != nil {
		return LogEntry{}, fmt.Errorf("could not read length of protobuf message: %w", err)
	}

	buf := make([]byte, size)
	if _, err := io.ReadFull(r, buf); err != nil {
		return LogEntry{}, fmt.Errorf("could not read protobuf messsage: %w", err)
	}

	pbEntry := &pb.LogEntry{}
	if err := proto.Unmarshal(buf, pbEntry); err != nil {
		return LogEntry{}, fmt.Errorf("could not unmarshal protobuf message: %w", err)
	}

	entry := LogEntry{
		Index:     pbEntry.GetIndex(),
		Term:      pbEntry.GetTerm(),
		Data:      pbEntry.GetData(),
		Offset:    pbEntry.GetOffset(),
		EntryType: LogEntryType(pbEntry.EntryType),
	}

	return entry, nil
}

// persistentLog implements the Log interface. Not concurrent safe.
type persistentLog struct {
	// The in-memory log entries of the log.
	entries []*LogEntry

	// The file that the log is written to.
	file *os.File

	// The directory where the log is persisted to.
	logDir string
}

// NewLog creates a new Log instance.
//
// The file containing the log will be created at path/log/log.bin.
// Any directories on the path that do not exist will be created.
func NewLog(path string) (Log, error) {
	logDir := filepath.Join(path, "log")
	if err := os.MkdirAll(logDir, fs.ModePerm); err != nil {
		return nil, fmt.Errorf("could not make directories for log file: %w", err)
	}

	// Delete any temporary files or directories that may have been partially written before a crash.
	if err := fileutil.RemoveTmpFiles(logDir); err != nil {
		return nil, fmt.Errorf("could not remove temporary files: %w", err)
	}

	return &persistentLog{logDir: logDir}, nil
}

func (l *persistentLog) Open() error {
	logFilename := filepath.Join(l.logDir, "log.bin")
	logFile, err := os.OpenFile(logFilename, os.O_RDWR|os.O_CREATE, 0o666)
	if err != nil {
		return fmt.Errorf("could not open or create log file: %w", err)
	}
	l.file = logFile
	l.entries = make([]*LogEntry, 0)
	return nil
}

func (l *persistentLog) Replay() error {
	reader := bufio.NewReader(l.file)

	for {
		entry, err := decodeLogEntry(reader)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return fmt.Errorf("could not decode log entry: %w", err)
		}
		l.entries = append(l.entries, &entry)
	}

	// The log must always contain at least one entry.
	// The first entry is a placeholder entry used for indexing into the log.
	if len(l.entries) == 0 {
		entry := &LogEntry{}
		if err := encodeLogEntry(l.file, entry); err != nil {
			return fmt.Errorf("could not encode log entry: %w", err)
		}
		if err := l.file.Sync(); err != nil {
			return fmt.Errorf("could not sync log file: %w", err)
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
		return fmt.Errorf("could not close log file: %w", err)
	}
	l.entries = nil
	l.file = nil
	return nil
}

func (l *persistentLog) GetEntry(index uint64) (*LogEntry, error) {
	if l.file == nil {
		return nil, errors.New("could not get entry: log not open")
	}
	if !l.Contains(index) {
		return nil, fmt.Errorf("could not get entry: index %d does not exist", index)
	}

	logIndex := index - l.entries[0].Index
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
		return errors.New("could not append entries: log not open")
	}

	for _, entry := range entries {
		offset, err := l.file.Seek(0, io.SeekCurrent)
		if err != nil {
			return fmt.Errorf("could not seek log file: %w", err)
		}
		entry.Offset = offset
		if err := encodeLogEntry(l.file, entry); err != nil {
			return fmt.Errorf("could not encode log entry: %w", err)
		}
	}

	if err := l.file.Sync(); err != nil {
		return fmt.Errorf("could not sync log file: %w", err)
	}

	l.entries = append(l.entries, entries...)

	return nil
}

func (l *persistentLog) Truncate(index uint64) error {
	if l.file == nil {
		return errors.New("could not truncate log: log not open")
	}
	if !l.Contains(index) {
		return fmt.Errorf("could not truncate log: index %d does not exist", index)
	}

	logIndex := index - l.entries[0].Index
	size := l.entries[logIndex].Offset

	if err := l.file.Truncate(size); err != nil {
		return fmt.Errorf("could not truncate log file: %w", err)
	}
	if err := l.file.Sync(); err != nil {
		return fmt.Errorf("could not sync log file: %w", err)
	}

	if _, err := l.file.Seek(size, io.SeekStart); err != nil {
		return fmt.Errorf("could not seek log file: %w", err)
	}

	l.entries = l.entries[:logIndex]

	return nil
}

func (l *persistentLog) Compact(index uint64) error {
	if l.file == nil {
		return errors.New("could not compact log: log not open")
	}
	if !l.Contains(index) {
		return fmt.Errorf("could not compact log: index %d does not exist", index)
	}

	logIndex := index - l.entries[0].Index
	newEntries := make([]*LogEntry, uint64(len(l.entries))-logIndex)
	copy(newEntries[:], l.entries[logIndex:])

	tmpFile, err := os.CreateTemp(l.logDir, "tmp-")
	if err != nil {
		return fmt.Errorf("could not create temporary file: %w", err)
	}

	for _, entry := range newEntries {
		offset, err := tmpFile.Seek(0, io.SeekCurrent)
		if err != nil {
			return fmt.Errorf("could not seek temporary file: %w", err)
		}
		entry.Offset = offset
		if err := encodeLogEntry(tmpFile, entry); err != nil {
			return fmt.Errorf("could not encode log entry: %w", err)
		}
	}

	if err := l.rename(tmpFile); err != nil {
		return fmt.Errorf("could not rename temporary file: %w", err)
	}

	l.entries = newEntries

	return nil
}

func (l *persistentLog) DiscardEntries(index uint64, term uint64) error {
	if l.file == nil {
		return errors.New("could not discard log: log not open")
	}

	tmpFile, err := os.CreateTemp(l.logDir, "tmp-log")
	if err != nil {
		return fmt.Errorf("could not create temporary file: %w", err)
	}

	entry := &LogEntry{Index: index, Term: term}
	if err := encodeLogEntry(tmpFile, entry); err != nil {
		return fmt.Errorf("could not encode log entry: %w", err)
	}

	if err := l.rename(tmpFile); err != nil {
		return fmt.Errorf("could not rename temporary file: %w", err)
	}

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

func (l *persistentLog) Size() int {
	return len(l.entries) - 1
}

func (l *persistentLog) rename(tmpFile *os.File) error {
	// Delete the temporary file if the rename is not successful.
	success := false
	defer func() {
		if !success {
			_ = os.Remove(tmpFile.Name())
		}
	}()

	// Ensure all data is on disk and the files are closed
	// before performing the rename.
	if err := tmpFile.Sync(); err != nil {
		return fmt.Errorf("could not sync file: %w", err)
	}
	if err := tmpFile.Close(); err != nil {
		return fmt.Errorf("could not close file: %w", err)
	}
	if err := l.file.Sync(); err != nil {
		return fmt.Errorf("could not sync file: %w", err)
	}
	if err := l.file.Close(); err != nil {
		return fmt.Errorf("could not close file: %w", err)
	}

	// Perform the rename.
	if err := os.Rename(tmpFile.Name(), l.file.Name()); err != nil {
		return fmt.Errorf("could not perform rename: %w", err)
	}

	// Ensure temporary file is not removed.
	success = true

	// Open the log file again and prepare it for writes.
	fileName := filepath.Join(l.logDir, "log.bin")
	file, err := os.OpenFile(fileName, os.O_RDWR, 0o666)
	if err != nil {
		return fmt.Errorf("could not open file: %w", err)
	}
	l.file = file
	if _, err := l.file.Seek(0, io.SeekEnd); err != nil {
		return fmt.Errorf("could not seek file: %w", err)
	}

	return nil
}
