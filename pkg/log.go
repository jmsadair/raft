package raft

type LogEntry struct {
	term  uint64
	index uint64
	data  []byte
}

func NewLogEntry(index uint64, term uint64, data []byte) *LogEntry {
	return &LogEntry{term: term, index: index, data: data}
}

func (e *LogEntry) Term() uint64 {
	return e.term
}

func (e *LogEntry) Index() uint64 {
	return e.index
}

func (e *LogEntry) Data() []byte {
	return e.data
}

func (e *LogEntry) IsConflict(other *LogEntry) bool {
	return e.index == other.index && e.term != other.term
}

// Log supports appending and retrieving log entries in
// in a durable manner. Must be concurrent-safe.
type Log interface {
	// GetEntry returns the log entry located at index.
	GetEntry(index uint64) (*LogEntry, error)

	// AppendEntry appends a log entry to the log.
	AppendEntry(entry *LogEntry)

	// AppendEntries appends multiple log entries to the log.
	AppendEntries(entries []*LogEntry)

	// Truncate deletes all log entries with index greater
	// than or equal to the provided index.
	Truncate(index uint64) error

	// Compact deletes all log entries with index less than
	// or equal to the provided index.
	Compact(index uint64) error

	// Contains returns true if the index exists in the log and
	// false otherwise.
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
