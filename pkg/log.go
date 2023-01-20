package raft

// Log supports appending and retrieving log entries in
// in a durable manner. Must be concurrent-safe.
type Log interface {
	// GetEntry returns the log entry located at index.
	GetEntry(index uint64) (*LogEntry, error)

	// AppendEntry appends a log entry to the log.
	AppendEntry(entry *LogEntry) error

	// AppendEntries appends multiple log entries to the log.
	AppendEntries(entries []*LogEntry) error

	// Truncate deletes all log entries with index greater
	// than or equal to the provided index.
	Truncate(index uint64) error

	// Contains returns true if the index exists in the log and
	// false otherwise.
	Contains(index uint64) bool

	// FirstIndex returns the smallest index that exists in the log and
	// zero if the log is empty.
	FirstIndex() uint64

	// LastIndex returns the largest index that exists in the log and zero
	// if the log is empty.
	LastIndex() uint64

	// LastTerm returns the largest term in the log and zero if the log
	// is empty.
	LastTerm() uint64
}
