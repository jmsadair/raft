package raft

import "os"

// Log provides an interface for retrieving and storing
// log entries persistently.
type Log interface {
	// Open opens the log for retrieving and storing log entries. Require that the log is closed.
	Open() error

	// Close closes the log. Require that the log is open.
	Close() error

	// IsOpen returns true if the log is open and false otherwise.
	IsOpen() bool

	// GetEntry retrieves the log entry with the provided index from
	// the log. Require that a log entry with the provided index exists
	// in the log and that the log is open.
	GetEntry(index uint64) (*LogEntry, error)

	// Contains returns true if the log contains the provided index
	// and false otherwise.
	Contains(index uint64) bool

	// AppendEntries appends the provided log entries to the log;
	// returns the index of the last entry appended to the log or 0
	// if no entries were appended. Require that the log is is open.
	AppendEntries(entries ...*LogEntry) (uint64, error)

	// Truncate truncates the log starting from the provided index.
	// Require that a log entry with the provided index exists in the log
	// and that the log is open.
	Truncate(from uint64) error

	// LastTerm returns the last term written to the log. If the log is
	// empty, returns 0.
	LastTerm() uint64

	// FirstIndex returns the earliest index written to the log. If the log
	// is empty, returns 0.
	FirstIndex() uint64

	// LastIndex returns the last index written to the log. If the log is empty,
	// returns 0.
	LastIndex() uint64

	// Path returns the path to the file associated with the log.
	Path() string

	// File returns the file that is used to persist the log. If the log is
	// not open, returns nil.
	File() *os.File

	// Size returns the number of log entries in the log.
	Size() int
}

func NewLog(path string) Log {
	return NewPersistentLog(path)
}
