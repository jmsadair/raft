package raft

import (
	"time"
)

const (
	defaultElectionTimeout       = time.Duration(200 * time.Millisecond)
	defaultHeartbeat             = time.Duration(50 * time.Millisecond)
	defaultSnapshotInterval      = time.Duration(200 * time.Millisecond)
	defaultMaxEntriesPerSnapshot = 100
)

// Logger supports logging message at the debug, info, warn, error, and
// fatal level.
type Logger interface {
	// Debug logs a message at debug level.
	Debug(args ...any)

	// Debugf logs a formatted message at debug level.
	Debugf(format string, args ...any)

	// Info logs a message at info level.
	Info(args ...any)

	// Infof logs a formatted message at info level.
	Infof(format string, args ...any)

	// Warn logs a message at warn level.
	Warn(args ...any)

	// Warnf logs a formatted message at warn level.
	Warnf(format string, args ...any)

	// Error logs a message at error level.
	Error(args ...any)

	// Errorf logs a formatted message at error level.
	Errorf(format string, args ...any)

	// Fatal logs a message at fatal level.
	Fatal(args ...any)

	// Fatalf logs a formatted message at fatal level.
	Fatalf(format string, args ...any)
}

type options struct {
	// Minimum election timeout in milliseconds. A random time
	// between electionTimeout and 2 * electionTimeout will be
	// chosen to determine when a server will hold an election.
	electionTimeout time.Duration

	// The interval in milliseconds between AppendEntries RPCs that
	// the leader will send to the followers.
	heartbeatInterval time.Duration

	// Minimum snapshot interval in milliseconds. A random time
	// between snapshotInterval and 2 * snapshotInterval will
	// be chosen to determine the interval between the servers checks
	// to see if it needs to snapshot.
	snapshotInterval time.Duration

	// The maximum number of log entries before a snapshot is triggered.
	// If the number of log entries since the last snapshot meets or
	// exceeds maxEntriesPerSnapshot, a snapshot will be taken.
	maxEntriesPerSnapshot uint64

	// Indicates whether raft should attempt to restore from the most recent
	// snapshot upon initialization.
	restoreFromSnapshot bool

	// A logger for debugging and important events.
	logger Logger
}

type Option func(options *options) error

// WithElectionTimeout sets the election timeout for the raft server.
func WithElectionTimeout(time time.Duration) Option {
	return func(options *options) error {
		options.electionTimeout = time
		return nil
	}
}

// WithHeartbeatIntervals sets the heartbeat interval for the raft server.
func WithHeartbeatInterval(time time.Duration) Option {
	return func(options *options) error {
		options.heartbeatInterval = time
		return nil
	}
}

// WithSnapshotInterval sets the snapshot interval for the raft server.
func WithSnapshotInterval(time time.Duration) Option {
	return func(options *options) error {
		options.snapshotInterval = time
		return nil
	}
}

// WithMaxLogEntries sets the maximum log entries for the raft server.
func WithMaxLogEntries(maxEntriesPerSnapshot uint64) Option {
	return func(options *options) error {
		options.maxEntriesPerSnapshot = maxEntriesPerSnapshot
		return nil
	}
}

// WithRestoreFromSnapshot is used to indicate whether the raft server
// should attempt to restore from snapshot upon initialization.
func WithRestoreFromSnapshot(restoreFromSnapshot bool) Option {
	return func(options *options) error {
		options.restoreFromSnapshot = restoreFromSnapshot
		return nil
	}
}

// WithLogger sets the logger used by the raft node.
func WithLogger(logger Logger) Option {
	return func(options *options) error {
		options.logger = logger
		return nil
	}
}
