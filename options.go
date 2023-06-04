package raft

import (
	"time"
)

const (
	defaultElectionTimeout       = time.Duration(300 * time.Millisecond)
	defaultHeartbeat             = time.Duration(50 * time.Millisecond)
	defaultMaxEntriesPerSnapshot = 100
)

// Logger supports logging messages at the debug, info, warn, error, and fatal level.
type Logger interface {
	// Debug logs a message at debug level.
	Debug(args ...interface{})

	// Debugf logs a formatted message at debug level.
	Debugf(format string, args ...interface{})

	// Info logs a message at info level.
	Info(args ...interface{})

	// Infof logs a formatted message at info level.
	Infof(format string, args ...interface{})

	// Warn logs a message at warn level.
	Warn(args ...interface{})

	// Warnf logs a formatted message at warn level.
	Warnf(format string, args ...interface{})

	// Error logs a message at error level.
	Error(args ...interface{})

	// Errorf logs a formatted message at error level.
	Errorf(format string, args ...interface{})

	// Fatal logs a message at fatal level.
	Fatal(args ...interface{})

	// Fatalf logs a formatted message at fatal level.
	Fatalf(format string, args ...interface{})
}

type options struct {
	// Minimum election timeout in milliseconds. A random time
	// between electionTimeout and 2 * electionTimeout will be
	// chosen to determine when a server will hold an election.
	electionTimeout time.Duration

	// The interval in milliseconds between AppendEntries RPCs that
	// the leader will send to the followers.
	heartbeatInterval time.Duration

	// The maximum number of log entries before a snapshot is triggered.
	// If the number of log entries since the last snapshot meets or
	// exceeds maxEntriesPerSnapshot, a snapshot will be taken.
	maxEntriesPerSnapshot int

	// Indicates whether raft should use snapshots.
	snapshottingEnabled bool

	// A logger for debugging and important events.
	logger Logger
}

type Option func(options *options) error

// WithElectionTimeout sets the election timeout for the Raft server.
func WithElectionTimeout(time time.Duration) Option {
	return func(options *options) error {
		options.electionTimeout = time
		return nil
	}
}

// WithHeartbeatInterval sets the heartbeat interval for the Raft server.
func WithHeartbeatInterval(time time.Duration) Option {
	return func(options *options) error {
		options.heartbeatInterval = time
		return nil
	}
}

// WithMaxLogEntriesPerSnapshot sets the maximum log entries per snapshot for the Raft server.
func WithMaxLogEntriesPerSnapshot(maxEntriesPerSnapshot int) Option {
	return func(options *options) error {
		options.maxEntriesPerSnapshot = maxEntriesPerSnapshot
		return nil
	}
}

// WithSnapshotting is used to indicate whether the Raft server should use
func WithSnapshotting(snaphottingEnabled bool) Option {
	return func(options *options) error {
		options.snapshottingEnabled = snaphottingEnabled
		return nil
	}
}

// WithLogger sets the logger used by the Raft server.
func WithLogger(logger Logger) Option {
	return func(options *options) error {
		options.logger = logger
		return nil
	}
}
