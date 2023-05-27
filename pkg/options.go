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

// Logger supports logging messages at the debug, info, warn, error, and fatal level.
type Logger interface {
	// Debug logs a message at debug level.
	// Arguments:
	//   args: One or more arguments representing the message to be logged.
	Debug(args ...interface{})

	// Debugf logs a formatted message at debug level.
	// Arguments:
	//   format: A string representing the format of the log message.
	//   args: Arguments to be formatted and logged according to the format string.
	Debugf(format string, args ...interface{})

	// Info logs a message at info level.
	// Arguments:
	//   args: One or more arguments representing the message to be logged.
	Info(args ...interface{})

	// Infof logs a formatted message at info level.
	// Arguments:
	//   format: A string representing the format of the log message.
	//   args: Arguments to be formatted and logged according to the format string.
	Infof(format string, args ...interface{})

	// Warn logs a message at warn level.
	// Arguments:
	//   args: One or more arguments representing the message to be logged.
	Warn(args ...interface{})

	// Warnf logs a formatted message at warn level.
	// Arguments:
	//   format: A string representing the format of the log message.
	//   args: Arguments to be formatted and logged according to the format string.
	Warnf(format string, args ...interface{})

	// Error logs a message at error level.
	// Arguments:
	//   args: One or more arguments representing the message to be logged.
	Error(args ...interface{})

	// Errorf logs a formatted message at error level.
	// Arguments:
	//   format: A string representing the format of the log message.
	//   args: Arguments to be formatted and logged according to the format string.
	Errorf(format string, args ...interface{})

	// Fatal logs a message at fatal level.
	// Arguments:
	//   args: One or more arguments representing the message to be logged.
	Fatal(args ...interface{})

	// Fatalf logs a formatted message at fatal level.
	// Arguments:
	//   format: A string representing the format of the log message.
	//   args: Arguments to be formatted and logged according to the format string.
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

// WithElectionTimeout sets the election timeout for the Raft server.
//
// Parameters:
//
//	time: A duration representing the election timeout value.
//
// Returns:
//
//	An Option function that sets the election timeout in the options.
func WithElectionTimeout(time time.Duration) Option {
	return func(options *options) error {
		options.electionTimeout = time
		return nil
	}
}

// WithHeartbeatInterval sets the heartbeat interval for the Raft server.
//
// Parameters:
//
//	time: A duration representing the heartbeat interval value.
//
// Returns:
//
//	An Option function that sets the heartbeat interval in the options.
func WithHeartbeatInterval(time time.Duration) Option {
	return func(options *options) error {
		options.heartbeatInterval = time
		return nil
	}
}

// WithSnapshotInterval sets the snapshot interval for the Raft server.
//
// Parameters:
//
//	time: A duration representing the snapshot interval value.
//
// Returns:
//
//	An Option function that sets the snapshot interval in the options.
func WithSnapshotInterval(time time.Duration) Option {
	return func(options *options) error {
		options.snapshotInterval = time
		return nil
	}
}

// WithMaxLogEntries sets the maximum log entries for the Raft server.
//
// Parameters:
//
//	maxEntriesPerSnapshot: An unsigned integer representing the maximum log entries value.
//
// Returns:
//
//	An Option function that sets the maximum log entries in the options.
func WithMaxLogEntries(maxEntriesPerSnapshot uint64) Option {
	return func(options *options) error {
		options.maxEntriesPerSnapshot = maxEntriesPerSnapshot
		return nil
	}
}

// WithRestoreFromSnapshot is used to indicate whether the Raft server should attempt to restore
// from a snapshot upon initialization.
//
// Parameters:
//
//	restoreFromSnapshot: A boolean indicating whether to restore from a snapshot.
//
// Returns:
//
//	An Option function that sets the restore from snapshot flag in the options.
func WithRestoreFromSnapshot(restoreFromSnapshot bool) Option {
	return func(options *options) error {
		options.restoreFromSnapshot = restoreFromSnapshot
		return nil
	}
}

// WithLogger sets the logger used by the Raft server.
//
// Parameters:
//
//	logger: A Logger implementation for logging messages.
//
// Returns:
//
//	An Option function that sets the logger in the options.
func WithLogger(logger Logger) Option {
	return func(options *options) error {
		options.logger = logger
		return nil
	}
}
