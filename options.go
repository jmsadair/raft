package raft

import (
	"time"
)

const (
	defaultElectionTimeout = time.Duration(150 * time.Millisecond)
	defaultHeartbeat       = time.Duration(50 * time.Millisecond)
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
	// The minimum amount of time in milliseconds that must elapse
	// before a raft node starts an election.
	electionTimeout time.Duration

	// The interval in milliseconds between AppendEntries RPCs that
	// the leader will send to the followers.
	heartbeatInterval time.Duration

	// A logger for debugging and important events.
	logger Logger
}

type Option func(options *options) error

// WithElectionTimeout sets the election timeout for the raft node.
func WithElectionTimeout(time time.Duration) Option {
	return func(options *options) error {
		options.electionTimeout = time
		return nil
	}
}

// WithHeartbeatIntervals sets the heartbeat interval for the raft node.
func WithHeartbeatInterval(time time.Duration) Option {
	return func(options *options) error {
		options.heartbeatInterval = time
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
