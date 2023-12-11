package raft

import (
	"time"

	"github.com/jmsadair/raft/internal/errors"
)

const (
	minElectionTimeout     = time.Duration(100 * time.Millisecond)
	maxElectionTimeout     = time.Duration(2000 * time.Millisecond)
	defaultElectionTimeout = time.Duration(300 * time.Millisecond)

	minHeartbeat     = time.Duration(25 * time.Millisecond)
	maxHeartbeat     = time.Duration(300 * time.Millisecond)
	defaultHeartbeat = time.Duration(50 * time.Millisecond)

	minMaxEntriesPerRPC     = 50
	maxMaxEntriesPerRPC     = 500
	defaultMaxEntriesPerRPC = 100

	minLeaseDuration     = time.Duration(25 * time.Millisecond)
	maxLeaseDuration     = time.Duration(1000 * time.Millisecond)
	defaultLeaseDuration = time.Duration(300 * time.Millisecond)
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

	// The maximum number of log entries that will be transmitted via
	// an AppendEntries RPC.
	maxEntriesPerRPC int

	// The duration that a lease remains valid upon renewal.
	leaseDuration time.Duration

	// A logger for debugging and important events.
	logger Logger
}

// Option is a function that updates the options associated with Raft.
type Option func(options *options) error

// WithElectionTimeout sets the election timeout for the Raft server.
func WithElectionTimeout(time time.Duration) Option {
	return func(options *options) error {
		if time < minElectionTimeout || time > maxElectionTimeout {
			return errors.New("election timeout value is invalid: minimum = %v, maximum = %v",
				minElectionTimeout, maxElectionTimeout)
		}
		options.electionTimeout = time
		return nil
	}
}

// WithHeartbeatInterval sets the heartbeat interval for the Raft server.
func WithHeartbeatInterval(time time.Duration) Option {
	return func(options *options) error {
		if time < minHeartbeat || time > maxHeartbeat {
			return errors.New("heartbeat interval value is invalid: minimum = %v, maximum = %v",
				minHeartbeat, maxHeartbeat)
		}
		options.heartbeatInterval = time
		return nil
	}
}

// WithMaxEntriesPerRPC sets the maximum number of log entries that can be
// transmitted via an AppendEntries RPC.
func WithMaxEntriesPerRPC(maxEntriesPerRPC int) Option {
	return func(options *options) error {
		if maxEntriesPerRPC < minMaxEntriesPerRPC || maxEntriesPerRPC > maxMaxEntriesPerRPC {
			return errors.New(
				"maximum entries per RPC value is invalid: minimum = %v, maximum = %v",
				minMaxEntriesPerRPC,
				maxMaxEntriesPerRPC,
			)
		}
		options.maxEntriesPerRPC = maxEntriesPerRPC
		return nil
	}
}

// WithLeaseDuration sets the duration for which a lease remains valid upon
// renewal. A longer lease duration generally corresponds to higher performance
// of read-only operations but increases the likelihood that stale data is read.
// Likewise, a shorter lease duration corresponds to lesser performance of read-only
// operations but decreases the likelihood of stale data being read.
func WithLeaseDuration(leaseDuration time.Duration) Option {
	return func(options *options) error {
		if leaseDuration < minLeaseDuration || leaseDuration > maxLeaseDuration {
			return errors.New("lease duration is invalid: minimum = %v, maximum = %v",
				minLeaseDuration, maxLeaseDuration)
		}
		options.leaseDuration = leaseDuration
		return nil
	}
}

// WithLogger sets the logger used by the Raft.
func WithLogger(logger Logger) Option {
	return func(options *options) error {
		if logger == nil {
			return errors.New("logger must not be nil")
		}
		options.logger = logger
		return nil
	}
}
