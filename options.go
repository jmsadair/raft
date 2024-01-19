package raft

import (
	"errors"
	"fmt"
	"time"
)

const (
	minElectionTimeout     = time.Duration(100 * time.Millisecond)
	maxElectionTimeout     = time.Duration(2000 * time.Millisecond)
	defaultElectionTimeout = time.Duration(300 * time.Millisecond)

	minHeartbeat     = time.Duration(25 * time.Millisecond)
	maxHeartbeat     = time.Duration(300 * time.Millisecond)
	defaultHeartbeat = time.Duration(50 * time.Millisecond)

	minLeaseDuration     = time.Duration(25 * time.Millisecond)
	maxLeaseDuration     = time.Duration(500 * time.Millisecond)
	defaultLeaseDuration = time.Duration(100 * time.Millisecond)
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

	// The duration that a lease remains valid upon renewal.
	leaseDuration time.Duration

	// A logger for debugging and important events.
	logger Logger

	// A provided log that can be used by raft.
	log Log

	// A provided state storage that can be used by raft.
	stateStorage StateStorage

	// A provided snapshot storage that can be used by raft.
	snapshotStorage SnapshotStorage

	// A provided network transport that can be used by raft.
	transport Transport
}

// Option is a function that updates the options associated with Raft.
type Option func(options *options) error

// WithElectionTimeout sets the election timeout for raft.
func WithElectionTimeout(time time.Duration) Option {
	return func(options *options) error {
		if time < minElectionTimeout || time > maxElectionTimeout {
			return fmt.Errorf("election timeout value is invalid: minimum = %v, maximum = %v",
				minElectionTimeout, maxElectionTimeout)
		}
		options.electionTimeout = time
		return nil
	}
}

// WithHeartbeatInterval sets the heartbeat interval for raft.
func WithHeartbeatInterval(time time.Duration) Option {
	return func(options *options) error {
		if time < minHeartbeat || time > maxHeartbeat {
			return fmt.Errorf("heartbeat interval value is invalid: minimum = %v, maximum = %v",
				minHeartbeat, maxHeartbeat)
		}
		options.heartbeatInterval = time
		return nil
	}
}

// WithLeaseDuration sets the duration for which a lease remains valid upon
// renewal. The lease should generally remain valid for a much smaller amount of
// time than the election timeout.
func WithLeaseDuration(leaseDuration time.Duration) Option {
	return func(options *options) error {
		if leaseDuration < minLeaseDuration || leaseDuration > maxLeaseDuration {
			return fmt.Errorf("lease duration is invalid: minimum = %v, maximum = %v",
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

// WithLog sets the log that will be used by raft. This is useful
// if you wish to use your own implementation of a log.
func WithLog(log Log) Option {
	return func(options *options) error {
		if log == nil {
			return errors.New("log must not be nil")
		}
		options.log = log
		return nil
	}
}

// WithStateStorage sets the state storage that will be used by raft.
// This is useful if you wish to use your own implementation of a state storage.
func WithStateStorage(stateStorage StateStorage) Option {
	return func(options *options) error {
		if stateStorage == nil {
			return errors.New("state storage must not be nil")
		}
		options.stateStorage = stateStorage
		return nil
	}
}

// WithSnapshotStorage sets the snapshot storage that will be used by raft.
// This is useful if you wish to use your own implementation of a snapshot storage.
func WithSnapshotStorage(snapshotStorage SnapshotStorage) Option {
	return func(options *options) error {
		if snapshotStorage == nil {
			return errors.New("snapshot storage must not be nil")
		}
		options.snapshotStorage = snapshotStorage
		return nil
	}
}

// WithTransport sets the network transport that will be used by raft.
// This is useful if you wish to use your own implementation of a transport.
func WithTransport(transport Transport) Option {
	return func(options *options) error {
		if transport == nil {
			return errors.New("transport must not be nil")
		}
		options.transport = transport
		return nil
	}
}
