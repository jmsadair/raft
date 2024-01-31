package logging

import (
	"io"
	"log"
	"os"
)

var (
	defaultWriter io.Writer = os.Stderr
	defaultPrefix string    = "raft:"
	defaultFlag   int       = log.LstdFlags
)

type options struct {
	// The mechanism that the logger will use to log messages.
	writer io.Writer

	// The prefix that the logger will write before any message.
	prefix string

	// The flags for the logger.
	flag int

	// The level of the logger: debug, info, warn, error, fatal.
	level Level

	// Indicates whether the log level was set.
	levelSet bool
}

type Option func(options *options) error

// WithWriter sets the writer that will be used by the logger.
func WithWriter(w io.Writer) Option {
	return func(options *options) error {
		options.writer = w
		return nil
	}
}

// WithPrefix sets the prefix for the log.ger
func WithPrefix(prefix string) Option {
	return func(options *options) error {
		options.prefix = prefix
		return nil
	}
}

// WithFlag sets the flags used by the logger.
func WithFlag(flag int) Option {
	return func(options *options) error {
		options.flag = flag
		return nil
	}
}

// WithLevel sets the level of the logger.
func WithLevel(level Level) Option {
	return func(options *options) error {
		options.level = level
		options.levelSet = true
		return nil
	}
}
