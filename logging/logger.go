package logging

import (
	"fmt"
	"log"
	"os"
)

// Level defines the log severity levels.
type Level int32

// Enumeration of log levels from least to most severe.
const (
	Debug Level = iota
	Info
	Warn
	Error
	Fatal
)

// Logger represents the logging structure with configurable options.
type Logger struct {
	// Logging options that determine behavior such as output destination and log level.
	options options

	// The underlying standard logger.
	base *log.Logger
}

// String provides a string representation of the logging level.
func (l Level) String() string {
	switch l {
	case Debug:
		return "DEBUG"
	case Info:
		return "INFO"
	case Warn:
		return "WARN"
	case Error:
		return "ERROR"
	case Fatal:
		return "FATAL"
	default:
		panic("invalid log level")
	}
}

// NewLogger creates a new logger instance with the provided options.
// If no options are provided, default values are used.
func NewLogger(opts ...Option) (*Logger, error) {
	var options options
	for _, opt := range opts {
		if err := opt(&options); err != nil {
			return nil, err
		}
	}

	if options.writer == nil {
		options.writer = defaultWriter
	}
	if options.flag == 0 {
		options.flag = defaultFlag
	}
	if options.prefix == "" {
		options.prefix = defaultPrefix
	}
	if !options.levelSet {
		options.level = Info
	}

	return &Logger{
		options: options,
		base:    log.New(options.writer, options.prefix, options.flag),
	}, nil
}

// Debug logs a debug message with the given arguments.
func (l *Logger) Debug(args ...any) {
	if l.options.level > Debug {
		return
	}
	l.print("DEBUG: ", args)
}

// Debugf logs a formatted debug message.
func (l *Logger) Debugf(format string, args ...any) {
	l.Debug(fmt.Sprintf(format, args...))
}

// Info logs an informational message.
func (l *Logger) Info(args ...any) {
	if l.options.level > Info {
		return
	}
	l.print("INFO: ", args)
}

// Infof logs a formatted informational message.
func (l *Logger) Infof(format string, args ...any) {
	l.Info(fmt.Sprintf(format, args...))
}

// Warn logs a warning message.
func (l *Logger) Warn(args ...any) {
	if l.options.level > Warn {
		return
	}
	l.print("WARN: ", args)
}

// Warnf logs a formatted warning message.
func (l *Logger) Warnf(format string, args ...any) {
	l.Warn(fmt.Sprintf(format, args...))
}

// Error logs an error message.
func (l *Logger) Error(args ...any) {
	if l.options.level > Error {
		return
	}
	l.print("ERROR: ", args)
}

// Errorf logs a formatted error message.
func (l *Logger) Errorf(format string, args ...any) {
	l.Error(fmt.Sprintf(format, args...))
}

// Fatal logs a fatal error message and then terminates the program.
func (l *Logger) Fatal(args ...any) {
	if l.options.level > Fatal {
		return
	}
	l.print("FATAL: ", args)
	os.Exit(1)
}

// Fatalf logs a formatted fatal error message and then terminates the program.
func (l *Logger) Fatalf(format string, args ...any) {
	l.Fatal(fmt.Sprintf(format, args...))
}

// print is a utility function that prints a log message to the logger's output with the given prefix.
func (l *Logger) print(prefix string, args ...any) {
	args = append([]any{prefix}, args...)
	l.base.Print(args...)
}
