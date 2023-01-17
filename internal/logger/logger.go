package logger

import (
	"fmt"
	"log"
	"os"
)

type Level int32

const (
	Debug Level = iota
	Info
	Warn
	Error
	Fatal
)

type Logger struct {
	options options
	base    *log.Logger
}

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

	return &Logger{options: options, base: log.New(options.writer, options.prefix, options.flag)}, nil
}

func (l *Logger) Debug(args ...any) {
	if l.options.level > Debug {
		return
	}
	l.print("DEBUG: ", args)
}

func (l *Logger) Debugf(format string, args ...any) {
	l.Debug(fmt.Sprintf(format, args...))
}

func (l *Logger) Info(args ...any) {
	if l.options.level > Info {
		return
	}
	l.print("INFO: ", args)
}

func (l *Logger) Infof(format string, args ...any) {
	l.Info(fmt.Sprintf(format, args...))
}

func (l *Logger) Warn(args ...any) {
	if l.options.level > Warn {
		return
	}
	l.print("WARN: ", args)
}

func (l *Logger) Warnf(format string, args ...any) {
	l.Warn(fmt.Sprintf(format, args...))
}

func (l *Logger) Error(args ...any) {
	if l.options.level > Error {
		return
	}
	l.print("ERROR: ", args)
}

func (l *Logger) Errorf(format string, args ...any) {
	l.Error(fmt.Sprintf(format, args...))
}

func (l *Logger) Fatal(args ...any) {
	if l.options.level > Fatal {
		return
	}
	l.print("FATAL: ", args)
	os.Exit(1)
}

func (l *Logger) Fatalf(format string, args ...any) {
	l.Fatal(fmt.Sprintf(format, args...))
}

func (l *Logger) print(prefix string, args ...any) {
	args = append([]any{prefix}, args...)
	l.base.Print(args...)
}
