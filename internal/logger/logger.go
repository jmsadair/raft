package logger

import (
	"fmt"
	"io"
	"log"
	"os"
)

var (
	defaultWriter io.Writer = os.Stderr
	defaultPrefix string    = "raft "
	defaultFlag   int       = log.LstdFlags
	defaultLevel  Level     = Debug
)

func DefaultLogger() Logger {
	return Logger{level: defaultLevel, base: log.New(defaultWriter, defaultPrefix, defaultFlag)}
}

type Level int32

const (
	Debug Level = iota
	Info
	Warn
	Error
	Fatal
)

type Logger struct {
	level Level
	base  *log.Logger
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

func New(w io.Writer, prefix string, flag int) *Logger {
	return &Logger{level: Debug, base: log.New(w, prefix, flag)}
}

func NewWithLevel(level Level, w io.Writer, prefix string, flag int) *Logger {
	return &Logger{level: level, base: log.New(w, prefix, flag)}
}

func (l *Logger) Debug(args ...any) {
	if l.level > Debug {
		return
	}
	l.print("DEBUG: ", args)
}

func (l *Logger) Debugf(format string, args ...any) {
	l.Debug(fmt.Sprintf(format, args...))
}

func (l *Logger) Info(args ...any) {
	if l.level > Info {
		return
	}
	l.print("INFO: ", args)
}

func (l *Logger) Infof(format string, args ...any) {
	l.Info(fmt.Sprintf(format, args...))
}

func (l *Logger) Warn(args ...any) {
	if l.level > Warn {
		return
	}
	l.print("WARN: ", args)
}

func (l *Logger) Warnf(format string, args ...any) {
	l.Warn(fmt.Sprintf(format, args...))
}

func (l *Logger) Error(args ...any) {
	if l.level > Error {
		return
	}
	l.print("ERROR: ", args)
}

func (l *Logger) Errorf(format string, args ...any) {
	l.Error(fmt.Sprintf(format, args...))
}

func (l *Logger) Fatal(args ...any) {
	if l.level > Fatal {
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
