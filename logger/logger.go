package logger

import (
	"fmt"
	"io"
	"log"
	"os"
	"sync"
)

var (
	writer io.Writer = os.Stderr
	prefix string    = "raft"
	flag   int       = log.LstdFlags
	level  Level     = Debug
	logger *Logger   = NewWithLevel(level, writer, prefix, flag)
	mu     sync.Mutex
)

func SetDefaultWriter(newWriter io.Writer) {
	mu.Lock()
	defer mu.Unlock()
	writer = newWriter
}

func SetDefaultPrefix(newPrefix string) {
	mu.Lock()
	defer mu.Unlock()
	prefix = newPrefix
}

func SetDefaultFlag(newFlag int) {
	mu.Lock()
	defer mu.Unlock()
	flag = newFlag
}

func SetDefaultLevel(newLevel Level) {
	mu.Lock()
	defer mu.Unlock()
	level = newLevel
}

func GetLogger() Logger {
	mu.Lock()
	defer mu.Unlock()
	return *logger
}

func SetLogger(l Logger) {
	mu.Lock()
	defer mu.Unlock()
	logger = &l
}

type Logger struct {
	level Level
	base  *log.Logger
}

type Level int32

const (
	Debug Level = iota
	Info
	Warn
	Error
	Fatal
)

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
