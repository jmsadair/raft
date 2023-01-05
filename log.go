package raft

import (
	"log"
	"os"
	"path/filepath"
)

type Log struct {
	path        string
	file        *os.File
	entries     []*LogEntry
	commitIndex uint64
	lastApplied uint64
}

func NewLog(path string) *Log {
	return &Log{path: filepath.Join(path, "log"), entries: make([]*LogEntry, 0)}
}

func (l *Log) Open() error {
	file, err := os.OpenFile(l.path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Fatalf("failed to open log file: %s", err.Error())
	}
	l.file = file

	// TODO: read persisted data into memory.

	return nil
}
