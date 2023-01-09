package raft

import "fmt"

// Log Errors

type EntryConflictError struct {
	Index        uint64
	Term         uint64
	ExistingTerm uint64
}

func (e *EntryConflictError) Error() string {
	return fmt.Sprintf("conflicting entry at index %d, terms not equal: %d != %d (provided != existing)", e.Index, e.ExistingTerm, e.Term)
}
