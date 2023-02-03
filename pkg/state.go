package raft

import (
	"sync"
	"time"
)

type State uint32

const (
	Leader State = iota
	Follower
	Candidate
	Shutdown
)

func (s State) String() string {
	switch s {
	case Leader:
		return "leader"
	case Follower:
		return "follower"
	case Candidate:
		return "candidate"
	case Shutdown:
		return "shutdown"
	default:
		panic("invalid state")
	}
}

type RaftState struct {
	state             State
	commitIndex       uint64
	lastApplied       uint64
	currentTerm       uint64
	votedFor          string
	lastSnapshotIndex uint64
	lastSnapshotTerm  uint64
	lastContact       time.Time
	mu                sync.RWMutex
}

func (rs *RaftState) setState(state State) {
	rs.mu.Lock()
	defer rs.mu.Unlock()
	rs.state = state
}

func (rs *RaftState) getState() State {
	rs.mu.RLock()
	defer rs.mu.RUnlock()
	return rs.state
}

func (rs *RaftState) setCommitIndex(commitIndex uint64) {
	rs.mu.Lock()
	defer rs.mu.Unlock()
	rs.commitIndex = commitIndex
}

func (rs *RaftState) getCommitIndex() uint64 {
	rs.mu.RLock()
	defer rs.mu.RUnlock()
	return rs.commitIndex
}

func (rs *RaftState) setLastApplied(lastApplied uint64) {
	rs.mu.Lock()
	defer rs.mu.Unlock()
	rs.lastApplied = lastApplied
}

func (rs *RaftState) getLastApplied() uint64 {
	rs.mu.RLock()
	defer rs.mu.RUnlock()
	return rs.lastApplied
}

func (rs *RaftState) setCurrentTerm(currentTerm uint64) {
	rs.mu.Lock()
	defer rs.mu.Unlock()
	rs.currentTerm = currentTerm
}

func (rs *RaftState) getCurrentTerm() uint64 {
	rs.mu.RLock()
	defer rs.mu.RUnlock()
	return rs.currentTerm
}

func (rs *RaftState) setVotedFor(votedFor string) {
	rs.mu.Lock()
	defer rs.mu.Unlock()
	rs.votedFor = votedFor
}

func (rs *RaftState) getVotedFor() string {
	rs.mu.RLock()
	defer rs.mu.RUnlock()
	return rs.votedFor
}

func (rs *RaftState) setLastContact(lastContact time.Time) {
	rs.mu.Lock()
	defer rs.mu.Unlock()
	rs.lastContact = lastContact
}

func (rs *RaftState) getLastContact() time.Time {
	rs.mu.RLock()
	defer rs.mu.RUnlock()
	return rs.lastContact
}

func (rs *RaftState) setLastSnapshotIndex(lastSnapshotIndex uint64) {
	rs.mu.Lock()
	defer rs.mu.Unlock()
	rs.lastSnapshotIndex = lastSnapshotIndex
}

func (rs *RaftState) getLastSnapshotIndex() uint64 {
	rs.mu.RLock()
	defer rs.mu.RUnlock()
	return rs.lastSnapshotIndex
}

func (rs *RaftState) setLastSnapshotTerm(lastSnapshotTerm uint64) {
	rs.mu.Lock()
	defer rs.mu.Unlock()
	rs.lastSnapshotTerm = lastSnapshotTerm
}

func (rs *RaftState) getLastSnapshotTerm() uint64 {
	rs.mu.RLock()
	defer rs.mu.RUnlock()
	return rs.lastSnapshotTerm
}
