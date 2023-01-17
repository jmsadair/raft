package raft

import "sync"

type State uint32

const (
	Leader State = iota
	Follower
	Candidate
	Stopped
)

func (s State) String() string {
	switch s {
	case Leader:
		return "leader"
	case Follower:
		return "follower"
	case Candidate:
		return "candidate"
	case Stopped:
		return "stopped"
	default:
		panic("invalid state")
	}
}

// Concurrent-safe state for each raft node.
type raftState struct {
	// The current state of the node: leader, candidate, follower, or stopped.
	state State

	// Index of highest log entry known to be committed.
	commitIndex uint64

	// Index of highest log entry applied to state machine.
	lastApplied uint64

	// The latest term that the raft node has seen.
	currentTerm uint64

	// The candidate that received the vote in the current term (empty string if none).
	votedFor string

	mu sync.RWMutex
}

func NewRaftState() *raftState {
	return &raftState{}
}

func (rs *raftState) setState(state State) {
	rs.mu.Lock()
	defer rs.mu.Unlock()
	rs.state = state
}

func (rs *raftState) getState() State {
	rs.mu.RLock()
	defer rs.mu.RUnlock()
	return rs.state
}

func (rs *raftState) setCommitIndex(index uint64) {
	rs.mu.Lock()
	defer rs.mu.Unlock()
	rs.commitIndex = index
}

func (rs *raftState) getCommitIndex() uint64 {
	rs.mu.RLock()
	defer rs.mu.RUnlock()
	return rs.commitIndex
}

func (rs *raftState) setLastApplied(lastApplied uint64) {
	rs.mu.Lock()
	defer rs.mu.Unlock()
	rs.lastApplied = lastApplied
}

func (rs *raftState) setCurrentTerm(term uint64) {
	rs.mu.Lock()
	defer rs.mu.Unlock()
	rs.currentTerm = term
}

func (rs *raftState) getCurrentTerm() uint64 {
	rs.mu.RLock()
	defer rs.mu.RUnlock()
	return rs.currentTerm
}

func (rs *raftState) setVotedFor(votedFor string) {
	rs.mu.Lock()
	defer rs.mu.Unlock()
	rs.votedFor = votedFor
}

func (rs *raftState) getVotedFor() string {
	rs.mu.RLock()
	defer rs.mu.RUnlock()
	return rs.votedFor
}
