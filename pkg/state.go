package raft

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
