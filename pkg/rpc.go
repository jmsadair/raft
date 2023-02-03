package raft

import pb "github.com/jmsadair/raft/internal/protobuf"

type AppendEntriesRPC struct {
	request    *pb.AppendEntriesRequest
	responseCh chan *pb.AppendEntriesResponse
}

type AppendEntriesMessage struct {
	response           *pb.AppendEntriesResponse
	peer               *Peer
	numEntriesAppended uint64
}

type RequestVoteRPC struct {
	request    *pb.RequestVoteRequest
	responseCh chan *pb.RequestVoteResponse
}

type InstallSnapshotRPC struct {
	request    *pb.InstallSnapshotRequest
	responseCh chan *pb.InstallSnapshotResponse
}

type InstallSnapshotMessage struct {
	response          *pb.InstallSnapshotResponse
	peer              *Peer
	lastIncludedIndex uint64
}
