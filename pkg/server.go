package raft

import (
	"context"
	"net"
	"sync"

	"github.com/jmsadair/raft/internal/errors"
	pb "github.com/jmsadair/raft/internal/protobuf"
	"google.golang.org/grpc"
)

type Server struct {
	pb.UnimplementedRaftServer
	listenInterface net.Addr
	listener        net.Listener
	server          *grpc.Server
	raft            *Raft
	wg              sync.WaitGroup
}

func makeEntries(protoEntries []*pb.LogEntry) []*LogEntry {
	entries := make([]*LogEntry, len(protoEntries))
	for i, protoEntry := range protoEntries {
		entry := &LogEntry{index: protoEntry.GetIndex(), term: protoEntry.GetTerm(), data: protoEntry.GetData()}
		entries[i] = entry
	}
	return entries
}

func makeRequestVoteRequest(request *pb.RequestVoteRequest) RequestVoteRequest {
	return RequestVoteRequest{
		candidateID:  request.GetCandidateId(),
		term:         request.GetTerm(),
		lastLogIndex: request.GetLastLogIndex(),
		lastLogTerm:  request.GetLastLogTerm(),
	}
}

func makeProtoRequestVoteResponse(response RequestVoteResponse) *pb.RequestVoteResponse {
	return &pb.RequestVoteResponse{
		Term:        response.term,
		VoteGranted: response.voteGranted,
	}
}

func makeAppendEntriesRequest(request *pb.AppendEntriesRequest) AppendEntriesRequest {
	return AppendEntriesRequest{
		leaderID:     request.GetLeaderId(),
		term:         request.GetTerm(),
		leaderCommit: request.GetLeaderCommit(),
		prevLogIndex: request.GetPrevLogIndex(),
		prevLogTerm:  request.GetPrevLogTerm(),
		entries:      makeEntries(request.GetEntries()),
	}
}

func makeProtoAppendEntriesResponse(response AppendEntriesResponse) *pb.AppendEntriesResponse {
	return &pb.AppendEntriesResponse{
		Success: response.success,
		Term:    response.term,
	}
}

func NewServer(id string, peers []Peer, log Log, storage Storage, snapshotStorage SnapshotStorage, fsm StateMachine, listenInterface net.Addr, responseCh chan<- CommandResponse, opts ...Option) (*Server, error) {
	raft, err := NewRaft(id, peers, log, storage, snapshotStorage, fsm, responseCh, opts...)
	if err != nil {
		return nil, errors.WrapError(err, "failed to create new server: %s", err.Error())
	}

	server := &Server{
		listenInterface: listenInterface,
		raft:            raft,
	}

	return server, nil
}

func (s *Server) Start(ready <-chan interface{}) error {
	listener, err := net.Listen(s.listenInterface.Network(), s.listenInterface.String())
	if err != nil {
		return errors.WrapError(err, "failed to start server: %s", err.Error())
	}

	s.listener = listener
	var opts []grpc.ServerOption
	s.server = grpc.NewServer(opts...)
	pb.RegisterRaftServer(s.server, s)

	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		<-ready
		s.raft.Start()
		go s.server.Serve(listener)
	}()

	return nil
}

func (s *Server) Stop() {
	if s.server == nil {
		return
	}

	s.wg.Wait()
	s.server.GracefulStop()
	s.raft.Stop()
	s.listener.Close()
	s.server = nil
}

func (s *Server) Status() Status {
	return s.raft.Status()
}

func (s *Server) IsStarted() bool {
	return s.server != nil
}

func (s *Server) SubmitCommand(command Command) (uint64, uint64, error) {
	return s.raft.SubmitCommand(command)
}

func (s *Server) AppendEntries(ctx context.Context, request *pb.AppendEntriesRequest) (*pb.AppendEntriesResponse, error) {
	return makeProtoAppendEntriesResponse(s.raft.appendEntries(makeAppendEntriesRequest(request))), nil
}

func (s *Server) RequestVote(ctx context.Context, request *pb.RequestVoteRequest) (*pb.RequestVoteResponse, error) {
	return makeProtoRequestVoteResponse(s.raft.requestVote(makeRequestVoteRequest(request))), nil
}

func (s *Server) InstallSnapshot(ctx context.Context, request *pb.InstallSnapshotRequest) (*pb.InstallSnapshotResponse, error) {
	return nil, nil
}
