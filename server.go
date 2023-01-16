package raft

import (
	"context"
	"net"

	"github.com/jmsadair/raft/internal/errors"
	pb "github.com/jmsadair/raft/internal/protobuf"
	"google.golang.org/grpc"
)

type raftServer struct {
	pb.UnimplementedRaftServer
	listenInterface string
	listener        net.Listener
	server          *grpc.Server
	raft            *Raft
}

func NewServer(raft *Raft, listenInterface string) *raftServer {
	return &raftServer{listenInterface: listenInterface, raft: raft}
}

func (s *raftServer) Start() error {
	listener, err := net.Listen("tcp", s.listenInterface)
	if err != nil {
		return errors.WrapError(err, "failed to start server: %s", err.Error())
	}
	s.listener = listener
	s.server = grpc.NewServer(nil)
	pb.RegisterRaftServer(s.server, s)
	go s.server.Serve(listener)
	return nil
}

func (s *raftServer) Stop() error {
	s.server.GracefulStop()
	if err := s.listener.Close(); err != nil {
		return errors.WrapError(err, "failed to stop server: %s", err.Error())
	}
	return nil
}

func (s *raftServer) AppendEntries(ctx context.Context, request *pb.AppendEntriesRequest) (*pb.AppendEntriesResponse, error) {
	return s.raft.appendEntries(request), nil
}

func (s *raftServer) RequestVote(ctx context.Context, request *pb.RequestVoteRequest) (*pb.RequestVoteResponse, error) {
	return s.raft.requestVote(request), nil
}
