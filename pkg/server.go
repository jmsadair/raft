package raft

import (
	"context"
	"net"

	"github.com/jmsadair/raft/internal/errors"
	pb "github.com/jmsadair/raft/internal/protobuf"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/internal/status"
)

type Server struct {
	pb.UnimplementedRaftServer
	listenInterface net.Addr
	listener        net.Listener
	server          *grpc.Server
	raft            *Raft
}

func NewServer(id string, peers []*Peer, log Log, storage Storage, snapshotStorage SnapshotStorage, fsm StateMachine, listenInterface net.Addr, responseCh chan<- CommandResponse, opts ...Option) (*Server, error) {
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
	go s.server.Serve(listener)
	go func() {
		<-ready
		s.raft.Start()
	}()
	return nil
}

func (s *Server) Stop() error {
	if s.server == nil {
		return errors.WrapError(nil, "server has not been started")
	}
	s.server.GracefulStop()
	s.listener.Close()
	s.raft.Stop()
	s.server = nil
	s.listener = nil
	return nil
}

func (s *Server) IsStarted() bool {
	return s.server != nil
}

func (s *Server) SubmitCommand(command Command) (uint64, error) {
	return s.raft.SubmitCommand(command)
}

func (s *Server) AppendEntries(ctx context.Context, request *pb.AppendEntriesRequest) (*pb.AppendEntriesResponse, error) {
	if request == nil {
		return nil, status.Errorf(codes.InvalidArgument, "received nil request")
	}

	rpc := AppendEntriesRPC{request: request, responseCh: make(chan *pb.AppendEntriesResponse)}
	s.raft.appendEntriesCh <- rpc
	response := <-rpc.responseCh
	return response, nil
}

func (s *Server) RequestVote(ctx context.Context, request *pb.RequestVoteRequest) (*pb.RequestVoteResponse, error) {
	if request == nil {
		return nil, status.Errorf(codes.InvalidArgument, "received nil request")
	}

	rpc := RequestVoteRPC{request: request, responseCh: make(chan *pb.RequestVoteResponse)}
	s.raft.requestVoteCh <- rpc
	response := <-rpc.responseCh
	return response, nil
}

func (s *Server) InstallSnapshot(ctx context.Context, request *pb.InstallSnapshotRequest) (*pb.InstallSnapshotResponse, error) {
	if request == nil {
		return nil, status.Errorf(codes.InvalidArgument, "received nil request")
	}

	rpc := InstallSnapshotRPC{request: request, responseCh: make(chan *pb.InstallSnapshotResponse)}
	s.raft.installSnapshotCh <- rpc
	response := <-rpc.responseCh
	return response, nil
}
