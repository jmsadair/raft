package raft

import (
	"context"
	"net"

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
}

func NewServer(id string, peers []*Peer, log Log, storage Storage, listenInterface net.Addr, opts ...Option) (*Server, error) {
	raft, err := newRaft(id, peers, log, storage, opts...)
	if err != nil {
		return nil, errors.WrapError(err, "failed tro create new server: %s", err.Error())
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
	// TODO: configure grpc server options.
	var opts []grpc.ServerOption
	s.server = grpc.NewServer(opts...)
	pb.RegisterRaftServer(s.server, s)
	go s.server.Serve(listener)
	go func() {
		<-ready
		s.raft.start()
	}()
	return nil
}

func (s *Server) Stop() error {
	if s.server == nil {
		return errors.WrapError(nil, "server has not been started")
	}
	s.server.GracefulStop()
	s.raft.stop()
	s.server = nil
	s.listener = nil
	return nil
}

func (s *Server) IsStarted() bool {
	return s.server != nil
}

func (s *Server) Replicate(command []byte) error {
	return s.raft.replicate(command)
}

func (s *Server) AppendEntries(ctx context.Context, request *pb.AppendEntriesRequest) (*pb.AppendEntriesResponse, error) {
	return s.raft.appendEntries(request), nil
}

func (s *Server) RequestVote(ctx context.Context, request *pb.RequestVoteRequest) (*pb.RequestVoteResponse, error) {
	return s.raft.requestVote(request), nil
}
