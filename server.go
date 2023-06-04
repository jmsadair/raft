package raft

import (
	"context"
	"net"
	"sync"

	"github.com/jmsadair/raft/internal/errors"
	pb "github.com/jmsadair/raft/internal/protobuf"
	"google.golang.org/grpc"
)

// GrpcServer is a wrapper for a RaftCore instance. It serves requests
// for Raft using protobuf and gRPC.
type GrpcServer struct {
	pb.UnimplementedRaftServer
	listenInterface net.Addr
	listener        net.Listener
	server          *grpc.Server
	raft            *RaftCore
	wg              sync.WaitGroup
}

// NewGrpcServer creates a new instance of GrpcServer.
func NewGrpcServer(
	id string,
	peers []*GrpcPeer,
	log Log, storage Storage,
	snapshotStorage SnapshotStorage,
	fsm StateMachine,
	listenInterface net.Addr,
	responseCh chan<- CommandResponse,
	opts ...Option) (*GrpcServer, error) {
	// Convert the array of GrpcPeer instances to Peer instances.
	raftPeers := make([]Peer, len(peers))
	for i := 0; i < len(peers); i++ {
		raftPeers[i] = peers[i]
	}

	raft, err := NewRaftCore(id, raftPeers, log, storage, snapshotStorage, fsm, responseCh, opts...)
	if err != nil {
		return nil, errors.WrapError(err, "failed to create new server: %s", err.Error())
	}

	server := &GrpcServer{
		listenInterface: listenInterface,
		raft:            raft,
	}

	return server, nil
}

// Start starts the GrpcServer. It listens for incoming connections on the configured
// network address and starts the Raft instance. It also starts serving gRPC requests on the
// listener. The provided channel is used to signal to the server that it should start serving
// requests.
func (s *GrpcServer) Start(ready <-chan interface{}) error {
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

// Stop stops the GrpcServer.
// It gracefully stops the gRPC server, stops the Raft instance, and closes the listener.
func (s *GrpcServer) Stop() {
	if s.server == nil {
		return
	}

	s.wg.Wait()
	s.server.GracefulStop()
	s.raft.Stop()
	s.listener.Close()
	s.server = nil
}

// Status returns the status of the GrpcServer.
// It retrieves the status from the underlying Raft instance.
func (s *GrpcServer) Status() Status {
	return s.raft.Status()
}

// IsStarted checks if the GrpcServer is started.
// It returns true if the server is started, false otherwise.
func (s *GrpcServer) IsStarted() bool {
	return s.server != nil
}

// SubmitCommand submits a command to the GrpcServer for processing.
// It forwards the command to the underlying RaftCore instance for handling
// and returns the index and term assigned to the command, as well as
// an error if submitting the command failed.
func (s *GrpcServer) SubmitCommand(command Command) (uint64, uint64, error) {
	return s.raft.SubmitCommand(command)
}

// AppendEntries handles the AppendEntries gRPC request.
// It converts the request to the internal representation, invokes the AppendEntries function on the RaftCore instance,
// and returns the response.
func (s *GrpcServer) AppendEntries(ctx context.Context, request *pb.AppendEntriesRequest) (*pb.AppendEntriesResponse, error) {
	appendEntriesRequest := makeAppendEntriesRequest(request)
	appendEntriesResponse := &AppendEntriesResponse{}
	if err := s.raft.AppendEntries(&appendEntriesRequest, appendEntriesResponse); err != nil {
		return nil, err
	}
	return makeProtoAppendEntriesResponse(*appendEntriesResponse), nil
}

// RequestVote handles the RequestVote gRPC request.
// It converts the request to the internal representation, invokes the RequestVote function on the RaftCore instance,
// and returns the response.
func (s *GrpcServer) RequestVote(ctx context.Context, request *pb.RequestVoteRequest) (*pb.RequestVoteResponse, error) {
	requestVoteRequest := makeRequestVoteRequest(request)
	requestVoteResponse := &RequestVoteResponse{}
	if err := s.raft.RequestVote(&requestVoteRequest, requestVoteResponse); err != nil {
		return nil, err
	}
	return makeProtoRequestVoteResponse(*requestVoteResponse), nil
}

// InstallSnapshot handles the InstallSnapshot gRPC request.
// It converts the request to the internal representation, invokes the InstallSnapshot function on the RaftCore instance,
// and returns the response.
func (s *GrpcServer) InstallSnapshot(ctx context.Context, request *pb.InstallSnapshotRequest) (*pb.InstallSnapshotResponse, error) {
	installSnapshotRequest := makeInstallSnapshotRequest(request)
	installSnapshotResponse := &InstallSnapshotResponse{}
	if err := s.raft.InstallSnapshot(&installSnapshotRequest, installSnapshotResponse); err != nil {
		return nil, err
	}
	return makeProtoInstallSnapshotResponse(*installSnapshotResponse), nil
}
