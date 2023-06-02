package raft

import (
	"context"
	"net"
	"sync"

	"github.com/jmsadair/raft/internal/errors"
	pb "github.com/jmsadair/raft/internal/protobuf"
	"google.golang.org/grpc"
)

// ProtobufServer is a wrapper for a Raft instance. It serves requests
// for Raft using protobuf and gRPC.
type ProtobufServer struct {
	pb.UnimplementedRaftServer
	listenInterface net.Addr
	listener        net.Listener
	server          *grpc.Server
	raft            *Raft
	wg              sync.WaitGroup
}

// NewProtobufServer creates a new instance of ProtobufServer.
func NewProtobufServer(
	id string,
	peers []*ProtobufPeer,
	log Log, storage Storage,
	snapshotStorage SnapshotStorage,
	fsm StateMachine,
	listenInterface net.Addr,
	responseCh chan<- CommandResponse,
	opts ...Option) (*ProtobufServer, error) {
	// Convert the array of ProtobufPeer instances to an array abstract Peer instances.
	raftPeers := make([]Peer, len(peers))
	for i := 0; i < len(peers); i++ {
		raftPeers[i] = peers[i]
	}

	raft, err := NewRaft(id, raftPeers, log, storage, snapshotStorage, fsm, responseCh, opts...)
	if err != nil {
		return nil, errors.WrapError(err, "failed to create new server: %s", err.Error())
	}

	server := &ProtobufServer{
		listenInterface: listenInterface,
		raft:            raft,
	}

	return server, nil
}

// Start starts the ProtobufServer. It listens for incoming connections on the configured
// network address and starts the Raft instance. It also starts serving gRPC requests on the
// listener. The provided channel is used to signal to the server that it should start serving
// requests.
func (s *ProtobufServer) Start(ready <-chan interface{}) error {
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

// Stop stops the ProtobufServer.
// It gracefully stops the gRPC server, stops the Raft instance, and closes the listener.
func (s *ProtobufServer) Stop() {
	if s.server == nil {
		return
	}

	s.wg.Wait()
	s.server.GracefulStop()
	s.raft.Stop()
	s.listener.Close()
	s.server = nil
}

// Status returns the status of the ProtobufServer.
// It retrieves the status from the underlying Raft instance.
func (s *ProtobufServer) Status() Status {
	return s.raft.Status()
}

// IsStarted checks if the ProtobufServer is started.
// It returns true if the server is started, false otherwise.
func (s *ProtobufServer) IsStarted() bool {
	return s.server != nil
}

// SubmitCommand submits a command to the ProtobufServer for processing.
// It forwards the command to the underlying Raft instance for handling
// and returns the index and term assigned to the command, as well as
// an error if submitting the command failed.
func (s *ProtobufServer) SubmitCommand(command Command) (uint64, uint64, error) {
	return s.raft.SubmitCommand(command)
}

// AppendEntries handles the AppendEntries gRPC request.
// It converts the request to the internal representation, invokes the AppendEntries function on the Raft instance,
// and returns the response.
func (s *ProtobufServer) AppendEntries(ctx context.Context, request *pb.AppendEntriesRequest) (*pb.AppendEntriesResponse, error) {
	appendEntriesRequest := makeAppendEntriesRequest(request)
	appendEntriesResponse := &AppendEntriesResponse{}
	if err := s.raft.AppendEntries(&appendEntriesRequest, appendEntriesResponse); err != nil {
		return nil, err
	}
	return makeProtoAppendEntriesResponse(*appendEntriesResponse), nil
}

// RequestVote handles the RequestVote gRPC request.
// It converts the request to the internal representation, invokes the RequestVote function on the Raft instance,
// and returns the response.
func (s *ProtobufServer) RequestVote(ctx context.Context, request *pb.RequestVoteRequest) (*pb.RequestVoteResponse, error) {
	requestVoteRequest := makeRequestVoteRequest(request)
	requestVoteResponse := &RequestVoteResponse{}
	if err := s.raft.RequestVote(&requestVoteRequest, requestVoteResponse); err != nil {
		return nil, err
	}
	return makeProtoRequestVoteResponse(*requestVoteResponse), nil
}

// InstallSnapshot handles the InstallSnapshot gRPC request.
// It converts the request to the internal representation, invokes the InstallSnapshot function on the Raft instance,
// and returns the response.
func (s *ProtobufServer) InstallSnapshot(ctx context.Context, request *pb.InstallSnapshotRequest) (*pb.InstallSnapshotResponse, error) {
	installSnapshotRequest := makeInstallSnapshotRequest(request)
	installSnapshotResponse := &InstallSnapshotResponse{}
	if err := s.raft.InstallSnapshot(&installSnapshotRequest, installSnapshotResponse); err != nil {
		return nil, err
	}
	return makeProtoInstallSnapshotResponse(*installSnapshotResponse), nil
}
