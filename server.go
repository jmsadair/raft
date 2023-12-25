package raft

import (
	"context"
	"net"
	"sync"
	"time"

	"github.com/jmsadair/raft/internal/errors"
	pb "github.com/jmsadair/raft/internal/protobuf"
	"google.golang.org/grpc"
)

// Server is a wrapper for a Raft instance that implements the logic of the Raft consensus algorithm.
// It serves requests for Raft using protobuf and gRPC, making it capable of providing replication
// and fault tolerance.
type Server struct {
	pb.UnimplementedRaftServer
	listenInterface net.Addr
	listener        net.Listener
	server          *grpc.Server
	raft            Protocol
	wg              sync.WaitGroup
}

// NewServer creates a new instance of a Server with a raft instance that satisfies the Protocol interface.
func NewServer(raft Protocol) (*Server, error) {
	server := &Server{
		listenInterface: raft.Status().Address,
		raft:            raft,
	}
	return server, nil
}

// Start starts the server. It listens for incoming connections on the configured network address, starts the Raft instance,
// and serves gRPC requests on the listener. The provided channel is used to signal the server to start serving requests.
func (s *Server) Start(ready <-chan interface{}) error {
	listener, err := net.Listen(s.listenInterface.Network(), s.listenInterface.String())
	if err != nil {
		return errors.WrapError(err, "failed to start server")
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

// Stop stops the server.
// It gracefully stops the gRPC server, stops the Raft instance, and closes the listener.
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

// Status returns the status of the server.
// It retrieves the status from the underlying Raft instance.
// The status includes the ID, commit index, last-applied index,
// term, and state of the Raft instance.
func (s *Server) Status() Status {
	return s.raft.Status()
}

// IsStarted checks if the server is started.
// It returns true if the server is started, false otherwise.
func (s *Server) IsStarted() bool {
	return s.server != nil
}

// SubmitOperation submits an operation (as bytes) to the server for processing.
// It forwards the operation to the underlying Protocol instance for handling
// and returns a future for the response to the operation.
func (s *Server) SubmitOperation(
	operation []byte,
	operationType OperationType,
	timeout time.Duration,
) *OperationResponseFuture {
	return s.raft.SubmitOperation(operation, operationType, timeout)
}

// AppendEntries handles the AppendEntries gRPC request.
// It converts the request to the internal representation, invokes the AppendEntries function on the Raft instance,
// and returns the response.
func (s *Server) AppendEntries(
	ctx context.Context,
	request *pb.AppendEntriesRequest,
) (*pb.AppendEntriesResponse, error) {
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
func (s *Server) RequestVote(
	ctx context.Context,
	request *pb.RequestVoteRequest,
) (*pb.RequestVoteResponse, error) {
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
func (s *Server) InstallSnapshot(
	ctx context.Context,
	request *pb.InstallSnapshotRequest,
) (*pb.InstallSnapshotResponse, error) {
	installSnapshotRequest := makeInstallSnapshotRequest(request)
	installSnapshotResponse := &InstallSnapshotResponse{}
	if err := s.raft.InstallSnapshot(&installSnapshotRequest, installSnapshotResponse); err != nil {
		return nil, err
	}
	return makeProtoInstallSnapshotResponse(*installSnapshotResponse), nil
}
