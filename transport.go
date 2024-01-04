package raft

import (
	"context"
	"net"
	"sync"

	"github.com/jmsadair/raft/internal/errors"
	pb "github.com/jmsadair/raft/internal/protobuf"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// TransportFactory creates new instances of the Transport interface.
type TransportFactory func(address string) (Transport, error)

// Transport is an interface representing a component responsible for
// sending RPCs to a remote node.
type Transport interface {
	// Connect establishes a connection.
	Connect() error

	// Disconnect tears down a connection.
	Disconnect() error

	// AppendEntries sends an append entries request.
	AppendEntries(request AppendEntriesRequest) (AppendEntriesResponse, error)

	// RequestVote sends a request vote request.
	RequestVote(request RequestVoteRequest) (RequestVoteResponse, error)

	// InstallSnapshot sends a install snapshot request.
	InstallSnapshot(request InstallSnapshotRequest) (InstallSnapshotResponse, error)
}

// transport implements the Transport interface.
type transport struct {
	// The gRPC client for making Raft protocol calls to the transport.
	client pb.RaftClient

	// The network address of this transport.
	address net.Addr

	// The gRPC client connection to communicate with the transport.
	conn *grpc.ClientConn

	// Prevents a race condition between disconnect/connect and the RPCs.
	mu sync.RWMutex
}

// NewTransport creates a new Transport instance with the provided address.
func NewTransport(address string) (Transport, error) {
	resolvedAddress, err := net.ResolveTCPAddr("tcp", address)
	if err != nil {
		return nil, errors.WrapError(err, "failed to create transport")
	}
	return &transport{address: resolvedAddress}, nil
}

func (p *transport) Connect() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.client != nil {
		return nil
	}

	conn, err := grpc.Dial(
		p.address.String(),
		[]grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())}...)
	if err != nil {
		return errors.WrapError(err, "failed to make connection")
	}

	p.client = pb.NewRaftClient(conn)
	p.conn = conn

	return nil
}

func (p *transport) Disconnect() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.client == nil {
		return nil
	}

	if err := p.conn.Close(); err != nil {
		return errors.WrapError(err, "failed while closing connection to transport")
	}

	p.conn = nil
	p.client = nil

	return nil
}

func (p *transport) AppendEntries(request AppendEntriesRequest) (AppendEntriesResponse, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()

	if p.client == nil {
		return AppendEntriesResponse{}, errors.New(
			"AppendEntries RPC failed, no connection established",
		)
	}

	pbRequest := makeProtoAppendEntriesRequest(request)
	pbResponse, err := p.client.AppendEntries(
		context.Background(),
		pbRequest,
		[]grpc.CallOption{}...)
	if err != nil {
		return AppendEntriesResponse{}, errors.WrapError(
			err,
			"AppendEntries RPC failed",
		)
	}

	return makeAppendEntriesResponse(pbResponse), nil
}

func (p *transport) RequestVote(request RequestVoteRequest) (RequestVoteResponse, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()

	if p.client == nil {
		return RequestVoteResponse{}, errors.New(
			"RequestVote RPC failed, no connection established",
		)
	}

	pbRequest := makeProtoRequestVoteRequest(request)
	pbResponse, err := p.client.RequestVote(context.Background(), pbRequest, []grpc.CallOption{}...)
	if err != nil {
		return RequestVoteResponse{}, errors.WrapError(
			err,
			"RequestVote RPC failed",
		)
	}

	return makeRequestVoteResponse(pbResponse), nil
}

func (p *transport) InstallSnapshot(
	request InstallSnapshotRequest,
) (InstallSnapshotResponse, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()

	if p.client == nil {
		return InstallSnapshotResponse{}, errors.New(
			"InstallSnapshot RPC failed, no connection established",
		)
	}

	pbRequest := makeProtoInstallSnapshotRequest(request)
	pbResponse, err := p.client.InstallSnapshot(
		context.Background(),
		pbRequest,
		[]grpc.CallOption{}...)
	if err != nil {
		return InstallSnapshotResponse{}, errors.WrapError(
			err,
			"InstallSnapshot RPC failed",
		)
	}

	return makeInstallSnapshotResponse(pbResponse), nil
}
