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

const (
	errNoConn                = "no connection established with peer: ID = %s"
	errFailedConnect         = "failed to connect to peer: ID = %s, err = %s"
	errFailedCloseConnect    = "failed to close connection with peer: ID = %s, err = %s"
	errFailedAppendEntries   = "failed to invoke AppendEntries RPC on peer: ID = %s, err = %s"
	errFailedRequestVote     = "failed to invoke RequestVote RPC on peer: ID = %s, err = %s"
	errFailedInstallSnapshot = "failed to invoke InstallSnapshot RPC on peer: ID = %s, err = %s"
)

// Peer is an interface representing a component responsible for establishing a connection
// with and making RPCs to a Raft server.
type Peer interface {
	// ID returns the ID of the peer.
	ID() string

	// Address returns the network address of the peer.
	Connect() error

	// Connect establishes a connection with the peer.
	Disconnect() error

	// AppendEntries sends an AppendEntriesRequest to the peer and returns an AppendEntriesResponse and an error
	// if the request was unsuccessful.
	AppendEntries(request AppendEntriesRequest) (AppendEntriesResponse, error)

	// RequestVote sends a RequestVoteRequest to the peer and returns a RequestVoteResponse and an error
	// if the request was unsuccessful.
	RequestVote(request RequestVoteRequest) (RequestVoteResponse, error)

	// InstallSnapshot sends a InstallSnapshotRequest to the peer and returns a InstallSnapshotResponse and an error
	// if the request was unsuccessful.
	InstallSnapshot(request InstallSnapshotRequest) (InstallSnapshotResponse, error)
}

// peer is an implementation of the Peer interface that is responsible for establishing
// a connection with a remote server using gRPC. This is implementation is concurrent
// safe.
type peer struct {
	// The gRPC client for making Raft protocol calls to the peer.
	client pb.RaftClient

	// The ID of this peer.
	id string

	// The network address of this peer.
	address net.Addr

	// The gRPC client connection to communicate with the peer.
	conn *grpc.ClientConn

	// Prevents a race condition bewteen disconnect/connect and the RPCs.
	mu sync.RWMutex
}

// newPeer creates a new instance of a peer with
// the provided ID and network address.
func newPeer(id string, address net.Addr) *peer {
	return &peer{id: id, address: address}
}

func (p *peer) ID() string {
	return p.id
}

func (p *peer) Address() net.Addr {
	return p.address
}

func (p *peer) Connect() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.client != nil {
		return nil
	}

	conn, err := grpc.Dial(p.address.String(), []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())}...)
	if err != nil {
		return errors.WrapError(err, errFailedConnect, p.id, err.Error())
	}

	p.client = pb.NewRaftClient(conn)
	p.conn = conn

	return nil
}

func (p *peer) Disconnect() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.client == nil {
		return nil
	}

	if err := p.conn.Close(); err != nil {
		return errors.WrapError(err, errFailedCloseConnect, p.id, err.Error())
	}

	p.conn = nil
	p.client = nil

	return nil
}

func (p *peer) AppendEntries(request AppendEntriesRequest) (AppendEntriesResponse, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()

	if p.client == nil {
		return AppendEntriesResponse{}, errors.WrapError(nil, errNoConn, p.id)
	}

	pbRequest := makeProtoAppendEntriesRequest(request)
	pbResponse, err := p.client.AppendEntries(context.Background(), pbRequest, []grpc.CallOption{}...)
	if err != nil {
		return AppendEntriesResponse{}, errors.WrapError(err, errFailedAppendEntries, p.id, err.Error())
	}

	return makeAppendEntriesResponse(pbResponse), nil
}

func (p *peer) RequestVote(request RequestVoteRequest) (RequestVoteResponse, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()

	if p.client == nil {
		return RequestVoteResponse{}, errors.WrapError(nil, errNoConn, p.id)
	}

	pbRequest := makeProtoRequestVoteRequest(request)
	pbResponse, err := p.client.RequestVote(context.Background(), pbRequest, []grpc.CallOption{}...)
	if err != nil {
		return RequestVoteResponse{}, errors.WrapError(err, errFailedRequestVote, p.id, err.Error())
	}

	return makeRequestVoteResponse(pbResponse), nil
}

func (p *peer) InstallSnapshot(request InstallSnapshotRequest) (InstallSnapshotResponse, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()

	if p.client == nil {
		return InstallSnapshotResponse{}, errors.WrapError(nil, errNoConn, p.id)
	}

	pbRequest := makeProtoInstallSnapshotRequest(request)
	pbResponse, err := p.client.InstallSnapshot(context.Background(), pbRequest, []grpc.CallOption{}...)
	if err != nil {
		return InstallSnapshotResponse{}, errors.WrapError(err, errFailedInstallSnapshot, p.id, err.Error())
	}

	return makeInstallSnapshotResponse(pbResponse), nil
}
