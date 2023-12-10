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

var errNoConnectionToPeer = errors.New("no connection with peer")

// Peer is an interface representing a component responsible for establishing a connection
// with and making RPCs to a Raft server.
type Peer interface {
	// ID returns the ID of the peer.
	ID() string

	// Address returns the network address of the peer.
	Address() net.Addr

	// Connect establishes a connection with the peer.
	Connect() error

	// Disconnect tears down a connection with the peer.
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
// a connection with a remote server using gRPC. This implementation is concurrent
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

	// Prevents a race condition between disconnect/connect and the RPCs.
	mu sync.RWMutex
}

// NewPeer creates a new Peer instance. The function establishes a gRPC client
// for making Raft protocol calls to the peer. This function is concurrent-safe.
func NewPeer(id string, address net.Addr) Peer {
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

	conn, err := grpc.Dial(
		p.address.String(),
		[]grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())}...)
	if err != nil {
		return errors.WrapError(err, "failed to connect to peer: ID = %s, address = %s",
			p.id, p.address.String())
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
		return errors.WrapError(err, "failed to close connection to to peer: ID = %s, address = %s",
			p.id, p.address.String())
	}

	p.conn = nil
	p.client = nil

	return nil
}

func (p *peer) AppendEntries(request AppendEntriesRequest) (AppendEntriesResponse, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()

	if p.client == nil {
		return AppendEntriesResponse{}, errNoConnectionToPeer
	}

	pbRequest := makeProtoAppendEntriesRequest(request)
	pbResponse, err := p.client.AppendEntries(
		context.Background(),
		pbRequest,
		[]grpc.CallOption{}...)
	if err != nil {
		return AppendEntriesResponse{}, errors.WrapError(
			err,
			"AppendEntries RPC failed: ID = %s, address = %s",
			p.id,
			p.address.String(),
		)
	}

	return makeAppendEntriesResponse(pbResponse), nil
}

func (p *peer) RequestVote(request RequestVoteRequest) (RequestVoteResponse, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()

	if p.client == nil {
		return RequestVoteResponse{}, errNoConnectionToPeer
	}

	pbRequest := makeProtoRequestVoteRequest(request)
	pbResponse, err := p.client.RequestVote(context.Background(), pbRequest, []grpc.CallOption{}...)
	if err != nil {
		return RequestVoteResponse{}, errors.WrapError(
			err,
			"RequestVote RPC failed: ID = %s, address = %s",
			p.id,
			p.address.String(),
		)
	}

	return makeRequestVoteResponse(pbResponse), nil
}

func (p *peer) InstallSnapshot(request InstallSnapshotRequest) (InstallSnapshotResponse, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()

	if p.client == nil {
		return InstallSnapshotResponse{}, errNoConnectionToPeer
	}

	pbRequest := makeProtoInstallSnapshotRequest(request)
	pbResponse, err := p.client.InstallSnapshot(
		context.Background(),
		pbRequest,
		[]grpc.CallOption{}...)
	if err != nil {
		return InstallSnapshotResponse{}, errors.WrapError(
			err,
			"InstallSnapshot RPC failed: ID = %s, address = %s",
			p.id,
			p.address.String(),
		)
	}

	return makeInstallSnapshotResponse(pbResponse), nil
}
