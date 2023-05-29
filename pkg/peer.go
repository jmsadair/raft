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
	errNoConn = "no connection established with peer %s"
)

// Peer is an interface representing a component responsible for establishing a connection
// with and making RPCs to a Raft server.
type Peer interface {
	// Id returns the ID of the peer.
	//
	// Returns:
	//     - string: The ID of the peer.
	Id() string

	// Address returns the network address of the peer.
	//
	// Returns:
	//     - net.Addr: The network address of the peer.
	Address() net.Addr

	// Clone creates a new instance of Peer with the same ID and network address.
	//
	// Returns:
	//     - Peer: A new instance of Peer with the same ID and network address.
	Clone() Peer

	// Connect establishes a connection with the peer.
	//
	// Returns:
	//     - error: An error if the connection establishment fails.
	Connect() error

	// Disconnect terminates the connection with the peer.
	//
	// Returns:
	//     - error: An error if the disconnection fails.
	Disconnect() error

	// Connected indicates whether a connection has been established with the peer.
	//
	// Returns:
	//     - bool: True if a connection is established, false otherwise.
	Connected() bool

	// AppendEntries sends an AppendEntriesRequest to the peer and returns an AppendEntriesResponse and an error
	// if the request was unsuccessful.
	//
	// Parameters:
	//     - request: The AppendEntriesRequest to be sent.
	//
	// Returns:
	//     - AppendEntriesResponse: The response received from the peer.
	//     - error: An error if sending the request fails.
	AppendEntries(request AppendEntriesRequest) (AppendEntriesResponse, error)

	// RequestVote sends a RequestVoteRequest to the peer and returns a RequestVoteResponse and an error
	// if the request was unsuccessful.
	//
	// Parameters:
	//     - request: The RequestVoteRequest to be sent.
	//
	// Returns:
	//     - RequestVoteResponse: The response received from the peer.
	//     - error: An error if sending the request fails.
	RequestVote(request RequestVoteRequest) (RequestVoteResponse, error)

	// InstallSnapshot sends a InstallSnapshotRequest to the peer and returns a InstallSnapshotResponse and an error
	// if the request was unsuccessful.
	//
	// Parameters:
	//	   - request: The InstallSnapshotRequest to be sent.
	//
	// Returns:
	//     - InstallSnapshotResponse: The response recieved from the peer.
	//     - error: An error if sending the request fails.
	InstallSnapshot(request InstallSnapshotRequest) (InstallSnapshotResponse, error)

	// SetNextIndex sets the next log index associated with the peer.
	//
	// Parameters:
	//     - nextIndex: The next log index to be set.
	SetNextIndex(nextIndex uint64)

	// NextIndex gets the next log index associated with the peer.
	//
	// Returns:
	//     - uint64: The next log index associated with the peer.
	NextIndex() uint64

	// SetMatchIndex sets the log match index associated with the peer.
	//
	// Parameters:
	//     - matchIndex: The log match index to be set.
	SetMatchIndex(matchIndex uint64)

	// MatchIndex gets the log match index associated with the peer.
	//
	// Returns:
	//     - uint64: The log match index associated with the peer.
	MatchIndex() uint64
}

// ProtobufPeer is an implementation of Peer that is responsible for establishing
// a connection with a server using protobuf.
type ProtobufPeer struct {
	id         string
	address    net.Addr
	nextIndex  uint64
	matchIndex uint64
	conn       *grpc.ClientConn
	client     pb.RaftClient
	mu         sync.Mutex
}

// NewProtobufPeer creates a new instance of a ProtobufPeer.
//
// Returns:
//   - *ProtobufPeer: a pointer to the created ProtobufPeer instance.
func NewProtobufPeer(id string, address net.Addr) *ProtobufPeer {
	return &ProtobufPeer{id: id, address: address}
}

func (p *ProtobufPeer) Id() string {
	return p.id
}

func (p *ProtobufPeer) Address() net.Addr {
	return p.address
}

func (p *ProtobufPeer) Clone() Peer {
	return NewProtobufPeer(p.id, p.address)
}

func (p *ProtobufPeer) Connect() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.client != nil {
		return nil
	}

	conn, err := grpc.Dial(p.address.String(), []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())}...)
	if err != nil {
		return errors.WrapError(err, "failed to connect to peer: %s", err.Error())
	}

	p.client = pb.NewRaftClient(conn)
	p.conn = conn

	return nil
}

func (p *ProtobufPeer) Disconnect() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.client == nil {
		return nil
	}

	if err := p.conn.Close(); err != nil {
		return errors.WrapError(err, "failed to close connection with peer: %s", err.Error())
	}

	p.conn = nil
	p.client = nil

	return nil
}

func (p *ProtobufPeer) Connected() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.client != nil
}

func (p *ProtobufPeer) AppendEntries(request AppendEntriesRequest) (AppendEntriesResponse, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.client == nil {
		return AppendEntriesResponse{}, errors.WrapError(nil, errNoConn, p.id)
	}

	pbRequest := makeProtoAppendEntriesRequest(request)
	pbResponse, err := p.client.AppendEntries(context.Background(), pbRequest, []grpc.CallOption{}...)
	if err != nil {
		return AppendEntriesResponse{}, err
	}

	return makeAppendEntriesResponse(pbResponse), nil
}

func (p *ProtobufPeer) RequestVote(request RequestVoteRequest) (RequestVoteResponse, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.client == nil {
		return RequestVoteResponse{}, errors.WrapError(nil, errNoConn, p.id)
	}

	pbRequest := makeProtoRequestVoteRequest(request)
	pbResponse, err := p.client.RequestVote(context.Background(), pbRequest, []grpc.CallOption{}...)
	if err != nil {
		return RequestVoteResponse{}, err
	}

	return makeRequestVoteResponse(pbResponse), nil
}

func (p *ProtobufPeer) InstallSnapshot(request InstallSnapshotRequest) (InstallSnapshotResponse, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.client == nil {
		return InstallSnapshotResponse{}, errors.WrapError(nil, errNoConn, p.id)
	}

	pbRequest := makeProtoInstallSnapshotRequest(request)
	pbResponse, err := p.client.InstallSnapshot(context.Background(), pbRequest, []grpc.CallOption{}...)
	if err != nil {
		return InstallSnapshotResponse{}, err
	}

	return makeInstallSnapshotResponse(pbResponse), nil
}

func (p *ProtobufPeer) SetNextIndex(index uint64) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.nextIndex = index
}

func (p *ProtobufPeer) NextIndex() uint64 {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.nextIndex
}

func (p *ProtobufPeer) SetMatchIndex(index uint64) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.matchIndex = index
}

func (p *ProtobufPeer) MatchIndex() uint64 {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.matchIndex
}
