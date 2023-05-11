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
	errConnEstablished = "connection already established with peer %s"
	errNoConn          = "no connection established with peer %s"
)

type Peer interface {
	// Id returns the ID of the peer.
	Id() string

	// Address returns the network address of the peer.
	Address() net.Addr

	// Clone creates a new instance of Peer with the same ID and network address.
	Clone() Peer

	// Connect establishes a connection with the peer.
	Connect() error

	// Disconnect terminates the connection with the peer.
	Disconnect() error

	// AppendEntries sends an AppendEntriesRequest to the peer and returns an AppendEntriesResponse and an error
	// if the request was unsuccessful.
	AppendEntries(request AppendEntriesRequest) (AppendEntriesResponse, error)

	// RequestVote sends a RequestVoteRequest to the peer and returns a RequestVoteResponse and an error
	// if the request was unsuccessful.
	RequestVote(request RequestVoteRequest) (RequestVoteResponse, error)

	// SetNextIndex sets the next log index associated with the peer.
	SetNextIndex(nextIndex uint64)

	// NextIndex gets the next log index associated with the peer.
	NextIndex() uint64

	// SetMatchIndex sets the log match index associated with the peer.
	SetMatchIndex(matchIndex uint64)

	// MatchIndex gets the log match index associated with the peer.
	MatchIndex() uint64
}

// makeProtoEntries converts an array of LogEntry objects to an array of pb.LogEntry objects.
func makeProtoEntries(entries []*LogEntry) []*pb.LogEntry {
	protoEntries := make([]*pb.LogEntry, len(entries))
	for i, entry := range entries {
		protoEntry := &pb.LogEntry{Index: entry.index, Term: entry.term, Data: entry.data}
		protoEntries[i] = protoEntry
	}
	return protoEntries
}

// makeProtoRequestVoteRequest converts a RequestVoteRequest object to a pb.RequestVoteRequest object.
func makeProtoRequestVoteRequest(request RequestVoteRequest) *pb.RequestVoteRequest {
	return &pb.RequestVoteRequest{
		CandidateId:  request.candidateID,
		Term:         request.term,
		LastLogIndex: request.lastLogIndex,
		LastLogTerm:  request.lastLogTerm,
	}
}

// makeRequestVoteResponse converts a pb.RequestVoteResponse object to a RequestVoteResponse object.
func makeRequestVoteResponse(response *pb.RequestVoteResponse) RequestVoteResponse {
	return RequestVoteResponse{
		term:        response.GetTerm(),
		voteGranted: response.GetVoteGranted(),
	}
}

// makeProtoAppendEntriesRequest converts an AppendEntriesRequest object to a pb.AppendEntriesRequest object.
func makeProtoAppendEntriesRequest(request AppendEntriesRequest) *pb.AppendEntriesRequest {
	return &pb.AppendEntriesRequest{
		LeaderId:     request.leaderID,
		Term:         request.term,
		LeaderCommit: request.leaderCommit,
		PrevLogIndex: request.prevLogIndex,
		PrevLogTerm:  request.prevLogTerm,
		Entries:      makeProtoEntries(request.entries),
	}
}

// makeAppendEntriesResponse converts a pb.AppendEntriesResponse object to an AppendEntriesResponse object.
func makeAppendEntriesResponse(response *pb.AppendEntriesResponse) AppendEntriesResponse {
	return AppendEntriesResponse{
		success: response.GetSuccess(),
		term:    response.GetTerm(),
	}
}

type ProtobufPeer struct {
	id         string
	address    net.Addr
	nextIndex  uint64
	matchIndex uint64
	conn       *grpc.ClientConn
	client     pb.RaftClient
	mu         sync.Mutex
}

// NewProtobufPeer creates a new instance of ProtobufPeer with the specified ID and network address.
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
		return errors.WrapError(nil, errConnEstablished, p.id)
	}

	// TODO: Fix grpc dial options.
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

func (p *ProtobufPeer) AppendEntries(request AppendEntriesRequest) (AppendEntriesResponse, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.client == nil {
		return AppendEntriesResponse{}, errors.WrapError(nil, errNoConn, p.id)
	}

	pbRequest := makeProtoAppendEntriesRequest(request)

	pbResponse, err := p.client.AppendEntries(context.Background(), pbRequest, []grpc.CallOption{}...)
	if err != nil {
		return AppendEntriesResponse{}, nil
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

	return makeRequestVoteResponse(pbResponse), err
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
