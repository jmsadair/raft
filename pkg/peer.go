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

type Peer struct {
	id         string
	address    net.Addr
	nextIndex  uint64
	matchIndex uint64
	conn       *grpc.ClientConn
	client     pb.RaftClient
	mu         sync.Mutex
}

func NewPeer(id string, address net.Addr) *Peer {
	return &Peer{id: id, address: address}
}

func (p *Peer) Id() string {
	return p.id
}

func (p *Peer) Address() net.Addr {
	return p.address
}

func (p *Peer) Clone() *Peer {
	return NewPeer(p.id, p.address)
}

func (p *Peer) connect() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.client != nil {
		return errors.WrapError(nil, errConnEstablished, p.id)
	}

	conn, err := grpc.Dial(p.address.String(), []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())}...)
	if err != nil {
		return errors.WrapError(err, "failed to connect to peer: %s", err.Error())
	}

	p.client = pb.NewRaftClient(conn)
	p.conn = conn

	return nil
}

func (p *Peer) disconnect() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.client == nil {
		return errors.WrapError(nil, errNoConn, p.id)
	}

	if err := p.conn.Close(); err != nil {
		return errors.WrapError(err, "failed to close connection with peer: %s", err.Error())
	}

	p.conn = nil
	p.client = nil

	return nil
}

func (p *Peer) isConnected() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.client != nil
}

func (p *Peer) appendEntries(request *pb.AppendEntriesRequest) (*pb.AppendEntriesResponse, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.client == nil {
		return nil, errors.WrapError(nil, errNoConn, p.id)
	}

	return p.client.AppendEntries(context.Background(), request, []grpc.CallOption{}...)
}

func (p *Peer) requestVote(request *pb.RequestVoteRequest) (*pb.RequestVoteResponse, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.client == nil {
		return nil, errors.WrapError(nil, errNoConn, p.id)
	}

	return p.client.RequestVote(context.Background(), request, []grpc.CallOption{}...)
}

func (p *Peer) installSnapshot(request *pb.InstallSnapshotRequest) (*pb.InstallSnapshotResponse, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.client == nil {
		return nil, errors.WrapError(nil, errNoConn, p.id)
	}

	return p.client.InstallSnapshot(context.Background(), request, []grpc.CallOption{}...)
}

func (p *Peer) setNextIndex(index uint64) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.nextIndex = index
}

func (p *Peer) getNextIndex() uint64 {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.nextIndex
}

func (p *Peer) setMatchIndex(index uint64) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.matchIndex = index
}

func (p *Peer) getMatchIndex() uint64 {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.matchIndex
}
