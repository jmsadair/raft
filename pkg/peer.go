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
	if p.client != nil {
		return errors.WrapError(nil, "connection already established with peer %s", p.id)
	}
	// TODO: Configure grpc client options.
	conn, err := grpc.Dial(p.address.String(), []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())}...)
	if err != nil {
		return errors.WrapError(err, "failed to connect to peer: %s", err.Error())
	}
	p.client = pb.NewRaftClient(conn)
	p.conn = conn
	return nil
}

func (p *Peer) disconnect() error {
	if p.conn == nil {
		return errors.WrapError(nil, "no connection established with peer %s", p.id)
	}
	if err := p.conn.Close(); err != nil {
		return errors.WrapError(err, "failed to close connection with peer: %s", err.Error())
	}
	return nil
}

func (p *Peer) appendEntries(request *pb.AppendEntriesRequest) (*pb.AppendEntriesResponse, error) {
	if p.client == nil {
		return nil, errors.WrapError(nil, "no connection established with peer %s", p.id)
	}
	return p.client.AppendEntries(context.Background(), request, []grpc.CallOption{}...)
}

func (p *Peer) requestVote(request *pb.RequestVoteRequest) (*pb.RequestVoteResponse, error) {
	if p.client == nil {
		return nil, errors.WrapError(nil, "no connection established with peer %s", p.id)
	}
	return p.client.RequestVote(context.Background(), request, []grpc.CallOption{}...)
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
