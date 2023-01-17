package raft

import (
	"context"
	"fmt"
	"sync"

	pb "github.com/jmsadair/raft/internal/protobuf"
	"google.golang.org/grpc"
)

type Peer struct {
	id         string
	address    string
	nextIndex  uint64
	matchIndex uint64
	client     pb.RaftClient
	mu         sync.Mutex
}

func NewPeer(id, address string) *Peer {
	return &Peer{id: id, address: address}
}

func (p *Peer) connect() error {
	conn, err := grpc.Dial(p.address, nil)
	if err != nil {
		return err
	}
	p.client = pb.NewRaftClient(conn)
	return nil
}

func (p *Peer) appendEntries(request *pb.AppendEntriesRequest) (*pb.AppendEntriesResponse, error) {
	if p.client == nil {
		return nil, fmt.Errorf("no connection established with peer %s", p.id)
	}
	return p.client.AppendEntries(context.Background(), request, nil)
}

func (p *Peer) requestVote(request *pb.RequestVoteRequest) (*pb.RequestVoteResponse, error) {
	if p.client == nil {
		return nil, fmt.Errorf("no connection established with peer %s", p.id)
	}
	return p.client.RequestVote(context.Background(), request, nil)
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
