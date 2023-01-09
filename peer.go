package raft

import (
	"context"
	"fmt"

	pb "github.com/jmsadair/raft/internal/protobuf"
	"google.golang.org/grpc"
)

type Peer struct {
	id      string
	address string
	client  pb.RaftClient
}

func NewPeer(id, address string) *Peer {
	return &Peer{id: id, address: address}
}

func (p *Peer) Connect() error {
	conn, err := grpc.Dial(p.address, nil)
	if err != nil {
		return err
	}
	p.client = pb.NewRaftClient(conn)
	return nil
}

func (p *Peer) AppendEntries(request *pb.AppendEntriesRequest) (*pb.AppendEntriesResponse, error) {
	if p.client == nil {
		return nil, fmt.Errorf("no connection established with peer %s", p.id)
	}
	return p.client.AppendEntries(context.Background(), request, nil)
}

func (p *Peer) RequestVote(request *pb.RequestVoteRequest) (*pb.RequestVoteResponse, error) {
	if p.client == nil {
		return nil, fmt.Errorf("no connection established with peer %s", p.id)
	}
	return p.client.RequestVote(context.Background(), request, nil)
}
