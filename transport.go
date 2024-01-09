package raft

import (
	"context"
	"fmt"
	"net"
	"sync"
	"time"

	pb "github.com/jmsadair/raft/internal/protobuf"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/proto"
)

const shutdownGracePeriod = 300 * time.Millisecond

// Transport represents the underlying transport mechanism used by a node in a cluster
// to send and recieve RPCs.
type Transport interface {
	// Run will start serving incoming RPCs recieved at the local network address.
	Run() error

	// Shutdown will stop the serving of incoming RPCs.
	Shutdown() error

	// AppendEntries sends an append entries request to the provided address.
	SendAppendEntries(address string, request AppendEntriesRequest) (AppendEntriesResponse, error)

	// RequestVote sends a request vote request to the peer to the provided address.
	SendRequestVote(address string, request RequestVoteRequest) (RequestVoteResponse, error)

	// InstallSnapshot sends a install snapshot request to the provided address.
	SendInstallSnapshot(
		address string,
		request InstallSnapshotRequest,
	) (InstallSnapshotResponse, error)

	// RegisterAppendEntriesHandler registers the function the that will be called when an
	// AppendEntries RPC is received.
	RegisterAppendEntriesHandler(handler func(*AppendEntriesRequest, *AppendEntriesResponse) error)

	// RegisterRequestVoteHandler registers the function that will be called when a
	// RequestVote RPC is received.
	RegisterRequestVoteHandler(handler func(*RequestVoteRequest, *RequestVoteResponse) error)

	// RegisterInstallSnapshotHandler registers the function that will called when an
	// InstallSnapshot RPC is received.
	RegsiterInstallSnapshotHandler(
		handler func(*InstallSnapshotRequest, *InstallSnapshotResponse) error,
	)

	// Connect establishes a connection with the provided address if there is not
	// an existing one.
	Connect(address string) error

	// Close tears down a connection with the provided address if there is one.
	Close(address string) error

	// EncodeConfiguration accepts a configuration and encodes it such that it can be
	// decoded by DecodeConfiguration.
	EncodeConfiguration(configuration map[string]string) ([]byte, error)

	// DecodeConfiguration accepts a byte representation of a configuration and decodes
	// it into a configuration.
	DecodeConfiguration(data []byte) (map[string]string, error)

	// Address returns the local network address.
	Address() string
}

// transport is an implementation of the Transport interface.
type transport struct {
	pb.UnimplementedRaftServer

	// The local network address.
	address net.Addr

	// The RPC server for raft.
	server *grpc.Server

	// The function that is called when an AppendEntries RPC is received.
	appendEntriesHandler func(*AppendEntriesRequest, *AppendEntriesResponse) error

	// The function that is called when a RequestVote RPC is received.
	requestVoteHandler func(*RequestVoteRequest, *RequestVoteResponse) error

	// The function that is called when an InstallSnapshot RPC is recieved.
	installSnapshotHandler func(*InstallSnapshotRequest, *InstallSnapshotResponse) error

	// The connections to the nodes in the cluster. Maps address to connection.
	connections map[string]*grpc.ClientConn

	// The clients used to make RPCs. Maps address to client.
	clients map[string]pb.RaftClient

	mu sync.RWMutex
}

// NewTransport creates a new instance of Transport that will
// serve incoming RPCs at the provided address.
func NewTransport(address string) (Transport, error) {
	resolvedAddress, err := net.ResolveTCPAddr("tcp", address)
	if err != nil {
		return nil, err
	}

	transport := &transport{
		address:     resolvedAddress,
		connections: make(map[string]*grpc.ClientConn),
		clients:     make(map[string]pb.RaftClient),
	}

	return transport, nil
}

func (t *transport) Run() error {
	listener, err := net.Listen(t.address.Network(), t.address.String())
	t.server = grpc.NewServer()
	pb.RegisterRaftServer(t.server, t)
	if err != nil {
		return err
	}
	go t.server.Serve(listener)
	return nil
}

func (t *transport) Shutdown() error {
	stopped := make(chan interface{})

	go func() {
		t.server.GracefulStop()
		close(stopped)
	}()

	select {
	case <-time.After(shutdownGracePeriod):
		t.server.Stop()
	case <-stopped:
		t.server.Stop()
	}

	return nil
}

func (t *transport) SendAppendEntries(
	address string,
	request AppendEntriesRequest,
) (AppendEntriesResponse, error) {
	t.mu.RLock()
	defer t.mu.RUnlock()

	client, ok := t.clients[address]
	if !ok {
		return AppendEntriesResponse{}, fmt.Errorf(
			"could not make AppendEntries RPC: no connection to %s",
			address,
		)
	}

	pbRequest := makeProtoAppendEntriesRequest(request)
	pbResponse, err := client.AppendEntries(context.Background(), pbRequest)
	if err != nil {
		return AppendEntriesResponse{}, fmt.Errorf("could not make AppendEntries RPC: %w", err)
	}

	return makeAppendEntriesResponse(pbResponse), nil
}

func (t *transport) SendRequestVote(
	address string,
	request RequestVoteRequest,
) (RequestVoteResponse, error) {
	t.mu.RLock()
	defer t.mu.RUnlock()

	client, ok := t.clients[address]
	if !ok {
		return RequestVoteResponse{}, fmt.Errorf(
			"could not make RequestVote RPC: no connection to %s",
			address,
		)
	}

	pbRequest := makeProtoRequestVoteRequest(request)
	pbResponse, err := client.RequestVote(context.Background(), pbRequest)
	if err != nil {
		return RequestVoteResponse{}, fmt.Errorf("could not make RequestVote RPC: %w", err)
	}

	return makeRequestVoteResponse(pbResponse), nil
}

func (t *transport) SendInstallSnapshot(
	address string,
	request InstallSnapshotRequest,
) (InstallSnapshotResponse, error) {
	t.mu.RLock()
	defer t.mu.RUnlock()

	client, ok := t.clients[address]
	if !ok {
		return InstallSnapshotResponse{}, fmt.Errorf(
			"could not make InstallSnapshot RPC: no connection to %s",
			address,
		)
	}

	pbRequest := makeProtoInstallSnapshotRequest(request)
	pbResponse, err := client.InstallSnapshot(context.Background(), pbRequest)
	if err != nil {
		return InstallSnapshotResponse{}, fmt.Errorf("could not make InstallSnapshot RPC: %w", err)
	}

	return makeInstallSnapshotResponse(pbResponse), nil
}

func (t *transport) RegisterAppendEntriesHandler(
	handler func(*AppendEntriesRequest, *AppendEntriesResponse) error,
) {
	t.appendEntriesHandler = handler
}

func (t *transport) RegisterRequestVoteHandler(
	handler func(*RequestVoteRequest, *RequestVoteResponse) error,
) {
	t.requestVoteHandler = handler
}

func (t *transport) RegsiterInstallSnapshotHandler(
	handler func(*InstallSnapshotRequest, *InstallSnapshotResponse) error,
) {
	t.installSnapshotHandler = handler
}

func (t *transport) EncodeConfiguration(configuration map[string]string) ([]byte, error) {
	pbConfiguration := &pb.Configuration{Peers: configuration}
	return proto.Marshal(pbConfiguration)
}

func (t *transport) DecodeConfiguration(data []byte) (map[string]string, error) {
	pbConfiguration := &pb.Configuration{}
	if err := proto.Unmarshal(data, pbConfiguration); err != nil {
		return nil, err
	}
	return pbConfiguration.GetPeers(), nil
}

func (t *transport) Address() string {
	return t.address.String()
}

func (t *transport) Connect(address string) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	if _, ok := t.connections[address]; ok {
		return nil
	}

	creds := insecure.NewCredentials()
	dialOptions := grpc.WithTransportCredentials(creds)
	conn, err := grpc.Dial(address, dialOptions)
	if err != nil {
		return fmt.Errorf("could not estabslish connection: %w", err)
	}
	client := pb.NewRaftClient(conn)
	t.connections[address] = conn
	t.clients[address] = client

	return nil
}

func (t *transport) Close(address string) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	conn, ok := t.connections[address]
	if !ok {
		return nil
	}
	if err := conn.Close(); err != nil {
		return err
	}
	delete(t.connections, address)
	delete(t.clients, address)

	return nil
}

// AppendEntries handles the AppendEntries gRPC request.
// It converts the request to the internal representation, invokes the AppendEntries function on the Raft instance,
// and returns the response.
func (t *transport) AppendEntries(
	ctx context.Context,
	request *pb.AppendEntriesRequest,
) (*pb.AppendEntriesResponse, error) {
	appendEntriesRequest := makeAppendEntriesRequest(request)
	appendEntriesResponse := &AppendEntriesResponse{}
	if err := t.appendEntriesHandler(&appendEntriesRequest, appendEntriesResponse); err != nil {
		return nil, err
	}
	return makeProtoAppendEntriesResponse(*appendEntriesResponse), nil
}

// RequestVote handles the RequestVote gRPC request.
// It converts the request to the internal representation, invokes the RequestVote function on the Raft instance,
// and returns the response.
func (t *transport) RequestVote(
	ctx context.Context,
	request *pb.RequestVoteRequest,
) (*pb.RequestVoteResponse, error) {
	requestVoteRequest := makeRequestVoteRequest(request)
	requestVoteResponse := &RequestVoteResponse{}
	if err := t.requestVoteHandler(&requestVoteRequest, requestVoteResponse); err != nil {
		return nil, err
	}
	return makeProtoRequestVoteResponse(*requestVoteResponse), nil
}

// InstallSnapshot handles the InstallSnapshot gRPC request.
// It converts the request to the internal representation, invokes the InstallSnapshot function on the Raft instance,
// and returns the response.
func (t *transport) InstallSnapshot(
	ctx context.Context,
	request *pb.InstallSnapshotRequest,
) (*pb.InstallSnapshotResponse, error) {
	installSnapshotRequest := makeInstallSnapshotRequest(request)
	installSnapshotResponse := &InstallSnapshotResponse{}
	if err := t.installSnapshotHandler(&installSnapshotRequest, installSnapshotResponse); err != nil {
		return nil, err
	}
	return makeProtoInstallSnapshotResponse(*installSnapshotResponse), nil
}
