package raft

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sync"
	"time"

	pb "github.com/jmsadair/raft/internal/protobuf"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

const shutdownGracePeriod = 300 * time.Millisecond

// Transport represents the underlying transport mechanism used by a node in a cluster
// to send and recieve RPCs. It acts as both a server for a node and a client of other nodes.
type Transport interface {
	// Run will start serving incoming RPCs received at the local network address.
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

	// EncodeConfiguration accepts a configuration and encodes it such that it can be
	// decoded by DecodeConfiguration.
	EncodeConfiguration(configuration *Configuration) ([]byte, error)

	// DecodeConfiguration accepts a byte representation of a configuration and decodes
	// it into a configuration.
	DecodeConfiguration(data []byte) (Configuration, error)

	// Address returns the local network address.
	Address() string
}

// connectionManager handles creating new connections and closing existing ones.
// This implementation is concurrent safe.
type connectionManager struct {
	// The connections to the nodes in the cluster. Maps address to connection.
	connections map[string]*grpc.ClientConn

	// The clients used to make RPCs. Maps address to client.
	clients map[string]pb.RaftClient

	// The credentials each client will use.
	creds credentials.TransportCredentials

	mu sync.Mutex
}

func newConnectionManager(creds credentials.TransportCredentials) *connectionManager {
	return &connectionManager{
		connections: make(map[string]*grpc.ClientConn),
		clients:     make(map[string]pb.RaftClient),
		creds:       creds,
	}
}

// getClient will retrieve a client for the provided address. If one does not
// exist, it will be created.
func (c *connectionManager) getClient(address string) (pb.RaftClient, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if client, ok := c.clients[address]; ok {
		return client, nil
	}

	conn, err := grpc.Dial(address, grpc.WithTransportCredentials(c.creds))
	if err != nil {
		return nil, fmt.Errorf("could not establish connection: %w", err)
	}
	c.connections[address] = conn
	c.clients[address] = pb.NewRaftClient(conn)

	return c.clients[address], nil
}

// closeAll closes all open connections.
func (c *connectionManager) closeAll() {
	c.mu.Lock()
	defer c.mu.Unlock()

	for address, conn := range c.connections {
		conn.Close()
		delete(c.connections, address)
		delete(c.clients, address)
	}
}

// transport is an implementation of the Transport interface.
type transport struct {
	pb.UnimplementedRaftServer

	// Indicates whether the transport is started.
	running bool

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

	// Manages connections to other members of the cluster.
	connManager *connectionManager

	mu sync.RWMutex
}

// NewTransport creates a new instance of Transport that can
// be used to make RPCs and serve incoming RPCs at the provided
// address.
func NewTransport(address string) (Transport, error) {
	resolvedAddress, err := net.ResolveTCPAddr("tcp", address)
	if err != nil {
		return nil, fmt.Errorf("could not resove tcp address: %w", err)
	}
	creds := insecure.NewCredentials()
	connManager := newConnectionManager(creds)
	return &transport{address: resolvedAddress, connManager: connManager}, nil
}

func (t *transport) Run() error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.running {
		return nil
	}

	listener, err := net.Listen(t.address.Network(), t.address.String())
	if err != nil {
		return fmt.Errorf("could not create listener: %w", err)
	}

	t.server = grpc.NewServer()
	pb.RegisterRaftServer(t.server, t)
	go t.server.Serve(listener)
	t.running = true

	return nil
}

func (t *transport) Shutdown() error {
	t.mu.Lock()
	if !t.running {
		t.mu.Unlock()
		return nil
	}
	t.running = false
	t.mu.Unlock()

	stopped := make(chan interface{})
	defer t.connManager.closeAll()

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

	if !t.running {
		return AppendEntriesResponse{}, errors.New(
			"could not make AppendEntries RPC: transport is closed",
		)
	}

	client, err := t.connManager.getClient(address)
	if err != nil {
		return AppendEntriesResponse{}, fmt.Errorf("could not get client connection: %w", err)
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

	if !t.running {
		return RequestVoteResponse{}, errors.New(
			"could not make AppendEntries RPC: transport is closed",
		)
	}

	client, err := t.connManager.getClient(address)
	if err != nil {
		return RequestVoteResponse{}, fmt.Errorf("could not get client connection: %w", err)
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

	if !t.running {
		return InstallSnapshotResponse{}, errors.New(
			"could not make AppendEntries RPC: transport is closed",
		)
	}

	client, err := t.connManager.getClient(address)
	if err != nil {
		return InstallSnapshotResponse{}, fmt.Errorf("could not get client connection: %w", err)
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

func (t *transport) EncodeConfiguration(configuration *Configuration) ([]byte, error) {
	data, err := encodeConfiguration(configuration)
	if err != nil {
		return nil, fmt.Errorf("could not encode configuration: %w", err)
	}
	return data, nil
}

func (t *transport) DecodeConfiguration(data []byte) (Configuration, error) {
	configuration, err := decodeConfiguration(data)
	if err != nil {
		return Configuration{}, fmt.Errorf("could not decode configuration: %w", err)
	}
	return configuration, nil
}

func (t *transport) Address() string {
	return t.address.String()
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
		return nil, status.Error(codes.Unavailable, err.Error())
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
		return nil, status.Error(codes.Unavailable, err.Error())
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
		return nil, status.Error(codes.Unavailable, err.Error())
	}
	return makeProtoInstallSnapshotResponse(*installSnapshotResponse), nil
}
