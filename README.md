# raft
This library provides a simple, easy-to-understand, and reliable implementation of [Raft](https://en.wikipedia.org/wiki/Raft_(algorithm)) using Go. Raft is a [consensus](https://en.wikipedia.org/wiki/Consensus_(computer_science)) protocol designed to manage replicated logs in a distributed system. Its purpose is to ensure fault-tolerant coordination and consistency among a group of nodes, making it suitable for building reliable systems. Potential use case include distributed file systems, consistent key-value stores, and service discovery.

# Getting Started
First, make sure you have Go `1.19` or a higher version installed on your system. You can download and install Go from the official Go website: https://golang.org/

Next, write a package that encapsulates a state machine that you would like to replicate.

```go
package fsm

import (
	"bytes"
	"encoding/gob"
	"sync"

	"github.com/jmsadair/raft"
)

// Respresents the operation types for this state machine.
type OpType int

// This state machine only support increment and decrement operations.
const (
	Increment OpType = iota
	Decrement
)

// Op represents an operations that can be applied to the state machine.
type Op struct {
	// The client ID associated with the operation.
	ClientID uint64

	// The type of the operation: increment or decrement.
	OpType OpType
}

func (o *Op) Encode() ([]byte, error) {
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	if err := enc.Encode(o); err != nil {
		return buf.Bytes(), err
	}

	return buf.Bytes(), nil
}

func (o *Op) Decode(data []byte) error {
	buf := bytes.NewBuffer(data)
	dec := gob.NewDecoder(buf)
	if err := dec.Decode(o); err != nil {
		return err
	}

	return nil
}

// Result represents the outcome of applying an operation to the
// state machine.
type Result struct {
	// The ID of the client that submitted the operation.
	ClientID uint64

	// The current value of the counter.
	Count int

	// An error if there was an issue executing the operation.
	Error error
}

// StateMachine represents a simple counter state machine.
// The state machine must be concurrent safe.
type StateMachine struct {
	count     int
	lastIndex uint64
	lastTerm  uint64
	mu        sync.Mutex
}

func NewStateMachine() *StateMachine {
	gob.Register(Op{})
	return &StateMachine{}
}

func (sm *StateMachine) Apply(entry *raft.LogEntry) interface{} {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	// Save the term and index of the last seen entry for snapshotting.
	sm.lastIndex = entry.Index
	sm.lastTerm = entry.Term

	// Decode the operation.
	op := &Op{}
	if err := op.Decode(entry.Data); err != nil {
		return Result{ClientID: 0, Count: 0, Error: err}
	}

	// Apply the operation.
	switch op.OpType {
	case Increment:
		sm.count++
	case Decrement:
		sm.count--
	}

	return Result{ClientID: op.ClientID, Count: sm.count, Error: nil}
}

func (sm *StateMachine) Snapshot() (raft.Snapshot, error) {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	// Encode the state of the state machine.
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	if err := enc.Encode(sm.count); err != nil {
		return raft.Snapshot{}, err
	}

	// Create the snapshot.
	snapshot := raft.Snapshot{
		LastIncludedIndex: sm.lastIndex,
		LastIncludedTerm:  sm.lastTerm,
		Data:              buf.Bytes(),
	}

	return snapshot, nil
}

func (sm *StateMachine) Restore(snapshot *raft.Snapshot) error {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	// Decode the bytes of the snapshot.
	var count int
	buf := bytes.NewBuffer(snapshot.Data)
	dec := gob.NewDecoder(buf)
	if err := dec.Decode(&count); err != nil {
		return err
	}

	// Restore the state of the state machine.
	sm.count = count

	// Update the last seen index and term since the state has been
	// restored up to this point.
	sm.lastIndex = snapshot.LastIncludedIndex
	sm.lastTerm = snapshot.LastIncludedTerm

	return nil
}
```

Now, you can create your own server using Raft and the state machine you defined in the above package.

```go
package server

import (
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/jmsadair/raft"
	"your/app/package/fsm"
)

// A request to increment or decrement the value of a counter.
type UpdateCounterRequest struct {
	// The ID of the client making the request.
	ID uint64

	// The type of the operation: increment or decrement.
	OpType fsm.OpType
}

// A response to a request to update the value of a counter.
type UpdateCounterResponse struct {
	// The value of the counter after executing the operation in the request.
	Result int
}

// A simple server that uses raft to update the value of a counter.
type Server struct {
	// The ID of this server.
	id string

	// The address of this server.
	address net.Addr

	// The raft server.
	raft *raft.Server

	// Responses from the state machine.
	responseCh chan raft.CommandResponse

	// Maps client IDs to their respective result channel.
	clients map[uint64]chan int

	// Ensures all go routines have stopped when shutting down.
	wg sync.WaitGroup

	// Protects the client ID mapping.
	mu sync.Mutex
}

func NewServer(id string, address net.Addr, raft *raft.Server, responseCh chan raft.CommandResponse) *Server {
	return &Server{id: id, address: address, raft: raft, responseCh: responseCh, clients: make(map[uint64]chan int)}
}

// Start starts the server.
func (s *Server) Start() error {
	readyCh := make(chan interface{})
	if err := s.raft.Start(readyCh); err != nil {
		return err
	}
	s.wg.Add(1)
	go s.responseLoop()
	close(readyCh)

	// Your logic for making this server available to clients
	// ...
	// ...

	return nil
}

// Stop stops the server.
func (s *Server) Stop() {
	s.raft.Stop()
	s.wg.Wait()

	// Your logic for gracefully shutting down this server
	//...
	//...
}

// UpdateCounter is an RPC that accepts a request to either increment the value of a counter or decrement it.
func (s *Server) UpdateCounter(request *UpdateCounterRequest, response *UpdateCounterResponse) error {
	s.mu.Lock()
	resultCh := make(chan int)
	s.mu.Unlock()

	// Clean up the client's result channel when we are done.
	defer func() {
		s.mu.Lock()
		delete(s.clients, request.ID)
		s.mu.Unlock()
	}()

	// Create a state machine operation and encode it.
	op := &fsm.Op{ClientID: request.ID, OpType: request.OpType}
	bytes, err := op.Encode()
	if err != nil {
		return err
	}

	// Submit the command containing the bytes of the operation to raft.
	// This serer may not be the leader, and so it may reject the operation.
	command := raft.Command{Bytes: bytes}
	if _, _, err := s.raft.SubmitCommand(command); err != nil {
		return err
	}

	// Wait for a response. If we don't hear back after some amount of time, give up.
	for {
		select {
		case count := <-resultCh:
			response.Result = count
		case <-time.After(200 * time.Millisecond):
			return fmt.Errorf("request timed out")
		}
	}
}

// responseLoop listens to the response channel for responses from the state machine.
// It is important that this channel is always monitored otherwise raft may block.
func (s *Server) responseLoop() {
	defer s.wg.Done()

	// Listen for incoming responses from the state machine.
	// Note that raft will close this channel when it stopped.
	for response := range s.responseCh {
		result := response.Response.(fsm.Result)

		// Get the result channel for the client that submitted
		// the request.
		s.mu.Lock()
		resultCh := s.clients[result.ClientID]
		s.mu.Unlock()

		// Try to send the result back over the channel.
		select {
		case resultCh <- result.Count:
		default:
		}
	}
}
```

Finally, you can create a new `Server` in your application code as so:
```go
package main

import (
	"net"

	"github.com/jmsadair/raft"
	"your/app/package/fsm"
    "your/app/package/server"
)

const (
	// The ID of your server.
	serverID = "server-1"

	// The IDs of the raft servers in the cluster.
	raft1 = "raft-1"
	raft2 = "raft-2"
	raft3 = "raft-3"

	// The paths to the files that this raft server will use for persisting its state.
	// If the files exists, then the raft server will read it into memory and initialize
	// itself with the content. Otherwise, it will create new files.
	logPath      = "data/raft-1-log.bin"
	storagePath  = "data/raft-1-storage.bin"
	snapshotPath = "data/raft-1-snaphot.bin"
)

func makeAddress(ip string, port int) net.Addr {
	return &net.TCPAddr{
		IP:   net.ParseIP(ip),
		Port: port,
	}
}

func main() {
	// The peers that form the raft cluster.
	peers := map[string]net.Addr{
		raft1: makeAddress("172.0.0.1", 8080),
		raft2: makeAddress("172.0.0.2", 8080),
		raft3: makeAddress("172.0.0.3", 8080),
	}

	// The state machine we are replicating.
	fsm := fsm.NewStateMachine()

	// A channel for receiving responses from the state machine.
	responseCh := make(chan raft.CommandResponse)

	// Now, we can create a default raft server.
	raft, err := raft.NewServer(raft1, peers, fsm, logPath, storagePath, snapshotPath, responseCh)
	if err != nil {
		panic(err)
	}

	// Pass the raft instance and response channel to server you created and start it.
	serverAddress := makeAddress("172.0.0.4", 8080)
	server := server.NewServer(serverID, serverAddress, raft, responseCh)
	if err := server.Start(); err != nil {
		panic(err)
	}

	// The rest of your application logic
	// ...
	// ...
}
```

Note that this example has certain portions of code omitted for clarity and is not complete. It is only a basic demonstration of how this library can be used and is missing many features.