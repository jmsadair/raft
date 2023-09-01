/*
This library provides a simple, easy-to-understand, and reliable implementation of Raft using Go. Raft is a consensus
protocol designed to manage replicated logs in a distributed system. Its purpose is to ensure fault-tolerant coordination
and consistency among a group of nodes, making it suitable for building reliable systems. Potential use cases include
distributed file systems, consistent key-value stores, and service discovery.

There are two ways that this library can be used. The first way is to use the Raft implementation to create a custom server.
This may be useful if you wish to use a different communication protocol. The second way is to use the provided Server
implementation, which employs gRPC and protobuf. This is covered below.

To set up a server, the first step is to define the state machine that is to be replicated. This state machine must
implement that StateMachine interface, and it must be concurrent safe. Here is an example of a type that implements
the StateMachine interface.

	// Op represents an operation on the state machine.
	type Op int

	const (
	    // Increment increments the counter by one.
	    Increment Op = iota

	    // Decrement decrements the counter by one.
	    Decrement
	)

	// Result represents the result of applying an operation
	// to the state machine.
	type Result struct {
	    // The value of the counter after applying the operation.
	    Value int

	    // Any errors encountered while applying the operation.
	    Err error
	}

	// StateMachine represents a simple counter.
	type StateMachine struct {
	    // The current count.
	    count              int

	    // Makes the state machine concurrent safe.
	    mu                 sync.Mutex
	}

	func (sm *StateMachine) Apply(operation *raft.Operation) interface{} {
	    sm.mu.Lock()
	    defer sm.mu.Unlock()

	    // If the operation is read-only, just return the value of the counter.
	    // You might need to actually decode the operation for a more complex state machine.
	    if operation.IsReadOnly {
	        return Result{Value: sm.count, Error: nil}
	    }

	    // Decode the operation.
	    var decodedOp int
	    buf := bytes.NewBuffer(operation.Bytes)
	    dec := gob.NewDecoder(buf)
	    if err := dec.Decode(decodedOp); err != nil {
	        return Result{Err: err}
	    }

	    // Apply the operation.
	    op := decodedOp.(Op)
	    switch op {
	    case Increment:
	        sm.count++
	    case Decrement:
	        sm.count--
	    }

	    return Result{Value: sm.count, Error: nil}
	}

	func (sm *StateMachine) Snapshot() ([]byte, error) {
	    sm.mu.Lock()
	    defer sm.mu.Unlock()

	    // Encode the state of the state machine.
	    var buf bytes.Buffer
	    enc := gob.NewEncoder(&buf)
	    if err := enc.Encode(sm.count); err != nil {
	        return nil, err
	    }

	    return buf.bytes(), nil
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

	    return nil
	}

	func (sm *StateMachine) NeedSnapshot() bool {
	    s.mu.Lock()
	    defer s.mu.Unlock()

	    // This probably is not a realisitic condition for needing a snapshot, but
	    // this state machine is only a counter.
	    return s.lastIndex % 100 == 0
	}

Now, create a map that maps Raft IDs to their respective Peer. This map should contain the ID and corresponding Peer
of all the Raft instances in the cluster, including this one.

	peerAddr1, _ := net.ResolveTCPAddr("tcp", "127.0.0.1:8080")
	peerAddr2, _ := net.ResolveTCPAddr("tcp", "127.0.0.2:8080")
	peerAddr3, _ := net.ResolveTCPAddr("tcp", "127.0.0.3:8080")
	peers := map[string]string{
	    "raft-1": raft.NewPeer("raft-1", peerAddr1),
	    "raft-2": raft.NewPeer("raft-2", peerAddr2),
	    "raft-3": raft.NewPeer("raft-1", peerAddr3),
	}

Then, create the Log, Storage, and SnapshotStorage instances that Raft will use to persist its state. Note that if the
file at the path exists, Raft will read it into memory and initialize itself with the content. Otherwise, Raft will create
the file.

	log := raft.NewLog("raft-1-log.bin")
	storage := raft.NewStorage("raft-1-storage.bin")
	snapshotStorage := raft.NewSnapshotStorage("raft-1-snapshots.bin")

Next, create an instance of the StateMachine implementation.

	fsm := new(StateMachine)

A Raft instance may now be created as below.

	raft, err := raft.NewRaft("raft-1", peers, log, storage, snapshotStorage, fsm)

Note that you can also specify options such as election timeout and lease duration when creating a new
Raft instance. For example, the below code will create a Raft instance that uses 500 milliseconds as its
election timeout. If no options are provided, the default options are used.

	raft, err := raft.NewRaft("raft-1", peers, log, storage, snapshotStorage, fsm, responseCh, raft.WithElectionTimeout(500*time.Millisecond))

All that remains is to create a Server instance and start it.

	server, err := raft.NewServer(raft)

Here is how to start the Server instance.

	// This sends a signal to the Raft implementation to start.
	readyCh := make(chan interface{})

	// Once Start is called, the server is prepared to start receiving RPCs.
	err := server.Start(readyCh)
	if err != nil {
	    panic(err)
	}

	// Start Raft.
	close(readyCh)

Finally, here is how to submit an operation to the Server instance once it is started. A normal operation may be submitted or,
alternatively, a read-only operation may be submitted. Generally, read-only operations will offer much better performance than standard
operations due to the fact that they are not added to the log or replicated. However, read-only operations are implemented
using leases. This means that they are not safe - a read-only operation may read stale or incorrect data under certian
circumstances. If your application demands strong consistency, read-only operations should not be used.

When either type of operation is submitted, a future for an operation response is returned. This future can be awaited
to get the result of the operation. Note that the response to the operation may contain an error. It is important to
ensure that the error is nil before consuming the contents of the response because, if the error is not nil, the
contents of the response are not valid.

	// Encode an operation.
	var buffer bytes.Buffer
	encoder := gob.NewEncoder(&buffer)
	err := encoder.Encode(Increment)
	if err != nil {
		panic(err)
	}
	op := buffer.Bytes()

	// Sumbit a standard operation with a 200 ms timeout.
	future := server.SubmitOperation(op)
	response := future.Await()

	// Submit a read-only operation.
	// In this case, since the value of the counter is returned on a read-only operation, an empty operation is submitted.
	future := server.SubmitReadOnlyOperation([]byte{}, 200 * time.Millisecond)
	response := future.Await()

Be warned that this is a highly simplified example that demonstrates how raft may be used and some of its features.
This implementation leaves out many details that would typically be associated with a system that uses Raft such
as duplicate detection and retry mechanisms.
*/
package raft
