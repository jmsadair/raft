/*
This library provides a simple, easy-to-understand, and reliable implementation of Raft using Go. Raft is a consensus protocol designed to manage replicated logs
in a distributed system. Its purpose is to ensure fault-tolerant coordination and consistency among a group of nodes, making it suitable for building reliable
systems. Potential use cases include distributed file systems, consistent key-value stores, and service discovery.

There are two ways that this library can be used. The first way is to use the Raft implementation to create a custom server. This may be useful if you wish to
use a different communication protocol or the provided storage implementations are not sufficient for your use case. The second way is to use the provided Server
implementation. This is covered below.

To set up a server, the first step is to define the state machine that is to be replicated. This state machine must implement that StateMachine interface, and it must be concurrent safe.
Here is an example of a type that implements the StateMachine interface.

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

	    // The last index applied to the state machine. Used for snapshots.
	    lastIndex          uint64

	    // The term associated with the last applied index. Used for snapshots.
	    lastTerm           uint64

	    // Makes the state machine concurrent safe.
	    mu                 sync.Mutex
	}

	func (sm *StateMachine) Apply(entry *raft.LogEntry) interface{} {
	    sm.mu.Lock()
	    defer sm.mu.Unlock()

	    // Save the term and index of the last seen entry for snapshotting.
	    sm.lastIndex = entry.Index
	    sm.lastTerm = entry.Term

	    // Decode the operation.
	    var decodedOp int
	    buf := bytes.NewBuffer(entry.Data)
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

	func (sm *StateMachine) NeedSnapshot() bool {
	    s.mu.Lock()
	    defer s.mu.Unlock()

	    // This probably is not a realisitic condition for needing a snapshot, but
	    // this state machine is only a counter.
	    return s.lastIndex % 100 == 0
	}

Now, create a map that maps server IDs to their respective address. This map should contain the ID and address
of all the servers in the cluster, including this one.

	peers := map[string]net.Addr{
	    "raft-1": "127.0.0.0:8080",
	    "raft-2": "127.0.0.1:8080",
	    "raft-3": "127.0.0.2:8080",
	}

Then, select the paths for where the server persist its state. Note that if the file at the path exists, the server will read it into memory
and initialize itself with the content. Otherwise, the server will create the file. There are three paths the server expects: the log path, the storage path, and
the snapshot storage path. The log path specifies where the server will persist its log entries. The storage path specifies where the server will persist
its term and vote. The snapshot path specifies where the server will persist any snapshots that it takes.

	logPath := "raft-1-log"
	storagePath := "raft-1-storage"
	snapshotPath := "raft-1-snapshots"

Now, create the channel that responses from the state machine will be relayed over. Note that, when the server is started, it is important that
this channel is always being monitored. Otherwise, the internal Raft implementation will become blocked.

	responseCh := make(chan raft.CommandResponse)

Next, create an instance of the state machine implementation.

	fsm := new(StateMachine)

The server may now be created.

	server, err := raft.NewServer("raft-1", peers, fsm, logPath, storagePath, snapshotPath, responseCh)

Here is how to start the server.

	// This sends a signal to the Raft implementation to start.
	readyCh := make(chan interface)

	// Once Start is called, the server is prepared to start receiving RPCs.
	err := server.Start(readyCh)
	if err != nil {
	    panic(err)
	}

	// Start a go routine in the background to intercept responses from the state machine.
	go func() {
	    for response := range responseCh {
	        // Handle responses...
	    }
	}()

	// Start Raft.
	close(readyCh)
*/
package raft
