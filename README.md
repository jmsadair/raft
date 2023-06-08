# Raft
This library provides a simple, easy-to-understand, and reliable implementation of [Raft](https://en.wikipedia.org/wiki/Raft_(algorithm)) using Go. Raft is a [consensus](https://en.wikipedia.org/wiki/Consensus_(computer_science)) protocol designed to manage replicated logs in a distributed system. Its purpose is to ensure fault-tolerant coordination and consistency among a group of nodes, making it suitable for building reliable systems. Potential use cases include distributed file systems, consistent key-value stores, and service discovery.

# Getting Started
First, make sure you have Go `1.19` or a higher version installed on your system. You can download and install Go from the official Go website: https://golang.org/. Then, install the Raft library by running `go get -u github.com/jmsadair/raft`.

Once you have the library installed, you can create a Raft server for your application (or write your own server using the `Raft` implementation). Below is a basic
example of how you may do this. Note that this code omits many details for clarity.

```go
package main

import (
    "net"
    "github.com/jmsadair/raft"
)

const (
    // The ID of this raft server.
    raftID = "raft-1"

    // The IP addess of your server.
    raftIP = "172.0.0.1"
    
    // The port of your server.
    raftPort = 8080
    
    // The paths to the files that this raft server will use for persisting its state.
    // If the files exists, then the raft server will read it into memory and initialize
    // itself with the content. Otherwise, it will create new files.
    logPath      = "data/raft-1-log.bin"
    storagePath  = "data/raft-1-storage.bin"
    snapshotPath = "data/raft-1-snaphot.bin"
)

// Your custom state machine that will be replicated.
// This state machine must implement the raft.StateMachine interface.
// This state machine must also be concurrent safe.
type StateMachine struct {
    // ...
}

func (sm *StateMachine) Apply(entry *raft.LogEntry) interface{} {
    // ...
}

func (sm *StateMachine) Snapshot() (raft.Snapshot, error) {
    // ...
}

func (sm *StateMachine) Restore(snapshot *raft.Snapshot) error {
    // ...
}

func main() {
    // The peers that form the raft cluster. This mapping must include this server as well.
    peers := map[string]net.Addr{
        raftID: &net.TCPAddr{IP: net.ParseIP(raftIP), Port: raftPort},
        // Any other peers in the cluster.
        // ...
    }

    // The state machine we are replicating.
    fsm := new(StateMachine)

    // A channel for receiving responses from the state machine.
    responseCh := make(chan raft.CommandResponse)

    // Now, we can create a default raft server.
    raft, err := raft.NewServer(raft1, peers, fsm, logPath, storagePath, snapshotPath, responseCh)
    if err != nil {
        panic(err)
    }

    // This starts the server and returns a channel to signal when the underlying
    // raft implementation should start.
    readyCh := raft.Start()

    // Start a go routine to handle incoming responses from the state machine
    go func() {
        for response := range responseCh {
            // ...
        }
    }

    // Tell raft to start.
    close(readyCh)

    // The rest of your application logic.
    // ...
}
```
