package raft

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"net"
	"reflect"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/jmsadair/raft/internal/errors"

	"github.com/jmsadair/raft/internal/util"
	"github.com/stretchr/testify/require"
)

func validateLogEntry(t *testing.T, entry *LogEntry, expectedIndex uint64, expectedTerm uint64, expectedData []byte) {
	require.Equal(t, expectedIndex, entry.Index)
	require.Equal(t, expectedTerm, entry.Term)
	require.Equal(t, expectedData, entry.Data)
}

func validateSnapshot(t *testing.T, expected *Snapshot, actual *Snapshot) {
	require.Equal(t, expected.LastIncludedIndex, actual.LastIncludedIndex)
	require.Equal(t, expected.LastIncludedTerm, actual.LastIncludedTerm)
	require.Equal(t, expected.Data, actual.Data)
}

func makeOperations(numOperations int) [][]byte {
	operations := make([][]byte, numOperations)
	for i := 1; i <= numOperations; i++ {
		operations[i-1] = []byte(fmt.Sprintf("operation %d", i))
	}
	return operations
}

func makePeerMaps(numServers int) []map[string]Peer {
	clusterPeers := make([]map[string]Peer, numServers)
	for i := 0; i < numServers; i++ {
		clusterPeers[i] = make(map[string]Peer, numServers)
		for j := 0; j < numServers; j++ {
			peerID := fmt.Sprint(j)
			peerAddr, _ := net.ResolveTCPAddr("tcp", fmt.Sprintf("127.0.0.%d:8080", j))
			clusterPeers[i][peerID] = NewPeer(peerID, peerAddr)
		}
	}
	return clusterPeers
}

func encodeOperations(operations []Operation) ([]byte, error) {
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	if err := enc.Encode(operations); err != nil {
		return buf.Bytes(), err
	}
	return buf.Bytes(), nil
}

func decodeOperations(data []byte) ([]Operation, error) {
	var operations []Operation
	buf := bytes.NewBuffer(data)
	dec := gob.NewDecoder(buf)
	if err := dec.Decode(&operations); err != nil {
		return operations, err
	}
	return operations, nil
}

type stateMachineMock struct {
	operations   []Operation
	snapshotting bool
	snapshotSize int
	mu           sync.Mutex
}

func newStateMachineMock(snapshotting bool, snapshotSize int) *stateMachineMock {
	gob.Register(Operation{})
	return &stateMachineMock{operations: make([]Operation, 0), snapshotting: snapshotting, snapshotSize: snapshotSize}
}

func (s *stateMachineMock) Apply(operation *Operation) interface{} {
	s.mu.Lock()
	defer s.mu.Unlock()
	if operation.IsReadOnly {
		return len(s.operations)
	}
	s.operations = append(s.operations, *operation)
	return len(s.operations)
}

func (s *stateMachineMock) Snapshot() ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	snapshotBytes, err := encodeOperations(s.operations)
	if err != nil {
		return nil, fmt.Errorf("error encoding state machine state: %s", err.Error())
	}
	return snapshotBytes, nil
}

func (s *stateMachineMock) Restore(snapshot *Snapshot) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	entries, err := decodeOperations(snapshot.Data)
	if err != nil {
		return errors.WrapError(err, "error decoding state machine state")
	}

	s.operations = entries

	return nil
}

func (s *stateMachineMock) NeedSnapshot(stateSizeInBytes int64) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.snapshotting && len(s.operations)%s.snapshotSize == 0
}

func (s *stateMachineMock) appliedOperations() []Operation {
	s.mu.Lock()
	defer s.mu.Unlock()

	operationsCopy := make([]Operation, len(s.operations))
	copy(operationsCopy, s.operations)

	// Set the response channel to nil, since these operations will be compared to others.
	// Not all operations will necessarily have the same response channel.
	for i := range operationsCopy {
		operationsCopy[i].ResponseCh = nil
	}

	return operationsCopy
}

type testCluster struct {
	// The testing instance associated with the cluster.
	t *testing.T

	// The servers making up the cluster.
	servers []*Server

	// The raft instances that make up the cluster.
	rafts []*Raft

	// The peers fore each server, where peers[i] is the peers
	// for servers[i].
	peers []map[string]Peer

	// The log associated with each server, where logs[i] is the
	// log for servers[i]
	logs []Log

	// The snapshot storage associated with each server, where
	// snapshotStore[i] is the snapshot storage for servers[i]
	snapshotStores []SnapshotStorage

	// The storage associated with each server, where stores[i] is the
	// storage for servers[i]
	stores []Storage

	// The servers which are disconnected, where disconnected[i] being
	// true indicates servers[i] is disconnected.
	disconnected []bool

	// The state machine associated with each server, where fsm[i]
	// corresponds to the state machine for servers[i].
	fsm []*stateMachineMock

	// A channel to signal the shutdown of the cluster.
	shutdownCh chan interface{}

	// Indicates whether auto snapshotting will be used.
	snapshotting bool

	// The maximum number of log entries per snapshot if snapshotting is enabled.
	snapshotSize int

	mu sync.Mutex
}

func newCluster(t *testing.T, numServers int, snapshotting bool, snapshotSize int) *testCluster {
	servers := make([]*Server, numServers)
	rafts := make([]*Raft, numServers)
	logs := make([]Log, numServers)
	snapshotStores := make([]SnapshotStorage, numServers)
	stores := make([]Storage, numServers)
	fsm := make([]*stateMachineMock, numServers)
	disconnected := make([]bool, numServers)

	// The paths for all the persistent storage associated with the cluster.
	tmpDir := t.TempDir()
	snapshotFileFmt := tmpDir + "/raft-snapshots-%d"
	logFileFmt := tmpDir + "/raft-log-%d"
	storageFileFmt := tmpDir + "/raft-storage-%d"

	// Make peer map for each server.
	peers := makePeerMaps(numServers)

	// Create the raft and server instances.
	for i := 0; i < numServers; i++ {
		id := fmt.Sprint(i)
		fsm[i] = newStateMachineMock(snapshotting, snapshotSize)
		logs[i] = NewLog(fmt.Sprintf(logFileFmt, i))
		snapshotStores[i] = NewSnapshotStorage(fmt.Sprintf(storageFileFmt, i))
		stores[i] = NewStorage(fmt.Sprintf(snapshotFileFmt, i))

		raft, err := NewRaft(id, peers[i], logs[i], stores[i], snapshotStores[i], fsm[i])
		if err != nil {
			t.Fatalf("failed to create raft instance: err = %s", err.Error())
		}
		rafts[i] = raft

		server, err := NewServer(raft)
		if err != nil {
			t.Fatalf("failed to create server instance: err = %s", err.Error())
		}
		servers[i] = server
	}

	return &testCluster{
		t:              t,
		servers:        servers,
		rafts:          rafts,
		disconnected:   disconnected,
		peers:          peers,
		logs:           logs,
		snapshotStores: snapshotStores,
		stores:         stores,
		fsm:            fsm,
		shutdownCh:     make(chan interface{}),
		snapshotting:   snapshotting,
		snapshotSize:   snapshotSize,
	}
}

func (tc *testCluster) startCluster() {
	ready := make(chan interface{})
	for i, server := range tc.servers {
		if err := server.Start(ready); err != nil {
			tc.t.Fatalf("failed to start cluster server: server = %d, err = %s", i, err.Error())
		}
	}
	close(ready)
}

func (tc *testCluster) stopCluster() {
	for _, server := range tc.servers {
		server.Stop()
	}
	close(tc.shutdownCh)
}

func (tc *testCluster) submit(operation []byte, retry bool, expectFail bool, readOnly bool) {
	// Time between submission attempts. If no leader was found, allow for
	// an election to complete.
	electionTimeout := 200 * time.Millisecond

	// Allow for a maximum of three seconds if retry is enabled.
	start := time.Now()
	for time.Since(start).Seconds() < 3 {
		for j := 0; j < len(tc.servers); j++ {
			tc.mu.Lock()
			server := tc.servers[j]
			tc.mu.Unlock()

			// Submit an operation to a server. It might be a leader.
			var future *OperationResponseFuture
			if !readOnly {
				future = server.SubmitOperation(operation, 200*time.Millisecond)
			} else {
				future = server.SubmitReadOnlyOperation(operation, 200*time.Millisecond)
			}

			if response := future.Await(); response.Err == nil {
				if string(response.Operation.Bytes) != string(operation) {
					tc.t.Fatalf("response operation does not match submitted operation")
				}
				return
			}
		}

		if !retry {
			break
		}

		time.Sleep(electionTimeout)
	}

	if !expectFail {
		tc.t.Fatalf("cluster failed to apply Operation: operation = %s", string(operation))
	}
}

func (tc *testCluster) checkStateMachines(expectedMatches int, timeout time.Duration) {
	startTime := time.Now()
	appliedOperationsPerServer := make([][]Operation, len(tc.servers))

	for time.Since(startTime) < timeout {
		// Take the longest array of applied operations to be the source of truth.
		longestAppliedIndex := -1
		for i := 0; i < len(tc.servers); i++ {
			appliedOperations := tc.fsm[i].appliedOperations()
			if longestAppliedIndex == -1 || len(appliedOperations) > len(appliedOperationsPerServer[longestAppliedIndex]) {
				longestAppliedIndex = i
			}
			appliedOperationsPerServer[i] = appliedOperations
		}

		// Check if the other arrays of applied operations match the longest array of applied operations.
		matches := 1
		for i, applied := range appliedOperationsPerServer {
			if i == longestAppliedIndex {
				continue
			}
			if reflect.DeepEqual(appliedOperationsPerServer[longestAppliedIndex], applied) {
				matches++
			}
		}

		if matches >= expectedMatches {
			return
		}
	}

	tc.t.Fatalf("cluster state machines do not match")
}

func (tc *testCluster) checkLeaders(expectNoLeader bool) int {
	// Any leaders detected.
	leaders := make([]int, 0)

	// Time between checks for a leader. This amount should be large enough
	// to allow an election to take place.
	electionTimeout := 300 * time.Millisecond

	// A maximum of 5 seconds is given to successfully elect a leader.
	start := time.Now()
	for time.Since(start).Seconds() < 5 {
		for i := 0; i < len(tc.servers); i++ {
			tc.mu.Lock()
			server := tc.servers[i]
			tc.mu.Unlock()

			// Get the status of the server, it may be a leader.
			status := server.Status()

			// If the server is a leader, and it is connected, then it is
			// a legitimate leader. Leaders that are disconnected or
			// partitioned are ignored. It is assumed that disconnected
			// servers are either:
			// 1. Completely disconnected from all other servers - it
			//    cannot communicate with any other servers, and no other
			//    servers can communicate with it.
			// 2. In a minority partition - it may only communicate with
			//    a minority of the cluster. Members of the majority partition
			//    cannot communicate with it.
			tc.mu.Lock()
			if status.State == Leader && !tc.disconnected[i] {
				index, _ := strconv.Atoi(status.ID)
				leaders = append(leaders, index)
			}
			tc.mu.Unlock()
		}

		if len(leaders) > 1 {
			tc.t.Fatalf("cluster has more than one leader: leaders = %v", leaders)
		}

		if len(leaders) == 1 {
			break
		}

		// If not leaders were found, sleep for a sufficient amount of time to allow
		// an election to take place.
		time.Sleep(electionTimeout)
	}

	if len(leaders) == 0 && !expectNoLeader {
		tc.t.Fatal("cluster failed to elect a leader")
	}

	if len(leaders) != 0 && expectNoLeader {
		tc.t.Fatalf("cluster elected leader without quorum: leaders = %v", leaders)
	}

	if expectNoLeader {
		return -1
	}

	return leaders[0]
}

func (tc *testCluster) crashServer(server int) {
	tc.disconnectServer(server)
	tc.servers[server].Stop()
}

func (tc *testCluster) restartServer(server int) {
	tc.mu.Lock()
	defer tc.mu.Unlock()

	serverID := fmt.Sprint(server)

	tc.fsm[server] = newStateMachineMock(tc.snapshotting, tc.snapshotSize)

	newRaft, err := NewRaft(serverID, tc.peers[server], tc.logs[server], tc.stores[server], tc.snapshotStores[server],
		tc.fsm[server])
	if err != nil {
		tc.t.Fatalf("failed to create raft instance: err = %s", err.Error())
	}
	tc.rafts[server] = newRaft

	newServer, err := NewServer(newRaft)
	if err != nil {
		tc.t.Fatalf("failed to create cluster server: err = %s", err.Error())
	}

	tc.servers[server] = newServer

	readyCh := make(chan interface{})
	defer close(readyCh)
	if err := newServer.Start(readyCh); err != nil {
		tc.t.Fatalf("failed to start cluster server: server = %d, err = %s", server, err.Error())
	}

	for i := 0; i < len(tc.servers); i++ {
		if err := tc.rafts[i].connectPeer(serverID); err != nil {
			tc.t.Fatalf("error reconnecting peer: peer = %d, connectingTo = %d, err = %s", i, server, err.Error())
		}
	}

	tc.disconnected[server] = false
}

func (tc *testCluster) createPartition() {
	tc.mu.Lock()
	defer tc.mu.Unlock()

	// The number of servers in the partition.
	partitionSize := len(tc.servers) / 2

	// The servers in the partition.
	partitionSet := make(map[int]bool)

	// Choose random servers to partition.
	index := util.RandomInt(0, len(tc.servers))
	for i := 0; i < partitionSize; i++ {
		partitionSet[(index+i)%len(tc.servers)] = true
	}

	// Disconnect all servers in the partition set from those
	// that are not, but maintain connections between the servers
	// that are in the partition set.
	for i := 0; i < len(tc.servers); i++ {
		if _, ok := partitionSet[i]; ok {
			for j := 0; j < len(tc.servers); j++ {
				if _, ok := partitionSet[j]; ok {
					continue
				}
				if err := tc.rafts[i].disconnectPeer(fmt.Sprint(j)); err != nil {
					tc.t.Fatalf("error disconnecting peer: peer = %d, disconnectingFrom = %d, err = %s", i, j,
						err.Error())
				}
				if err := tc.rafts[j].disconnectPeer(fmt.Sprint(i)); err != nil {
					tc.t.Fatalf("error disconnecting peer: peer = %d, disconnectingFrom = %d, err = %s", j, i,
						err.Error())
				}
			}

			if err := tc.rafts[i].disconnectPeer(fmt.Sprint(i)); err != nil {
				tc.t.Fatalf("error disconnecting peer: peer = %d, disconnectingFrom = %d, err = %s", i, i,
					err.Error())
			}
		}
	}

	for index := range partitionSet {
		tc.disconnected[index] = true
	}
}

func (tc *testCluster) reconnectServer(server int) {
	tc.mu.Lock()
	defer tc.mu.Unlock()

	serverID := fmt.Sprint(server)

	// Reconnect this server to itself. Note that this has no effect
	// on the operation of the cluster. The only purpose of this is to
	// indicate that this server is connected and should operate as expected.
	if err := tc.rafts[server].disconnectPeer(serverID); err != nil {
		tc.t.Fatalf("error reconnecting peer: peer = %d, connectingTo = %d, err = %s", server, server, err.Error())
	}

	for i := 0; i < len(tc.servers); i++ {
		if server == i {
			continue
		}
		if err := tc.rafts[i].connectPeer(serverID); err != nil {
			tc.t.Fatalf("error reconnecting peer: peer = %d, connectingTo = %d, err = %s", i, server, err.Error())
		}
		if err := tc.rafts[server].connectPeer(fmt.Sprint(i)); err != nil {
			tc.t.Fatalf("error reconnecting peer: peer = %d, connectingTo = %d, err = %s", server, i, err.Error())
		}
	}

	tc.disconnected[server] = false
}

func (tc *testCluster) reconnectAllServers() {
	tc.mu.Lock()
	defer tc.mu.Unlock()

	for i := 0; i < len(tc.servers); i++ {
		for j := 0; j < len(tc.servers); j++ {
			if err := tc.rafts[i].connectPeer(fmt.Sprint(j)); err != nil {
				tc.t.Fatalf("error reconnecting peer: peer = %d, connectingTo = %d, err = %s", i, j, err.Error())
			}
		}
	}

	for i := 0; i < len(tc.servers); i++ {
		tc.disconnected[i] = false
	}
}

func (tc *testCluster) disconnectServer(server int) {
	tc.mu.Lock()
	defer tc.mu.Unlock()

	serverID := fmt.Sprint(server)

	// Disconnect this index from itself. Note that this has no effect
	// on the operation of the cluster. The only purpose of this is to
	// indicate that this index is disconnected and will not operate
	// as expected.
	if err := tc.rafts[server].disconnectPeer(serverID); err != nil {
		tc.t.Fatalf("error disconnecting peer: peer = %d, disconnectingFrom = %d, err = %s", server, server, err.Error())
	}

	for i := 0; i < len(tc.servers); i++ {
		if i == server {
			continue
		}
		if err := tc.rafts[i].disconnectPeer(serverID); err != nil {
			tc.t.Fatalf("error disconnecting peer: peer = %d, disconnectingFrom = %d, err = %s", i, server, err.Error())
		}
		if err := tc.rafts[server].disconnectPeer(fmt.Sprint(i)); err != nil {
			tc.t.Fatalf("error disconnecting peer: peer = %d, disconnectingFrom = %d, err = %s", server, i, err.Error())
		}
	}

	tc.disconnected[server] = true
}
