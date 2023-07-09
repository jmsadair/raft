package raft

import (
	"bytes"
	"encoding/gob"
	"fmt"
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

func makeOperations(numOperations int) []Operation {
	operations := make([]Operation, numOperations)
	for i := 1; i <= numOperations; i++ {
		operations[i-1] = Operation{Bytes: []byte(fmt.Sprintf("operation %d", i))}
	}

	return operations
}

func makePeerMaps(numServers int) []map[string]string {
	clusterPeers := make([]map[string]string, numServers)
	for i := 0; i < numServers; i++ {
		clusterPeers[i] = make(map[string]string, numServers)
		for j := 0; j < numServers; j++ {
			address := fmt.Sprintf("127.0.0.%d:8080", j)
			peerID := fmt.Sprint(j)
			clusterPeers[i][peerID] = address
		}
	}

	return clusterPeers
}

func encodeLogEntries(entries []*LogEntry) ([]byte, error) {
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	if err := enc.Encode(entries); err != nil {
		return buf.Bytes(), err
	}
	return buf.Bytes(), nil
}

func decodeLogEntries(data []byte) ([]*LogEntry, error) {
	var operations []*LogEntry
	buf := bytes.NewBuffer(data)
	dec := gob.NewDecoder(buf)
	if err := dec.Decode(&operations); err != nil {
		return operations, err
	}
	return operations, nil
}

type stateMachineMock struct {
	operations   []*LogEntry
	snapshotting bool
	snapshotSize int
	mu           sync.Mutex
}

func newStateMachineMock(snapshotting bool, snapshotSize int) *stateMachineMock {
	gob.Register(LogEntry{})
	return &stateMachineMock{operations: make([]*LogEntry, 0), snapshotting: snapshotting, snapshotSize: snapshotSize}
}

func (s *stateMachineMock) Apply(entry *LogEntry) interface{} {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.operations = append(s.operations, entry)
	return len(s.operations)
}

func (s *stateMachineMock) Snapshot() (Snapshot, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	snapshotBytes, err := encodeLogEntries(s.operations)
	if err != nil {
		return Snapshot{}, fmt.Errorf("error encoding state machine state: %s", err.Error())
	}

	var lastIncludedIndex uint64
	var lastIncludedTerm uint64
	if len(s.operations) == 0 {
		lastIncludedIndex = 0
		lastIncludedTerm = 0
	} else {
		lastIncludedIndex = s.operations[len(s.operations)-1].Index
		lastIncludedTerm = s.operations[len(s.operations)-1].Term
	}

	return Snapshot{LastIncludedIndex: lastIncludedIndex, LastIncludedTerm: lastIncludedTerm, Data: snapshotBytes}, nil
}

func (s *stateMachineMock) Restore(snapshot *Snapshot) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	entries, err := decodeLogEntries(snapshot.Data)
	if err != nil {
		return errors.WrapError(err, "error decoding state machine state")
	}

	s.operations = entries

	return nil
}

func (s *stateMachineMock) NeedSnapshot() bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.snapshotting && len(s.operations)%s.snapshotSize == 0
}

type storagePaths struct {
	logPath             string
	storagePath         string
	snapshotStoragePath string
}

type testCluster struct {
	// The testing instance associated with the cluster.
	t *testing.T

	// The servers making up the cluster.
	servers []*Server

	// The peers fore each server, where peers[i] is the peers
	// for servers[i].
	peers []map[string]string

	// The associated storage paths for each server, where
	// paths[i] is the paths for servers[i].
	paths []storagePaths

	// The servers which are disconnected, where disconnected[i] being
	// true indicates servers[i] is disconnected.
	disconnected []bool

	// The state machine associated with each server, where fsm[i]
	// corresponds to the state machine for servers[i].
	fsm []*stateMachineMock

	// The response channel associated with each server, where
	// responseCh[i] corresponds to the response channel for
	// servers[i]
	responseCh []chan OperationResponse

	// A channel to signal the shutdown of the cluster.
	shutdownCh chan interface{}

	// The operation responses associated with each server, where
	// operationResponses[i] corresponds to the responses for
	// servers[i]. The map maps indices to the associated operation
	// response.
	operationResponses []map[uint64]OperationResponse

	// Errors encountered during the execution of a background go
	// routine. Provides a means of shuttling the error from the background
	// go routine to the main, testing go routine. Note that serverErrors[i] corresponds
	// to any errors associated with servers[i].
	serverErrors []string

	// The last applied index for each server, where lastApplied[i]
	// corresponds to the last applied index for servers[i].
	lastApplied []uint64

	// Indicates whether auto snapshotting will be used.
	snapshotting bool

	// The maximum number of log entries per snapshot if snapshotting is enabled.
	snapshotSize int

	mu sync.Mutex

	wg sync.WaitGroup
}

func newCluster(t *testing.T, numServers int, snapshotting bool, snapshotSize int) *testCluster {
	servers := make([]*Server, numServers)
	fsm := make([]*stateMachineMock, numServers)
	replicateCh := make([]chan OperationResponse, numServers)
	responses := make([]map[uint64]OperationResponse, numServers)
	serverErrors := make([]string, numServers)
	lastApplied := make([]uint64, numServers)
	disconnected := make([]bool, numServers)
	paths := make([]storagePaths, numServers)

	// The paths for all the persistent storage associated with the cluster.
	tmpDir := t.TempDir()
	snapshotFileFmt := tmpDir + "/raft-snapshots-%d"
	logFileFmt := tmpDir + "/raft-log-%d"
	storageFileFmt := tmpDir + "/raft-storage-%d"

	// Make peer map for each server.
	peers := makePeerMaps(numServers)

	for i := 0; i < numServers; i++ {
		replicateCh[i] = make(chan OperationResponse)
		fsm[i] = newStateMachineMock(snapshotting, snapshotSize)
		responses[i] = make(map[uint64]OperationResponse)
		paths[i] = storagePaths{logPath: fmt.Sprintf(logFileFmt, i), storagePath: fmt.Sprintf(storageFileFmt, i),
			snapshotStoragePath: fmt.Sprintf(snapshotFileFmt, i)}
		id := fmt.Sprint(i)

		server, err := NewServer(id, peers[i], fsm[i], paths[i].logPath, paths[i].storagePath, paths[i].snapshotStoragePath, replicateCh[i])
		if err != nil {
			t.Fatalf("failed to create cluster server: server = %d, err = %s", i, err.Error())
		}

		servers[i] = server
	}

	return &testCluster{
		t:                  t,
		servers:            servers,
		disconnected:       disconnected,
		peers:              peers,
		paths:              paths,
		fsm:                fsm,
		responseCh:         replicateCh,
		operationResponses: responses,
		serverErrors:       serverErrors,
		lastApplied:        lastApplied,
		shutdownCh:         make(chan interface{}),
		snapshotting:       snapshotting,
		snapshotSize:       snapshotSize,
	}
}

func (tc *testCluster) startCluster() {
	ready := make(chan interface{})
	for i, server := range tc.servers {
		if err := server.Start(ready); err != nil {
			tc.t.Fatalf("failed to start cluster server: server = %d, err = %s", i, err.Error())
		}
		tc.wg.Add(1)
		go tc.applyLoop(i)
	}
	close(ready)
}

func (tc *testCluster) stopCluster() {
	for _, server := range tc.servers {
		server.Stop()
	}
	close(tc.shutdownCh)
	tc.wg.Wait()
}

func (tc *testCluster) submit(operation Operation, retry bool, expectFail bool, expectedApplied int) {
	// Time between submission attempts. If no leader was found, allow for
	// an election to complete.
	electionTimeout := 200 * time.Millisecond

	// Allow for a maximum of five seconds if retry is enabled.
	start := time.Now()
	for time.Since(start).Seconds() < 5 {
		for j := 0; j < len(tc.servers); j++ {
			tc.mu.Lock()
			server := tc.servers[j]
			tc.mu.Unlock()

			// Submit an operation to a server. It might be a leader.
			index, term, err := server.SubmitOperation(operation)
			if err != nil {
				continue
			}

			// See if the operation is applied.
			for k := 0; k < 10; k++ {
				time.Sleep(25 * time.Millisecond)
				successful := tc.checkApplied(index, expectedApplied)
				if successful {
					if expectFail {
						tc.t.Fatalf("cluster applied a operation without quorum: operation = %s", string(operation.Bytes))
					}
					return
				}

				// If the server's term changed, then the operation
				// is definitely not going to be applied.
				status := server.Status()
				if status.Term != term {
					break
				}
			}
		}

		if !retry {
			break
		}

		time.Sleep(electionTimeout)
	}

	if !expectFail {
		tc.t.Fatalf("cluster failed to apply Operation: operation = %s", string(operation.Bytes))
	}
}

func (tc *testCluster) checkLeaders(expectNoLeader bool) int {
	// Any leaders detected.
	leaders := make([]int, 0)

	// Time between checks for a leader. This amount should be large enough
	// to allow an election to take place.
	electionTimeout := 300 * time.Millisecond

	// A maximum of 3 seconds is given to successfully elect a leader.
	start := time.Now()
	for time.Since(start).Seconds() < 3 {
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
			//    did not communicate with it.
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

func (tc *testCluster) checkLogs(index int, response OperationResponse) {
	tc.mu.Lock()
	defer tc.mu.Unlock()

	// The last applied index should be monotonically increasing.
	expectedIndex := tc.lastApplied[index] + 1
	if response.Index != expectedIndex {
		tc.serverErrors[index] = fmt.Sprintf("cluster applied Operations out of order: server = %d, expectedIndex = %d, actualIndex = %d",
			index, expectedIndex, response.Index)
		return
	}

	tc.operationResponses[index][response.Index] = response
	tc.lastApplied[index]++
}

func (tc *testCluster) checkSnapshot(index int, response OperationResponse) {
	tc.mu.Lock()
	defer tc.mu.Unlock()

	snapshots := tc.servers[index].ListSnapshots()

	for _, snapshot := range snapshots {
		if snapshot.LastIncludedIndex+1 == response.Index {
			// Decode the snapshot data into entries.
			appliedEntries, err := decodeLogEntries(snapshot.Data)
			if err != nil {
				tc.serverErrors[index] = err.Error()
				return
			}

			if len(appliedEntries) == 0 {
				tc.serverErrors[index] = fmt.Sprintf("cluster took snapshot that was empty: server = %d", index)
				return
			}

			actualLastIncludedIndex := appliedEntries[len(appliedEntries)-1].Index
			actualLastIncludedTerm := appliedEntries[len(appliedEntries)-1].Term

			// Check the last included index matches with the last included index in the snapshot bytes.
			if actualLastIncludedIndex != snapshot.LastIncludedIndex {
				tc.serverErrors[index] = fmt.Sprintf("cluster took snapshot with incorrect last included index: server = %d, lastIncludedIndex = %d, actualLastIncludedIdex = %d", index, snapshot.LastIncludedIndex, actualLastIncludedIndex)
			}

			// Check the last included term matches with the last included term in the snapshot bytes.
			if actualLastIncludedTerm != snapshot.LastIncludedTerm {
				tc.serverErrors[index] = fmt.Sprintf("cluster took snapshot with incorrect last included term: server = %d, lastIncludedTerm = %d, actualLastIncludedTerm = %d", index, snapshot.LastIncludedTerm, actualLastIncludedTerm)
			}

			// Update this server's responses with the operations included in the snapshot.
			for _, entry := range appliedEntries {
				tc.operationResponses[index][entry.Index] = OperationResponse{Index: entry.Index, Term: entry.Term, Operation: entry.Data}
			}

			tc.operationResponses[index][response.Index] = response
			tc.lastApplied[index] = response.Index
			return
		}
	}

	tc.serverErrors[index] = fmt.Sprintf("cluster applied Operations out of order: server = %d, expectedIndex = %d, actualIndex = %d",
		index, tc.lastApplied[index]+1, response.Index)
}

func (tc *testCluster) checkApplied(index uint64, expectedApplied int) bool {
	tc.mu.Lock()
	defer tc.mu.Unlock()

	// The expected operation response from all the servers.
	// All servers should have the same operation response at a
	// given index.
	var expectedOperationResponse OperationResponse

	// The number of servers that have applied the operation at the provided index.
	hasApplied := 0

	for i := 0; i < len(tc.servers); i++ {
		if tc.serverErrors[i] != "" {
			tc.t.Fatalf(tc.serverErrors[i])
		}

		if operationResponse, ok := tc.operationResponses[i][index]; ok {
			// Ensure the Operations match.
			if hasApplied != 0 && string(operationResponse.Operation) != string(expectedOperationResponse.Operation) {
				tc.t.Fatalf("cluster applied different Operations at the same index: index = %d, Operation1 = %s, Operation2 = %s",
					index, string(expectedOperationResponse.Operation), string(operationResponse.Operation))
			}
			expectedOperationResponse = operationResponse
			hasApplied++
		}
	}

	return hasApplied >= expectedApplied
}

func (tc *testCluster) applyLoop(index int) {
	defer tc.wg.Done()

	for response := range tc.responseCh[index] {
		// If the index was greater than the expected index, then either
		// a snapshot must have been installed or there was an error.
		if response.Index > tc.lastApplied[index]+1 {
			tc.checkSnapshot(index, response)
		} else {
			tc.checkLogs(index, response)
		}
	}
}

func (tc *testCluster) crashServer(server int) {
	// Do not acquire lock here - will cause deadlock.
	tc.disconnectServer(server)
	tc.servers[server].Stop()
}

func (tc *testCluster) restartServer(server int) {
	tc.mu.Lock()
	defer tc.mu.Unlock()

	serverID := fmt.Sprint(server)

	tc.responseCh[server] = make(chan OperationResponse)
	tc.fsm[server] = newStateMachineMock(tc.snapshotting, tc.snapshotSize)

	newServer, err := NewServer(serverID, tc.peers[server], tc.fsm[server], tc.paths[server].logPath,
		tc.paths[server].storagePath, tc.paths[server].snapshotStoragePath, tc.responseCh[server])
	if err != nil {
		tc.t.Fatalf("failed to start cluster server: server = %d, err = %s", server, err.Error())
	}

	snapshot, _ := tc.servers[server].raft.snapshotStorage.LastSnapshot()
	tc.lastApplied[server] = snapshot.LastIncludedIndex
	tc.servers[server] = newServer
	tc.operationResponses[server] = make(map[uint64]OperationResponse)

	tc.wg.Add(1)
	go tc.applyLoop(server)

	readyCh := make(chan interface{})
	defer close(readyCh)
	if err := newServer.Start(readyCh); err != nil {
		tc.t.Fatalf("failed to start cluster server: server = %d, err = %s", server, err.Error())
	}

	for i := 0; i < len(tc.servers); i++ {
		if err := tc.servers[i].raft.connectPeer(serverID); err != nil {
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
				if err := tc.servers[i].raft.disconnectPeer(fmt.Sprint(j)); err != nil {
					tc.t.Fatalf("error disconnecting peer: peer = %d, disconnectingFrom = %d, err = %s", i, j, err.Error())
				}
				if err := tc.servers[j].raft.disconnectPeer(fmt.Sprint(i)); err != nil {
					tc.t.Fatalf("error disconnecting peer: peer = %d, disconnectingFrom = %d, err = %s", j, i, err.Error())
				}
			}

			if err := tc.servers[i].raft.disconnectPeer(fmt.Sprint(i)); err != nil {
				tc.t.Fatalf("error disconnecting peer: peer = %d, disconnectingFrom = %d, err = %s", i, i, err.Error())
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
	if err := tc.servers[server].raft.disconnectPeer(serverID); err != nil {
		tc.t.Fatalf("error reconnecting peer: peer = %d, connectingTo = %d, err = %s", server, server, err.Error())
	}

	for i := 0; i < len(tc.servers); i++ {
		if server == i {
			continue
		}
		if err := tc.servers[i].raft.connectPeer(serverID); err != nil {
			tc.t.Fatalf("error reconnecting peer: peer = %d, connectingTo = %d, err = %s", i, server, err.Error())
		}
		if err := tc.servers[server].raft.connectPeer(fmt.Sprint(i)); err != nil {
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
			if err := tc.servers[i].raft.connectPeer(fmt.Sprint(j)); err != nil {
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
	if err := tc.servers[server].raft.disconnectPeer(serverID); err != nil {
		tc.t.Fatalf("error disconnecting peer: peer = %d, disconnectingFrom = %d, err = %s", server, server, err.Error())
	}

	for i := 0; i < len(tc.servers); i++ {
		if i == server {
			continue
		}
		if err := tc.servers[i].raft.disconnectPeer(serverID); err != nil {
			tc.t.Fatalf("error disconnecting peer: peer = %d, disconnectingFrom = %d, err = %s", i, server, err.Error())
		}
		if err := tc.servers[server].raft.disconnectPeer(fmt.Sprint(i)); err != nil {
			tc.t.Fatalf("error disconnecting peer: peer = %d, disconnectingFrom = %d, err = %s", server, i, err.Error())
		}
	}

	tc.disconnected[server] = true
}
