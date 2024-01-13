package raft

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"io"
	"reflect"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/jmsadair/raft/internal/logger"
	"github.com/jmsadair/raft/internal/util"
	"github.com/stretchr/testify/require"
)

func validateLogEntry(
	t *testing.T,
	entry *LogEntry,
	expectedIndex uint64,
	expectedTerm uint64,
	expectedData []byte,
	expectedType LogEntryType,
) {
	require.Equal(t, expectedIndex, entry.Index)
	require.Equal(t, expectedTerm, entry.Term)
	require.Equal(t, expectedData, entry.Data)
	require.Equal(t, expectedType, entry.EntryType)
}

func makeOperations(numOperations int) [][]byte {
	operations := make([][]byte, numOperations)
	for i := 1; i <= numOperations; i++ {
		operations[i-1] = []byte(fmt.Sprintf("operation %d", i))
	}
	return operations
}

func makeClusterConfiguration(numServers int) Configuration {
	members := make(map[string]string, numServers)
	isVoter := make(map[string]bool, numServers)
	for i := 0; i < numServers; i++ {
		id := fmt.Sprint(i)
		address := fmt.Sprintf("127.0.0.%d:8080", i)
		members[id] = address
		isVoter[id] = true
	}

	return Configuration{Members: members, IsVoter: isVoter}
}

func makeRaft(
	id string,
	address string,
	dataPath string,
	snapshotting bool,
	snapshotSize int,
) (*Raft, error) {
	logger, err := makeLogger(id)
	if err != nil {
		return nil, err
	}
	fsm := newStateMachineMock(snapshotting, snapshotSize)
	raft, err := NewRaft(id, address, fsm, dataPath, WithLogger(logger))
	if err != nil {
		return nil, err
	}
	return raft, nil
}

func makeLogger(id string) (Logger, error) {
	prefix := fmt.Sprintf("raft-%s:", id)
	level := logger.Debug
	return logger.NewLogger(logger.WithLevel(level), logger.WithPrefix(prefix))
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
	return &stateMachineMock{
		operations:   make([]Operation, 0),
		snapshotting: snapshotting,
		snapshotSize: snapshotSize,
	}
}

func (s *stateMachineMock) Apply(operation *Operation) interface{} {
	s.mu.Lock()
	defer s.mu.Unlock()
	if operation.OperationType == LeaseBasedReadOnly ||
		operation.OperationType == LinearizableReadOnly {
		return len(s.operations)
	}
	s.operations = append(s.operations, *operation)
	return len(s.operations)
}

func (s *stateMachineMock) Snapshot(snapshotWriter io.Writer) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	snapshotBytes, err := encodeOperations(s.operations)
	if err != nil {
		return fmt.Errorf("error taking snapshot of state machine: error = %v", err)
	}

	if _, err := snapshotWriter.Write(snapshotBytes); err != nil {
		return fmt.Errorf("error taking snapshot of state machine: error = %v", err)
	}

	return nil
}

func (s *stateMachineMock) Restore(snapshotReader io.Reader) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	var buf bytes.Buffer
	_, err := io.Copy(&buf, snapshotReader)
	if err != nil {
		return fmt.Errorf("error restoring state machine: error = %v", err)
	}
	bytes := buf.Bytes()

	entries, err := decodeOperations(bytes)
	if err != nil {
		return fmt.Errorf("error restoring state machine: error = %v", err)
	}

	s.operations = entries

	return nil
}

func (s *stateMachineMock) NeedSnapshot(logSize int) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.snapshotting && logSize%s.snapshotSize == 0
}

func (s *stateMachineMock) appliedOperations() []Operation {
	s.mu.Lock()
	defer s.mu.Unlock()

	operationsCopy := make([]Operation, len(s.operations))
	copy(operationsCopy, s.operations)
	return operationsCopy
}

type testCluster struct {
	// The testing instance associated with the cluster.
	t *testing.T

	// The raft instances that make up the cluster.
	rafts []*Raft

	// The ID, address, and voting status of all cluster members.
	configuration Configuration

	// The directories containing the persisted state for each
	// node, where dirs[i] is the directory for rafts[i].
	dirs []string

	// The nodes which are disconnected, where disconnected[i] being
	// true indicates rafts[i] is disconnected.
	disconnected []bool

	// The state machine associated with each node, where fsm[i]
	// corresponds to the state machine for rafts[i].
	fsm []*stateMachineMock

	// Indicates whether auto snapshotting will be used.
	snapshotting bool

	// The maximum number of log entries per snapshot if snapshotting is enabled.
	snapshotSize int

	mu sync.Mutex
}

func newCluster(t *testing.T, numServers int, snapshotting bool, snapshotSize int) *testCluster {
	rafts := make([]*Raft, numServers)
	dirs := make([]string, numServers)
	fsm := make([]*stateMachineMock, numServers)
	disconnected := make([]bool, numServers)
	configuration := makeClusterConfiguration(numServers)

	// Create nodes.
	for i := 0; i < numServers; i++ {
		id := fmt.Sprint(i)
		tmpDir := t.TempDir()
		raft, err := makeRaft(id, configuration.Members[id], tmpDir, snapshotting, snapshotSize)
		if err != nil {
			t.Fatalf("failed to create raft instance: error = %v", err)
		}
		fsm[i] = raft.fsm.(*stateMachineMock)
		dirs[i] = tmpDir
		rafts[i] = raft
	}

	return &testCluster{
		t:             t,
		rafts:         rafts,
		disconnected:  disconnected,
		configuration: configuration,
		fsm:           fsm,
		dirs:          dirs,
		snapshotting:  snapshotting,
		snapshotSize:  snapshotSize,
	}
}

func (tc *testCluster) startCluster() {
	for _, node := range tc.rafts {
		if err := node.Bootstrap(tc.configuration.Members); err != nil {
			tc.t.Fatalf("failed to bootstrap node: error = %v", err)
		}
		if err := node.Start(); err != nil {
			tc.t.Fatalf("failed to start node: error = %v", err)
		}
	}
}

func (tc *testCluster) stopCluster() {
	for _, node := range tc.rafts {
		node.Stop()
	}
	for _, node := range tc.rafts {
		t := node.transport.(*transport)
		if len(t.connections) != 0 {
			tc.t.Fatalf(
				"unclosed connections: id = %s, address = %s, connections = %v",
				node.id,
				node.address,
				t.connections,
			)
		}
	}
}

func (tc *testCluster) submit(
	retry bool,
	expectFail bool,
	operationType OperationType,
	operations ...[]byte,
) {
	// Time between submission attempts. If no leader was found, allow for
	// an election to complete.
	electionTimeout := 200 * time.Millisecond

	for _, operation := range operations {
		// Attempt to submit the operation.
		// Allow for a maximum of three seconds if retry is enabled.
		index := 0
		start := time.Now()
		for time.Since(start).Seconds() < 3 {
			tc.mu.Lock()
			if index >= len(tc.rafts) && !retry {
				tc.mu.Unlock()
				break
			}
			index %= len(tc.rafts)
			node := tc.rafts[index]
			index++
			tc.mu.Unlock()

			// Submit the operation. This node might be the leader.
			operationFuture := node.SubmitOperation(operation, operationType, 200*time.Millisecond)
			response := operationFuture.Await()
			if err := response.Error(); err == nil {
				if expectFail {
					tc.t.Fatal("expected the operation to fail, but it was successful")
				}
				result := response.Success()
				if string(result.Operation.Bytes) != string(operation) {
					tc.t.Fatal("operation response does not match submitted operation")
				}
				return
			}

			tc.mu.Lock()
			if index < len(tc.rafts) {
				tc.mu.Unlock()
				continue
			}
			tc.mu.Unlock()

			time.Sleep(electionTimeout)
		}

		if !expectFail {
			tc.t.Fatalf("cluster failed to apply operation: operation = %s", string(operation))
		}
	}
}

func (tc *testCluster) addNonVoter() {
	var newNode *Raft
	tmpDir := tc.t.TempDir()
	configuration := makeClusterConfiguration(len(tc.configuration.Members) + 1)

	tc.mu.Lock()
	for id, address := range configuration.Members {
		if isVoter, ok := tc.configuration.IsVoter[id]; ok {
			configuration.IsVoter[id] = isVoter
		} else {
			// Create the node to be added.
			raft, err := makeRaft(id, address, tmpDir, tc.snapshotting, tc.snapshotSize)
			if err != nil {
				tc.t.Fatalf("failed to make raft: error = %v", err)
			}
			newNode = raft

			configuration.IsVoter[id] = false

			// The new node will be started at as a non-voter initially.
			if err := raft.Start(); err != nil {
				tc.t.Fatalf("failed to start raft: error = %v", err)
			}

		}
	}
	tc.mu.Unlock()

	tc.addServer(newNode.id, newNode.address, false)

	// Update the test cluster with the successfully added node.
	tc.mu.Lock()
	defer tc.mu.Unlock()
	tc.configuration = configuration
	tc.rafts = append(tc.rafts, newNode)
	tc.dirs = append(tc.dirs, tmpDir)
	tc.fsm = append(tc.fsm, newNode.fsm.(*stateMachineMock))
	tc.disconnected = append(tc.disconnected, false)
}

func (tc *testCluster) promoteNonVoters() {
	nonVoters := make(map[string]string)
	tc.mu.Lock()
	for id, address := range tc.configuration.Members {
		if isVoter := tc.configuration.IsVoter[id]; !isVoter {
			nonVoters[id] = address
		}
	}
	tc.mu.Unlock()

	for id, address := range nonVoters {
		tc.addServer(id, address, true)
		tc.mu.Lock()
		tc.configuration.IsVoter[id] = true
		tc.mu.Unlock()
	}
}

func (tc *testCluster) addServer(id string, address string, voter bool) {
	// Attempt to add the node to the cluster.
	// Allow for a maximum of three seconds.
	start := time.Now()
	timeout := 500 * time.Millisecond
	index := 0
	for time.Since(start).Seconds() < 3 {
		tc.mu.Lock()
		index %= len(tc.rafts)
		if index >= len(tc.rafts) {
			tc.t.Fatal("the cluster must contain atleast one node")
		}
		node := tc.rafts[index]
		index++
		tc.mu.Unlock()

		// Submit the request to add a  member to the cluster.
		// This node might be the leader.
		future := node.AddServer(id, address, voter, timeout)
		response := future.Await()
		if err := response.Error(); err != nil {
			continue
		}

		// Make sure the configuration contains the server added.
		configuration := response.Success()
		if configurationAddress, ok := configuration.Members[id]; !ok ||
			address != configurationAddress ||
			configuration.IsVoter[id] != voter {
			tc.t.Fatalf(
				"node is missing from configuration, has incorrect address, or has incorrect voting status",
			)
		}

		return
	}

	tc.t.Fatalf("cluster took too long to add a new server")
}

func (tc *testCluster) checkStateMachines(expectedMatches int, timeout time.Duration) {
	startTime := time.Now()
	appliedOperationsPerServer := make([][]Operation, len(tc.rafts))

	for time.Since(startTime) < timeout {
		// Take the longest array of applied operations to be the source of truth.
		longestAppliedIndex := -1
		for i := 0; i < len(tc.rafts); i++ {
			appliedOperations := tc.fsm[i].appliedOperations()
			if longestAppliedIndex == -1 ||
				len(appliedOperations) > len(appliedOperationsPerServer[longestAppliedIndex]) {
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
			// Make sure operations are monotically increasing.
			for _, operations := range appliedOperationsPerServer {
				tc.checkMonotonicity(operations)
			}
			return
		}
	}

	// Find where two state machines differ.
	for i := 0; i < len(appliedOperationsPerServer); i++ {
		for j := 0; j < len(appliedOperationsPerServer); j++ {
			applied1 := appliedOperationsPerServer[i]
			applied2 := appliedOperationsPerServer[j]
			if i == j {
				continue
			}
			tc.compareOperations(i, applied1, j, applied2)
		}
	}
}

func (tc *testCluster) checkMonotonicity(operations []Operation) {
	lastIndex := uint64(0)
	for _, operation := range operations {
		if operation.LogIndex < lastIndex {
			tc.t.Fatalf(
				"operations are not monotonic - indices should never decrease: lastIndex = %d, index = %d",
				lastIndex,
				operation.LogIndex,
			)
		}
	}
}

func (tc *testCluster) compareOperations(
	node1 int,
	operations1 []Operation,
	node2 int,
	operations2 []Operation,
) {
	if reflect.DeepEqual(operations1, operations2) {
		return
	}

	for k := 0; k < util.Min(len(operations1), len(operations2)); k++ {
		operation1 := operations1[k]
		operation2 := operations2[k]
		if reflect.DeepEqual(operation1, operation2) {
			return
		}
		tc.t.Fatalf(
			"state machines do not match: id1 = %d, index1 = %d, term1 = %d, id2 = %d, index2 = %d, term2 = %d",
			node1,
			operation1.LogIndex,
			operation1.LogTerm,
			node2,
			operation2.LogIndex,
			operation2.LogTerm,
		)
	}

	tc.t.Fatalf(
		"state machines do not match: one state machine has more operations than another: id1 = %d, id2 = %d",
		node1,
		node2,
	)
}

func (tc *testCluster) checkLeaders(expectNoLeader bool) int {
	// Any leaders detected.
	leaders := make([]int, 0)

	// Time between checks for a leader.
	// This amount should be large enough to allow an election to take place.
	electionTimeout := 300 * time.Millisecond

	// Check the nodes to see which, if any, are in the leader state.
	// A maximum of 5 seconds is given to successfully elect a leader.
	index := 0
	start := time.Now()
	for time.Since(start).Seconds() < 5 {
		tc.mu.Lock()
		index %= len(tc.rafts)
		node := tc.rafts[index]

		// Get the status of the node, it may be a leader.
		status := node.Status()

		// If the node is a leader, and it is connected, then it is
		// a legitimate leader. Leaders that are disconnected or
		// partitioned are ignored. It is assumed that disconnected
		// nodes are either:
		// 1. Completely disconnected from all other nodes - it
		//    cannot communicate with any other nodes, and no other
		//    nodes can communicate with it.
		// 2. In a minority partition - it may only communicate with
		//    a minority of the cluster. Members of the majority partition
		//    cannot communicate with it.
		if status.State == Leader && !tc.disconnected[index] {
			index, _ := strconv.Atoi(status.ID)
			leaders = append(leaders, index)
		}

		if len(leaders) > 1 {
			tc.t.Fatalf("cluster has more than one leader: leaders = %v", leaders)
		}

		index++

		// Iterate over all the nodes atleast one before returning to check for multiple leaders.
		if index < len(tc.rafts) {
			tc.mu.Unlock()
			continue
		}
		tc.mu.Unlock()

		if len(leaders) == 1 {
			break
		}

		// If no leaders were found, sleep for a sufficient amount of time to allow
		// an election to take place.
		time.Sleep(electionTimeout)
	}

	if len(leaders) == 0 && !expectNoLeader {
		tc.t.Fatal("cluster failed to elect a leader in a reasonable amount of time")
	}

	if len(leaders) != 0 && expectNoLeader {
		tc.t.Fatalf("cluster elected leader without quorum: leaders = %v", leaders)
	}

	if expectNoLeader {
		return -1
	}

	return leaders[0]
}

func (tc *testCluster) crashServer(node int) {
	tc.mu.Lock()
	defer tc.mu.Unlock()
	tc.rafts[node].Stop()
}

func (tc *testCluster) restartServer(node int) {
	tc.mu.Lock()
	defer tc.mu.Unlock()

	id := fmt.Sprint(node)
	raft, err := makeRaft(
		id,
		tc.configuration.Members[id],
		tc.dirs[node],
		tc.snapshotting,
		tc.snapshotSize,
	)
	if err != nil {
		tc.t.Fatalf("failed to create raft instance: error = %v", err)
	}
	tc.fsm[node] = raft.fsm.(*stateMachineMock)
	tc.rafts[node] = raft

	if err := raft.Start(); err != nil {
		tc.t.Fatalf("failed to start node: error = %v", err)
	}
}

func (tc *testCluster) createPartition() {
	tc.mu.Lock()
	defer tc.mu.Unlock()

	// The number of nodes in the partition.
	partitionSize := len(tc.rafts) / 2

	// The nodes in the partition.
	partitionSet := make(map[int]bool)

	// Choose random nodes to partition.
	index := util.RandomInt(0, len(tc.rafts))
	for i := 0; i < partitionSize; i++ {
		partitionSet[(index+i)%len(tc.rafts)] = true
	}

	// Disconnect all nodes in the partition set from those
	// that are not, but maintain connections between the nodes
	// that are in the partition set.
	for i := 0; i < len(tc.rafts); i++ {
		if _, ok := partitionSet[i]; ok {
			for j := 0; j < len(tc.rafts); j++ {
				if _, ok := partitionSet[j]; ok {
					continue
				}
				address1 := tc.configuration.Members[fmt.Sprint(j)]
				address2 := tc.configuration.Members[fmt.Sprint(i)]
				if err := tc.rafts[i].transport.Close(address1); err != nil {
					tc.t.Fatalf(
						"failed disconnecting node: id = %d, disconnectingFrom = %d, error = %v",
						i,
						j,
						err,
					)
				}
				if err := tc.rafts[j].transport.Close(address2); err != nil {
					tc.t.Fatalf(
						"failed disconnecting node: id = %d, disconnectingFrom = %d, error = %v",
						j,
						i,
						err,
					)
				}
			}
		}
	}

	for index := range partitionSet {
		tc.disconnected[index] = true
	}
}

func (tc *testCluster) reconnectServer(node int) {
	tc.mu.Lock()
	defer tc.mu.Unlock()

	id := fmt.Sprint(node)

	for i := 0; i < len(tc.rafts); i++ {
		if i == node {
			continue
		}

		address1 := tc.configuration.Members[id]
		address2 := tc.configuration.Members[fmt.Sprint(i)]
		if err := tc.rafts[i].transport.Connect(address1); err != nil {
			tc.t.Fatalf(
				"failed reconnecting node: id = %d, connectingTo = %d, error = %v",
				i,
				node,
				err,
			)
		}
		if err := tc.rafts[node].transport.Connect(address2); err != nil {
			tc.t.Fatalf(
				"failed reconnecting node: id = %d, connectingTo = %d, error = %v",
				node,
				i,
				err,
			)
		}
	}

	tc.disconnected[node] = false
}

func (tc *testCluster) reconnectAllServers() {
	tc.mu.Lock()
	defer tc.mu.Unlock()

	for i := 0; i < len(tc.rafts); i++ {
		for j := 0; j < len(tc.rafts); j++ {
			if i == j {
				continue
			}
			address := tc.configuration.Members[fmt.Sprint(j)]
			if err := tc.rafts[i].transport.Connect(address); err != nil {
				tc.t.Fatalf(
					"failed reconnecting node: id = %d, connectingTo = %d, error = %v",
					i,
					j,
					err,
				)
			}
		}
	}

	for i := 0; i < len(tc.rafts); i++ {
		tc.disconnected[i] = false
	}
}

func (tc *testCluster) disconnectServer(node int) {
	tc.mu.Lock()
	defer tc.mu.Unlock()

	id := fmt.Sprint(node)

	for i := 0; i < len(tc.rafts); i++ {
		if i == node {
			continue
		}

		address1 := tc.configuration.Members[id]
		address2 := tc.configuration.Members[fmt.Sprint(i)]
		if err := tc.rafts[i].transport.Close(address1); err != nil {
			tc.t.Fatalf(
				"failed disconnecting node: id = %d, disconnectingFrom = %d, error = %v",
				i,
				node,
				err.Error(),
			)
		}
		if err := tc.rafts[node].transport.Close(address2); err != nil {
			tc.t.Fatalf(
				"failed disconnecting node: id = %d, disconnectingFrom = %d, error = %v",
				node,
				i,
				err,
			)
		}
	}

	tc.disconnected[node] = true
}
