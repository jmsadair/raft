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

func makeClusterConfiguration(numServers int) map[string]string {
	cluster := make(map[string]string, numServers)
	for i := 0; i < numServers; i++ {
		id := fmt.Sprint(i)
		address := fmt.Sprintf("127.0.0.%d:8080", i)
		cluster[id] = address
	}
	return cluster
}

func makeRaft(
	id string,
	dataPath string,
	cluster map[string]string,
	noStart bool,
	snapshotting bool,
	snapshotSize int,
) (*Raft, error) {
	log, err := NewLog(dataPath)
	if err != nil {
		return nil, err
	}
	snapshots, err := NewSnapshotStorage(dataPath)
	if err != nil {
		return nil, err
	}
	stateStore, err := NewStateStorage(dataPath)
	if err != nil {
		return nil, err
	}
	logger, err := makeLogger(id)
	if err != nil {
		return nil, err
	}
	transport, err := NewTransport(cluster[id])
	if err != nil {
		return nil, err
	}
	fsm := newStateMachineMock(snapshotting, snapshotSize)

	raft, err := NewRaft(
		id,
		cluster,
		transport,
		log,
		stateStore,
		snapshots,
		fsm,
		WithLogger(logger),
	)
	if err != nil {
		return nil, err
	}

	// If noStart is true, the caller is not expecting to call start.
	// Open the log and replay it so that testing is possible.
	if noStart {
		if err := raft.log.Open(); err != nil {
			return nil, err
		}
		if err := raft.log.Replay(); err != nil {
			return nil, err
		}
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

	// Set the response channel to nil, since these operations will be compared to others.
	// Not all operations will necessarily have the same response channel.
	for i := range operationsCopy {
		operationsCopy[i].responseCh = nil
	}

	return operationsCopy
}

type testCluster struct {
	// The testing instance associated with the cluster.
	t *testing.T

	// The raft instances that make up the cluster.
	rafts []*Raft

	// The ID and address of each raft node in the cluster.
	cluster map[string]string

	// The log associated with each server, where logs[i] is the
	// log for servers[i]
	logs []Log

	// The snapshot storage associated with each server, where
	// snapshotStore[i] is the snapshot storage for servers[i]
	snapshotStores []SnapshotStorage

	// The storage associated with each server, where stores[i] is the
	// storage for servers[i]
	stores []StateStorage

	// The servers which are disconnected, where disconnected[i] being
	// true indicates servers[i] is disconnected.
	disconnected []bool

	// The state machine associated with each server, where fsm[i]
	// corresponds to the state machine for servers[i].
	fsm []*stateMachineMock

	// Indicates whether auto snapshotting will be used.
	snapshotting bool

	// The maximum number of log entries per snapshot if snapshotting is enabled.
	snapshotSize int

	mu sync.Mutex
}

func newCluster(t *testing.T, numServers int, snapshotting bool, snapshotSize int) *testCluster {
	rafts := make([]*Raft, numServers)
	logs := make([]Log, numServers)
	snapshotStores := make([]SnapshotStorage, numServers)
	stores := make([]StateStorage, numServers)
	fsm := make([]*stateMachineMock, numServers)
	disconnected := make([]bool, numServers)

	// ID and address of all members of the cluster.
	cluster := makeClusterConfiguration(numServers)

	// Create the raft and server instances.
	for i := 0; i < numServers; i++ {
		id := fmt.Sprint(i)
		tmpDir := t.TempDir()
		raft, err := makeRaft(id, tmpDir, cluster, false, snapshotting, snapshotSize)
		if err != nil {
			t.Fatalf("failed to create raft instance: error = %v", err)
		}
		fsm[i] = raft.fsm.(*stateMachineMock)
		logs[i] = raft.log
		snapshotStores[i] = raft.snapshotStorage
		stores[i] = raft.stateStorage
		rafts[i] = raft
	}

	return &testCluster{
		t:              t,
		rafts:          rafts,
		disconnected:   disconnected,
		cluster:        cluster,
		logs:           logs,
		snapshotStores: snapshotStores,
		stores:         stores,
		fsm:            fsm,
		snapshotting:   snapshotting,
		snapshotSize:   snapshotSize,
	}
}

func (tc *testCluster) startCluster() {
	for _, node := range tc.rafts {
		node.Start()
	}
}

func (tc *testCluster) stopCluster() {
	for _, node := range tc.rafts {
		node.Stop()
	}
}

func (tc *testCluster) submit(
	operation []byte,
	retry bool,
	expectFail bool,
	operationType OperationType,
) {
	// Time between submission attempts. If no leader was found, allow for
	// an election to complete.
	electionTimeout := 200 * time.Millisecond

	// Allow for a maximum of three seconds if retry is enabled.
	start := time.Now()
	for time.Since(start).Seconds() < 3 {
		for j := 0; j < len(tc.rafts); j++ {
			tc.mu.Lock()
			server := tc.rafts[j]
			tc.mu.Unlock()

			// Submit the operation.
			future := server.SubmitOperation(operation, operationType, 200*time.Millisecond)
			if response := future.Await(); response.Err == nil {
				if expectFail {
					tc.t.Fatalf("expected operation to fail, but it was successful")
				}
				if string(response.Operation.Bytes) != string(operation) {
					tc.t.Fatalf("operation response does not match submitted operation")
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
		tc.t.Fatalf("cluster failed to apply operation: operation = %s", string(operation))
	}
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
			return
		}
	}

	// Find the first log index where two logs differ.
	for i := 0; i < len(appliedOperationsPerServer); i++ {
		for j := 0; j < len(appliedOperationsPerServer); j++ {
			applied1 := appliedOperationsPerServer[i]
			applied2 := appliedOperationsPerServer[j]
			if i == j {
				continue
			}
			if reflect.DeepEqual(applied1, applied2) {
				continue
			}
			for k := 0; k < util.Min(len(applied1), len(applied2)); k++ {
				op1 := applied1[k]
				op2 := applied2[k]
				if reflect.DeepEqual(op1, op2) {
					continue
				}
				tc.t.Fatalf(
					"cluster state machines do not match: fsm %d != fsm %d: index1 = %d term1 = %d operation1 = %s index2 = %d term2 = %d operation2 = %s",
					i,
					j,
					op1.LogIndex,
					op1.LogTerm,
					string(op1.Bytes),
					op2.LogIndex,
					op2.LogTerm,
					string(op2.Bytes),
				)
			}
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
		for i := 0; i < len(tc.rafts); i++ {
			tc.mu.Lock()
			server := tc.rafts[i]
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

func (tc *testCluster) crashServer(server int) {
	tc.disconnectServer(server)
	tc.rafts[server].Stop()
}

func (tc *testCluster) restartServer(server int) {
	tc.mu.Lock()
	defer tc.mu.Unlock()

	id := fmt.Sprint(server)

	tc.fsm[server] = newStateMachineMock(tc.snapshotting, tc.snapshotSize)

	logger, err := makeLogger(id)
	if err != nil {
		tc.t.Fatalf("failed to create logger instance: error = %v", err)
	}

	transport, err := NewTransport(tc.cluster[id])
	if err != nil {
		tc.t.Fatalf("failed to create transport instance: error = %v", err)
	}

	newRaft, err := NewRaft(
		id,
		tc.cluster,
		transport,
		tc.logs[server],
		tc.stores[server],
		tc.snapshotStores[server],
		tc.fsm[server],
		WithLogger(logger),
	)
	if err != nil {
		tc.t.Fatalf("failed to create raft instance: error = %v", err)
	}
	newRaft.Start()
	tc.rafts[server] = newRaft

	for i := 0; i < len(tc.rafts); i++ {
		if err := tc.rafts[i].transport.Connect(tc.cluster[id]); err != nil {
			tc.t.Fatalf(
				"failed reconnecting peer: id = %d, connectingTo = %d, error = %v",
				i,
				server,
				err,
			)
		}
	}

	tc.disconnected[server] = false
}

func (tc *testCluster) createPartition() {
	tc.mu.Lock()
	defer tc.mu.Unlock()

	// The number of servers in the partition.
	partitionSize := len(tc.rafts) / 2

	// The servers in the partition.
	partitionSet := make(map[int]bool)

	// Choose random servers to partition.
	index := util.RandomInt(0, len(tc.rafts))
	for i := 0; i < partitionSize; i++ {
		partitionSet[(index+i)%len(tc.rafts)] = true
	}

	// Disconnect all servers in the partition set from those
	// that are not, but maintain connections between the servers
	// that are in the partition set.
	for i := 0; i < len(tc.rafts); i++ {
		if _, ok := partitionSet[i]; ok {
			for j := 0; j < len(tc.rafts); j++ {
				if _, ok := partitionSet[j]; ok {
					continue
				}
				if err := tc.rafts[i].transport.Close(tc.cluster[fmt.Sprint(j)]); err != nil {
					tc.t.Fatalf(
						"failed disconnecting peer: id = %d, disconnectingFrom = %d, error = %v",
						i,
						j,
						err,
					)
				}
				if err := tc.rafts[j].transport.Close(tc.cluster[fmt.Sprint(i)]); err != nil {
					tc.t.Fatalf(
						"failed disconnecting peer: id = %d, disconnectingFrom = %d, error = %v",
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

func (tc *testCluster) reconnectServer(server int) {
	tc.mu.Lock()
	defer tc.mu.Unlock()

	id := fmt.Sprint(server)

	for i := 0; i < len(tc.rafts); i++ {
		if err := tc.rafts[i].transport.Connect(tc.cluster[id]); err != nil {
			tc.t.Fatalf(
				"failed reconnecting peer: id = %d, connectingTo = %d, error = %v",
				i,
				server,
				err,
			)
		}
		if err := tc.rafts[server].transport.Connect(tc.cluster[fmt.Sprint(i)]); err != nil {
			tc.t.Fatalf(
				"failed reconnecting peer: id = %d, connectingTo = %d, error = %v",
				server,
				i,
				err,
			)
		}
	}

	tc.disconnected[server] = false
}

func (tc *testCluster) reconnectAllServers() {
	tc.mu.Lock()
	defer tc.mu.Unlock()

	for i := 0; i < len(tc.rafts); i++ {
		for j := 0; j < len(tc.rafts); j++ {
			id := fmt.Sprint(j)
			address := tc.cluster[id]
			if err := tc.rafts[i].transport.Connect(address); err != nil {
				tc.t.Fatalf(
					"failed reconnecting peer: id = %d, connectingTo = %d, error = %v",
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

func (tc *testCluster) disconnectServer(server int) {
	tc.mu.Lock()
	defer tc.mu.Unlock()

	id := fmt.Sprint(server)

	for i := 0; i < len(tc.rafts); i++ {
		if err := tc.rafts[i].transport.Close(tc.cluster[id]); err != nil {
			tc.t.Fatalf(
				"failed disconnecting peer: id = %d, disconnectingFrom = %d, error = %v",
				i,
				server,
				err.Error(),
			)
		}
		if err := tc.rafts[server].transport.Close(tc.cluster[fmt.Sprint(i)]); err != nil {
			tc.t.Fatalf(
				"failed disconnecting peer: id = %d, disconnectingFrom = %d, error = %v",
				server,
				i,
				err,
			)
		}
	}

	tc.disconnected[server] = true
}
