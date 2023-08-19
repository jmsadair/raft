package raft

import (
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/fortytw2/leaktest"
	"github.com/jmsadair/raft/internal/util"
)

// Set by environment variable. Indicates whether snapshotting
// is on or off. If auto snapshotting is on, all tests
// (excluding the manual snapshot tests) will be run with snapshotting
// enabled.
var snapshotting bool

// The size of snapshots if snapshotting is enabled.
var snapshotSize int

// TestMain sets up the Raft tests.
func TestMain(m *testing.M) {
	snapshotting = os.Getenv("SNAPSHOTS") == "true"
	snapshotSize, _ = strconv.Atoi(os.Getenv("SNAPSHOT_SIZE"))
	exitCode := m.Run()
	os.Exit(exitCode)
}

// TestSingleServerElection checks whether a cluster consisting of
// a single server can elect a leader.
func TestSingleServerElection(t *testing.T) {
	defer leaktest.CheckTimeout(t, 1*time.Second)

	cluster := newCluster(t, 1, snapshotting, snapshotSize)

	cluster.startCluster()
	defer cluster.stopCluster()

	cluster.checkLeaders(false)
}

// TestBasicElection checks whether a cluster can elect a leader
// when there are no failures.
func TestBasicElection(t *testing.T) {
	defer leaktest.CheckTimeout(t, 1*time.Second)

	cluster := newCluster(t, 3, snapshotting, snapshotSize)

	cluster.startCluster()
	defer cluster.stopCluster()

	cluster.checkLeaders(false)
}

// TestElectLeaderDisconnect checks whether a cluster can
// still elect a leader when a single server is Disconnected.
func TestElectLeaderDisconnect(t *testing.T) {
	defer leaktest.CheckTimeout(t, 1*time.Second)

	cluster := newCluster(t, 3, snapshotting, snapshotSize)

	cluster.startCluster()
	defer cluster.stopCluster()

	// Disconnect the leader.
	leader := cluster.checkLeaders(false)
	cluster.disconnectServer(leader)

	// See if the cluster can still elect a new leader.
	cluster.checkLeaders(false)
}

// TestFailElectLeaderDisconnect checks whether a leader is
// elected when a majority of the servers are Disconnected.
func TestFailElectLeaderDisconnect(t *testing.T) {
	defer leaktest.CheckTimeout(t, 1*time.Second)

	cluster := newCluster(t, 3, snapshotting, snapshotSize)

	cluster.startCluster()
	defer cluster.stopCluster()

	// Disconnect the leader and one other server, leaving
	// only one server that is capable of communicating.
	disconnectServer1 := cluster.checkLeaders(false)
	disconnectServer2 := (disconnectServer1 + 1) % 3
	cluster.disconnectServer(disconnectServer1)
	cluster.disconnectServer(disconnectServer2)

	// Check if the server can elect itself as the leader.
	// This should not be successful.
	cluster.checkLeaders(true)
}

// TestSingleServerSubmit checks whether a cluster consisting of
// a single server can commit a command.
func TestSingleServerSubmit(t *testing.T) {
	defer leaktest.CheckTimeout(t, 1*time.Second)

	cluster := newCluster(t, 1, snapshotting, snapshotSize)

	cluster.startCluster()
	defer cluster.stopCluster()

	cluster.checkLeaders(false)
	operations := makeOperations(1)
	cluster.submit(operations[0], true, false, false)

	cluster.checkStateMachines(1, 3*time.Second)
}

// TestSingleSubmit checks whether the cluster can successfully
// commit a single command when there are no failures.
func TestBasicSubmit(t *testing.T) {
	defer leaktest.CheckTimeout(t, 1*time.Second)

	cluster := newCluster(t, 3, snapshotting, snapshotSize)

	cluster.startCluster()
	defer cluster.stopCluster()

	cluster.checkLeaders(false)
	operations := makeOperations(1)
	cluster.submit(operations[0], true, false, false)

	cluster.checkStateMachines(3, 3*time.Second)
}

// TestMultipleSubmit checks whether a cluster can successfully
// commit multiple operations when there are no failures.
func TestMultipleSubmit(t *testing.T) {
	defer leaktest.CheckTimeout(t, 1*time.Second)

	cluster := newCluster(t, 5, snapshotting, snapshotSize)

	cluster.startCluster()
	defer cluster.stopCluster()

	cluster.checkLeaders(false)
	operations := makeOperations(200)
	for _, command := range operations {
		cluster.submit(command, true, false, false)
	}

	cluster.checkStateMachines(5, 3*time.Second)
}

// TestConcurrentSubmit test whether operations are correctly
// applied when there are multiple clients submitting operations
// at the same time.
func TestConcurrentSubmit(t *testing.T) {
	defer leaktest.CheckTimeout(t, 1*time.Second)

	cluster := newCluster(t, 5, snapshotting, snapshotSize)

	cluster.startCluster()
	defer cluster.stopCluster()

	cluster.checkLeaders(false)
	operations := makeOperations(200)

	var wg sync.WaitGroup

	// Simulates a client submitting operations.
	client := func(operations [][]byte, readyCh chan interface{}) {
		defer wg.Done()
		<-readyCh
		for _, operation := range operations {
			cluster.submit(operation, true, false, false)
		}
	}

	// The number of clients submitting operations concurrently.
	numClients := 10

	// The number of command each client will submit.
	operationsPerClient := len(operations) / numClients

	// Signals to the clients that they can start submitting operations.
	readyCh := make(chan interface{})

	// Spin up the clients with their respective operations.
	for i := 0; i < numClients; i++ {
		clientOperations := operations[i*operationsPerClient : (i+1)*operationsPerClient]
		wg.Add(1)
		go client(clientOperations, readyCh)
	}

	// Allow clients to start and wait until they are done.
	close(readyCh)
	wg.Wait()

	cluster.checkStateMachines(5, 3*time.Second)
}

// TestSubmitDisconnect checks that a cluster can still
// commit operations after the leader is disconnected.
func TestSubmitDisconnect(t *testing.T) {
	defer leaktest.CheckTimeout(t, 1*time.Second)

	cluster := newCluster(t, 3, snapshotting, snapshotSize)

	cluster.startCluster()
	defer cluster.stopCluster()

	// Disconnect the leader and see if operations are still committed.
	leader := cluster.checkLeaders(false)
	cluster.disconnectServer(leader)
	operations := makeOperations(20)
	for _, command := range operations {
		cluster.submit(command, true, false, false)
	}

	cluster.checkStateMachines(2, 3*time.Second)
}

// TestSubmitDisconnectRejoin checks that a cluster correctly
// handles leaders being disconnected and rejoining after operations
// are submitted.
func TestSubmitDisconnectRejoin(t *testing.T) {
	defer leaktest.CheckTimeout(t, 1*time.Second)

	cluster := newCluster(t, 5, snapshotting, snapshotSize)

	cluster.startCluster()
	defer cluster.stopCluster()

	// Disconnect the first leader.
	leader1 := cluster.checkLeaders(false)

	// Submit some operations with this leader.
	operations := makeOperations(80)
	for i := 0; i < 20; i++ {
		cluster.submit(operations[i], true, false, false)
	}

	// Disconnect the leader.
	cluster.disconnectServer(leader1)

	// Submit some more operations. Note that we only expect
	// 4 servers to apply the command.
	for i := 20; i < 40; i++ {
		cluster.submit(operations[i], true, false, false)
	}

	// Disconnect the second leader.
	leader2 := cluster.checkLeaders(false)

	// Submit some more operations. Note that we only expect
	// 3 servers to apply the command.
	for i := 40; i < 60; i++ {
		cluster.submit(operations[i], true, false, false)
	}

	// Allow the old leaders to rejoin.
	cluster.reconnectServer(leader1)
	cluster.reconnectServer(leader2)

	// Submit some more operations. All servers should apply the
	// command now.
	for i := 60; i < 80; i++ {
		cluster.submit(operations[i], true, false, false)
	}

	cluster.checkStateMachines(5, 3*time.Second)
}

// TestSubmitDisconnectFail checks that a cluster is unable to
// commit operations when a majority of the servers are completely
// disconnected from the cluster but still online.
func TestSubmitDisconnectFail(t *testing.T) {
	defer leaktest.CheckTimeout(t, 1*time.Second)

	cluster := newCluster(t, 5, snapshotting, snapshotSize)

	cluster.startCluster()
	defer cluster.stopCluster()

	// Disconnect the leader and two other servers, leaving
	// only a minority of the server able to communicate.
	leader := cluster.checkLeaders(false)
	cluster.disconnectServer(leader)
	cluster.disconnectServer((leader + 1) % 5)
	cluster.disconnectServer((leader + 2) % 5)

	// Try to submit some operations. This should be unsuccessful
	// since only a minority of the cluster can communicate.
	operations := makeOperations(1)
	for _, command := range operations {
		cluster.submit(command, true, true, false)
	}
}

// TestUnreliableNetwork tests whether a cluster can still make
// progress submitting multiple operations when multiple servers
// become disconnected from the rest of the cluster.
func TestUnreliableNetwork(t *testing.T) {
	defer leaktest.CheckTimeout(t, 1*time.Second)

	cluster := newCluster(t, 5, snapshotting, snapshotSize)

	done := int32(0)
	wg := sync.WaitGroup{}
	unreliableNetRoutine := func() {
		defer wg.Done()
		for atomic.LoadInt32(&done) == 0 {
			// Allow the cluster to make some progress with no failures.
			randomTime := util.RandomTimeout(700*time.Millisecond, 900*time.Millisecond)
			time.Sleep(randomTime * time.Millisecond)

			// Disconnect two random servers.
			disconnect1 := util.RandomInt(0, 5)
			disconnect2 := (disconnect1 + 1) % 5
			cluster.disconnectServer(disconnect1)
			cluster.disconnectServer(disconnect2)

			// Allow the cluster to make progress while the servers are disconnected.
			randomTime = util.RandomTimeout(700*time.Millisecond, 900*time.Millisecond)
			time.Sleep(randomTime * time.Millisecond)

			// Reconnect the servers.
			cluster.reconnectAllServers()
		}
	}

	cluster.startCluster()
	defer cluster.stopCluster()
	cluster.checkLeaders(false)

	// Start disconnecting random servers.
	wg.Add(1)
	go unreliableNetRoutine()

	// See if we can commit operations in the face of recurring partitions.
	operations := makeOperations(300)
	for _, command := range operations {
		cluster.submit(command, true, false, false)
	}

	atomic.StoreInt32(&done, 1)
	wg.Wait()

	cluster.checkStateMachines(5, 3*time.Second)
}

// TestBasicPartition checks that a cluster can still make
// progress submitting multiple operations when there is a single
// partition.
func TestBasicPartition(t *testing.T) {
	defer leaktest.CheckTimeout(t, 1*time.Second)

	cluster := newCluster(t, 5, snapshotting, snapshotSize)

	cluster.startCluster()
	defer cluster.stopCluster()

	// Wait for a leader.
	cluster.checkLeaders(false)

	// Partition the cluster.
	cluster.createPartition()

	// Wait for a leader.
	cluster.checkLeaders(false)

	operations := makeOperations(50)
	for _, command := range operations {
		cluster.submit(command, true, false, false)
	}

	// Heal the partition.
	cluster.reconnectAllServers()

	cluster.checkStateMachines(5, 3*time.Second)
}

// TestMultiPartition checks whether a cluster can still make
// progress submitting multiple operations in the presence of
// multiple and changing partitions.
func TestMultiPartition(t *testing.T) {
	defer leaktest.CheckTimeout(t, 1*time.Second)

	cluster := newCluster(t, 5, snapshotting, snapshotSize)

	// A go routine to crash random servers every so often.
	done := int32(0)
	wg := sync.WaitGroup{}
	partitionRoutine := func() {
		defer wg.Done()
		for atomic.LoadInt32(&done) == 0 {
			// Allow the cluster to make some progress with no failures.
			randomTime := util.RandomTimeout(700*time.Millisecond, 900*time.Millisecond)
			time.Sleep(randomTime * time.Millisecond)

			// Make a new partition.
			cluster.createPartition()

			// Allow the cluster to make progress with the partition.
			randomTime = util.RandomTimeout(700*time.Millisecond, 900*time.Millisecond)
			time.Sleep(randomTime * time.Millisecond)

			// Heal the partition.
			cluster.reconnectAllServers()
		}
	}

	cluster.startCluster()
	defer cluster.stopCluster()
	cluster.checkLeaders(false)

	// Start partitioning
	wg.Add(1)
	go partitionRoutine()

	// See if we can commit operations in the face of recurring partitions.
	operations := makeOperations(300)
	for _, command := range operations {
		cluster.submit(command, true, false, false)
	}

	atomic.StoreInt32(&done, 1)
	wg.Wait()

	cluster.checkStateMachines(5, 3*time.Second)
}

// TestBasicCrash checks that a cluster can still make
// progress after a single server crashes.
func TestBasicCrash(t *testing.T) {
	defer leaktest.CheckTimeout(t, 1*time.Second)

	cluster := newCluster(t, 5, snapshotting, snapshotSize)

	cluster.startCluster()
	defer cluster.stopCluster()

	// Wait for a leader and submit some operations.
	leader := cluster.checkLeaders(false)
	operations := makeOperations(200)
	for i := 0; i < 25; i++ {
		cluster.submit(operations[i], true, false, false)
	}

	// Crash the leader and see if we can still make progress.
	cluster.crashServer(leader)
	for i := 25; i < len(operations); i++ {
		cluster.submit(operations[i], true, false, false)
	}

	cluster.checkStateMachines(4, 3*time.Second)
}

// TestCrashRejoin checks that a cluster correctly
// handles a server crashing and coming back online
// after operations are submitted.
func TestCrashRejoin(t *testing.T) {
	defer leaktest.CheckTimeout(t, 1*time.Second)

	cluster := newCluster(t, 5, snapshotting, snapshotSize)

	cluster.startCluster()
	defer cluster.stopCluster()

	// Wait for a leader and submit some operations.
	leader := cluster.checkLeaders(false)
	operations := makeOperations(200)
	for i := 0; i < 25; i++ {
		cluster.submit(operations[i], true, false, false)
	}

	// Crash the leader and see if we can still make progress.
	cluster.crashServer(leader)
	for i := 25; i < 150; i++ {
		cluster.submit(operations[i], true, false, false)
	}

	// Allow the leader to rejoin and see if we can make progress
	// committing operations.
	cluster.restartServer(leader)
	for i := 150; i < len(operations); i++ {
		cluster.submit(operations[i], true, false, false)
	}

	cluster.checkStateMachines(5, 3*time.Second)
}

// TestMultiCrash checks if a cluster can still make
// progress committing operations in the face of multiple
// crashes.
func TestMultiCrash(t *testing.T) {
	defer leaktest.CheckTimeout(t, 1*time.Second)

	cluster := newCluster(t, 5, snapshotting, snapshotSize)

	// A go routine to crash random servers every so often.
	done := int32(0)
	wg := sync.WaitGroup{}
	crashRoutine := func() {
		defer wg.Done()
		for atomic.LoadInt32(&done) == 0 {
			// Allow the cluster to make some progress with no failures.
			randomTime := util.RandomTimeout(700*time.Millisecond, 900*time.Millisecond)
			time.Sleep(randomTime * time.Millisecond)

			// Crash two random servers.
			crash1 := util.RandomInt(0, 5)
			crash2 := (crash1 + 1) % 5
			cluster.crashServer(crash1)
			cluster.crashServer(crash2)

			// Allow the cluster to make progress while the servers are offline.
			randomTime = util.RandomTimeout(700*time.Millisecond, 900*time.Millisecond)
			time.Sleep(randomTime * time.Millisecond)

			// Bring the servers back online.
			cluster.restartServer(crash1)
			cluster.restartServer(crash2)
		}
	}

	cluster.startCluster()
	defer cluster.stopCluster()
	cluster.checkLeaders(false)

	// Start crashing servers.
	wg.Add(1)
	go crashRoutine()

	// See if we can commit operations in the face of multiple crashes.
	operations := makeOperations(300)
	for _, command := range operations {
		cluster.submit(command, true, false, false)
	}

	atomic.StoreInt32(&done, 1)
	wg.Wait()

	cluster.checkStateMachines(5, 3*time.Second)
}

// TestDisconnectCrashPartition checks whether the cluster can still
// make progress when there are disconnections, crashes, and partitions.
func TestDisconnectCrashPartition(t *testing.T) {
	defer leaktest.CheckTimeout(t, 1*time.Second)

	cluster := newCluster(t, 5, snapshotting, snapshotSize)

	// A go routine to crash, disconnect, and partition random servers every so often.
	done := int32(0)
	wg := sync.WaitGroup{}
	failureRoutine := func() {
		defer wg.Done()
		for atomic.LoadInt32(&done) == 0 {
			// Allow the cluster to make some progress with no failures.
			randomTime := util.RandomTimeout(700*time.Millisecond, 900*time.Millisecond)
			time.Sleep(randomTime * time.Millisecond)

			// Choose a random type of failure.
			action := util.RandomInt(0, 3)

			switch action {
			// Crash a single server.
			case 0:
				crash := util.RandomInt(0, 5)
				cluster.crashServer(crash)
				randomTime = util.RandomTimeout(700*time.Millisecond, 900*time.Millisecond)
				time.Sleep(randomTime * time.Millisecond)
				cluster.restartServer(crash)
			// Disconnect a single server.
			case 1:
				disconnect := util.RandomInt(0, 5)
				cluster.disconnectServer(disconnect)
				randomTime = util.RandomTimeout(200*time.Millisecond, 400*time.Millisecond)
				time.Sleep(randomTime * time.Millisecond)
				cluster.reconnectAllServers()
			// Partition the servers into two separate groups.
			case 2:
				cluster.createPartition()
				randomTime = util.RandomTimeout(700*time.Millisecond, 900*time.Millisecond)
				time.Sleep(randomTime * time.Millisecond)
				cluster.reconnectAllServers()
			}
		}
	}

	cluster.startCluster()
	defer cluster.stopCluster()
	cluster.checkLeaders(false)

	// Start causing failures.
	wg.Add(1)
	go failureRoutine()

	// See if we can commit operations in the face of random network and server failures.
	// Submit enough operations to ensure that a variety of failures occur.
	operations := makeOperations(500)
	for _, command := range operations {
		cluster.submit(command, true, false, false)
	}

	atomic.StoreInt32(&done, 1)
	wg.Wait()

	cluster.checkStateMachines(5, 3*time.Second)
}

// TestAllCrash checks that a cluster can still make
// progress committing operations after all the servers
// crash and come back online.
func TestAllCrash(t *testing.T) {
	defer leaktest.CheckTimeout(t, 1*time.Second)

	cluster := newCluster(t, 5, snapshotting, snapshotSize)

	cluster.startCluster()
	defer cluster.stopCluster()

	// Wait for a leader and submit some operations.
	cluster.checkLeaders(false)
	operations := makeOperations(50)
	for i := 0; i < 25; i++ {
		cluster.submit(operations[i], true, false, false)
	}

	// Crash all servers.
	for i := 0; i < 5; i++ {
		cluster.crashServer(i)
	}

	// Restart all the servers.
	for i := 0; i < 5; i++ {
		cluster.restartServer(i)
	}

	// Wait for another leader and submit more operations.
	cluster.checkLeaders(false)
	for i := 25; i < len(operations); i++ {
		cluster.submit(operations[i], true, false, false)
	}

	cluster.checkStateMachines(5, 3*time.Second)
}

// TestBasicReadOnly checks that a read-only operation submitted under normal conditions
// returns the latest state from the state machine without error.
func TestBasicReadOnly(t *testing.T) {
	defer leaktest.CheckTimeout(t, 1*time.Second)

	cluster := newCluster(t, 5, snapshotting, snapshotSize)

	cluster.startCluster()
	defer cluster.stopCluster()

	// Wait for a leader and submit some operations.
	cluster.checkLeaders(false)
	operations := makeOperations(1)
	cluster.submit(operations[0], true, false, false)
	cluster.submit([]byte{}, true, false, true)
}

// TestSingleServerReadOnly checks that a read-only operations are successful in the single server case.
func TestSingleServerReadOnly(t *testing.T) {
	defer leaktest.CheckTimeout(t, 1*time.Second)

	cluster := newCluster(t, 1, snapshotting, snapshotSize)

	cluster.startCluster()
	defer cluster.stopCluster()

	// Wait for a leader and submit some operations.
	cluster.checkLeaders(false)
	operations := makeOperations(10)
	for _, command := range operations {
		cluster.submit(command, true, false, false)
	}

	cluster.submit([]byte{}, true, false, true)
}

// TestReadOnlyFail checks that a read-only operation submitted when a leader has not received heartbeats
// from a majority of the partition is rejected.
func TestReadOnlyFail(t *testing.T) {
	defer leaktest.CheckTimeout(t, 1*time.Second)

	cluster := newCluster(t, 3, snapshotting, snapshotSize)

	cluster.startCluster()
	defer cluster.stopCluster()

	// Wait for a leader.
	leader := cluster.checkLeaders(false)

	// Disconnect the other two servers in the cluster.
	cluster.disconnectServer((leader + 1) % 3)
	cluster.disconnectServer((leader + 2) % 3)

	// Give the lease some time to expire.
	time.Sleep(defaultLeaseDuration)

	// Make sure the read-only operation fails.
	cluster.submit([]byte{}, true, true, true)
}

// TestReadOnlyDisconnect checks that a new leader will renew its lease
// after the old one is disconnected, thereby allowing successful read-only
// operations.
func TestReadOnlyDisconnect(t *testing.T) {
	defer leaktest.CheckTimeout(t, 1*time.Second)

	cluster := newCluster(t, 5, snapshotting, snapshotSize)

	cluster.startCluster()
	defer cluster.stopCluster()

	// Submit some operations to the initial leader.
	leader := cluster.checkLeaders(false)
	operations := makeOperations(10)
	for _, command := range operations {
		cluster.submit(command, true, false, false)
	}

	// Disconnect the leader and wait for a new one.
	cluster.disconnectServer(leader)
	cluster.checkLeaders(false)

	// Give the leader a bit of time to make sure its lease gets renewed.
	time.Sleep(defaultElectionTimeout)

	// Check that read-only operation is successful.
	cluster.submit([]byte{}, true, false, true)
}
