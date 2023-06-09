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

// Set by environment variable. Indicates whether auto
// snapshotting is on or off. If auto snapshotting is on,
// all tests (exclusing the manual snapshot tests) will be
// ran with auto snapshotting enabled.
var autoSnapshotting bool

// The size of snapshots if auto snapshotting is enabled.
var autoSnapshotSize int

// TestMain sets up the Raft tests.
func TestMain(m *testing.M) {
	autoSnapshotting = os.Getenv("SNAPSHOTS") == "true"
	autoSnapshotSize, _ = strconv.Atoi(os.Getenv("SNAPSHOT_SIZE"))
	exitCode := m.Run()
	os.Exit(exitCode)
}

// TestSingleServerElection checks whether a cluster consisting of
// a single server can elect a leader.
func TestSingleServerElection(t *testing.T) {
	defer leaktest.CheckTimeout(t, 1*time.Second)

	cluster := newCluster(t, 1, autoSnapshotting, autoSnapshotSize)

	cluster.startCluster()
	defer cluster.stopCluster()

	cluster.checkLeaders(false)
}

// TestBasicElection checks whether a cluster can elect a leader
// when there are no failures.
func TestBasicElection(t *testing.T) {
	defer leaktest.CheckTimeout(t, 1*time.Second)

	cluster := newCluster(t, 3, autoSnapshotting, autoSnapshotSize)

	cluster.startCluster()
	defer cluster.stopCluster()

	cluster.checkLeaders(false)
}

// TestElectLeaderDisconnect checks whether a cluster can
// still elect a leader when a single server is Disconnected.
func TestElectLeaderDisconnect(t *testing.T) {
	defer leaktest.CheckTimeout(t, 1*time.Second)

	cluster := newCluster(t, 3, autoSnapshotting, autoSnapshotSize)

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

	cluster := newCluster(t, 3, autoSnapshotting, autoSnapshotSize)

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

	cluster := newCluster(t, 1, autoSnapshotting, autoSnapshotSize)

	cluster.startCluster()
	defer cluster.stopCluster()

	cluster.checkLeaders(false)
	commands := makeCommands(1)
	cluster.submit(commands[0], false, false, 1)
}

// TestSingleSubmit checks whether the cluster can successfully
// commit a single command when there are no failures.
func TestBasicSubmit(t *testing.T) {
	defer leaktest.CheckTimeout(t, 1*time.Second)

	cluster := newCluster(t, 3, autoSnapshotting, autoSnapshotSize)

	cluster.startCluster()
	defer cluster.stopCluster()

	cluster.checkLeaders(false)
	commands := makeCommands(1)
	cluster.submit(commands[0], false, false, 3)
}

// TestMultipleSubmit checks whether a cluster can successfully
// commit multiple commands when there are no failures.
func TestMultipleSubmit(t *testing.T) {
	defer leaktest.CheckTimeout(t, 1*time.Second)

	cluster := newCluster(t, 5, autoSnapshotting, autoSnapshotSize)

	cluster.startCluster()
	defer cluster.stopCluster()

	cluster.checkLeaders(false)
	commands := makeCommands(200)
	for _, command := range commands {
		cluster.submit(command, false, false, 5)
	}
}

// TestConcurrentSubmit test whether commands are correctly
// applied when there are multiple clients submitting commands
// at the same time.
func TestConcurrentSubmit(t *testing.T) {
	defer leaktest.CheckTimeout(t, 1*time.Second)

	cluster := newCluster(t, 5, autoSnapshotting, autoSnapshotSize)

	cluster.startCluster()
	defer cluster.stopCluster()

	cluster.checkLeaders(false)
	commands := makeCommands(200)

	var wg sync.WaitGroup

	// Simulates a client submitting commands.
	client := func(commands []Command, readyCh chan interface{}) {
		defer wg.Done()
		<-readyCh
		for _, command := range commands {
			cluster.submit(command, false, false, 5)
		}
	}

	// The number of clients submitting commands concurrently.
	numClients := 10

	// The number of command each client will submit.
	commandsPerClient := len(commands) / numClients

	// Signals to the clients that they can start submitting commands.
	readyCh := make(chan interface{})

	// Spin up the clients with their respective commands.
	for i := 0; i < numClients; i++ {
		clientCommands := commands[i*commandsPerClient : (i+1)*commandsPerClient]
		wg.Add(1)
		go client(clientCommands, readyCh)
	}

	// Allow clients to start and wait until they are done.
	close(readyCh)
	wg.Wait()

	if t.Failed() {
		t.Fatal("concurrent apply operations failed")
	}
}

// TestSubmitDisconnect checks that a cluster can still
// commit commands after the leader is disconnected.
func TestSubmitDisconnect(t *testing.T) {
	defer leaktest.CheckTimeout(t, 1*time.Second)

	cluster := newCluster(t, 3, autoSnapshotting, autoSnapshotSize)

	cluster.startCluster()
	defer cluster.stopCluster()

	// Disconnect the leader and see if commands are still committed.
	leader := cluster.checkLeaders(false)
	cluster.disconnectServer(leader)
	commands := makeCommands(20)
	for _, command := range commands {
		cluster.submit(command, true, false, 2)
	}
}

// TestSubmitDisconnectRejoin checks that a cluster correctly
// handles leaders being disconnected and rejoining after commands
// are submitted.
func TestSubmitDisconnectRejoin(t *testing.T) {
	defer leaktest.CheckTimeout(t, 1*time.Second)

	cluster := newCluster(t, 5, autoSnapshotting, autoSnapshotSize)

	cluster.startCluster()
	defer cluster.stopCluster()

	// Disconnect the first leader.
	leader1 := cluster.checkLeaders(false)

	// Submit some commands with this leader.
	commands := makeCommands(80)
	for i := 0; i < 20; i++ {
		cluster.submit(commands[i], false, false, 5)
	}

	// Disconnect the leader.
	cluster.disconnectServer(leader1)

	// Submit some more commands. Note that we only expect
	// 4 servers to apply the command.
	for i := 20; i < 40; i++ {
		cluster.submit(commands[i], true, false, 4)
	}

	// Disconnect the second leader.
	leader2 := cluster.checkLeaders(false)

	// Submit some more commands. Note that we only expect
	// 3 servers to apply the command.
	for i := 40; i < 60; i++ {
		cluster.submit(commands[i], true, false, 3)
	}

	// Allow the old leaders to rejoin.
	cluster.reconnectServer(leader1)
	cluster.reconnectServer(leader2)

	// Submit some more commands. All servers should apply the
	// command now.
	for i := 60; i < 80; i++ {
		cluster.submit(commands[i], true, false, 5)
	}
}

// TestSubmitDisconnectFail checks that a cluster is unable to
// commit commands when a majority of the servers are completely
// disconnected from the cluster but still online.
func TestSubmitDisconnectFail(t *testing.T) {
	defer leaktest.CheckTimeout(t, 1*time.Second)

	cluster := newCluster(t, 5, autoSnapshotting, autoSnapshotSize)

	cluster.startCluster()
	defer cluster.stopCluster()

	// Disconnect the leader and two other servers, leaving
	// only a minority of the server able to communicate.
	leader := cluster.checkLeaders(false)
	cluster.disconnectServer(leader)
	cluster.disconnectServer((leader + 1) % 5)
	cluster.disconnectServer((leader + 2) % 5)

	// Try to submit some commands. This should be unsuccessful
	// since only a minority of the cluster can communicate.
	commands := makeCommands(20)
	for _, command := range commands {
		cluster.submit(command, false, true, 1)
	}
}

// TestUnreliableNetwork tests whether a cluster can still make
// progress submitting multiple commands when multiple servers
// become disconnected from the rest of the clutster.
func TestUnreliableNetwork(t *testing.T) {
	defer leaktest.CheckTimeout(t, 1*time.Second)

	cluster := newCluster(t, 5, autoSnapshotting, autoSnapshotSize)

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

	// See if we can commit commands in the face of recurring partitions.
	commands := makeCommands(300)
	for _, command := range commands {
		cluster.submit(command, true, false, 3)
	}

	atomic.StoreInt32(&done, 1)
	wg.Wait()
}

// TestBasicPartition checks that a cluster can still make
// progress submitting multiple commands when there is a single
// partition.
func TestBasicPartition(t *testing.T) {
	defer leaktest.CheckTimeout(t, 1*time.Second)

	cluster := newCluster(t, 5, autoSnapshotting, autoSnapshotSize)

	cluster.startCluster()
	defer cluster.stopCluster()

	// Wait for a leader.
	cluster.checkLeaders(false)

	// Partition the cluster.
	cluster.createPartition()

	// Wait for a leader.
	cluster.checkLeaders(false)

	commands := makeCommands(50)
	for _, command := range commands {
		cluster.submit(command, true, false, 3)
	}

	// Heal the partition.
	cluster.reconnectAllServers()
}

// TestMultiPartition checks whether a cluster can still make
// progress submitting multiple commands in the prescence of
// multiple and changing partitions.
func TestMultiPartition(t *testing.T) {
	defer leaktest.CheckTimeout(t, 1*time.Second)

	cluster := newCluster(t, 5, autoSnapshotting, autoSnapshotSize)

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

	// See if we can commit commands in the face of recurring partitions.
	commands := makeCommands(300)
	for _, command := range commands {
		cluster.submit(command, true, false, 3)
	}

	atomic.StoreInt32(&done, 1)
	wg.Wait()
}

// TestBasicCrash checks that a cluster can still make
// progress after a single server crashes.
func TestBasicCrash(t *testing.T) {
	defer leaktest.CheckTimeout(t, 1*time.Second)

	cluster := newCluster(t, 5, autoSnapshotting, autoSnapshotSize)

	cluster.startCluster()
	defer cluster.stopCluster()

	// Wait for a leader and submit some commands.
	leader := cluster.checkLeaders(false)
	commands := makeCommands(200)
	for i := 0; i < 25; i++ {
		cluster.submit(commands[i], false, false, 5)
	}

	// Crash the leader and see if we can still make progress.
	cluster.crashServer(leader)
	for i := 25; i < len(commands); i++ {
		cluster.submit(commands[i], true, false, 4)
	}
}

// TestCrashRejoin checks that a cluster correctly
// handles a server crashing and coming back online
// after commands are submitted.
func TestCrashRejoin(t *testing.T) {
	defer leaktest.CheckTimeout(t, 1*time.Second)

	cluster := newCluster(t, 5, autoSnapshotting, autoSnapshotSize)

	cluster.startCluster()
	defer cluster.stopCluster()

	// Wait for a leader and submit some commands.
	leader := cluster.checkLeaders(false)
	commands := makeCommands(200)
	for i := 0; i < 25; i++ {
		cluster.submit(commands[i], false, false, 5)
	}

	// Crash the leader and see if we can still make progress.
	cluster.crashServer(leader)
	for i := 25; i < 150; i++ {
		cluster.submit(commands[i], true, false, 4)
	}

	// Allow the leader to rejoin and see if we can make progress
	// committing commands.
	cluster.restartServer(leader)
	for i := 150; i < len(commands); i++ {
		cluster.submit(commands[i], true, false, 5)
	}
}

// TestMultiCrash checks if a cluster can still make
// progress committing commands in the face of multiple
// crashes.
func TestMultiCrash(t *testing.T) {
	defer leaktest.CheckTimeout(t, 1*time.Second)

	cluster := newCluster(t, 5, autoSnapshotting, autoSnapshotSize)

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

	// See if we can commit commands in the face of multiple crashes.
	commands := makeCommands(300)
	for _, command := range commands {
		cluster.submit(command, true, false, 3)
	}

	atomic.StoreInt32(&done, 1)
	wg.Wait()
}

// TestDisconnectCrashPartition checks whether the cluster can still
// make progress when there are disconnections, crashes, and partitions.
func TestDisconnectCrashPartition(t *testing.T) {
	defer leaktest.CheckTimeout(t, 1*time.Second)

	cluster := newCluster(t, 5, autoSnapshotting, autoSnapshotSize)

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

	// See if we can commit commands in the face of random network and server failures.
	// Submit enough commands to ensure that a variety of failures occur.
	commands := makeCommands(500)
	for _, command := range commands {
		cluster.submit(command, true, false, 3)
	}

	atomic.StoreInt32(&done, 1)
	wg.Wait()
}

// TestAllCrash checks that a cluster can still make
// progress committing commands after all the servers
// crash and come back online.
func TestAllCrash(t *testing.T) {
	defer leaktest.CheckTimeout(t, 1*time.Second)

	cluster := newCluster(t, 5, autoSnapshotting, autoSnapshotSize)

	cluster.startCluster()
	defer cluster.stopCluster()

	// Wait for a leader and submit some commands.
	cluster.checkLeaders(false)
	commands := makeCommands(50)
	for i := 0; i < 25; i++ {
		cluster.submit(commands[i], false, false, 5)
	}

	// Crash all servers.
	for i := 0; i < 5; i++ {
		cluster.crashServer(i)
	}

	// Restart all the servers.
	for i := 0; i < 5; i++ {
		cluster.restartServer(i)
	}

	// Wait for another leader and submit more commands.
	cluster.checkLeaders(false)
	for i := 25; i < len(commands); i++ {
		cluster.submit(commands[i], true, false, 5)
	}
}

// TestManualSnapshotBasic checks that a cluster of
// servers can correctly take a single snapshot when
// no commands are being submitted concurrently and there
// are no failures.
func TestManualSnapshotBasic(t *testing.T) {
	defer leaktest.CheckTimeout(t, 1*time.Second)

	// Make sure that auto snapshots are not enabled.
	cluster := newCluster(t, 5, false, 0)

	cluster.startCluster()
	defer cluster.stopCluster()

	// Wait for a leader and submit some commands.
	cluster.checkLeaders(false)
	commands := makeCommands(100)
	for _, command := range commands {
		cluster.submit(command, false, false, 5)
	}

	// Force all servers to take a snapshot and ensure the snapshot is
	// correct.
	for i := 0; i < 5; i++ {
		cluster.takeAndValidateSnapshot(i)
	}
}

// TestManualSnapshotConcurrent checks that a cluster of
// servers can correctly take multiple snapshots when
// commands are being submitted concurrently and there
// are no failures.
func TestManualSnapshotConcurrent(t *testing.T) {
	defer leaktest.CheckTimeout(t, 1*time.Second)

	// Make sure auto snapshots are not enabled.
	cluster := newCluster(t, 5, autoSnapshotting, autoSnapshotSize)

	// A go routine to take snapshots every so often.
	done := int32(0)
	wg := sync.WaitGroup{}
	snapshotRoutine := func() {
		defer wg.Done()
		for atomic.LoadInt32(&done) == 0 {
			// Allow the cluster to make some progress.
			randomTime := util.RandomTimeout(700*time.Millisecond, 900*time.Millisecond)
			time.Sleep(randomTime * time.Millisecond)

			// For each server to take a snapshot and ensure it is correct.
			for i := 0; i < 5; i++ {
				cluster.takeAndValidateSnapshot(i)
			}
		}
	}

	cluster.startCluster()
	defer cluster.stopCluster()
	cluster.checkLeaders(false)

	// Start snapshots.
	wg.Add(1)
	go snapshotRoutine()

	// Wait for a leader and submit some commands.
	cluster.checkLeaders(false)
	commands := makeCommands(300)
	for _, command := range commands {
		cluster.submit(command, false, false, 5)
	}

	atomic.StoreInt32(&done, 1)
	wg.Wait()

	if t.Failed() {
		t.Fatal("concurrent snapshot operations failed")
	}
}
