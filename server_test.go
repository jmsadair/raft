package raft

import (
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jmsadair/raft/internal/util"
	"go.uber.org/goleak"
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
	goleak.VerifyTestMain(m)
}

// TestSingleServerElection checks whether a cluster consisting of
// a single server can elect a leader.
func TestSingleServerElection(t *testing.T) {
	cluster := newCluster(t, 1, snapshotting, snapshotSize)

	cluster.startCluster()
	defer cluster.stopCluster()

	cluster.checkLeaders(false)
}

// TestBasicElection checks whether a cluster can elect a leader
// when there are no failures.
func TestBasicElection(t *testing.T) {
	cluster := newCluster(t, 3, snapshotting, snapshotSize)

	cluster.startCluster()
	defer cluster.stopCluster()

	cluster.checkLeaders(false)
}

// TestElectLeaderDisconnect checks whether a cluster can
// still elect a leader when a single server is Disconnected.
func TestElectLeaderDisconnect(t *testing.T) {
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
	cluster := newCluster(t, 3, snapshotting, snapshotSize)

	cluster.startCluster()
	defer cluster.stopCluster()

	// Disconnect the leader and one other server, leaving
	// only one server that is capable of communicating.
	disconnected1 := cluster.checkLeaders(false)
	disconnected2 := (disconnected1 + 1) % 3
	cluster.disconnectServer(disconnected1)
	cluster.disconnectServer(disconnected2)

	// Check if the server can elect itself as the leader.
	// This should not be successful.
	cluster.checkLeaders(true)
}

// TestAddSingleServer checks that a single node can be added as a non-voting member
// and then promoted to a voting member without issue.
func TestAddSingleServer(t *testing.T) {
	cluster := newCluster(t, 3, snapshotting, snapshotSize)

	cluster.startCluster()
	defer cluster.stopCluster()

	cluster.checkLeaders(false)

	// Add a non-voting member to the cluster and then make it a voter.
	cluster.addNonVoter()
	cluster.promoteNonVoters()
}

// TestNotVoterElectionSuccess checks that non-voting members do not prevent
// a leader from being elected. Only voting members should be considered when
// considering quorum - if the cluster originally has 3 voting members and 2
// non-voting members are added, leadership should only require 2 votes.
func TestNonVoterElectionSuccess(t *testing.T) {
	cluster := newCluster(t, 3, snapshotting, snapshotSize)

	cluster.startCluster()
	defer cluster.stopCluster()

	cluster.checkLeaders(false)

	// Add non-voting members.
	cluster.addNonVoter()
	cluster.addNonVoter()

	// Disconnect the leader.
	disconnected1 := cluster.checkLeaders(false)
	cluster.disconnectServer(disconnected1)

	// Check that a new leader can be elected.
	cluster.checkLeaders(false)
	cluster.reconnectServer(disconnected1)
}

// TestNotVoterElectionFail checks that non-voting members are unable
// to elect a leader. Non-voting members cannot vote in elections.
func TestNonVoterElectionFailure(t *testing.T) {
	cluster := newCluster(t, 3, snapshotting, snapshotSize)

	cluster.startCluster()
	defer cluster.stopCluster()

	cluster.checkLeaders(false)

	// Add non-voting members.
	cluster.addNonVoter()
	cluster.addNonVoter()

	// Disconnect the leader.
	disconnected1 := cluster.checkLeaders(false)
	cluster.disconnectServer(disconnected1)

	// Disconnect the next leader.
	disconnected2 := cluster.checkLeaders(false)
	cluster.disconnectServer(disconnected2)

	// Make sure another leader is not elected.
	// This should not be possible since a majority of the voting members are down.
	cluster.checkLeaders(true)

	cluster.reconnectAllServers()
}

// TestSingleServerSubmit checks whether a cluster consisting of
// a single server can commit a command.
func TestSingleServerSubmit(t *testing.T) {
	cluster := newCluster(t, 1, snapshotting, snapshotSize)

	cluster.startCluster()
	defer cluster.stopCluster()

	cluster.checkLeaders(false)
	operations := makeOperations(1)
	cluster.submit(true, false, Replicated, operations...)

	cluster.checkStateMachines(1, 3*time.Second)
}

// TestSingleSubmit checks whether the cluster can successfully
// commit a single command when there are no failures.
func TestSingleSubmit(t *testing.T) {
	cluster := newCluster(t, 3, snapshotting, snapshotSize)

	cluster.startCluster()
	defer cluster.stopCluster()

	cluster.checkLeaders(false)
	operations := makeOperations(1)
	cluster.submit(true, false, Replicated, operations...)

	cluster.checkStateMachines(3, 3*time.Second)
}

// TestMultipleSubmit checks whether a cluster can successfully
// commit multiple operations when there are no failures.
func TestMultipleSubmit(t *testing.T) {
	cluster := newCluster(t, 5, snapshotting, snapshotSize)

	cluster.startCluster()
	defer cluster.stopCluster()

	cluster.checkLeaders(false)
	operations := makeOperations(1000)
	cluster.submit(true, false, Replicated, operations...)

	cluster.checkStateMachines(5, 3*time.Second)
}

// TestConcurrentSubmit test whether operations are correctly
// applied when there are multiple clients submitting operations
// at the same time.
func TestConcurrentSubmit(t *testing.T) {
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
		cluster.submit(true, false, Replicated, operations...)
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

// TestSubmitMembership checks that submitted operations are correctly
// replicated to new members of the cluster.
func TestSubmitMembership(t *testing.T) {
	cluster := newCluster(t, 3, snapshotting, snapshotSize)

	cluster.startCluster()
	defer cluster.stopCluster()

	// Submit some operations to the cluster.
	operations := makeOperations(300)
	cluster.submit(true, false, Replicated, operations[:100]...)

	// Add a non-voting member.
	cluster.addNonVoter()

	// Submit some more operations to the cluster.
	cluster.submit(true, false, Replicated, operations[100:200]...)

	// Promote the non-voter.
	cluster.promoteNonVoters()

	// Submit some operations to the clutser.
	cluster.submit(true, false, Replicated, operations[200:]...)

	cluster.checkStateMachines(4, 3*time.Second)
}

// TestSubmitFailMembership checks that a cluster is unable to commit
// operations when a majority of its voting members are down. Non-voting
// members should not affect quorum.
func TestSubmitFailMembership(t *testing.T) {
	cluster := newCluster(t, 3, snapshotting, snapshotSize)

	cluster.startCluster()
	defer cluster.stopCluster()

	// Add a non-voting member.
	cluster.addNonVoter()

	// Disconnect the leader.
	disconnect1 := cluster.checkLeaders(false)
	cluster.disconnectServer(disconnect1)

	// Disconnect the next leader
	disconnect2 := cluster.checkLeaders(false)
	cluster.disconnectServer(disconnect2)

	// This should fail since a majority of voting members are down.
	operations := makeOperations(1)
	cluster.submit(true, true, Replicated, operations...)
}

// TestSubmitConcurrentMembers checks that concurrently submitted operations are correctly
// replicated to new memebers of the cluster.
func TestSubmitConcurrentMembership(t *testing.T) {
	cluster := newCluster(t, 3, snapshotting, snapshotSize)

	cluster.startCluster()
	defer cluster.stopCluster()

	cluster.checkLeaders(false)
	operations := makeOperations(1000)

	var wg sync.WaitGroup

	// Simulates a client submitting operations.
	client := func(operations [][]byte, readyCh chan interface{}) {
		defer wg.Done()
		<-readyCh
		cluster.submit(true, false, Replicated, operations...)
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

	// Allow clients to start.
	close(readyCh)

	// Add a non-voter and then promote it.
	cluster.addNonVoter()
	time.Sleep(100 * time.Millisecond)
	cluster.promoteNonVoters()

	// Wait for clients to finish.
	wg.Wait()

	cluster.checkStateMachines(4, 3*time.Second)
}

// TestSubmitDisconnect checks that a cluster can still
// commit operations after the leader is disconnected.
func TestSubmitDisconnect(t *testing.T) {
	cluster := newCluster(t, 3, snapshotting, snapshotSize)

	cluster.startCluster()
	defer cluster.stopCluster()

	// Disconnect the leader and see if operations are still committed.
	leader := cluster.checkLeaders(false)
	cluster.disconnectServer(leader)
	operations := makeOperations(20)
	cluster.submit(true, false, Replicated, operations...)

	cluster.checkStateMachines(2, 3*time.Second)
}

// TestSubmitDisconnectRejoin checks that a cluster correctly
// handles leaders being disconnected and rejoining after operations
// are submitted.
func TestSubmitDisconnectRejoin(t *testing.T) {
	cluster := newCluster(t, 5, snapshotting, snapshotSize)

	cluster.startCluster()
	defer cluster.stopCluster()

	// Disconnect the first leader.
	leader1 := cluster.checkLeaders(false)

	// Submit some operations with this leader.
	operations := makeOperations(80)
	cluster.submit(true, false, Replicated, operations[:20]...)

	// Disconnect the leader.
	cluster.disconnectServer(leader1)

	// Submit some more operations.
	cluster.submit(true, false, Replicated, operations[20:40]...)

	// Disconnect the second leader.
	leader2 := cluster.checkLeaders(false)

	// Submit some more operations.
	cluster.submit(true, false, Replicated, operations[40:60]...)

	// Allow the old leaders to rejoin.
	cluster.reconnectServer(leader1)
	cluster.reconnectServer(leader2)

	// Submit some more operations.
	cluster.submit(true, false, Replicated, operations[60:]...)

	cluster.checkStateMachines(5, 3*time.Second)
}

// TestSubmitDisconnectFail checks that a cluster is unable to
// commit operations when a majority of the servers are completely
// disconnected from the cluster but still online.
func TestSubmitDisconnectFail(t *testing.T) {
	cluster := newCluster(t, 5, snapshotting, snapshotSize)

	cluster.startCluster()
	defer cluster.stopCluster()

	// Disconnect the leader and two other servers, leaving
	// only a minority of the server able to communicate.
	leader := cluster.checkLeaders(false)
	cluster.disconnectServer(leader)
	cluster.disconnectServer((leader + 1) % 5)
	cluster.disconnectServer((leader + 2) % 5)

	// Try to submit an operation. This should be unsuccessful
	// since only a minority of the cluster can communicate.
	operations := makeOperations(1)
	cluster.submit(true, true, Replicated, operations...)
}

// TestUnreliableNetwork tests whether a cluster can still make
// progress submitting multiple operations when multiple servers
// become disconnected from the rest of the cluster.
func TestUnreliableNetwork(t *testing.T) {
	cluster := newCluster(t, 5, snapshotting, snapshotSize)

	done := int32(0)
	wg := sync.WaitGroup{}
	unreliableNetRoutine := func() {
		defer wg.Done()
		for atomic.LoadInt32(&done) == 0 {
			// Allow the cluster to make some progress with no failures.
			randomTime := util.RandomTimeout(100*time.Millisecond, 300*time.Millisecond)
			time.Sleep(randomTime * time.Millisecond)

			// Disconnect two random servers.
			disconnect1 := util.RandomInt(0, 5)
			disconnect2 := (disconnect1 + 1) % 5
			cluster.disconnectServer(disconnect1)
			cluster.disconnectServer(disconnect2)

			// Allow the cluster to make progress while the servers are disconnected.
			randomTime = util.RandomTimeout(300*time.Millisecond, 500*time.Millisecond)
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

	// See if we can commit operations in the face of network failures.
	operations := makeOperations(500)
	cluster.submit(true, false, Replicated, operations...)

	atomic.StoreInt32(&done, 1)
	wg.Wait()

	cluster.checkStateMachines(5, 3*time.Second)
}

// TestBasicPartition checks that a cluster can still make
// progress submitting multiple operations when there is a single
// partition.
func TestBasicPartition(t *testing.T) {
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
	cluster.submit(true, false, Replicated, operations...)

	// Heal the partition.
	cluster.reconnectAllServers()

	cluster.checkStateMachines(5, 3*time.Second)
}

// TestMultiPartition checks whether a cluster can still make
// progress submitting multiple operations in the presence of
// multiple and changing partitions.
func TestMultiPartition(t *testing.T) {
	cluster := newCluster(t, 5, snapshotting, snapshotSize)

	// A go routine to crash random servers every so often.
	done := int32(0)
	wg := sync.WaitGroup{}
	partitionRoutine := func() {
		defer wg.Done()
		for atomic.LoadInt32(&done) == 0 {
			// Allow the cluster to make some progress with no failures.
			randomTime := util.RandomTimeout(100*time.Millisecond, 300*time.Millisecond)
			time.Sleep(randomTime * time.Millisecond)

			// Make a new partition.
			cluster.createPartition()

			// Allow the cluster to make progress with the partition.
			randomTime = util.RandomTimeout(300*time.Millisecond, 500*time.Millisecond)
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
	operations := makeOperations(500)
	cluster.submit(true, false, Replicated, operations...)

	atomic.StoreInt32(&done, 1)
	wg.Wait()

	cluster.checkStateMachines(5, 3*time.Second)
}

// TestMultiPartition checks whether a cluster can still make
// progress submitting multiple operations in the presence of
// multiple partitions and changing membership.
func TestMultiPartitionMembership(t *testing.T) {
	cluster := newCluster(t, 5, snapshotting, snapshotSize)

	// A go routine to crash random servers every so often.
	done := int32(0)
	wg := sync.WaitGroup{}
	partitionRoutine := func() {
		defer wg.Done()
		for atomic.LoadInt32(&done) == 0 {
			// Allow the cluster to make some progress with no failures.
			randomTime := util.RandomTimeout(100*time.Millisecond, 300*time.Millisecond)
			time.Sleep(randomTime * time.Millisecond)

			// Make a new partition.
			cluster.createPartition()

			// Allow the cluster to make progress with the partition.
			randomTime = util.RandomTimeout(300*time.Millisecond, 500*time.Millisecond)
			time.Sleep(randomTime * time.Millisecond)

			// Heal the partition.
			cluster.reconnectAllServers()
		}
	}

	addServerRoutine := func() {
		defer wg.Done()

		// Add two non-voting members.
		randomTime := util.RandomTimeout(100*time.Millisecond, 300*time.Millisecond)
		time.Sleep(randomTime)
		cluster.addNonVoter()
		randomTime = util.RandomTimeout(100*time.Millisecond, 300*time.Millisecond)
		time.Sleep(randomTime)
		cluster.addNonVoter()

		// Promote the non-voting members to voting members.
		randomTime = util.RandomTimeout(100*time.Millisecond, 300*time.Millisecond)
		time.Sleep(randomTime)
		cluster.promoteNonVoters()
	}

	cluster.startCluster()
	defer cluster.stopCluster()
	cluster.checkLeaders(false)

	// Start partitioning
	wg.Add(2)
	go partitionRoutine()
	go addServerRoutine()

	// See if we can commit operations in the face of recurring partitions.
	operations := makeOperations(1000)
	cluster.submit(true, false, Replicated, operations...)

	atomic.StoreInt32(&done, 1)
	wg.Wait()

	cluster.checkStateMachines(7, 3*time.Second)
}

// TestBasicCrash checks that a cluster can still make
// progress after a single server crashes.
func TestBasicCrash(t *testing.T) {
	cluster := newCluster(t, 5, snapshotting, snapshotSize)

	cluster.startCluster()
	defer cluster.stopCluster()

	// Wait for a leader and submit some operations.
	leader := cluster.checkLeaders(false)
	operations := makeOperations(200)
	cluster.submit(true, false, Replicated, operations[:25]...)

	// Crash the leader and see if we can still make progress.
	cluster.crashServer(leader)
	cluster.submit(true, false, Replicated, operations[25:]...)

	cluster.checkStateMachines(4, 3*time.Second)
}

// TestCrashRejoin checks that a cluster correctly
// handles a server crashing and coming back online
// after operations are submitted.
func TestCrashRejoin(t *testing.T) {
	cluster := newCluster(t, 5, snapshotting, snapshotSize)

	cluster.startCluster()
	defer cluster.stopCluster()

	// Wait for a leader and submit some operations.
	leader := cluster.checkLeaders(false)
	operations := makeOperations(200)
	cluster.submit(true, false, Replicated, operations[:25]...)

	// Crash the leader and see if we can still make progress.
	cluster.crashServer(leader)
	cluster.submit(true, false, Replicated, operations[25:150]...)

	// Allow the leader to rejoin and see if we can make progress
	// committing operations.
	cluster.restartServer(leader)
	cluster.submit(true, false, Replicated, operations[150:]...)

	cluster.checkStateMachines(5, 3*time.Second)
}

// TestMultiCrashMembership checks if a cluster can still make
// progress committing operations and handle new member joining the
// cluster in the face of multiple crashes. The added members may also
// be crashed
func TestMultiCrashMembership(t *testing.T) {
	cluster := newCluster(t, 5, snapshotting, snapshotSize)

	// A go routine to crash random servers every so often.
	done := int32(0)
	wg := sync.WaitGroup{}
	crashRoutine := func() {
		defer wg.Done()
		for atomic.LoadInt32(&done) == 0 {
			// Allow the cluster to make some progress with no failures.
			randomTime := util.RandomTimeout(100*time.Millisecond, 300*time.Millisecond)
			time.Sleep(randomTime * time.Millisecond)

			// Crash two random servers.
			crash1 := util.RandomInt(0, 5)
			crash2 := (crash1 + 1) % 5
			cluster.crashServer(crash1)
			cluster.crashServer(crash2)

			// Allow the cluster to make progress while the servers are offline.
			randomTime = util.RandomTimeout(300*time.Millisecond, 500*time.Millisecond)
			time.Sleep(randomTime * time.Millisecond)

			// Bring the servers back online.
			cluster.restartServer(crash1)
			cluster.restartServer(crash2)
		}
	}

	addServerRoutine := func() {
		defer wg.Done()

		// Add two non-voting members.
		randomTime := util.RandomTimeout(100*time.Millisecond, 300*time.Millisecond)
		time.Sleep(randomTime)
		cluster.addNonVoter()
		randomTime = util.RandomTimeout(100*time.Millisecond, 300*time.Millisecond)
		time.Sleep(randomTime)
		cluster.addNonVoter()

		// Promote the non-voting members to voting members.
		randomTime = util.RandomTimeout(100*time.Millisecond, 300*time.Millisecond)
		time.Sleep(randomTime)
		cluster.promoteNonVoters()
	}

	cluster.startCluster()
	defer cluster.stopCluster()
	cluster.checkLeaders(false)

	// Start crashing servers.
	wg.Add(2)
	go crashRoutine()
	go addServerRoutine()

	// See if we can commit operations in the face of multiple crashes.
	operations := makeOperations(1000)
	cluster.submit(true, false, Replicated, operations...)

	atomic.StoreInt32(&done, 1)
	wg.Wait()

	cluster.checkStateMachines(7, 3*time.Second)
}

// TestMultiCrash checks if a cluster can still make
// progress committing operations in the face of multiple
// crashes.
func TestMultiCrash(t *testing.T) {
	cluster := newCluster(t, 5, snapshotting, snapshotSize)

	// A go routine to crash random servers every so often.
	done := int32(0)
	wg := sync.WaitGroup{}
	crashRoutine := func() {
		defer wg.Done()
		for atomic.LoadInt32(&done) == 0 {
			// Allow the cluster to make some progress with no failures.
			randomTime := util.RandomTimeout(100*time.Millisecond, 300*time.Millisecond)
			time.Sleep(randomTime * time.Millisecond)

			// Crash two random servers.
			crash1 := util.RandomInt(0, 5)
			crash2 := (crash1 + 1) % 5
			cluster.crashServer(crash1)
			cluster.crashServer(crash2)

			// Allow the cluster to make progress while the servers are offline.
			randomTime = util.RandomTimeout(300*time.Millisecond, 500*time.Millisecond)
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
	operations := makeOperations(500)
	cluster.submit(true, false, Replicated, operations...)

	atomic.StoreInt32(&done, 1)
	wg.Wait()

	cluster.checkStateMachines(5, 3*time.Second)
}

// TestDisconnectCrashPartition checks whether the cluster can still
// make progress when there are disconnections, crashes, and partitions.
func TestDisconnectCrashPartition(t *testing.T) {
	cluster := newCluster(t, 5, snapshotting, snapshotSize)

	// A go routine to crash, disconnect, and partition random servers every so often.
	done := int32(0)
	wg := sync.WaitGroup{}
	failureRoutine := func() {
		defer wg.Done()
		for atomic.LoadInt32(&done) == 0 {
			// Allow the cluster to make some progress with no failures.
			randomTime := util.RandomTimeout(100*time.Millisecond, 300*time.Millisecond)
			time.Sleep(randomTime * time.Millisecond)

			// Choose a random type of failure.
			action := util.RandomInt(0, 3)

			switch action {
			// Crash a single server.
			case 0:
				crash := util.RandomInt(0, 5)
				cluster.crashServer(crash)
				randomTime = util.RandomTimeout(300*time.Millisecond, 500*time.Millisecond)
				time.Sleep(randomTime * time.Millisecond)
				cluster.restartServer(crash)
			// Disconnect a single server.
			case 1:
				disconnect := util.RandomInt(0, 5)
				cluster.disconnectServer(disconnect)
				randomTime = util.RandomTimeout(300*time.Millisecond, 500*time.Millisecond)
				time.Sleep(randomTime * time.Millisecond)
				cluster.reconnectAllServers()
			// Partition the servers into two separate groups.
			case 2:
				cluster.createPartition()
				randomTime = util.RandomTimeout(300*time.Millisecond, 500*time.Millisecond)
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
	operations := makeOperations(1000)
	cluster.submit(true, false, Replicated, operations...)

	atomic.StoreInt32(&done, 1)
	wg.Wait()

	cluster.checkStateMachines(5, 3*time.Second)
}

// TestAllCrash checks that a cluster can still make
// progress committing operations after all the servers
// crash and come back online.
func TestAllCrash(t *testing.T) {
	cluster := newCluster(t, 5, snapshotting, snapshotSize)

	cluster.startCluster()
	defer cluster.stopCluster()

	// Wait for a leader and submit some operations.
	cluster.checkLeaders(false)
	operations := makeOperations(50)
	cluster.submit(true, false, Replicated, operations[:25]...)

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
	cluster.submit(true, false, Replicated, operations[25:]...)

	cluster.checkStateMachines(5, 3*time.Second)
}

// TestBasicReadOnly checks that a read-only operation submitted under normal conditions
// are successful.
func TestBasicReadOnly(t *testing.T) {
	cluster := newCluster(t, 5, snapshotting, snapshotSize)

	cluster.startCluster()
	defer cluster.stopCluster()

	// Wait for a leader and submit some operations.
	cluster.checkLeaders(false)
	operations := makeOperations(1)
	cluster.submit(true, false, Replicated, operations...)
	cluster.submit(true, false, LeaseBasedReadOnly, []byte{})
}

// TestSingleServerReadOnly checks that read-only operations are successful in the single server case.
func TestSingleServerReadOnly(t *testing.T) {
	cluster := newCluster(t, 1, snapshotting, snapshotSize)

	cluster.startCluster()
	defer cluster.stopCluster()

	// Wait for a leader and submit some operations.
	cluster.checkLeaders(false)
	operations := makeOperations(10)
	cluster.submit(true, false, Replicated, operations...)

	cluster.submit(true, false, LeaseBasedReadOnly, []byte{})
	cluster.submit(true, false, LinearizableReadOnly, []byte{})
}

// TestReadOnlyFail checks that a read-only operation submitted when a leader has not received heartbeats
// from a majority of the partition is rejected.
func TestReadOnlyFail(t *testing.T) {
	cluster := newCluster(t, 3, snapshotting, snapshotSize)

	cluster.startCluster()
	defer cluster.stopCluster()

	// Wait for a leader.
	leader := cluster.checkLeaders(false)

	// Disconnect the other two servers in the cluster.
	cluster.disconnectServer((leader + 1) % 3)
	cluster.disconnectServer((leader + 2) % 3)

	// Linearizable read-only operation should fail since the heartbeats
	// of the leader will not be succesful.
	cluster.submit(true, true, LinearizableReadOnly, []byte{})

	// Give the lease some time to expire.
	time.Sleep(defaultLeaseDuration)

	// Make sure the read-only operation fails.
	cluster.submit(true, true, LeaseBasedReadOnly, []byte{})
}

// TestReadOnlyDisconnect checks that a new leader will renew its lease
// after the old one is disconnected, thereby allowing successful read-only
// operations.
func TestReadOnlyDisconnect(t *testing.T) {
	cluster := newCluster(t, 5, snapshotting, snapshotSize)

	cluster.startCluster()
	defer cluster.stopCluster()

	// Submit some operations to the initial leader.
	leader := cluster.checkLeaders(false)
	operations := makeOperations(10)
	cluster.submit(true, false, Replicated, operations...)

	// Disconnect the leader and wait for a new one.
	cluster.disconnectServer(leader)
	cluster.checkLeaders(false)

	// Give the leader a bit of time to make sure its lease gets renewed.
	time.Sleep(defaultElectionTimeout)

	// Check that read-only operation is successful.
	cluster.submit(true, false, LeaseBasedReadOnly, []byte{})
}
