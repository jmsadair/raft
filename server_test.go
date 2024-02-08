package raft

import (
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jmsadair/raft/internal/random"
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
	cluster := newCluster(t, 1, snapshotting, snapshotSize, 0)

	cluster.startCluster()
	defer cluster.stopCluster()

	cluster.checkLeaders(false)
}

// TestBasicElection checks whether a cluster can elect a leader
// when there are no failures.
func TestBasicElection(t *testing.T) {
	cluster := newCluster(t, 3, snapshotting, snapshotSize, 0)

	cluster.startCluster()
	defer cluster.stopCluster()

	cluster.checkLeaders(false)
}

// TestElectLeaderDisconnect checks whether a cluster can
// still elect a leader when a single server is Disconnected.
func TestElectLeaderDisconnect(t *testing.T) {
	cluster := newCluster(t, 3, snapshotting, snapshotSize, 0)

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
	cluster := newCluster(t, 3, snapshotting, snapshotSize, 0)

	cluster.startCluster()
	defer cluster.stopCluster()

	// Disconnect the leader and one other server, leaving
	// only one server that is capable of communicating.
	disconnected1 := cluster.checkLeaders(false)
	cluster.disconnectServer(disconnected1)
	cluster.disconnectRandom()

	// Check if the server can elect itself as the leader.
	// This should not be successful.
	cluster.checkLeaders(true)
}

// TestRemoveLeaderElection checks that a cluster can still elect a leader when the
// current one is removed.
func TestRemoveLeaderElection(t *testing.T) {
	cluster := newCluster(t, 5, snapshotting, snapshotSize, 0)

	cluster.startCluster()
	defer cluster.stopCluster()

	leader := cluster.checkLeaders(false)
	cluster.removeServer(leader)
	cluster.checkLeaders(false)
}

// TestNotVoterElectionSuccess checks that non-voting members do not prevent
// a leader from being elected. Only voting members should be considered when
// considering quorum - if the cluster originally has 3 voting members and 2
// non-voting members are added, leadership should only require 2 votes.
func TestNonVoterElectionSuccess(t *testing.T) {
	cluster := newCluster(t, 3, snapshotting, snapshotSize, 0)

	cluster.startCluster()
	defer cluster.stopCluster()

	cluster.checkLeaders(false)

	// Add non-voting members.
	id1, address1 := cluster.unusedIDandAddress()
	cluster.addServer(id1, address1, false)
	id2, address2 := cluster.unusedIDandAddress()
	cluster.addServer(id2, address2, false)

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
	cluster := newCluster(t, 3, snapshotting, snapshotSize, 0)

	cluster.startCluster()
	defer cluster.stopCluster()

	cluster.checkLeaders(false)

	// Add non-voting members.
	id1, address1 := cluster.unusedIDandAddress()
	cluster.addServer(id1, address1, false)
	id2, address2 := cluster.unusedIDandAddress()
	cluster.addServer(id2, address2, false)

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

// TestNewMembersCanLead checks that voting members added to
// an existing cluster are able to be elected as leaders.
func TestNewMembersCanLead(t *testing.T) {
	cluster := newCluster(t, 3, snapshotting, snapshotSize, 0)

	cluster.startCluster()
	defer cluster.stopCluster()

	nodes := cluster.nodeIDs()

	cluster.checkLeaders(false)

	// Add three new nodes to the cluster.
	id1, address1 := cluster.unusedIDandAddress()
	cluster.addServer(id1, address1, false)
	cluster.addServer(id1, address1, true)
	id2, address2 := cluster.unusedIDandAddress()
	cluster.addServer(id2, address2, false)
	cluster.addServer(id2, address2, true)
	id3, address3 := cluster.unusedIDandAddress()
	cluster.addServer(id3, address3, false)
	cluster.addServer(id3, address3, true)

	// Remove the original nodes.
	for _, node := range nodes {
		cluster.removeServer(node)
	}

	// Make sure a leader was elected.
	cluster.checkLeaders(false)
}

// TestSingleServerSubmit checks whether a cluster consisting of
// a single server can commit a single operation.
func TestSingleServerSubmit(t *testing.T) {
	cluster := newCluster(t, 1, snapshotting, snapshotSize, 0)

	cluster.startCluster()
	defer cluster.stopCluster()

	cluster.checkLeaders(false)
	operations := makeOperations(1)
	cluster.submit(false, Replicated, operations...)

	cluster.checkStateMachines(1, operations)
}

// TestSubmit checks whether the cluster can successfully
// commit a single operation when there are no failures.
func TestSubmit(t *testing.T) {
	cluster := newCluster(t, 3, snapshotting, snapshotSize, 0)

	cluster.startCluster()
	defer cluster.stopCluster()

	cluster.checkLeaders(false)
	operations := makeOperations(1)
	cluster.submit(false, Replicated, operations...)

	cluster.checkStateMachines(3, operations)
}

// TestMultipleSubmit checks whether a cluster can successfully
// commit multiple operations when there are no failures.
func TestMultipleSubmit(t *testing.T) {
	cluster := newCluster(t, 5, snapshotting, snapshotSize, 0)

	cluster.startCluster()
	defer cluster.stopCluster()

	cluster.checkLeaders(false)
	operations := makeOperations(1000)
	cluster.submit(false, Replicated, operations...)

	cluster.checkStateMachines(5, operations)
}

// TestConcurrentSubmit test whether operations are correctly
// applied when there are multiple clients submitting operations
// at the same time.
func TestConcurrentSubmit(t *testing.T) {
	cluster := newCluster(t, 5, snapshotting, snapshotSize, 0)

	cluster.startCluster()
	defer cluster.stopCluster()

	cluster.checkLeaders(false)
	operations := makeOperations(200)

	var wg sync.WaitGroup

	// Simulates a client submitting operations.
	client := func(operations [][]byte, readyCh chan interface{}) {
		defer wg.Done()
		<-readyCh
		cluster.submit(false, Replicated, operations...)
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

	cluster.checkStateMachines(5, operations)
}

// TestNewMemberSubmit checks that submitted operations are correctly
// replicated to new members of the cluster.
func TestNewMemberSubmit(t *testing.T) {
	cluster := newCluster(t, 3, snapshotting, snapshotSize, 0)

	cluster.startCluster()
	defer cluster.stopCluster()

	// Submit some operations to the cluster.
	operations := makeOperations(300)
	cluster.submit(false, Replicated, operations[:100]...)

	// Add a non-voting member.
	id1, address1 := cluster.unusedIDandAddress()
	cluster.addServer(id1, address1, false)

	// Submit some more operations to the cluster.
	cluster.submit(false, Replicated, operations[100:200]...)

	// Promote the non-voter.
	cluster.addServer(id1, address1, true)

	// Submit some operations to the clutser.
	cluster.submit(false, Replicated, operations[200:]...)

	cluster.checkStateMachines(4, operations)
}

// TestNonVoterSubmitFail checks that a cluster is unable to commit
// operations when a majority of its voting members are down. Non-voting
// members should not affect quorum.
func TestNonVoterSubmitFail(t *testing.T) {
	cluster := newCluster(t, 3, snapshotting, snapshotSize, 0)

	cluster.startCluster()
	defer cluster.stopCluster()

	cluster.checkLeaders(false)

	// Add a non-voting member.
	id1, address1 := cluster.unusedIDandAddress()
	cluster.addServer(id1, address1, false)

	// Disconnect the leader.
	disconnect1 := cluster.checkLeaders(false)
	cluster.disconnectServer(disconnect1)

	// Disconnect the next leader
	disconnect2 := cluster.checkLeaders(false)
	cluster.disconnectServer(disconnect2)

	// This should fail since a majority of voting members are down.
	operations := makeOperations(1)
	cluster.submit(true, Replicated, operations...)
}

// TestRemoveSubmitSuccess checks that operations can be committed
// after members of the cluster are removed.
func TestRemoveSubmitSuccess(t *testing.T) {
	cluster := newCluster(t, 5, snapshotting, snapshotSize, 0)

	cluster.startCluster()
	defer cluster.stopCluster()

	nodes := cluster.nodeIDs()

	// Remove the leader.
	remove1 := cluster.checkLeaders(false)
	cluster.crashServer(remove1)

	// Remove another node.
	for _, remove2 := range nodes {
		if remove1 == remove2 {
			continue
		}
		cluster.crashServer(remove2)
		break
	}

	cluster.checkLeaders(false)

	// Check if operations can be submitted.
	operations := makeOperations(10)
	cluster.submit(false, Replicated, operations...)
}

// TestSubmitConcurrentMembers checks that concurrently submitted operations are committed
// in the face of membership changes.
func TestSubmitConcurrentMembership(t *testing.T) {
	cluster := newCluster(t, 3, snapshotting, snapshotSize, 0)

	cluster.startCluster()
	defer cluster.stopCluster()

	nodes := cluster.nodeIDs()
	operations := makeOperations(1000)

	cluster.checkLeaders(false)

	var wg sync.WaitGroup

	// Simulates a client submitting operations.
	client := func(operations [][]byte, readyCh chan interface{}) {
		defer wg.Done()
		<-readyCh
		cluster.submit(false, Replicated, operations...)
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
	id1, address1 := cluster.unusedIDandAddress()
	cluster.addServer(id1, address1, false)
	time.Sleep(200 * time.Millisecond)
	cluster.addServer(id1, address1, true)

	// Remove a random server that was  part of the existing cluster.
	remove := random.RandomInt(0, len(nodes))
	cluster.removeServer(nodes[remove])

	// Wait for clients to finish.
	wg.Wait()

	cluster.checkStateMachines(4, operations)
}

// TestSubmitDisconnect checks that a cluster can still
// commit operations after the leader is disconnected.
func TestSubmitDisconnect(t *testing.T) {
	cluster := newCluster(t, 3, snapshotting, snapshotSize, 0)

	cluster.startCluster()
	defer cluster.stopCluster()

	// Disconnect the leader and see if operations are still committed.
	leader := cluster.checkLeaders(false)
	cluster.disconnectServer(leader)
	operations := makeOperations(20)
	cluster.submit(false, Replicated, operations...)

	cluster.checkStateMachines(2, operations)
}

// TestSubmitDisconnectRejoin checks that a cluster correctly
// handles leaders being disconnected and rejoining after operations
// are submitted.
func TestSubmitDisconnectRejoin(t *testing.T) {
	cluster := newCluster(t, 5, snapshotting, snapshotSize, 0)

	cluster.startCluster()
	defer cluster.stopCluster()

	// Disconnect the first leader.
	leader1 := cluster.checkLeaders(false)

	// Submit some operations with this leader.
	operations := makeOperations(80)
	cluster.submit(false, Replicated, operations[:20]...)

	// Disconnect the leader.
	cluster.disconnectServer(leader1)

	// Submit some more operations.
	cluster.submit(false, Replicated, operations[20:40]...)

	// Disconnect the second leader.
	leader2 := cluster.checkLeaders(false)

	// Submit some more operations.
	cluster.submit(false, Replicated, operations[40:60]...)

	// Allow the old leaders to rejoin.
	cluster.reconnectServer(leader1)
	cluster.reconnectServer(leader2)

	// Submit some more operations.
	cluster.submit(false, Replicated, operations[60:]...)

	cluster.checkStateMachines(5, operations)
}

// TestSubmitDisconnectFail checks that a cluster is unable to
// commit operations when a majority of the servers are completely
// disconnected from the cluster but still online.
func TestSubmitDisconnectFail(t *testing.T) {
	cluster := newCluster(t, 5, snapshotting, snapshotSize, 0)

	cluster.startCluster()
	defer cluster.stopCluster()

	// Disconnect three nodes, leaving only a minority of the server able to communicate.
	nodes := cluster.nodeIDs()
	for i := 0; i < 3; i++ {
		cluster.disconnectServer(nodes[i])
	}

	// Try to submit an operation. This should be unsuccessful
	// since only a minority of the cluster can communicate.
	operations := makeOperations(1)
	cluster.submit(true, Replicated, operations...)
}

// TestMultiDisconnect tests whether a cluster can still make
// progress submitting multiple operations when nodes are
// being disconnected and reconnected to the cluster.
func TestUMultiDisconnect(t *testing.T) {
	cluster := newCluster(t, 5, snapshotting, snapshotSize, 0)

	done := int32(0)
	wg := sync.WaitGroup{}
	unreliableNetRoutine := func() {
		defer wg.Done()
		for atomic.LoadInt32(&done) == 0 {
			// Allow the cluster to make some progress with no failures.
			randomTime := random.RandomTimeout(50*time.Millisecond, 100*time.Millisecond)
			time.Sleep(randomTime * time.Millisecond)

			// Disconnect two random nodes.
			nodes := cluster.nodeIDs()
			disconnect1 := random.RandomInt(0, len(nodes))
			disconnect2 := (disconnect1 + 1) % len(nodes)
			cluster.disconnectServer(nodes[disconnect1])
			cluster.disconnectServer(nodes[disconnect2])

			// Allow the cluster to make progress while the servers are disconnected.
			randomTime = random.RandomTimeout(50*time.Millisecond, 100*time.Millisecond)
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
	cluster.submit(false, Replicated, operations...)

	atomic.StoreInt32(&done, 1)
	wg.Wait()

	cluster.checkStateMachines(5, operations)
}

// TestUnreliableNetwork tests whether a cluster can still make
// progress submitting multiple operations when 50 percent of the
// message sent over the network are dropped.
func TestUnreliableNetwork(t *testing.T) {
	lossPercent := 50
	cluster := newCluster(t, 5, snapshotting, snapshotSize, lossPercent)

	cluster.startCluster()
	defer cluster.stopCluster()
	cluster.checkLeaders(false)

	// See if we can commit operations in the face of network failures.
	operations := makeOperations(500)
	cluster.submit(false, Replicated, operations...)

	cluster.checkStateMachines(5, operations)
}

// TestBasicPartition checks that a cluster can still make
// progress submitting multiple operations when there is a single
// partition.
func TestBasicPartition(t *testing.T) {
	cluster := newCluster(t, 5, snapshotting, snapshotSize, 0)

	cluster.startCluster()
	defer cluster.stopCluster()

	// Wait for a leader.
	cluster.checkLeaders(false)

	// Partition the cluster.
	cluster.createPartition()

	// Wait for a leader.
	cluster.checkLeaders(false)

	operations := makeOperations(50)
	cluster.submit(false, Replicated, operations...)

	// Heal the partition.
	cluster.reconnectAllServers()

	cluster.checkStateMachines(5, operations)
}

// TestMultiPartition checks whether a cluster can still make
// progress submitting multiple operations in the presence of
// multiple partitions.
func TestMultiPartition(t *testing.T) {
	cluster := newCluster(t, 5, snapshotting, snapshotSize, 0)

	// A go routine to crash random servers every so often.
	done := int32(0)
	var wg sync.WaitGroup
	partitionRoutine := func() {
		defer wg.Done()
		for atomic.LoadInt32(&done) == 0 {
			// Allow the cluster to make some progress with no failures.
			randomTime := random.RandomTimeout(100*time.Millisecond, 300*time.Millisecond)
			time.Sleep(randomTime * time.Millisecond)

			// Make a new partition.
			cluster.createPartition()

			// Allow the cluster to make progress with the partition.
			randomTime = random.RandomTimeout(300*time.Millisecond, 500*time.Millisecond)
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
	cluster.submit(false, Replicated, operations...)

	atomic.StoreInt32(&done, 1)
	wg.Wait()

	cluster.checkStateMachines(5, operations)
}

// TestMultiPartitionMembership checks whether a cluster can still make
// progress submitting multiple operations in the presence of
// multiple and changing partitions and changing membership.
func TestMultiPartitionMembership(t *testing.T) {
	cluster := newCluster(t, 3, snapshotting, snapshotSize, 0)

	// A go routine to create partitions.
	done := int32(0)
	var wg sync.WaitGroup
	partitionRoutine := func() {
		defer wg.Done()
		for atomic.LoadInt32(&done) == 0 {
			// Allow the cluster to make some progress with no failures.
			randomTime := random.RandomTimeout(100*time.Millisecond, 300*time.Millisecond)
			time.Sleep(randomTime * time.Millisecond)

			// Make a new partition.
			cluster.createPartition()

			// Allow the cluster to make progress with the partition.
			randomTime = random.RandomTimeout(300*time.Millisecond, 500*time.Millisecond)
			time.Sleep(randomTime * time.Millisecond)

			// Heal the partition.
			cluster.reconnectAllServers()
		}
	}

	cluster.startCluster()
	defer cluster.stopCluster()
	cluster.checkLeaders(false)

	// Start partitioning.
	wg.Add(1)
	go partitionRoutine()

	// Submit some operations.
	operations := makeOperations(1000)
	cluster.submit(false, Replicated, operations[:250]...)

	// Add a member as a non-voter.
	id, address := cluster.unusedIDandAddress()
	cluster.addServer(id, address, false)

	// Submit some more operations.
	cluster.submit(false, Replicated, operations[250:500]...)

	// Promote the non-voter to a voter.
	cluster.addServer(id, address, true)

	// Remove a server.
	nodes := cluster.nodeIDs()
	cluster.removeServer(nodes[random.RandomInt(0, len(nodes))])

	// Submit some more operations.
	cluster.submit(false, Replicated, operations[500:]...)

	atomic.StoreInt32(&done, 1)
	wg.Wait()

	cluster.checkStateMachines(3, operations)
}

// TestBasicCrash checks that a cluster can still make
// progress after a single server crashes.
func TestBasicCrash(t *testing.T) {
	cluster := newCluster(t, 5, snapshotting, snapshotSize, 0)

	cluster.startCluster()
	defer cluster.stopCluster()

	// Wait for a leader and submit some operations.
	leader := cluster.checkLeaders(false)
	operations := makeOperations(200)
	cluster.submit(false, Replicated, operations[:25]...)

	// Crash the leader and see if we can still make progress.
	cluster.crashServer(leader)
	cluster.submit(false, Replicated, operations[25:]...)

	cluster.checkStateMachines(4, operations)
}

// TestCrashRejoin checks that a cluster correctly
// handles a server crashing and coming back online
// after operations are submitted.
func TestCrashRejoin(t *testing.T) {
	cluster := newCluster(t, 5, snapshotting, snapshotSize, 0)

	cluster.startCluster()
	defer cluster.stopCluster()

	// Wait for a leader and submit some operations.
	leader := cluster.checkLeaders(false)
	operations := makeOperations(200)
	cluster.submit(false, Replicated, operations[:25]...)

	// Crash the leader and see if we can still make progress.
	cluster.crashServer(leader)
	cluster.submit(false, Replicated, operations[25:150]...)

	// Allow the leader to rejoin and see if we can make progress
	// committing operations.
	cluster.restartServer(leader)
	cluster.submit(false, Replicated, operations[150:]...)

	cluster.checkStateMachines(5, operations)
}

// TestMultiCrashMembership checks if a cluster can still make
// progress committing operations and handle new member joining the
// cluster in the face of multiple crashes. The added members may also
// be crashed
func TestMultiCrashMembership(t *testing.T) {
	cluster := newCluster(t, 3, snapshotting, snapshotSize, 0)

	// A go routine to crash random servers every so often.
	done := int32(0)
	var wg sync.WaitGroup
	crashRoutine := func() {
		defer wg.Done()
		for atomic.LoadInt32(&done) == 0 {
			// Allow the cluster to make some progress with no failures.
			randomTime := random.RandomTimeout(100*time.Millisecond, 300*time.Millisecond)
			time.Sleep(randomTime * time.Millisecond)

			// Crash a random node.
			cluster.crashRandom()

			// Allow the cluster to make progress while the node is offline.
			randomTime = random.RandomTimeout(300*time.Millisecond, 500*time.Millisecond)
			time.Sleep(randomTime * time.Millisecond)

			// Bring the node back online.
			cluster.restartServers()
		}
	}

	cluster.startCluster()
	defer cluster.stopCluster()
	cluster.checkLeaders(false)

	// Start crashing servers.
	wg.Add(1)
	go crashRoutine()

	// Submit some operations.
	operations := makeOperations(1000)
	cluster.submit(false, Replicated, operations[:250]...)

	// Add a member as a non-voter.
	id, address := cluster.unusedIDandAddress()
	cluster.addServer(id, address, false)

	// Submit some more operations.
	cluster.submit(false, Replicated, operations[250:500]...)

	// Promote the non-voter to a voter.
	cluster.addServer(id, address, true)

	// Remove a server.
	nodes := cluster.nodeIDs()
	cluster.removeServer(nodes[random.RandomInt(0, len(nodes))])

	// Submit some more operations.
	cluster.submit(false, Replicated, operations[500:]...)

	atomic.StoreInt32(&done, 1)
	wg.Wait()

	cluster.checkStateMachines(3, operations)
}

// TestMultiCrash checks if a cluster can still make
// progress committing operations in the face of multiple
// crashes.
func TestMultiCrash(t *testing.T) {
	cluster := newCluster(t, 5, snapshotting, snapshotSize, 0)

	// A go routine to crash random servers every so often.
	done := int32(0)
	var wg sync.WaitGroup
	crashRoutine := func() {
		defer wg.Done()
		for atomic.LoadInt32(&done) == 0 {
			// Allow the cluster to make some progress with no failures.
			randomTime := random.RandomTimeout(100*time.Millisecond, 300*time.Millisecond)
			time.Sleep(randomTime * time.Millisecond)

			// Crash two random nodes.
			cluster.crashRandom()
			cluster.crashRandom()

			// Allow the cluster to make progress while the nodes are offline.
			randomTime = random.RandomTimeout(300*time.Millisecond, 500*time.Millisecond)
			time.Sleep(randomTime * time.Millisecond)

			// Bring the nodes back online.
			cluster.restartServers()

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
	cluster.submit(false, Replicated, operations...)

	atomic.StoreInt32(&done, 1)
	wg.Wait()

	cluster.checkStateMachines(5, operations)
}

// TestDisconnectCrashPartition checks whether the cluster can still
// make progress when there are disconnections, crashes, and partitions and
// the network is lossy.
func TestDisconnectCrashPartition(t *testing.T) {
	lossPercent := 20
	cluster := newCluster(t, 5, snapshotting, snapshotSize, lossPercent)

	// A go routine to crash, disconnect, and partition random servers every so often.
	done := int32(0)
	wg := sync.WaitGroup{}
	failureRoutine := func() {
		defer wg.Done()
		for atomic.LoadInt32(&done) == 0 {
			// Allow the cluster to make some progress with no failures.
			randomTime := random.RandomTimeout(100*time.Millisecond, 300*time.Millisecond)
			time.Sleep(randomTime * time.Millisecond)

			// Choose a random type of failure.
			action := random.RandomInt(0, 3)

			switch action {
			// Crash a single server.
			case 0:
				crash := cluster.crashRandom()
				randomTime = random.RandomTimeout(300*time.Millisecond, 500*time.Millisecond)
				time.Sleep(randomTime * time.Millisecond)
				cluster.restartServer(crash)
			// Disconnect a single server.
			case 1:
				disconnect := cluster.disconnectRandom()
				randomTime = random.RandomTimeout(300*time.Millisecond, 500*time.Millisecond)
				time.Sleep(randomTime * time.Millisecond)
				cluster.reconnectServer(disconnect)
			// Partition the servers into two separate groups.
			case 2:
				cluster.createPartition()
				randomTime = random.RandomTimeout(300*time.Millisecond, 500*time.Millisecond)
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
	cluster.submit(false, Replicated, operations...)

	atomic.StoreInt32(&done, 1)
	wg.Wait()

	cluster.checkStateMachines(5, operations)
}

// TestAllCrash checks that a cluster can still make
// progress committing operations after all the servers
// crash and come back online.
func TestAllCrash(t *testing.T) {
	cluster := newCluster(t, 5, snapshotting, snapshotSize, 0)

	cluster.startCluster()
	defer cluster.stopCluster()

	nodes := cluster.nodeIDs()

	// Wait for a leader and submit some operations.
	cluster.checkLeaders(false)
	operations := makeOperations(50)
	cluster.submit(false, Replicated, operations[:25]...)

	// Crash all servers.
	for _, node := range nodes {
		cluster.crashServer(node)
	}

	// Restart all the servers.
	for _, node := range nodes {
		cluster.restartServer(node)
	}

	// Wait for another leader and submit more operations.
	cluster.checkLeaders(false)
	cluster.submit(false, Replicated, operations[25:]...)

	cluster.checkStateMachines(5, operations)
}

// TestBasicReadOnly checks that a read-only operation submitted under normal conditions
// are successful.
func TestBasicReadOnly(t *testing.T) {
	cluster := newCluster(t, 5, snapshotting, snapshotSize, 0)

	cluster.startCluster()
	defer cluster.stopCluster()

	// Wait for a leader and submit some operations.
	cluster.checkLeaders(false)
	operations := makeOperations(1)
	cluster.submit(false, Replicated, operations...)
	cluster.submit(false, LeaseBasedReadOnly, []byte{})
}

// TestSingleServerReadOnly checks that read-only operations are successful in the single server case.
func TestSingleServerReadOnly(t *testing.T) {
	cluster := newCluster(t, 1, snapshotting, snapshotSize, 0)

	cluster.startCluster()
	defer cluster.stopCluster()

	// Wait for a leader and submit some operations.
	cluster.checkLeaders(false)
	operations := makeOperations(10)
	cluster.submit(false, Replicated, operations...)

	cluster.submit(false, LeaseBasedReadOnly, []byte{})
	cluster.submit(false, LinearizableReadOnly, []byte{})
}

// TestReadOnlyFail checks that a read-only operation submitted when a leader has not received heartbeats
// from a majority of the cluster is rejected.
func TestReadOnlyFail(t *testing.T) {
	cluster := newCluster(t, 3, snapshotting, snapshotSize, 0)

	cluster.startCluster()
	defer cluster.stopCluster()

	// Wait for a leader.
	leader := cluster.checkLeaders(false)

	// Disconnect the other two servers in the cluster.
	cluster.disconnectServer(leader)
	cluster.disconnectRandom()

	// Give the lease some time to expire.
	time.Sleep(defaultLeaseDuration)

	// Linearizable read-only operation should fail since the heartbeats
	// of the leader will not be succesful.
	cluster.submit(true, LinearizableReadOnly, []byte{})

	// Lease-based read-only operation should fail too since the lease cannot be renewed.
	cluster.submit(true, LeaseBasedReadOnly, []byte{})
}

// TestReadOnlyDisconnect checks that a new leader will renew its lease
// after the old one is disconnected, thereby allowing successful read-only
// operations.
func TestReadOnlyDisconnect(t *testing.T) {
	cluster := newCluster(t, 5, snapshotting, snapshotSize, 0)

	cluster.startCluster()
	defer cluster.stopCluster()

	// Submit some operations to the initial leader.
	leader := cluster.checkLeaders(false)
	operations := makeOperations(10)
	cluster.submit(false, Replicated, operations...)

	// Disconnect the leader and wait for a new one.
	cluster.disconnectServer(leader)
	cluster.checkLeaders(false)

	// Give the leader a bit of time to make sure its lease gets renewed.
	time.Sleep(defaultElectionTimeout)

	// Check that read-only operation is successful.
	cluster.submit(false, LeaseBasedReadOnly, []byte{})
}
