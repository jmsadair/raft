package raft

import (
	"fmt"
	"net"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/fortytw2/leaktest"
	"github.com/jmsadair/raft/internal/errors"
	"github.com/jmsadair/raft/internal/util"
)

func makePeers(numServers int) [][]*Peer {
	ipPrefix := "127.0.0."
	port := 8080
	clusterPeers := make([][]*Peer, numServers)
	for i := 0; i < numServers; i++ {
		clusterPeers[i] = make([]*Peer, numServers)
		for j := 0; j < numServers; j++ {
			ip := ipPrefix + fmt.Sprint(j)
			peerId := fmt.Sprint(j)
			clusterPeers[i][j] = NewPeer(peerId, &net.TCPAddr{IP: net.ParseIP(ip), Port: port})
		}
	}

	return clusterPeers
}

type TestCluster struct {
	t                *testing.T
	peers            [][]*Peer
	servers          []*Server
	connected        [][]bool
	logs             []Log
	snapshotStores   []SnapshotStorage
	stateMachines    []StateMachine
	stores           []Storage
	replicateCh      []chan CommandResponse
	shutdownCh       chan interface{}
	commandResponses []map[uint64]CommandResponse
	serverErrors     []string
	commands         []Command
	lastApplied      []uint64
	mu               sync.Mutex
	wg               sync.WaitGroup
}

func newCluster(t *testing.T, numServers int) (*TestCluster, error) {
	servers := make([]*Server, numServers)
	snapshotStores := make([]SnapshotStorage, numServers)
	stateMachines := make([]StateMachine, numServers)
	logs := make([]Log, numServers)
	stores := make([]Storage, numServers)
	replicateCh := make([]chan CommandResponse, numServers)
	responses := make([]map[uint64]CommandResponse, numServers)
	expected := make([]Command, 0)
	connected := make([][]bool, numServers)
	peers := makePeers(numServers)
	serverErrors := make([]string, numServers)
	lastApplied := make([]uint64, numServers)

	for i := 0; i < numServers; i++ {
		replicateCh[i] = make(chan CommandResponse)
		snapshotStores[i] = NewSnapshotStorageMock()
		stateMachines[i] = NewStateMachineMock()
		logs[i] = NewLogMock(0, 0)
		stores[i] = NewStorageMock()
		connected[i] = make([]bool, numServers)
		responses[i] = make(map[uint64]CommandResponse)

		for j := 0; j < numServers; j++ {
			connected[i][j] = true
		}

		id := peers[0][i].Id()
		address := peers[0][i].Address()

		server, err := NewServer(id, peers[i], logs[i], stores[i], snapshotStores[i], stateMachines[i], address, replicateCh[i])
		if err != nil {
			return nil, errors.WrapError(err, "error creating cluster server: %s", err.Error())
		}
		servers[i] = server
	}

	return &TestCluster{
		t:                t,
		peers:            peers,
		servers:          servers,
		connected:        connected,
		snapshotStores:   snapshotStores,
		stateMachines:    stateMachines,
		stores:           stores,
		logs:             logs,
		replicateCh:      replicateCh,
		commandResponses: responses,
		commands:         expected,
		serverErrors:     serverErrors,
		lastApplied:      lastApplied,
		shutdownCh:       make(chan interface{}),
	}, nil
}

func (tc *TestCluster) startCluster() {
	ready := make(chan interface{})
	for i, server := range tc.servers {
		if err := server.Start(ready); err != nil {
			tc.t.Fatalf("error starting cluster server: %s", err.Error())
		}
		tc.wg.Add(1)
		go tc.applyLoop(i)
	}
	close(ready)
}

func (tc *TestCluster) stopCluster() {
	for _, server := range tc.servers {
		server.Stop()
	}
	close(tc.shutdownCh)
	tc.wg.Wait()
}

func (tc *TestCluster) makeCommands(numCommands int) []Command {
	commands := make([]Command, numCommands)
	for i := 1; i <= numCommands; i++ {
		commands[i-1] = Command{Bytes: []byte(fmt.Sprintf("command %d", i))}
	}

	return commands
}

func (tc *TestCluster) submit(command Command, retry bool, expectFail bool, expectedApplied int) {
	for i := 0; i < 10; i++ {
		for j := 0; j < len(tc.servers); j++ {
			tc.mu.Lock()
			server := tc.servers[j]
			status := server.Status()

			if status.State != Leader {
				tc.mu.Unlock()
				continue
			}

			index, term, err := server.SubmitCommand(command)
			if err != nil {
				tc.mu.Unlock()
				continue
			}
			tc.mu.Unlock()

			for k := 0; k < 10; k++ {
				time.Sleep(25 * time.Millisecond)
				successful := tc.checkApplied(index, expectedApplied)
				if successful {
					if expectFail {
						tc.t.Fatalf("cluster applied command when it should not have")
					}
					return
				}
				status = server.Status()
				if status.Term != term {
					break
				}
			}
		}

		if !retry {
			break
		}

		time.Sleep(100 * time.Millisecond)
	}

	if !expectFail {
		tc.t.Fatalf("cluster failed to apply command: command = %v", command)
	}
}

func (tc *TestCluster) checkLeaders(expectNoLeader bool) string {
	leaders := make([]string, 0)

	// A maximum of 3 seconds is given to successfully elect a leader.
	electionTimeout := 300 * time.Millisecond
	for i := 0; i < 10; i++ {
		for j, server := range tc.servers {
			status := server.Status()

			// We need to check if the server is connected to the
			// cluster since it is possible to have two leaders if
			// one is disconnected.
			if status.State == Leader && tc.connected[j][j] {
				leaders = append(leaders, status.Id)
			}
		}
		if len(leaders) > 1 {
			tc.t.Fatal("cluster has more than one leader")
		}
		if len(leaders) == 1 {
			break
		}
		time.Sleep(electionTimeout)
	}

	if len(leaders) == 0 && !expectNoLeader {
		tc.t.Fatal("cluster failed to elect a leader")
	}
	if len(leaders) != 0 && expectNoLeader {
		tc.t.Fatal("cluster elected a leader when it should not have")
	}
	if expectNoLeader {
		return ""
	}

	return leaders[0]
}

func (tc *TestCluster) checkLogs(index int, response CommandResponse) {
	tc.mu.Lock()
	defer tc.mu.Unlock()

	expectedIndex := tc.lastApplied[index] + 1
	if response.Index != expectedIndex {
		tc.serverErrors[index] = fmt.Sprintf("command applied out of order: expected index %d, got index %d", expectedIndex, response.Index)
	}
	tc.commandResponses[index][response.Index] = response
	tc.lastApplied[index]++
}

func (tc *TestCluster) checkApplied(index uint64, expectedApplied int) bool {
	tc.mu.Lock()
	defer tc.mu.Unlock()

	var expectedCommandResponse CommandResponse
	hasApplied := 0
	for i := 0; i < len(tc.servers); i++ {
		if tc.serverErrors[i] != "" {
			tc.t.Fatalf(tc.serverErrors[i])
		}

		if commandResponse, ok := tc.commandResponses[i][index]; ok {
			if hasApplied != 0 && string(commandResponse.Command) != string(expectedCommandResponse.Command) {
				tc.t.Fatalf("server applied different commands at same index: index = %d, command1 = %v, command2 = %v",
					index, expectedCommandResponse.Command, commandResponse.Command)
			}
			expectedCommandResponse = commandResponse
			hasApplied++
		}
	}

	return hasApplied >= expectedApplied
}

func (tc *TestCluster) applyLoop(index int) {
	defer tc.wg.Done()

	for response := range tc.replicateCh[index] {
		tc.checkLogs(index, response)
	}
}

func (tc *TestCluster) crashServer(serverID string) {
	tc.mu.Lock()
	defer tc.mu.Unlock()

	index, _ := strconv.Atoi(serverID)
	tc.disconnectServerTwoWay(serverID)
	tc.servers[index].Stop()
}

func (tc *TestCluster) restartServer(serverID string) {
	tc.mu.Lock()
	defer tc.mu.Unlock()

	index, _ := strconv.Atoi(serverID)
	address := tc.peers[index][index].Address()

	tc.replicateCh[index] = make(chan CommandResponse)
	tc.stateMachines[index] = NewStateMachineMock()

	newServer, err := NewServer(serverID, tc.peers[index], tc.logs[index], tc.stores[index],
		tc.snapshotStores[index], tc.stateMachines[index], address, tc.replicateCh[index])
	if err != nil {
		tc.t.Fatalf("error restarting cluster server: %s", err.Error())
	}

	tc.servers[index] = newServer
	tc.lastApplied[index] = 0
	tc.commandResponses[index] = make(map[uint64]CommandResponse)

	tc.wg.Add(1)
	go tc.applyLoop(index)

	readyCh := make(chan interface{})
	defer close(readyCh)
	newServer.Start(readyCh)

	for i := 0; i < len(tc.servers); i++ {
		tc.peers[i][index].connect()
		tc.connected[i][index] = true
	}
}

func (tc *TestCluster) createPartition() {
	tc.mu.Lock()
	defer tc.mu.Unlock()

	partitionSize := len(tc.servers) / 2
	partitionSet := make(map[int]bool)
	index := util.RandomInt(0, len(tc.servers))

	for i := 0; i < partitionSize; i++ {
		partitionSet[(index+i)%len(tc.servers)] = true
	}

	for index := range partitionSet {
		for i := 0; i < len(tc.servers); i++ {
			// Disconnect the servers in partition set from
			// those that are not.
			if _, ok := partitionSet[i]; ok {
				tc.connected[index][index] = false
				for j := 0; j < len(tc.peers[i]); j++ {
					if _, ok := partitionSet[j]; ok {
						continue
					}
					tc.peers[i][j].disconnect()
					tc.connected[i][j] = false
				}
				continue
			}
			// Disconnect the server not in the partition set from
			// those that are.
			tc.connected[i][index] = false
			tc.peers[i][index].disconnect()
		}
	}
}

func (tc *TestCluster) healPartition() {
	tc.mu.Lock()
	defer tc.mu.Unlock()

	for i := 0; i < len(tc.servers); i++ {
		for j := 0; j < len(tc.servers); j++ {
			if tc.connected[i][j] {
				continue
			}
			tc.connected[i][j] = true
			tc.peers[i][j].connect()
		}
	}
}

func (tc *TestCluster) disconnectServerTwoWay(serverID string) {
	server, _ := strconv.Atoi(serverID)
	for i := 0; i < len(tc.peers); i++ {
		if i == server {
			for j := 0; j < len(tc.peers[i]); j++ {
				tc.peers[i][j].disconnect()
				tc.connected[i][j] = false
			}
			continue
		}
		tc.peers[i][server].disconnect()
		tc.connected[i][server] = false
	}
}

// TestBasicElection checks whether a cluster can elect a leader
// when there are no failures.
func TestBasicElection(t *testing.T) {
	defer leaktest.CheckTimeout(t, 1*time.Second)

	cluster, err := newCluster(t, 3)
	if err != nil {
		t.Fatalf("error creating new cluster: %s", err.Error())
	}

	cluster.startCluster()
	defer cluster.stopCluster()

	cluster.checkLeaders(false)
}

// TestElectLeaderDisconnect checks whether a cluster can
// still elect a leader when a single server is disconnected.
func TestElectLeaderDisconnect(t *testing.T) {
	defer leaktest.CheckTimeout(t, 1*time.Second)

	cluster, err := newCluster(t, 3)
	if err != nil {
		t.Fatalf("error creating new cluster: %s", err.Error())
	}

	cluster.startCluster()
	defer cluster.stopCluster()

	// Disconnect the leader.
	leader := cluster.checkLeaders(false)
	cluster.disconnectServerTwoWay(leader)

	// See if the cluster can still elect a new leader.
	cluster.checkLeaders(false)
}

// TestFailElectLeaderDisconnect checks whether a leader is able
// to be elected when a majority of the servers are disconnected.
func TestFailElectLeaderDisconnect(t *testing.T) {
	defer leaktest.CheckTimeout(t, 1*time.Second)

	cluster, err := newCluster(t, 3)
	if err != nil {
		t.Fatalf("error creating new cluster: %s", err.Error())
	}

	cluster.startCluster()
	defer cluster.stopCluster()

	// Disconnect the leader and one other server, leaving
	// only one server that is capable of communicating.
	disconnectServer1 := cluster.checkLeaders(false)
	serverID, _ := strconv.Atoi(disconnectServer1)
	disconnectServer2 := fmt.Sprint((serverID + 1) % 3)
	cluster.disconnectServerTwoWay(disconnectServer1)
	cluster.disconnectServerTwoWay(disconnectServer2)

	// Check if the server can elect itself as the leader.
	// This should not be successful.
	cluster.checkLeaders(true)
}

// TestSingleSubmit checks whether the cluster can successfully
// commit a single command when there are no failures.
func TestBasicSubmit(t *testing.T) {
	defer leaktest.CheckTimeout(t, 1*time.Second)

	cluster, err := newCluster(t, 3)
	if err != nil {
		t.Fatalf("error creating new cluster: %s", err.Error())
	}

	cluster.startCluster()
	defer cluster.stopCluster()

	cluster.checkLeaders(false)
	commands := cluster.makeCommands(1)
	cluster.submit(commands[0], false, false, 3)
}

// TestMultipleSubmit checks whether a cluster can successfully
// commit multiple commands when there are no failures.
func TestMultipleSubmit(t *testing.T) {
	defer leaktest.CheckTimeout(t, 1*time.Second)

	cluster, err := newCluster(t, 5)
	if err != nil {
		t.Fatalf("error creating new cluster: %s", err.Error())
	}

	cluster.startCluster()
	defer cluster.stopCluster()

	cluster.checkLeaders(false)
	commands := cluster.makeCommands(100)
	for _, command := range commands {
		cluster.submit(command, false, false, 5)
	}
}

// TestSubmitDisconnect checks that a cluster can still
// commit commands after a single server is disconnected.
func TestSubmitDisconnect(t *testing.T) {
	defer leaktest.CheckTimeout(t, 1*time.Second)

	cluster, err := newCluster(t, 3)
	if err != nil {
		t.Fatalf("error creating new cluster: %s", err.Error())
	}

	cluster.startCluster()
	defer cluster.stopCluster()

	// Disconnect the leader and see if commands are still committed.
	leader := cluster.checkLeaders(false)
	cluster.disconnectServerTwoWay(leader)
	commands := cluster.makeCommands(20)
	for _, command := range commands {
		cluster.submit(command, true, false, 2)
	}
}

// TestSubmitDisconnectFail checks that a cluster is unable to
// commit commands when a majority of the servers are unable to
// communicate.
func TestSubmitDisconnectFail(t *testing.T) {
	defer leaktest.CheckTimeout(t, 1*time.Second)

	cluster, err := newCluster(t, 5)
	if err != nil {
		t.Fatalf("error creating new cluster: %s", err.Error())
	}

	cluster.startCluster()
	defer cluster.stopCluster()

	// Disconnect the leader and two other servers, leaving
	// only a minority of the server able to communicate.
	leader := cluster.checkLeaders(false)
	serverID, _ := strconv.Atoi(leader)
	disconnectServer1 := fmt.Sprint((serverID + 1) % 5)
	disconnectServer2 := fmt.Sprint((serverID + 2) % 5)
	disconnectServer3 := fmt.Sprint((serverID + 3) % 5)
	cluster.disconnectServerTwoWay(disconnectServer1)
	cluster.disconnectServerTwoWay(disconnectServer2)
	cluster.disconnectServerTwoWay(disconnectServer3)

	// Try to submit some commands. This should be unsuccessful
	// since only a minority of the cluster can communicate.
	commands := cluster.makeCommands(20)
	for _, command := range commands {
		cluster.submit(command, false, true, 1)
	}
}

func TestBasicPartition(t *testing.T) {
	defer leaktest.CheckTimeout(t, 1*time.Second)

	cluster, err := newCluster(t, 5)
	if err != nil {
		t.Fatalf("error creating new cluster: %s", err.Error())
	}

	cluster.startCluster()
	defer cluster.stopCluster()

	// Wait for a leader.
	cluster.checkLeaders(false)

	// Partition the cluster.
	cluster.createPartition()

	// Wait for a leader.
	cluster.checkLeaders(false)

	commands := cluster.makeCommands(50)
	for _, command := range commands {
		cluster.submit(command, false, false, 3)
	}

	// Heal the partition.
	cluster.healPartition()
}

func TestMultiPartition(t *testing.T) {
	defer leaktest.CheckTimeout(t, 1*time.Second)

	cluster, err := newCluster(t, 5)
	if err != nil {
		t.Fatalf("error creating new cluster: %s", err.Error())
	}

	// A go routine to crash random servers every so often.
	done := int32(0)
	wg := sync.WaitGroup{}
	partitionRoutine := func() {
		defer wg.Done()
		for atomic.LoadInt32(&done) == 0 {
			randomTime := util.RandomTimeout(300*time.Millisecond, 500*time.Millisecond)
			time.Sleep(randomTime * time.Millisecond)
			cluster.createPartition()
			time.Sleep(300 * time.Millisecond)
			cluster.healPartition()
		}
	}

	cluster.startCluster()
	defer cluster.stopCluster()
	cluster.checkLeaders(false)

	// Start partitioning
	wg.Add(1)
	go partitionRoutine()

	// See if we can commit commands in the face of recurring partitions.
	commands := cluster.makeCommands(300)
	for _, command := range commands {
		cluster.submit(command, true, false, 3)
	}

	atomic.StoreInt32(&done, 1)
	wg.Wait()
}

// TestCrashRejoin checks that a cluster can still make
// progress after a single server crashes.
func TestCrashRejoin(t *testing.T) {
	defer leaktest.CheckTimeout(t, 1*time.Second)

	cluster, err := newCluster(t, 5)
	if err != nil {
		t.Fatalf("error creating new cluster: %s", err.Error())
	}

	cluster.startCluster()
	defer cluster.stopCluster()

	// Wait for a leader and submit some commands.
	leader := cluster.checkLeaders(false)
	commands := cluster.makeCommands(50)
	for i := 0; i < 25; i++ {
		cluster.submit(commands[i], false, false, 5)
	}

	// Crash the leader and see if we can still make progress.
	cluster.crashServer(leader)
	for i := 25; i < 40; i++ {
		cluster.submit(commands[i], true, false, 4)
	}

	// Allow the leader to rejoin and see if we can make progress
	// committing commands.
	cluster.restartServer(leader)
	cluster.checkLeaders(false)
	for i := 40; i < len(commands); i++ {
		cluster.submit(commands[i], true, false, 5)
	}
}

// TestMultiCrash checks if a cluster can still make
// progress committing commands in the face of multiple
// crashes.
func TestMultiCrash(t *testing.T) {
	defer leaktest.CheckTimeout(t, 1*time.Second)

	cluster, err := newCluster(t, 5)
	if err != nil {
		t.Fatalf("error creating new cluster: %s", err.Error())
	}

	// A go routine to crash random servers every so often.
	done := int32(0)
	wg := sync.WaitGroup{}
	crashRoutine := func() {
		defer wg.Done()
		for atomic.LoadInt32(&done) == 0 {
			time.Sleep(400 * time.Millisecond)
			crash1 := util.RandomInt(0, 5)
			crash2 := (crash1 + 1) % 5
			id1 := fmt.Sprint(crash1)
			id2 := fmt.Sprint(crash2)
			cluster.crashServer(id1)
			cluster.crashServer(id2)
			time.Sleep(300 * time.Millisecond)
			cluster.restartServer(id1)
			cluster.restartServer(id2)
		}
	}

	cluster.startCluster()
	defer cluster.stopCluster()
	cluster.checkLeaders(false)

	// Start crashing servers.
	wg.Add(1)
	go crashRoutine()

	// See if we can commit commands in the face of multiple crashes.
	commands := cluster.makeCommands(300)
	for _, command := range commands {
		cluster.submit(command, true, false, 3)
	}

	atomic.StoreInt32(&done, 1)
	wg.Wait()
}

// TestAllCrash checks that a cluster can still make
// progress committing commands after all of the servers
// crash and come back online.
func TestAllCrash(t *testing.T) {
	defer leaktest.CheckTimeout(t, 1*time.Second)

	cluster, err := newCluster(t, 5)
	if err != nil {
		t.Fatalf("error creating new cluster: %s", err.Error())
	}

	cluster.startCluster()
	defer cluster.stopCluster()

	// Wait for a leader and submit some commands.
	cluster.checkLeaders(false)
	commands := cluster.makeCommands(50)
	for i := 0; i < 25; i++ {
		cluster.submit(commands[i], false, false, 5)
	}

	// Crash all servers.
	for i := 0; i < 5; i++ {
		cluster.crashServer(fmt.Sprint(i))
	}

	// Restart all the servers.
	for i := 0; i < 5; i++ {
		cluster.restartServer(fmt.Sprint(i))
	}

	// Wait for another leader and submit more commands.
	cluster.checkLeaders(false)
	for i := 25; i < len(commands); i++ {
		cluster.submit(commands[i], true, false, 5)
	}
}
