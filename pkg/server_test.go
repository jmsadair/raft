package raft

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"net"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/fortytw2/leaktest"
	"github.com/jmsadair/raft/internal/errors"
	"github.com/jmsadair/raft/internal/util"
)

func makeProtobufPeers(numServers int) [][]*ProtobufPeer {
	port := 8080
	clusterPeers := make([][]*ProtobufPeer, numServers)
	for i := 0; i < numServers; i++ {
		clusterPeers[i] = make([]*ProtobufPeer, numServers)
		for j := 0; j < numServers; j++ {
			ip := fmt.Sprintf("127.0.0.%d", j)
			peerId := fmt.Sprint(j)
			clusterPeers[i][j] = NewProtobufPeer(peerId, &net.TCPAddr{IP: net.ParseIP(ip), Port: port})
		}
	}

	return clusterPeers
}

type TestCluster struct {
	t                *testing.T
	peers            [][]*ProtobufPeer
	servers          []*ProtobufServer
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
	snapshotting     bool
	snapshotSize     int
	mu               sync.Mutex
	wg               sync.WaitGroup
}

func newCluster(t *testing.T, numServers int) (*TestCluster, error) {
	servers := make([]*ProtobufServer, numServers)
	snapshotStores := make([]SnapshotStorage, numServers)
	stateMachines := make([]StateMachine, numServers)
	logs := make([]Log, numServers)
	stores := make([]Storage, numServers)
	replicateCh := make([]chan CommandResponse, numServers)
	responses := make([]map[uint64]CommandResponse, numServers)
	expected := make([]Command, 0)
	peers := makeProtobufPeers(numServers)
	serverErrors := make([]string, numServers)
	lastApplied := make([]uint64, numServers)
	tmpDir := t.TempDir()
	snapshotting := os.Getenv("SNAPSHOTS") == "true"
	snapshotSize, _ := strconv.Atoi(os.Getenv("SNAPSHOT_SIZE"))

	for i := 0; i < numServers; i++ {
		replicateCh[i] = make(chan CommandResponse)
		snapshotStores[i] = NewPersistentSnapshotStorage(tmpDir+fmt.Sprintf("/raft-snapshots-%d", i), new(ProtoSnapshotEncoder), new(ProtoSnapshotDecoder))
		stateMachines[i] = NewStateMachineMock()
		logs[i] = NewPersistentLog(tmpDir+fmt.Sprintf("/raft-log-%d", i), new(ProtoLogEncoder), new(ProtoLogDecoder))
		stores[i] = NewPersistentStorage(tmpDir+fmt.Sprintf("/raft-storage-%d", i), new(ProtoStorageEncoder), new(ProtoStorageDecoder))
		responses[i] = make(map[uint64]CommandResponse)

		id := peers[0][i].Id()
		address := peers[0][i].Address()

		server, err := NewProtobufServer(id, peers[i], logs[i], stores[i], snapshotStores[i], stateMachines[i], address, replicateCh[i],
			WithSnapshotting(snapshotting), WithMaxLogEntriesPerSnapshot(snapshotSize))
		if err != nil {
			return nil, errors.WrapError(err, "error creating cluster server: %s", err.Error())
		}

		servers[i] = server
	}

	return &TestCluster{
		t:                t,
		peers:            peers,
		servers:          servers,
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
		snapshotting:     snapshotting,
		snapshotSize:     snapshotSize,
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
	start := time.Now()

	// Allow for a maximum of five seconds if retry is enabled.
	for time.Since(start).Seconds() < 5 {
		for j := 0; j < len(tc.servers); j++ {
			tc.mu.Lock()
			server := tc.servers[j]
			tc.mu.Unlock()

			status := server.Status()
			if status.State != Leader {
				continue
			}

			index, term, err := server.SubmitCommand(command)
			if err != nil {
				continue
			}

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
	start := time.Now()
	electionTimeout := 300 * time.Millisecond

	// A maximum of 3 seconds is given to successfully elect a leader.
	for time.Since(start).Seconds() < 3 {
		for i := 0; i < len(tc.servers); i++ {
			tc.mu.Lock()
			server := tc.servers[i]
			tc.mu.Unlock()

			status := server.Status()

			tc.mu.Lock()
			if status.State == Leader && tc.peers[i][i].Connected() {
				leaders = append(leaders, status.ID)
			}
			tc.mu.Unlock()
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
		tc.serverErrors[index] = fmt.Sprintf("server %d applied command out of order: expected index %d, got index %d",
			index, expectedIndex, response.Index)
		return
	}

	tc.commandResponses[index][response.Index] = response
	tc.lastApplied[index]++
}

func (tc *TestCluster) checkSnapshot(index int, response CommandResponse) {
	tc.mu.Lock()
	defer tc.mu.Unlock()

	if response.Snapshot.LastIncludedIndex <= tc.lastApplied[index] {
		tc.serverErrors[index] = fmt.Sprintf("server %d applied command out of order: expected index %d, got index %d",
			index, tc.lastApplied[index]+1, response.Snapshot.LastIncludedIndex)
	}

	var appliedCommands []AppliedCommand
	data := bytes.NewBuffer(response.Snapshot.Data)
	dec := gob.NewDecoder(data)
	if err := dec.Decode(&appliedCommands); err != nil {
		tc.serverErrors[index] = fmt.Sprintf("failed to decode commands: %s", err)
		return
	}

	for _, command := range appliedCommands {
		tc.commandResponses[index][command.Index] = CommandResponse{Index: command.Index, Term: command.Term, Command: command.Command}
	}

	tc.lastApplied[index] = response.Snapshot.LastIncludedIndex
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
		if response.IsSnapshot {
			tc.checkSnapshot(index, response)
		} else {
			tc.checkLogs(index, response)
		}
	}
}

func (tc *TestCluster) crashServer(serverID string) {
	index, _ := strconv.Atoi(serverID)
	tc.DisconnectServerTwoWay(serverID)
	tc.servers[index].Stop()
}

func (tc *TestCluster) restartServer(serverID string) {
	tc.mu.Lock()
	defer tc.mu.Unlock()

	index, _ := strconv.Atoi(serverID)
	address := tc.peers[index][index].Address()

	tc.replicateCh[index] = make(chan CommandResponse)
	tc.stateMachines[index] = NewStateMachineMock()

	newServer, err := NewProtobufServer(serverID, tc.peers[index], tc.logs[index], tc.stores[index], tc.snapshotStores[index],
		tc.stateMachines[index], address, tc.replicateCh[index], WithSnapshotting(tc.snapshotting), WithMaxLogEntriesPerSnapshot(tc.snapshotSize))
	if err != nil {
		tc.t.Fatalf("error restarting cluster server: %s", err.Error())
	}

	if tc.snapshotting {
		snapshot, _ := tc.snapshotStores[index].LastSnapshot()
		tc.lastApplied[index] = snapshot.LastIncludedIndex
	} else {
		tc.lastApplied[index] = 0
	}

	tc.servers[index] = newServer
	tc.commandResponses[index] = make(map[uint64]CommandResponse)

	tc.wg.Add(1)
	go tc.applyLoop(index)

	readyCh := make(chan interface{})
	defer close(readyCh)
	if err := newServer.Start(readyCh); err != nil {
		tc.t.Fatalf("error restarting cluster server: %s", err.Error())
	}

	for i := 0; i < len(tc.servers); i++ {
		if err := tc.peers[i][index].Connect(); err != nil {
			tc.t.Fatalf("error reconnecting peer: %s", err.Error())
		}
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

	for i := 0; i < len(tc.servers); i++ {
		if _, ok := partitionSet[i]; ok {
			for j := 0; j < len(tc.servers); j++ {
				if _, ok := partitionSet[j]; ok {
					continue
				}
				if err := tc.peers[i][j].Disconnect(); err != nil {
					tc.t.Fatalf("error disconnecting peer: %s", err.Error())
				}
				if err := tc.peers[j][i].Disconnect(); err != nil {
					tc.t.Fatalf("error disconnecting peer: %s", err.Error())
				}
			}

			if err := tc.peers[i][i].Disconnect(); err != nil {
				tc.t.Fatalf("error disconnecting peer: %s", err.Error())
			}
		}
	}
}

func (tc *TestCluster) reconnectAllServers() {
	tc.mu.Lock()
	defer tc.mu.Unlock()

	for i := 0; i < len(tc.servers); i++ {
		for j := 0; j < len(tc.servers); j++ {
			if tc.peers[i][j].Connected() {
				continue
			}
			if err := tc.peers[i][j].Connect(); err != nil {
				tc.t.Fatalf("error reconnecting peer: %s", err.Error())
			}
		}
	}
}

func (tc *TestCluster) DisconnectServerTwoWay(serverID string) {
	tc.mu.Lock()
	defer tc.mu.Unlock()

	server, _ := strconv.Atoi(serverID)
	if err := tc.peers[server][server].Disconnect(); err != nil {
		tc.t.Fatalf("error disconnecting peer: %s", err.Error())
	}
	for i := 0; i < len(tc.peers); i++ {
		if i == server {
			continue
		}
		if err := tc.peers[i][server].Disconnect(); err != nil {
			tc.t.Fatalf("error disconnecting peer: %s", err.Error())
		}
		if err := tc.peers[server][i].Disconnect(); err != nil {
			tc.t.Fatalf("error disconnecting peer: %s", err.Error())
		}
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
// still elect a leader when a single server is Disconnected.
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
	cluster.DisconnectServerTwoWay(leader)

	// See if the cluster can still elect a new leader.
	cluster.checkLeaders(false)
}

// TestFailElectLeaderDisconnect checks whether a leader is
// elected when a majority of the servers are Disconnected.
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
	cluster.DisconnectServerTwoWay(disconnectServer1)
	cluster.DisconnectServerTwoWay(disconnectServer2)

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
	commands := cluster.makeCommands(200)
	for _, command := range commands {
		cluster.submit(command, false, false, 5)
	}
}

// TestSubmitDisconnect checks that a cluster can still
// commit commands after a single server is Disconnected.
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
	cluster.DisconnectServerTwoWay(leader)
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
	cluster.DisconnectServerTwoWay(disconnectServer1)
	cluster.DisconnectServerTwoWay(disconnectServer2)
	cluster.DisconnectServerTwoWay(disconnectServer3)

	// Try to submit some commands. This should be unsuccessful
	// since only a minority of the cluster can communicate.
	commands := cluster.makeCommands(20)
	for _, command := range commands {
		cluster.submit(command, false, true, 1)
	}
}

// TestUnreliableNetwork tests whether a cluster can still make
// progress submitting multiple commands when multiple servers
// become disconnected from the rest of the clutster.
func TestUnreliableNetwork(t *testing.T) {
	defer leaktest.CheckTimeout(t, 1*time.Second)

	cluster, err := newCluster(t, 5)
	if err != nil {
		t.Fatalf("error creating new cluster: %s", err.Error())
	}

	done := int32(0)
	wg := sync.WaitGroup{}
	unreliableNetRoutine := func() {
		defer wg.Done()
		for atomic.LoadInt32(&done) == 0 {
			randomTime := util.RandomTimeout(700*time.Millisecond, 900*time.Millisecond)
			time.Sleep(randomTime * time.Millisecond)
			disconnect1 := util.RandomInt(0, 5)
			disconnect2 := (disconnect1 + 1) % 5
			id1 := fmt.Sprint(disconnect1)
			id2 := fmt.Sprint(disconnect2)
			cluster.DisconnectServerTwoWay(id1)
			cluster.DisconnectServerTwoWay(id2)
			randomTime = util.RandomTimeout(700*time.Millisecond, 900*time.Millisecond)
			time.Sleep(randomTime * time.Millisecond)
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
	commands := cluster.makeCommands(300)
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
	cluster.reconnectAllServers()
}

// TestMultiPartition checks whether a cluster can still make
// progress submitting multiple commands in the prescence of
// multiple and changing partitions.
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
			randomTime := util.RandomTimeout(700*time.Millisecond, 900*time.Millisecond)
			time.Sleep(randomTime * time.Millisecond)
			cluster.createPartition()
			randomTime = util.RandomTimeout(700*time.Millisecond, 900*time.Millisecond)
			time.Sleep(randomTime * time.Millisecond)
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
	commands := cluster.makeCommands(200)
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
	cluster.checkLeaders(false)
	for i := 150; i < len(commands); i++ {
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
			randomTime := util.RandomTimeout(700*time.Millisecond, 900*time.Millisecond)
			time.Sleep(randomTime * time.Millisecond)
			crash1 := util.RandomInt(0, 5)
			crash2 := (crash1 + 1) % 5
			id1 := fmt.Sprint(crash1)
			id2 := fmt.Sprint(crash2)
			cluster.crashServer(id1)
			cluster.crashServer(id2)
			randomTime = util.RandomTimeout(700*time.Millisecond, 900*time.Millisecond)
			time.Sleep(randomTime * time.Millisecond)
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
// progress committing commands after all the servers
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
