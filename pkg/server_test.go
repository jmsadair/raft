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
	"github.com/jmsadair/raft/internal/util"
)

const (
	errCreateClusterServer       = "failed to create cluster server: server = %d, err = %s"
	errStoppingClusterServer     = "failed to stop cluster server: server = %d, err = %s"
	errStartingClusterServer     = "failed to start cluster server: server = %d, err = %s"
	errMultipleLeaders           = "cluster has more than one leader: leaders = %v"
	errNoLeader                  = "cluster failed to elect a leader"
	errUnexpectedLeader          = "cluster elected leader without quorum: leaders = %v"
	errUnexpectedApply           = "cluster applied a command without quorum: command = %s"
	errFailApply                 = "cluster failed to apply command: command = %s"
	errOutOfOrder                = "cluster applied commands out of order: server = %d, expectedIndex = %d, actualIndex = %d"
	errDifferentCommandSameIndex = "cluster applied different commands at the same index: index = %d, command1 = %s, command2 = %s"
	errReconnectingPeer          = "error reconnecting peer: peer = %d, connectingTo = %d, err = %s"
	errDisconnectingPeer         = "error disconnecting peer: peer = %d, disconnectingFrom = %d, err = %s"
)

func makeCommands(numCommands int) []Command {
	commands := make([]Command, numCommands)
	for i := 1; i <= numCommands; i++ {
		commands[i-1] = Command{Bytes: []byte(fmt.Sprintf("command %d", i))}
	}

	return commands
}

func makeProtobufPeers(numServers int) [][]*ProtobufPeer {
	port := 8080
	clusterPeers := make([][]*ProtobufPeer, numServers)
	for i := 0; i < numServers; i++ {
		clusterPeers[i] = make([]*ProtobufPeer, numServers)
		for j := 0; j < numServers; j++ {
			ip := fmt.Sprintf("127.0.0.%d", j)
			peerID := fmt.Sprint(j)
			clusterPeers[i][j] = NewProtobufPeer(peerID, &net.TCPAddr{IP: net.ParseIP(ip), Port: port})
		}
	}

	return clusterPeers
}

type TestCluster struct {
	// The testing instance associated with the cluster.
	t *testing.T

	// The peers associated with each server, where
	// peers[i] corresponds to the peers for servers[i].
	peers [][]*ProtobufPeer

	// The servers making up the cluster.
	servers []*ProtobufServer

	// The log associated with each server, where
	// logs[i] corresponds to the log for servers[i].
	logs []Log

	// The snapshot store associated with each server,
	// where snapshotStores[i] corresponds to the snapshot
	// store for servers[i].
	snapshotStores []SnapshotStorage

	// The state machine associated with each server,
	// where stateMachines[i] corresponds to the state
	// machine for servers[i].
	stateMachines []StateMachine

	// The storage associated with each server, where
	// storage[i] corresponds to the storage for servers[i].
	stores []Storage

	// The response channel associated with each server, where
	// responseCh[i] corresponds to the response channel for
	// servers[i]
	responseCh []chan CommandResponse

	// A channel to signal the shutdown of the cluster.
	shutdownCh chan interface{}

	// The command responses associated with each server, where
	// commandResponses[i] corresponds to the responses for
	// servers[i]. The map maps indices to the associated command
	// response.
	commandResponses []map[uint64]CommandResponse

	// Errors encountered during the execution of a background go
	// routine. Provides a means of shuttling the error from the background
	// go routine to the main, testing go routine. Note that serverErrors[i] corresponds
	// to any errors associated with servers[i].
	serverErrors []string

	// The last applied index for each server, where lastApplied[i]
	// corresponds to last applied index for servers[i].
	lastApplied []uint64

	// Indicates whether snapshotting is enabled.
	// Set by enviroment variable.
	snapshotting bool

	// The maximum number of log entries per snapshot.
	// Set by environment variable.
	snapshotSize int

	mu sync.Mutex

	wg sync.WaitGroup
}

func newCluster(t *testing.T, numServers int, snapshotting bool, snapshotSize int) *TestCluster {
	servers := make([]*ProtobufServer, numServers)
	snapshotStores := make([]SnapshotStorage, numServers)
	stateMachines := make([]StateMachine, numServers)
	logs := make([]Log, numServers)
	stores := make([]Storage, numServers)
	replicateCh := make([]chan CommandResponse, numServers)
	responses := make([]map[uint64]CommandResponse, numServers)
	peers := makeProtobufPeers(numServers)
	serverErrors := make([]string, numServers)
	lastApplied := make([]uint64, numServers)

	// The paths for all of the persistent storage associated with the cluster.
	tmpDir := t.TempDir()
	snapshotFileFmt := tmpDir + "/raft-snapshots-%d"
	logFileFmt := tmpDir + "/raft-log-%d"
	storageFileFmt := tmpDir + "/raft-storage-%d"

	for i := 0; i < numServers; i++ {
		replicateCh[i] = make(chan CommandResponse)
		snapshotStores[i] = NewPersistentSnapshotStorage(fmt.Sprintf(snapshotFileFmt, i), new(ProtoSnapshotEncoder), new(ProtoSnapshotDecoder))
		stateMachines[i] = NewStateMachineMock()
		logs[i] = NewPersistentLog(fmt.Sprintf(logFileFmt, i), new(ProtoLogEncoder), new(ProtoLogDecoder))
		stores[i] = NewPersistentStorage(fmt.Sprintf(storageFileFmt, i), new(ProtoStorageEncoder), new(ProtoStorageDecoder))
		responses[i] = make(map[uint64]CommandResponse)

		id := peers[0][i].ID()
		address := peers[0][i].Address()

		server, err := NewProtobufServer(id, peers[i], logs[i], stores[i], snapshotStores[i], stateMachines[i], address, replicateCh[i],
			WithSnapshotting(snapshotting), WithMaxLogEntriesPerSnapshot(snapshotSize))
		if err != nil {
			t.Fatalf(errCreateClusterServer, i, err.Error())
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
		responseCh:       replicateCh,
		commandResponses: responses,
		serverErrors:     serverErrors,
		lastApplied:      lastApplied,
		shutdownCh:       make(chan interface{}),
		snapshotting:     snapshotting,
		snapshotSize:     snapshotSize,
	}
}

func (tc *TestCluster) startCluster() {
	ready := make(chan interface{})
	for i, server := range tc.servers {
		if err := server.Start(ready); err != nil {
			tc.t.Fatalf(errStartingClusterServer, i, err.Error())
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

func (tc *TestCluster) submit(command Command, retry bool, expectFail bool, expectedApplied int) {
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

			// Submit a command to a server. It might be a leader.
			index, term, err := server.SubmitCommand(command)
			if err != nil {
				continue
			}

			// See if the command is applied.
			for k := 0; k < 10; k++ {
				time.Sleep(25 * time.Millisecond)
				successful := tc.checkApplied(index, expectedApplied)
				if successful {
					if expectFail {
						tc.t.Fatalf(errUnexpectedApply, string(command.Bytes))
					}
					return
				}

				// If the server's term changed then the command
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
		tc.t.Fatalf(errFailApply, string(command.Bytes))
	}
}

func (tc *TestCluster) checkLeaders(expectNoLeader bool) string {
	// Any leaders detected.
	leaders := make([]string, 0)

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

			// If the server is a leader and it is connected, then it is
			// a legitimate leader. Leaders that are disconnected or
			// partitioned are ignored. It is assumed that disconnected
			// servers are either:
			// 1. Completely disconnected from all all other servers - it
			//    cannot communicate with any other servers and no other
			//    servers can communicate with it.
			// 2. In a minority partition - it may only communicate with
			//    a minority of the cluster. Members of the majority partition
			//    cannnot communicate with it.
			tc.mu.Lock()
			if status.State == Leader && tc.peers[i][i].Connected() {
				leaders = append(leaders, status.ID)
			}
			tc.mu.Unlock()
		}

		if len(leaders) > 1 {
			tc.t.Fatalf(errMultipleLeaders, leaders)
		}

		if len(leaders) == 1 {
			break
		}

		// If not leaders were found, sleep for a sufficient amount of time to allow
		// an election to take place.
		time.Sleep(electionTimeout)
	}

	if len(leaders) == 0 && !expectNoLeader {
		tc.t.Fatal(errNoLeader)
	}

	if len(leaders) != 0 && expectNoLeader {
		tc.t.Fatalf(errUnexpectedLeader, leaders)
	}

	if expectNoLeader {
		return ""
	}

	return leaders[0]
}

func (tc *TestCluster) checkLogs(index int, response CommandResponse) {
	tc.mu.Lock()
	defer tc.mu.Unlock()

	// The last applied index should be monotonically increasing.
	expectedIndex := tc.lastApplied[index] + 1
	if response.Index != expectedIndex {
		tc.serverErrors[index] = fmt.Sprintf(errOutOfOrder, index, expectedIndex, response.Index)
		return
	}

	tc.commandResponses[index][response.Index] = response
	tc.lastApplied[index]++
}

func (tc *TestCluster) checkSnapshot(index int, response CommandResponse) {
	tc.mu.Lock()
	defer tc.mu.Unlock()

	// A server should never install a snapshot with a last included index
	// less than its last applied index. This would mean that the server is taking
	// the state machine back in time, which is not allowed.
	if response.Snapshot.LastIncludedIndex <= tc.lastApplied[index] {
		tc.serverErrors[index] = fmt.Sprintf(errOutOfOrder, index, tc.lastApplied[index]+1, response.Snapshot.LastIncludedIndex)
	}

	// Decode the snapshot data into commands.
	var appliedCommands []AppliedCommand
	data := bytes.NewBuffer(response.Snapshot.Data)
	dec := gob.NewDecoder(data)
	if err := dec.Decode(&appliedCommands); err != nil {
		tc.serverErrors[index] = fmt.Sprintf("failed to decode commands: %s", err)
		return
	}

	// Update this server's responses with the commands included in the snapshot.
	for _, command := range appliedCommands {
		tc.commandResponses[index][command.Index] = CommandResponse{Index: command.Index, Term: command.Term, Command: command.Command}
	}

	tc.lastApplied[index] = response.Snapshot.LastIncludedIndex
}

func (tc *TestCluster) checkApplied(index uint64, expectedApplied int) bool {
	tc.mu.Lock()
	defer tc.mu.Unlock()

	// The expected command response from all of the servers.
	// All servers should have the same command response at a
	// given index.
	var expectedCommandResponse CommandResponse

	// The number of servers that have applied the command at the provided index.
	hasApplied := 0

	for i := 0; i < len(tc.servers); i++ {
		if tc.serverErrors[i] != "" {
			tc.t.Fatalf(tc.serverErrors[i])
		}

		if commandResponse, ok := tc.commandResponses[i][index]; ok {
			// Ensure the commands match.
			if hasApplied != 0 && string(commandResponse.Command) != string(expectedCommandResponse.Command) {
				tc.t.Fatalf(errDifferentCommandSameIndex, index, string(expectedCommandResponse.Command), string(commandResponse.Command))
			}
			expectedCommandResponse = commandResponse
			hasApplied++
		}
	}

	return hasApplied >= expectedApplied
}

func (tc *TestCluster) applyLoop(index int) {
	defer tc.wg.Done()

	for response := range tc.responseCh[index] {
		if response.IsSnapshot {
			tc.checkSnapshot(index, response)
		} else {
			tc.checkLogs(index, response)
		}
	}
}

func (tc *TestCluster) crashServer(serverID string) {
	index, _ := strconv.Atoi(serverID)
	tc.disconnectServer(serverID)
	tc.servers[index].Stop()
}

func (tc *TestCluster) restartServer(serverID string) {
	tc.mu.Lock()
	defer tc.mu.Unlock()

	index, _ := strconv.Atoi(serverID)
	address := tc.peers[index][index].Address()

	tc.responseCh[index] = make(chan CommandResponse)
	tc.stateMachines[index] = NewStateMachineMock()

	newServer, err := NewProtobufServer(serverID, tc.peers[index], tc.logs[index], tc.stores[index], tc.snapshotStores[index],
		tc.stateMachines[index], address, tc.responseCh[index], WithSnapshotting(tc.snapshotting), WithMaxLogEntriesPerSnapshot(tc.snapshotSize))
	if err != nil {
		tc.t.Fatalf(errStartingClusterServer, index, err.Error())
	}

	// If snapshotting is enabled, the server should restore its
	// state machine from its last snapshot and update its last
	// applied index to the last included index of the snapshot.
	// Otherwise, the server's last applied index should alawys
	// start at 0.
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
		tc.t.Fatalf(errStartingClusterServer, index, err.Error())
	}

	for i := 0; i < len(tc.servers); i++ {
		if err := tc.peers[i][index].Connect(); err != nil {
			tc.t.Fatalf(errReconnectingPeer, i, index, err.Error())
		}
	}
}

func (tc *TestCluster) createPartition() {
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
	// that are not, but maintain connections bewteen the servers
	// that are in the partition set.
	for i := 0; i < len(tc.servers); i++ {
		if _, ok := partitionSet[i]; ok {
			for j := 0; j < len(tc.servers); j++ {
				if _, ok := partitionSet[j]; ok {
					continue
				}
				if err := tc.peers[i][j].Disconnect(); err != nil {
					tc.t.Fatalf(errDisconnectingPeer, i, j, err.Error())
				}
				if err := tc.peers[j][i].Disconnect(); err != nil {
					tc.t.Fatalf(errDisconnectingPeer, j, i, err.Error())
				}
			}

			if err := tc.peers[i][i].Disconnect(); err != nil {
				tc.t.Fatalf(errDisconnectingPeer, i, i, err.Error())
			}
		}
	}
}

func (tc *TestCluster) reconnectServer(serverID string) {
	tc.mu.Lock()
	defer tc.mu.Unlock()

	index, _ := strconv.Atoi(serverID)

	// Reconnect this server to itself. Note that this has no effect
	// on the operation of the cluster. The only purpose of this is to
	// indicate that this server is connected and should operate as expected.
	if err := tc.peers[index][index].Connect(); err != nil {
		tc.t.Fatalf(errReconnectingPeer, index, index, err.Error())
	}

	for i := 0; i < len(tc.servers); i++ {
		if index == i {
			continue
		}
		if err := tc.peers[i][index].Connect(); err != nil {
			tc.t.Fatalf(errReconnectingPeer, i, index, err.Error())
		}
		if err := tc.peers[index][i].Connect(); err != nil {
			tc.t.Fatalf(errReconnectingPeer, index, i, err.Error())
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
				tc.t.Fatalf(errReconnectingPeer, i, j, err.Error())
			}
		}
	}
}

func (tc *TestCluster) disconnectServer(serverID string) {
	tc.mu.Lock()
	defer tc.mu.Unlock()

	// Disconnect this server from itself. Note that this has no effect
	// on the operation of the cluster. The only purpose of this is to
	// indicate that this server is disconnected and will not operate
	// as expected.
	server, _ := strconv.Atoi(serverID)
	if err := tc.peers[server][server].Disconnect(); err != nil {
		tc.t.Fatalf(errDisconnectingPeer, server, server, err.Error())
	}

	for i := 0; i < len(tc.peers); i++ {
		if i == server {
			continue
		}
		if err := tc.peers[i][server].Disconnect(); err != nil {
			tc.t.Fatalf(errDisconnectingPeer, i, server, err.Error())
		}
		if err := tc.peers[server][i].Disconnect(); err != nil {
			tc.t.Fatalf(errDisconnectingPeer, server, i, err.Error())
		}
	}
}

var snapshotting bool
var snapshotSize int

// TestMain sets up the Raft tests.
func TestMain(m *testing.M) {
	// Is snapshotting enabled for the tests?
	snapshotting = os.Getenv("SNAPSHOTS") == "true"

	// The number of log entries per snapshot.
	snapshotSize, _ = strconv.Atoi(os.Getenv("SNAPSHOT_SIZE"))

	exitCode := m.Run()

	os.Exit(exitCode)
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
	serverID, _ := strconv.Atoi(disconnectServer1)
	disconnectServer2 := fmt.Sprint((serverID + 1) % 3)
	cluster.disconnectServer(disconnectServer1)
	cluster.disconnectServer(disconnectServer2)

	// Check if the server can elect itself as the leader.
	// This should not be successful.
	cluster.checkLeaders(true)
}

// TestSingleSubmit checks whether the cluster can successfully
// commit a single command when there are no failures.
func TestBasicSubmit(t *testing.T) {
	defer leaktest.CheckTimeout(t, 1*time.Second)

	cluster := newCluster(t, 3, snapshotting, snapshotSize)

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

	cluster := newCluster(t, 5, snapshotting, snapshotSize)

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

	cluster := newCluster(t, 5, snapshotting, snapshotSize)

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

	cluster := newCluster(t, 3, snapshotting, snapshotSize)

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

	cluster := newCluster(t, 5, snapshotting, snapshotSize)

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

	cluster := newCluster(t, 5, snapshotting, snapshotSize)

	cluster.startCluster()
	defer cluster.stopCluster()

	// Disconnect the leader and two other servers, leaving
	// only a minority of the server able to communicate.
	leader := cluster.checkLeaders(false)
	serverID, _ := strconv.Atoi(leader)
	disconnectServer1 := fmt.Sprint((serverID + 1) % 5)
	disconnectServer2 := fmt.Sprint((serverID + 2) % 5)
	disconnectServer3 := fmt.Sprint((serverID + 3) % 5)
	cluster.disconnectServer(disconnectServer1)
	cluster.disconnectServer(disconnectServer2)
	cluster.disconnectServer(disconnectServer3)

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
			id1 := fmt.Sprint(disconnect1)
			id2 := fmt.Sprint(disconnect2)
			cluster.disconnectServer(id1)
			cluster.disconnectServer(id2)

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

	cluster := newCluster(t, 5, snapshotting, snapshotSize)

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

	// See if we can commit commands in the face of recurring partitions.
	commands := makeCommands(300)
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

	cluster := newCluster(t, 5, snapshotting, snapshotSize)

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
			id1 := fmt.Sprint(crash1)
			id2 := fmt.Sprint(crash2)
			cluster.crashServer(id1)
			cluster.crashServer(id2)

			// Allow the cluster to make progress while the servers are offline.
			randomTime = util.RandomTimeout(700*time.Millisecond, 900*time.Millisecond)
			time.Sleep(randomTime * time.Millisecond)

			// Bring the servers back online.
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
				id := fmt.Sprint(crash)
				cluster.crashServer(id)
				randomTime = util.RandomTimeout(700*time.Millisecond, 900*time.Millisecond)
				time.Sleep(randomTime * time.Millisecond)
				cluster.restartServer(id)
			// Disconnect a single server.
			case 1:
				disconnect := util.RandomInt(0, 5)
				id := fmt.Sprint(disconnect)
				cluster.disconnectServer(id)
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

	cluster := newCluster(t, 5, snapshotting, snapshotSize)

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
