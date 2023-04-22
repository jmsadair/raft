package raft

import (
	"fmt"
	"net"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/fortytw2/leaktest"
	"github.com/jmsadair/raft/internal/errors"
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
	connected        []bool
	snapshotStores   []SnapshotStorage
	stateMachines    []StateMachine
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
	replicateCh := make([]chan CommandResponse, numServers)
	responses := make([]map[uint64]CommandResponse, numServers)
	expected := make([]Command, 0)
	connected := make([]bool, numServers)
	peers := makePeers(numServers)
	serverErrors := make([]string, numServers)
	lastApplied := make([]uint64, numServers)

	for i := 0; i < numServers; i++ {
		replicateCh[i] = make(chan CommandResponse)
		snapshotStores[i] = NewSnapshotStorageMock()
		stateMachines[i] = NewStateMachineMock()
		connected[i] = true
		responses[i] = make(map[uint64]CommandResponse)

		log := NewLogMock(0, 0)
		storage := NewStorageMock()
		id := peers[0][i].Id()
		address := peers[0][i].Address()

		server, err := NewServer(id, peers[i], log, storage, snapshotStores[i], stateMachines[i], address, replicateCh[i])
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
		if err := server.Stop(); err != nil {
			tc.t.Fatalf("error stopping cluster server: %s", err.Error())
		}
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
		for j, server := range tc.servers {
			status := server.Status()
			if status.State == Leader && tc.connected[j] {
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
			if status.State == Leader && tc.connected[j] {
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

func (tc *TestCluster) disconnectServerOneWay(serverID string) {
	server, _ := strconv.Atoi(serverID)
	tc.connected[server] = false
	for i := 0; i < len(tc.peers); i++ {
		if i == server {
			continue
		}
		tc.peers[i][server].disconnect()
	}
}

func (tc *TestCluster) disconnectServerTwoWay(serverID string) {
	server, _ := strconv.Atoi(serverID)
	tc.connected[server] = false
	for i := 0; i < len(tc.peers); i++ {
		if i == server {
			for j := 0; j < len(tc.peers[i]); j++ {
				tc.peers[i][j].disconnect()
			}
			continue
		}
		tc.peers[i][server].disconnect()
	}
}

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

func TestElectLeaderDisconnect(t *testing.T) {
	defer leaktest.CheckTimeout(t, 1*time.Second)

	cluster, err := newCluster(t, 3)
	if err != nil {
		t.Fatalf("error creating new cluster: %s", err.Error())
	}

	cluster.startCluster()
	defer cluster.stopCluster()

	leader := cluster.checkLeaders(false)
	cluster.disconnectServerTwoWay(leader)
	cluster.checkLeaders(false)
}

func TestFailElectLeaderDisconnect(t *testing.T) {
	defer leaktest.CheckTimeout(t, 1*time.Second)

	cluster, err := newCluster(t, 3)
	if err != nil {
		t.Fatalf("error creating new cluster: %s", err.Error())
	}

	cluster.startCluster()
	defer cluster.stopCluster()

	disconnectServer1 := cluster.checkLeaders(false)
	serverID, _ := strconv.Atoi(disconnectServer1)
	disconnectServer2 := fmt.Sprint((serverID + 1) % 3)
	cluster.disconnectServerTwoWay(disconnectServer1)
	cluster.disconnectServerTwoWay(disconnectServer2)
	cluster.checkLeaders(true)
}

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

func TestSubmitDisconnect(t *testing.T) {
	defer leaktest.CheckTimeout(t, 1*time.Second)

	cluster, err := newCluster(t, 3)
	if err != nil {
		t.Fatalf("error creating new cluster: %s", err.Error())
	}

	cluster.startCluster()
	defer cluster.stopCluster()

	leader := cluster.checkLeaders(false)
	cluster.disconnectServerTwoWay(leader)
	commands := cluster.makeCommands(20)
	for _, command := range commands {
		cluster.submit(command, true, false, 2)
	}
}

func TestSubmitDisconnectFail(t *testing.T) {
	defer leaktest.CheckTimeout(t, 1*time.Second)

	cluster, err := newCluster(t, 5)
	if err != nil {
		t.Fatalf("error creating new cluster: %s", err.Error())
	}

	cluster.startCluster()
	defer cluster.stopCluster()

	leader := cluster.checkLeaders(false)
	serverID, _ := strconv.Atoi(leader)
	disconnectServer1 := fmt.Sprint((serverID + 1) % 5)
	disconnectServer2 := fmt.Sprint((serverID + 2) % 5)
	disconnectServer3 := fmt.Sprint((serverID + 3) % 5)
	cluster.disconnectServerTwoWay(disconnectServer1)
	cluster.disconnectServerTwoWay(disconnectServer2)
	cluster.disconnectServerTwoWay(disconnectServer3)
	commands := cluster.makeCommands(20)
	for _, command := range commands {
		cluster.submit(command, false, true, 1)
	}
}
