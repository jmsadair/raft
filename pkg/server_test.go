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
	"github.com/jmsadair/raft/internal/util"
)

func makeCommands(numCommands int) []Command {
	commands := make([]Command, numCommands)
	for i := 1; i <= numCommands; i++ {
		commands[i-1] = Command{Bytes: []byte(fmt.Sprintf("command %d", i))}
	}
	return commands
}

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
	t                  *testing.T
	peers              [][]*Peer
	servers            []*Server
	connected          []bool
	snapshotStores     []SnapshotStorage
	stateMachines      []StateMachine
	replicateCh        []chan CommandResponse
	shutdownCh         chan interface{}
	replicateResponses [][]CommandResponse
	applyIndex         uint64
	mu                 sync.Mutex
	wg                 sync.WaitGroup
}

func newCluster(t *testing.T, numServers int) (*TestCluster, error) {
	servers := make([]*Server, numServers)
	snapshotStores := make([]SnapshotStorage, numServers)
	stateMachines := make([]StateMachine, numServers)
	replicateCh := make([]chan CommandResponse, numServers)
	responses := make([][]CommandResponse, numServers)
	connected := make([]bool, numServers)
	peers := makePeers(numServers)

	for i := 0; i < numServers; i++ {
		replicateCh[i] = make(chan CommandResponse)
		responses[i] = make([]CommandResponse, 0)
		snapshotStores[i] = NewSnapshotStorageMock()
		stateMachines[i] = NewStateMachineMock()
		connected[i] = true

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
		t:                  t,
		peers:              peers,
		servers:            servers,
		connected:          connected,
		snapshotStores:     snapshotStores,
		stateMachines:      stateMachines,
		replicateCh:        replicateCh,
		replicateResponses: responses,
		shutdownCh:         make(chan interface{}),
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

func (tc *TestCluster) submit(command Command, retry bool) {
	for i := 0; i < 10; i++ {
		for _, server := range tc.servers {
			status := server.Status()
			if status.State == Leader {
				index, term, err := server.SubmitCommand(command)
				if err != nil {
					continue
				}
				for j := 0; j < 10; j++ {
					time.Sleep(100 * time.Millisecond)
					tc.mu.Lock()
					if tc.applyIndex >= index {
						tc.mu.Unlock()
						return
					}
					status = server.Status()
					if status.Term != term {
						tc.mu.Unlock()
						break
					}
					tc.mu.Unlock()
				}
			}
			time.Sleep(300 * time.Millisecond)
		}

		if !retry {
			break
		}
	}

	tc.t.Fatalf("cluster failed to apply command: command = %v", command)
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

	responses := tc.replicateResponses[index]
	expectedIndex := uint64(1)
	expectedCommand := fmt.Sprintf("command %d", expectedIndex)
	if len(tc.replicateResponses[index]) > 0 {
		expectedIndex = tc.replicateResponses[index][len(responses)-1].Index + 1
	}

	if response.Index != expectedIndex {
		tc.t.Fatalf("command applied out of order: expected index %d, got index %d", expectedIndex, response.Index)
	}
	if expectedCommand != string(response.Command) {
		tc.t.Fatalf("command does not match: expected command %s, got command %s", expectedCommand, string(response.Command))
	}

	tc.replicateResponses[index] = append(tc.replicateResponses[index], response)
	numApplied := 0

	// Check the last applied command of each server. If there
	// is a majority, update the apply index.
	for i := 0; i < len(tc.servers); i++ {
		if len(tc.replicateResponses[i]) == 0 {
			continue
		}
		if tc.applyIndex <= response.Index {
			numApplied += 1
		}
		if numApplied > len(tc.servers)/2 {
			tc.applyIndex = util.Max(response.Index, tc.applyIndex)
			return
		}
	}
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
	commands := makeCommands(1)
	cluster.submit(commands[0], false)
}
