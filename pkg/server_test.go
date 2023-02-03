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
	"github.com/stretchr/testify/require"
)

// Error strings used by TestCluster.
var (
	errMultipleLeader = "cluster has more than one leader"
	errNoLeader       = "cluster has no leader"
	errDisconnect     = "failed to disconnect from peer: %s"
	errConnect        = "failed to connect to peer: %s"
	errCreateCluster  = "failed to create cluster: %s"
	errStopCluster    = "failed to stop cluster: %s"
	errStartCluster   = "failed to start cluster: %s"
	errReplicate      = "failed to replicate command"
)

type TestCluster struct {
	peers              []*Peer
	servers            []*Server
	snapshotStores     []SnapshotStorage
	stateMachines      []StateMachine
	replicateCh        []chan CommandResponse
	shutdownCh         chan interface{}
	replicateResponses [][]CommandResponse
	mu                 []sync.Mutex
}

func newCluster(numServers int) (*TestCluster, error) {
	peers := make([]*Peer, numServers)
	servers := make([]*Server, numServers)
	snapshotStores := make([]SnapshotStorage, numServers)
	stateMachines := make([]StateMachine, numServers)
	replicateCh := make([]chan CommandResponse, numServers)
	responses := make([][]CommandResponse, numServers)

	ipPrefix := "127.0.0."
	port := 8080
	for i := 0; i < numServers; i++ {
		ip := ipPrefix + fmt.Sprint(i)
		peerId := fmt.Sprint(i)
		peers[i] = NewPeer(peerId, &net.TCPAddr{IP: net.ParseIP(ip), Port: port})
	}

	for i := 0; i < numServers; i++ {
		serverPeers := make([]*Peer, 0, numServers-1)
		for j := 0; j < numServers; j++ {
			if i != j {
				serverPeers = append(serverPeers, peers[j].Clone())
			}

			replicateCh[i] = make(chan CommandResponse)
			responses[i] = make([]CommandResponse, 0)
			snapshotStores[i] = NewSnapshotStorageMock()
			stateMachines[i] = NewStateMachineMock()

			log := NewLogMock()
			storage := NewStorageMock()

			server, err := NewServer(peers[i].id, serverPeers, log, storage, snapshotStores[i], stateMachines[i], peers[i].address, replicateCh[i])
			if err != nil {
				return nil, errors.WrapError(err, errCreateCluster, err.Error())
			}
			servers[i] = server
		}
	}

	return &TestCluster{
		peers:              peers,
		servers:            servers,
		snapshotStores:     snapshotStores,
		stateMachines:      stateMachines,
		replicateCh:        replicateCh,
		replicateResponses: responses,
		shutdownCh:         make(chan interface{}),
		mu:                 make([]sync.Mutex, numServers),
	}, nil
}

func (tc *TestCluster) startCluster() error {
	ready := make(chan interface{})
	for index, server := range tc.servers {
		if err := server.Start(ready); err != nil {
			return errors.WrapError(err, errStartCluster, err.Error())
		}
		go tc.processResponses(index)
	}
	close(ready)
	return nil
}

func (tc *TestCluster) stopCluster() error {
	for _, server := range tc.servers {
		if err := server.Stop(); err != nil {
			return errors.WrapError(err, errStopCluster, err.Error())
		}
	}
	close(tc.shutdownCh)
	return nil
}

func (tc *TestCluster) processResponses(id int) {
	for {
		select {
		case <-tc.shutdownCh:
			return
		case response := <-tc.replicateCh[id]:
			tc.mu[id].Lock()
			tc.replicateResponses[id] = append(tc.replicateResponses[id], response)
			tc.mu[id].Unlock()
		}
	}
}

func (tc *TestCluster) responses(id int) []CommandResponse {
	tc.mu[id].Lock()
	defer tc.mu[id].Unlock()
	responses := make([]CommandResponse, len(tc.replicateResponses[id]))
	copy(responses, tc.replicateResponses[id])
	return responses
}

func (tc *TestCluster) connectServerToPeer(id, peerId int) error {
	raft := tc.servers[id].raft
	for _, peer := range raft.peers {
		if peer.Id() == fmt.Sprint(peerId) {
			err := peer.connect()
			if err != nil {
				return errors.WrapError(err, errConnect, err.Error())
			}
			return nil
		}
	}
	return errors.WrapError(nil, errConnect, fmt.Sprintf("peer %d does not exist", peerId))
}

func (tc *TestCluster) disconnectServerFromPeer(id, peerId int) error {
	raft := tc.servers[id].raft
	for _, peer := range raft.peers {
		if peer.Id() == fmt.Sprint(peerId) {
			err := peer.disconnect()
			if err != nil {
				return errors.WrapError(err, errDisconnect, err.Error())
			}
			return nil
		}
	}
	return errors.WrapError(nil, errDisconnect, fmt.Sprintf("peer %d does not exist", peerId))
}

func (tc *TestCluster) connectServerToPeers(id int) error {
	raft := tc.servers[id].raft
	for _, peer := range raft.peers {
		err := peer.connect()
		if err != nil {
			return errors.WrapError(err, errConnect, err.Error())
		}
	}
	return nil
}

func (tc *TestCluster) disconnectServerFromPeers(id int) error {
	raft := tc.servers[id].raft
	for _, peer := range raft.peers {
		err := peer.disconnect()
		if err != nil {
			return errors.WrapError(err, errDisconnect, err.Error())
		}
	}
	return nil
}

func (tc *TestCluster) replicateWithRetry(command Command, numRetries int, expectedServersCommitted int) error {
	for i := 0; i < numRetries+1; i++ {
		leader, err := tc.hasOneLeader()
		if err != nil {
			return errors.WrapError(err, errReplicate)
		}

		index, _ := tc.servers[leader].Replicate(command)
		if index == 0 {
			continue
		}

		// Wait a bit for the command to be committed across all servers.
		time.Sleep(100 * time.Millisecond)

		if tc.numberCommitted(index) == expectedServersCommitted {
			return nil
		}
	}
	return errors.WrapError(nil, errReplicate)
}

func (tc *TestCluster) hasOneLeader() (int, error) {
	for i := 0; i < 10; i++ {
		leader := -1
		for _, server := range tc.servers {
			status := server.raft.Status()
			if status.State == Leader && leader != -1 {
				return -1, errors.WrapError(nil, errMultipleLeader)
			}
			if status.State == Leader {
				leader, _ = strconv.Atoi(status.Id)
			}
		}

		if leader != -1 {
			return leader, nil
		}

		// Wait for a bit to see if leader is elected.
		time.Sleep(time.Duration(100+50*i) * time.Millisecond)
	}

	return -1, errors.WrapError(nil, errNoLeader)
}

func (tc *TestCluster) numberCommitted(index uint64) int {
	committed := 0
	for _, server := range tc.servers {
		status := server.raft.Status()
		if status.CommitIndex >= index {
			committed++
		}
	}
	return committed
}

func (tc *TestCluster) snapshotStorage(id int) SnapshotStorage {
	return tc.snapshotStores[id]
}

func (tc *TestCluster) stateMachine(id int) StateMachine {
	return tc.stateMachines[id]
}

func makeCommands(numCommands int) []Command {
	commands := make([]Command, numCommands)
	for i := 0; i < numCommands; i++ {
		commands[i] = Command{Bytes: []byte(fmt.Sprintf("command%d", i))}
	}
	return commands
}

// TestBasicElection tests that a new cluster is able to successfully elect a leader.
func TestBasicElection(t *testing.T) {
	defer leaktest.CheckTimeout(t, 250*time.Millisecond)()

	numServers := 3
	cluster, err := newCluster(numServers)
	require.NoError(t, err)

	require.NoError(t, cluster.startCluster())

	_, err = cluster.hasOneLeader()
	require.NoError(t, err)

	require.NoError(t, cluster.stopCluster())
}

// TestLeaderFailElection tests that the cluster is able to elect a new leader after the
// original leader fails.
func TestLeaderFailElection(t *testing.T) {
	defer leaktest.CheckTimeout(t, 250*time.Millisecond)()

	numServers := 3
	cluster, err := newCluster(numServers)
	require.NoError(t, err)

	require.NoError(t, cluster.startCluster())

	leader, err := cluster.hasOneLeader()
	require.NoError(t, err)

	require.NoError(t, cluster.disconnectServerFromPeers(leader))

	_, err = cluster.hasOneLeader()
	require.NoError(t, err)

	require.NoError(t, cluster.stopCluster())
}

// TestReplicateSimple tests replicating a single command.
func TestReplicateSimple(t *testing.T) {
	defer leaktest.CheckTimeout(t, 250*time.Millisecond)()

	numServers := 3
	cluster, err := newCluster(numServers)
	require.NoError(t, err)

	require.NoError(t, cluster.startCluster())

	numCommands := 1
	commands := makeCommands(numCommands)
	require.NoError(t, cluster.replicateWithRetry(commands[0], 5, numServers))

	for id := 0; id < numServers; id++ {
		responses := cluster.responses(id)
		require.Len(t, responses, 1)
		response := responses[0]
		require.Equal(t, response.Command, commands[0].Bytes)
		require.Equal(t, int(response.Index), 1)
	}

	require.NoError(t, cluster.stopCluster())
}

// TestReplicateMultiple tests replicating multiple commands.
func TestReplicateMultiple(t *testing.T) {
	defer leaktest.CheckTimeout(t, 250*time.Millisecond)()

	numServers := 3
	cluster, err := newCluster(numServers)
	require.NoError(t, err)

	require.NoError(t, cluster.startCluster())

	numCommands := 10
	commands := makeCommands(numCommands)

	for _, command := range commands {
		require.NoError(t, cluster.replicateWithRetry(command, 5, numServers))
	}

	for id := 0; id < numServers; id++ {
		responses := cluster.responses(id)
		require.Len(t, responses, len(commands))
		for i, response := range responses {
			require.Equal(t, response.Command, commands[i].Bytes)
			require.Equal(t, int(response.Index), i+1)
		}
	}

	require.NoError(t, cluster.stopCluster())
}

// TestReplicationNoQuorum tests that replication is not successful if there is not a quorum.
func TestReplicateNoQuorum(t *testing.T) {
	defer leaktest.CheckTimeout(t, 250*time.Millisecond)()

	numServers := 5
	cluster, err := newCluster(numServers)
	require.NoError(t, err)

	require.NoError(t, cluster.startCluster())

	leader, err := cluster.hasOneLeader()
	require.NoError(t, err)

	require.NoError(t, cluster.disconnectServerFromPeer(leader, (leader+1)%numServers))
	require.NoError(t, cluster.disconnectServerFromPeer(leader, (leader+2)%numServers))
	require.NoError(t, cluster.disconnectServerFromPeer(leader, (leader+3)%numServers))

	command := Command{Bytes: []byte("test")}
	cluster.servers[leader].Replicate(command)

	for id := 0; id < numServers; id++ {
		responses := cluster.responses(id)
		require.Empty(t, responses)
	}

	require.NoError(t, cluster.stopCluster())
}

func TestTakeSnapshot(t *testing.T) {
	defer leaktest.CheckTimeout(t, 250*time.Millisecond)()
	numServers := 3
	cluster, err := newCluster(numServers)
	require.NoError(t, err)

	require.NoError(t, cluster.startCluster())

	numCommands := 100
	commands := makeCommands(numCommands)

	for _, command := range commands {
		require.NoError(t, cluster.replicateWithRetry(command, 5, numServers))
	}

	// Wait a bit for all the servers to finish taking snapshot.
	time.Sleep(2 * time.Second)

	require.NoError(t, cluster.stopCluster())

	for id := 0; id < numServers; id++ {
		snapshotStorage := cluster.snapshotStorage(id)
		snapshotList, err := snapshotStorage.ListSnapshots()
		require.NoError(t, err)
		require.Len(t, snapshotList, 1)
		last, err := snapshotStorage.LastSnapshot()
		require.NoError(t, err)
		fsm := cluster.stateMachine(id)
		fsmSnapshot, err := fsm.Snapshot()
		require.NoError(t, err)
		require.Equal(t, last.LastIncludedIndex, uint64(numCommands))
		require.Equal(t, last.Data, fsmSnapshot)
	}
}

func TestSendSnapshot(t *testing.T) {
	defer leaktest.CheckTimeout(t, 250*time.Millisecond)()
	numServers := 5
	cluster, err := newCluster(numServers)
	require.NoError(t, err)

	require.NoError(t, cluster.startCluster())

	leader, err := cluster.hasOneLeader()
	require.NoError(t, err)

	disconnected := (leader + 1) % numServers
	require.NoError(t, cluster.disconnectServerFromPeer(leader, disconnected))
	require.NoError(t, cluster.disconnectServerFromPeers(disconnected))

	numCommands := 110
	commands := makeCommands(numCommands)

	for _, command := range commands {
		require.NoError(t, cluster.replicateWithRetry(command, 5, numServers-1))
	}

	// Wait a bit for all the servers to finish taking snapshot.
	time.Sleep(2 * time.Second)

	require.NoError(t, cluster.connectServerToPeer(leader, disconnected))
	require.NoError(t, cluster.connectServerToPeers(disconnected))

	// Wait a bit for snapshot to send and log to be updated.
	time.Sleep(2 * time.Second)

	require.NoError(t, cluster.stopCluster())

	status := cluster.servers[disconnected].raft.Status()
	require.Equal(t, status.CommitIndex, uint64(numCommands))
	require.Equal(t, status.LastApplied, uint64(numCommands))
}
