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
	"github.com/stretchr/testify/require"
)

// Error strings used by TestCluster.
var (
	errMultipleLeader = "cluster has more than one leader"
	errNoLeader       = "cluster has no leader"
	errHasLeader      = "cluster has leader: %s"
	errDisconnect     = "failed to disconnect from peer: %s"
	errConnect        = "failed to connect to peer: %s"
	errCreateCluster  = "failed to create cluster: %s"
	errStopCluster    = "failed to stop cluster: %s"
	errStartCluster   = "failed to start cluster: %s"
	errNoConsensus    = "cluster does not have consensus"
	errCommitIndex    = "expected commit index %d, got commit index %d"
	errLastApplied    = "expected last applied %d, got last applied %d"
	errSnapshots      = "expected %d snapshots, got %d snapshots"
)

func makeCommands(numCommands int) []Command {
	commands := make([]Command, numCommands)
	for i := 0; i < numCommands; i++ {
		commands[i] = Command{Bytes: []byte(fmt.Sprintf("command%d", i))}
	}
	return commands
}

type TestCluster struct {
	peers              []*Peer
	servers            []*Server
	connected          []bool
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
	connected := make([]bool, numServers)

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
			connected[i] = true

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
		connected:          connected,
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

func (tc *TestCluster) connectServerToPeers(id int) error {
	raft := tc.servers[id].raft

	// Ensure outgoing RPCs from server with provided ID.
	for _, peer := range raft.peers {
		if peer.isConnected() {
			continue
		}
		if err := peer.connect(); err != nil {
			return errors.WrapError(err, errConnect, err.Error())
		}
	}

	// Ensure incoming RPCs from server with provided ID.
	for i := 0; i < len(tc.servers); i++ {
		if i == id {
			continue
		}
		peer := tc.servers[i].raft.peers[fmt.Sprint(id)]
		if peer.isConnected() {
			continue
		}
		if err := peer.connect(); err != nil {
			return errors.WrapError(err, errConnect, err.Error())
		}
	}

	tc.connected[id] = true

	return nil
}

func (tc *TestCluster) disconnectServerFromPeers(id int) error {
	raft := tc.servers[id].raft

	// Ensure no outgoing RPCs from server with provided ID.
	for _, peer := range raft.peers {
		if !peer.isConnected() {
			continue
		}
		if err := peer.disconnect(); err != nil {
			return errors.WrapError(err, errDisconnect, err.Error())
		}
	}

	// Ensure no incoming RPCs from server with provided ID.
	for i := 0; i < len(tc.servers); i++ {
		if i == id {
			continue
		}
		peer := tc.servers[i].raft.peers[fmt.Sprint(id)]
		if !peer.isConnected() {
			continue
		}
		if err := peer.disconnect(); err != nil {
			return errors.WrapError(err, errDisconnect, err.Error())
		}
	}

	tc.connected[id] = false

	return nil
}

func (tc *TestCluster) checkSubmitCommand(command Command, expectedServersCommitted int) error {
	for i := 0; i < 10; i++ {
		leader, err := tc.checkOneLeader()
		if err != nil {
			return errors.WrapError(err, errMultipleLeader)
		}

		index, _ := tc.servers[leader].SubmitCommand(command)
		if index == 0 {
			continue
		}

		// Wait a bit for the command to be committed across all servers.
		time.Sleep(100 * time.Millisecond)

		// Check that command has been committed to expected number of servers.
		committed := 0
		for _, server := range tc.servers {
			status := server.raft.Status()
			if status.CommitIndex >= index {
				committed++
			}
		}

		if committed == expectedServersCommitted {
			return nil
		}
	}

	return errors.WrapError(nil, errNoConsensus)
}

func (tc *TestCluster) checkOneLeader() (int, error) {
	for i := 0; i < 10; i++ {
		leader := -1
		for id, server := range tc.servers {
			if !tc.connected[id] {
				continue
			}
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
		time.Sleep(150 * time.Millisecond)
	}

	return -1, errors.WrapError(nil, errNoLeader)
}

func (tc *TestCluster) checkNoLeader() error {
	leader := -1
	for i := 0; i < 10; i++ {
		for id, server := range tc.servers {
			if !tc.connected[id] {
				continue
			}
			status := server.raft.Status()
			if status.State == Leader {
				leader, _ = strconv.Atoi(status.Id)
				break
			}
		}

		if leader == -1 {
			return nil
		}

		// Wait for a bit to see if servers enter candidate state.
		time.Sleep(150 * time.Millisecond)
	}

	return errors.WrapError(nil, errHasLeader, fmt.Sprint(leader))
}

func (tc *TestCluster) checkCommitIndex(id int, expectedCommitIndex uint64) error {
	commitIndex := uint64(0)

	for i := 0; i < 10; i++ {
		commitIndex = tc.servers[id].raft.Status().CommitIndex
		if commitIndex == expectedCommitIndex {
			return nil
		}
		time.Sleep(100 * time.Millisecond)
	}

	return errors.WrapError(nil, errCommitIndex, expectedCommitIndex, commitIndex)
}

func (tc *TestCluster) checkLastApplied(id int, expectedLastApplied uint64) error {
	lastApplied := uint64(0)

	for i := 0; i < 10; i++ {
		lastApplied = tc.servers[id].raft.Status().CommitIndex
		if lastApplied == expectedLastApplied {
			return nil
		}
		time.Sleep(100 * time.Millisecond)
	}

	return errors.WrapError(nil, errLastApplied, expectedLastApplied, lastApplied)
}

func (tc *TestCluster) checkSnapshots(id, expectedNumSnapshots int) ([]Snapshot, error) {
	numSnapshots := 0

	for i := 0; i < 10; i++ {
		snapshotStore := tc.snapshotStores[id]
		snapshots, _ := snapshotStore.ListSnapshots()
		numSnapshots = len(snapshots)
		if numSnapshots == expectedNumSnapshots {
			return snapshots, nil
		}
		time.Sleep(100 * time.Millisecond)
	}

	return nil, errors.WrapError(nil, errSnapshots, expectedNumSnapshots, numSnapshots)
}

// TestBasicElection tests that a new cluster is able to successfully elect a leader.
func TestBasicElection(t *testing.T) {
	defer leaktest.CheckTimeout(t, 250*time.Millisecond)()

	numServers := 3
	cluster, err := newCluster(numServers)
	require.NoError(t, err)

	require.NoError(t, cluster.startCluster())

	// Cluster should have exactly one leader.
	_, err = cluster.checkOneLeader()
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

	// Cluster should have exactly one leader.
	leader1, err := cluster.checkOneLeader()
	require.NoError(t, err)

	// Disconnect leader from peers.
	require.NoError(t, cluster.disconnectServerFromPeers(leader1))

	// CLuster should elect new leader.
	leader2, err := cluster.checkOneLeader()
	require.NoError(t, err)
	require.NotEqual(t, leader1, leader2)

	require.NoError(t, cluster.stopCluster())
}

func TestElectionNoQuorum(t *testing.T) {
	defer leaktest.CheckTimeout(t, 250*time.Millisecond)()

	numServers := 5
	cluster, err := newCluster(numServers)
	require.NoError(t, err)

	require.NoError(t, cluster.startCluster())

	// Cluster should have exactly one leader.
	leader, err := cluster.checkOneLeader()
	require.NoError(t, err)

	// Disconnect majority of servers to prevent quorum.
	require.NoError(t, cluster.disconnectServerFromPeers(leader))
	require.NoError(t, cluster.disconnectServerFromPeers((leader+1)%numServers))
	require.NoError(t, cluster.disconnectServerFromPeers((leader+2)%numServers))

	// No leader should be present since there is not quorum.
	require.NoError(t, cluster.checkNoLeader())

	// Rejoin a disconnected server to allow quorum
	require.NoError(t, cluster.connectServerToPeers(leader))

	// Leader should be elected now.
	_, err = cluster.checkOneLeader()
	require.NoError(t, err)

	require.NoError(t, cluster.stopCluster())
}

// TestReplicateSingle tests replicating a single command.
func TestReplicateSingle(t *testing.T) {
	defer leaktest.CheckTimeout(t, 250*time.Millisecond)()

	numServers := 3
	cluster, err := newCluster(numServers)
	require.NoError(t, err)

	require.NoError(t, cluster.startCluster())

	numCommands := 1
	commands := makeCommands(numCommands)
	require.NoError(t, cluster.checkSubmitCommand(commands[0], numServers))

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
		require.NoError(t, cluster.checkSubmitCommand(command, numServers))
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

	leader, err := cluster.checkOneLeader()
	require.NoError(t, err)

	require.NoError(t, cluster.disconnectServerFromPeers((leader+1)%numServers))
	require.NoError(t, cluster.disconnectServerFromPeers((leader+2)%numServers))
	require.NoError(t, cluster.disconnectServerFromPeers((leader+3)%numServers))

	command := Command{Bytes: []byte("test")}
	cluster.servers[leader].SubmitCommand(command)

	for id := 0; id < numServers; id++ {
		responses := cluster.responses(id)
		require.Empty(t, responses)
	}

	require.NoError(t, cluster.stopCluster())
}

// TestSingleSnapshot checks that servers take a snapshot once the log
// has grown past a predefined size.
func TestSingleSnapshot(t *testing.T) {
	defer leaktest.CheckTimeout(t, 250*time.Millisecond)()

	numServers := 3
	cluster, err := newCluster(numServers)
	require.NoError(t, err)

	require.NoError(t, cluster.startCluster())

	// Submit enough commands to force a snapshot.
	numCommands := defaultMaxEntriesPerSnapshot
	commands := makeCommands(numCommands)
	for _, command := range commands {
		require.NoError(t, cluster.checkSubmitCommand(command, numServers))
	}

	// Ensure that each server has a single, correct snapshot.
	for id := 0; id < numServers; id++ {
		snapshots, err := cluster.checkSnapshots(id, 1)
		require.NoError(t, err)
		fsm := cluster.stateMachines[id]
		fsmSnapshot, err := fsm.Snapshot()
		require.NoError(t, err)
		require.Equal(t, snapshots[len(snapshots)-1].LastIncludedIndex, uint64(numCommands))
		require.Equal(t, snapshots[len(snapshots)-1].Data, fsmSnapshot)
	}

	require.NoError(t, cluster.stopCluster())
}

// TestSendSnapshot checks that the leader will send a snapshot to a peer
// that is lagging behind.
func TestSendSnapshot(t *testing.T) {
	defer leaktest.CheckTimeout(t, 250*time.Millisecond)()

	numServers := 5
	cluster, err := newCluster(numServers)
	require.NoError(t, err)

	require.NoError(t, cluster.startCluster())

	leader, err := cluster.checkOneLeader()
	require.NoError(t, err)

	// Disconnect a server so that it will lag behind the others and need a snapshot
	// installed.
	require.NoError(t, cluster.disconnectServerFromPeers((leader+1)%numServers))

	// Submit enough commands to a force a snapshot.
	numCommands := 150
	commands := makeCommands(numCommands)
	for _, command := range commands {
		require.NoError(t, cluster.checkSubmitCommand(command, numServers-1))
	}

	// Reconnect the server so that it can have its snapshot installed.
	require.NoError(t, cluster.connectServerToPeers((leader+1)%numServers))

	// Check that server has correct commit index and last applied index now.
	require.NoError(t, cluster.checkCommitIndex((leader+1)%numServers, uint64(numCommands)))
	require.NoError(t, cluster.checkLastApplied((leader+1)%numServers, uint64(numCommands)))

	// Check that all servers have same state machine snapshot.
	fsmSnapshot, _ := cluster.stateMachines[leader].Snapshot()
	for id := 0; id < numServers; id++ {
		if id == leader {
			continue
		}
		other, _ := cluster.stateMachines[id].Snapshot()
		require.Equal(t, fsmSnapshot, other)
	}

	require.NoError(t, cluster.stopCluster())
}

// TestMultipleSnapshot checks that a snapshot will properly be installed when
// there are multiple peers lagging.
func TestSendMultipleSnapshot(t *testing.T) {
	defer leaktest.CheckTimeout(t, 250*time.Millisecond)()

	numServers := 5
	cluster, err := newCluster(numServers)
	require.NoError(t, err)

	require.NoError(t, cluster.startCluster())

	leader, err := cluster.checkOneLeader()
	require.NoError(t, err)

	// Disconnect a server so that it will lag.
	require.NoError(t, cluster.disconnectServerFromPeers((leader+1)%numServers))

	// Submit enough commands to force a snapshot. Disconnect another server
	// after replicating some commands.
	numCommands := 150
	commands := makeCommands(numCommands)
	disconnectIndex := util.RandomInt(1, 99)
	for index, command := range commands {
		if index == disconnectIndex {
			require.NoError(t, cluster.disconnectServerFromPeers((leader+2)%numServers))
		}

		if index < disconnectIndex {
			require.NoError(t, cluster.checkSubmitCommand(command, numServers-1))
		} else {
			require.NoError(t, cluster.checkSubmitCommand(command, numServers-2))
		}
	}

	// Reconnect the servers so that they can have its snapshot installed.
	require.NoError(t, cluster.connectServerToPeers((leader+1)%numServers))
	require.NoError(t, cluster.connectServerToPeers((leader+2)%numServers))

	// Check that servers have correct commit index and last applied index now.
	require.NoError(t, cluster.checkCommitIndex((leader+1)%numServers, uint64(numCommands)))
	require.NoError(t, cluster.checkLastApplied((leader+1)%numServers, uint64(numCommands)))
	require.NoError(t, cluster.checkCommitIndex((leader+2)%numServers, uint64(numCommands)))
	require.NoError(t, cluster.checkLastApplied((leader+2)%numServers, uint64(numCommands)))

	// Check that all servers have same state machine snapshot.
	fsmSnapshot, _ := cluster.stateMachines[leader].Snapshot()
	for id := 0; id < numServers; id++ {
		if id == leader {
			continue
		}
		other, _ := cluster.stateMachines[id].Snapshot()
		require.Equal(t, fsmSnapshot, other)
	}

	require.NoError(t, cluster.stopCluster())
}

// TestSendSnapshotLeaderFailure ensures that a snapshot will be installed on a lagging
// peer even in the case of a leader failure.
func TestSendSnapshotLeaderFailure(t *testing.T) {
	defer leaktest.CheckTimeout(t, 250*time.Millisecond)()

	numServers := 5
	cluster, err := newCluster(numServers)
	require.NoError(t, err)

	require.NoError(t, cluster.startCluster())

	leader, err := cluster.checkOneLeader()
	require.NoError(t, err)

	// Disconnect a server so that it will lag behind the others and need a snapshot
	// installed.
	require.NoError(t, cluster.disconnectServerFromPeers((leader+1)%numServers))

	// Submit enough commands to force a snapshot
	numCommands := 150
	commands := makeCommands(numCommands)
	for _, command := range commands {
		require.NoError(t, cluster.checkSubmitCommand(command, numServers-1))
	}

	// Disconnect the leader.
	require.NoError(t, cluster.disconnectServerFromPeers(leader))

	// Reconnect the server so that it can have its snapshot installed.
	require.NoError(t, cluster.connectServerToPeers((leader+1)%numServers))

	// Check that server has correct commit index and last applied index now.
	require.NoError(t, cluster.checkCommitIndex((leader+1)%numServers, uint64(numCommands)))
	require.NoError(t, cluster.checkLastApplied((leader+1)%numServers, uint64(numCommands)))

	// Check that all servers have same state machine snapshot.
	fsmSnapshot, _ := cluster.stateMachines[leader].Snapshot()
	for id := 0; id < numServers; id++ {
		if id == leader {
			continue
		}
		other, _ := cluster.stateMachines[id].Snapshot()
		require.Equal(t, fsmSnapshot, other)
	}

	require.NoError(t, cluster.stopCluster())
}
