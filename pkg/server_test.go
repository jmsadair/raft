package raft

import (
	"fmt"
	"net"
	"strconv"
	"sync"
	"testing"
	"time"

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
	replicateCh        []chan ReplicateResponse
	shutdownCh         chan interface{}
	replicateResponses [][]ReplicateResponse
	mu                 []sync.Mutex
}

func newCluster(numServers int) (*TestCluster, error) {
	peers := make([]*Peer, numServers)
	servers := make([]*Server, numServers)
	replicateCh := make([]chan ReplicateResponse, numServers)
	responses := make([][]ReplicateResponse, numServers)

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

			replicateCh[i] = make(chan ReplicateResponse)
			responses[i] = make([]ReplicateResponse, 0)
			log := NewLogMock()
			storage := NewStorageMock()
			fsm := NewStateMachineMock()

			server, err := NewServer(peers[i].id, serverPeers, log, storage, fsm, peers[i].address, replicateCh[i])
			if err != nil {
				return nil, errors.WrapError(err, errCreateCluster, err.Error())
			}
			servers[i] = server
		}
	}

	return &TestCluster{
		peers:              peers,
		servers:            servers,
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

func (tc *TestCluster) responses(id int) []ReplicateResponse {
	tc.mu[id].Lock()
	defer tc.mu[id].Unlock()
	responses := make([]ReplicateResponse, len(tc.replicateResponses[id]))
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

func (tc *TestCluster) replicateWithRetry(command []byte, numRetries int) error {
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

		if tc.numberCommitted(index) == len(tc.servers) {
			return nil
		}
	}
	return errors.WrapError(nil, errReplicate)
}

func (tc *TestCluster) hasOneLeader() (int, error) {
	for i := 0; i < 10; i++ {
		leader := -1
		for _, server := range tc.servers {
			id, _, _, state := server.raft.Status()
			if state == Leader && leader != -1 {
				return -1, errors.WrapError(nil, errMultipleLeader)
			}
			if state == Leader {
				leader, _ = strconv.Atoi(id)
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
		_, _, commitIndex, _ := server.raft.Status()
		if commitIndex >= index {
			committed++
		}
	}
	return committed
}

// TestBasicElection tests that a new cluster is able to successfully elect a leader.
func TestBasicElection(t *testing.T) {
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
	numServers := 3
	cluster, err := newCluster(numServers)
	require.NoError(t, err)

	require.NoError(t, cluster.startCluster())

	command := []byte("test")
	require.NoError(t, cluster.replicateWithRetry(command, 5))

	for id := 0; id < numServers; id++ {
		responses := cluster.responses(id)
		require.Len(t, responses, 1)
		response := responses[0]
		require.Equal(t, response.Command, command)
		require.Equal(t, int(response.Index), 1)
	}

	require.NoError(t, cluster.stopCluster())
}

// TestReplicateMultiple tests replicating multiple commands.
func TestReplicateMultiple(t *testing.T) {
	numServers := 3
	cluster, err := newCluster(numServers)
	require.NoError(t, err)

	require.NoError(t, cluster.startCluster())

	numCommands := 5
	commands := make([][]byte, numCommands)
	for i := 0; i < numCommands; i++ {
		commands[i] = []byte(fmt.Sprintf("command%d", i))
	}

	for _, command := range commands {
		require.NoError(t, cluster.replicateWithRetry(command, 5))
	}

	for id := 0; id < numServers; id++ {
		responses := cluster.responses(id)
		require.Len(t, responses, len(commands))
		for i, response := range responses {
			require.Equal(t, response.Command, commands[i])
			require.Equal(t, int(response.Index), i+1)
		}
	}

	require.NoError(t, cluster.stopCluster())
}

// TestReplicationNoQuorum tests that replication is not successful if there is not a quorum.
func TestReplicateNoQuorum(t *testing.T) {
	numServers := 5
	cluster, err := newCluster(numServers)
	require.NoError(t, err)

	require.NoError(t, cluster.startCluster())

	leader, err := cluster.hasOneLeader()
	require.NoError(t, err)

	require.NoError(t, cluster.disconnectServerFromPeer(leader, (leader+1)%numServers))
	require.NoError(t, cluster.disconnectServerFromPeer(leader, (leader+2)%numServers))
	require.NoError(t, cluster.disconnectServerFromPeer(leader, (leader+3)%numServers))

	command := []byte("test")
	cluster.servers[leader].Replicate(command)

	for id := 0; id < numServers; id++ {
		responses := cluster.responses(id)
		require.Empty(t, responses)
	}

	require.NoError(t, cluster.stopCluster())
}
