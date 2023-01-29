package raft

import (
	"fmt"
	"net"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/jmsadair/raft/internal/errors"
	"github.com/stretchr/testify/assert"
)

// Error strings used by TestCluster.
var (
	errMultipleLeader = "cluster has more than one leader"
	errNoLeader       = "cluster does not have a leader"
	errDisconnect     = "failed to disconnect from peer: %s"
	errConnect        = "failed to connect to peer: %s"
	errCreateCluster  = "failed to create cluster: %s"
	errStopCluster    = "failed to stop cluster: %s"
	errStartCluster   = "failed to start cluster: %s"
	errReplicate      = "failed to replicate command: %s"
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

func (tc *TestCluster) replicate(command []byte) error {
	leader, err := tc.clusterLeader()
	if err != nil {
		return errors.WrapError(err, errReplicate, err.Error())
	}
	err = tc.servers[leader].Replicate(command)
	if err != nil {
		return errors.WrapError(err, errReplicate, err.Error())
	}
	return nil
}

func (tc *TestCluster) clusterLeader() (int, error) {
	leader := -1
	for _, server := range tc.servers {
		id, _, state := server.raft.Status()
		if state == Leader && leader != -1 {
			return -1, errors.WrapError(nil, errMultipleLeader)
		}
		if state == Leader {
			leader, _ = strconv.Atoi(id)
		}
	}
	if leader == -1 {
		return 0, errors.WrapError(nil, errNoLeader)
	}
	return leader, nil
}

// TestBasicElection tests that a new cluster is able to successfully elect a leader.
func TestBasicElection(t *testing.T) {
	numServers := 3
	cluster, err := newCluster(numServers)
	assert.NoError(t, err)

	assert.NoError(t, cluster.startCluster())

	time.Sleep(500 * time.Millisecond)

	_, err = cluster.clusterLeader()
	assert.NoError(t, err)

	assert.NoError(t, cluster.stopCluster())
}

// TestLeaderFailElection tests that the cluster is able to elect a new leader after the
// original leader fails.
func TestLeaderFailElection(t *testing.T) {
	numServers := 3
	cluster, err := newCluster(numServers)
	assert.NoError(t, err)

	assert.NoError(t, cluster.startCluster())

	time.Sleep(500 * time.Millisecond)

	leader, err := cluster.clusterLeader()
	assert.NoError(t, err)

	assert.NoError(t, cluster.disconnectServerFromPeers(leader))

	time.Sleep(500 * time.Millisecond)

	_, err = cluster.clusterLeader()
	assert.NoError(t, err)

	assert.NoError(t, cluster.stopCluster())
}

// TestReplicateSimple tests replicating a single command.
func TestReplicateSimple(t *testing.T) {
	numServers := 3
	cluster, err := newCluster(numServers)
	assert.NoError(t, err)

	assert.NoError(t, cluster.startCluster())

	time.Sleep(500 * time.Millisecond)

	command := []byte("test")
	assert.NoError(t, cluster.replicate(command))

	time.Sleep(500 * time.Millisecond)

	for id := 0; id < numServers; id++ {
		responses := cluster.responses(id)
		assert.Len(t, responses, 1)
		response := responses[0]
		assert.Equal(t, response.Command, command)
		assert.Equal(t, int(response.Index), 1)
	}

	assert.NoError(t, cluster.stopCluster())
}

// TestReplicateMultiple tests replicating multiple commands.
func TestReplicateMultiple(t *testing.T) {
	numServers := 5
	cluster, err := newCluster(numServers)
	assert.NoError(t, err)

	assert.NoError(t, cluster.startCluster())

	time.Sleep(500 * time.Millisecond)

	commands := [][]byte{[]byte("command1"), []byte("command2"), []byte("command3"), []byte("command4")}
	for _, command := range commands {
		assert.NoError(t, cluster.replicate(command))
	}

	time.Sleep(500 * time.Millisecond)

	for id := 0; id < numServers; id++ {
		responses := cluster.responses(id)
		assert.Len(t, responses, len(commands))
		for i, response := range responses {
			assert.Equal(t, response.Command, commands[i])
			assert.Equal(t, int(response.Index), i+1)
		}
	}

	assert.NoError(t, cluster.stopCluster())
}

// TestReplicationNoQuorum tests that replication is not successful if there is not a quorum.
func TestReplicateNoQuorum(t *testing.T) {
	numServers := 5
	cluster, err := newCluster(numServers)
	assert.NoError(t, err)

	assert.NoError(t, cluster.startCluster())

	time.Sleep(500 * time.Millisecond)

	leader, err := cluster.clusterLeader()
	assert.NoError(t, err)

	assert.NoError(t, cluster.disconnectServerFromPeer(leader, (leader+1)%numServers))
	assert.NoError(t, cluster.disconnectServerFromPeer(leader, (leader+2)%numServers))
	assert.NoError(t, cluster.disconnectServerFromPeer(leader, (leader+3)%numServers))

	command := []byte("test")
	assert.NoError(t, cluster.replicate(command))

	// Wait a bit to make sure command was not committed.
	time.Sleep(1000 * time.Millisecond)

	for id := 0; id < numServers; id++ {
		responses := cluster.responses(id)
		assert.Empty(t, responses)
	}

	assert.NoError(t, cluster.stopCluster())
}
