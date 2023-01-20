package raft

import (
	"errors"
	"fmt"
	"net"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

var (
	multipleLeaderErr = errors.New("cluster has more than one leader")
	noLeaderErr       = errors.New("cluster does not have a leader")
)

type TestCluster struct {
	peers   []*Peer
	servers []*Server
}

func newCluster(numServers int) (*TestCluster, error) {
	peers := make([]*Peer, numServers)
	servers := make([]*Server, numServers)

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
			log := NewVolatileLog()
			storage := NewVolatileStorage()
			server, err := NewServer(peers[i].id, serverPeers, log, storage, peers[i].address)
			if err != nil {
				return nil, err
			}
			servers[i] = server
		}
	}

	return &TestCluster{peers: peers, servers: servers}, nil
}

func (tc *TestCluster) startCluster() error {
	ready := make(chan interface{})
	for _, server := range tc.servers {
		if err := server.Start(ready); err != nil {
			return err
		}
	}
	close(ready)
	return nil
}

func (tc *TestCluster) stopCluster() error {
	for _, server := range tc.servers {
		if server.IsStarted() {
			err := server.Stop()
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func (tc *TestCluster) stopServer(id int) {
	tc.servers[id].Stop()
}

func (tc *TestCluster) leader() (int, error) {
	leader := -1
	for _, server := range tc.servers {
		id, _, state := server.raft.status()
		if state == Leader && leader != -1 {
			return -1, multipleLeaderErr
		}
		if state == Leader {
			var err error
			leader, err = strconv.Atoi(id)
			if err != nil {
				return 0, err
			}
		}
	}

	if leader == -1 {
		return 0, noLeaderErr
	}
	return leader, nil
}

func TestBasicElection(t *testing.T) {
	numServers := 3
	cluster, err := newCluster(numServers)

	assert.NoError(t, err)
	assert.NoError(t, cluster.startCluster())

	time.Sleep(2 * time.Second)

	_, err = cluster.leader()

	assert.NoError(t, err)
	assert.NoError(t, cluster.stopCluster())
}

func TestLeaderFailElection(t *testing.T) {
	numServers := 3
	cluster, err := newCluster(numServers)

	assert.NoError(t, err)
	assert.NoError(t, cluster.startCluster())

	time.Sleep(2 * time.Second)

	leader, err := cluster.leader()
	assert.NoError(t, err)
	cluster.stopServer(leader)

	time.Sleep(2 * time.Second)

	_, err = cluster.leader()

	assert.NoError(t, err)
	assert.NoError(t, cluster.stopCluster())
}
