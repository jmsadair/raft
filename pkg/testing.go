package raft

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"net"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/jmsadair/raft/internal/errors"
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

func makeGrpcPeers(numServers int) [][]*GrpcPeer {
	port := 8080
	clusterPeers := make([][]*GrpcPeer, numServers)
	for i := 0; i < numServers; i++ {
		clusterPeers[i] = make([]*GrpcPeer, numServers)
		for j := 0; j < numServers; j++ {
			ip := fmt.Sprintf("127.0.0.%d", j)
			peerID := fmt.Sprint(j)
			clusterPeers[i][j] = NewGrpcPeer(peerID, &net.TCPAddr{IP: net.ParseIP(ip), Port: port})
		}
	}

	return clusterPeers
}

type StateMachineMock struct {
	commands []*LogEntry
	mu       sync.Mutex
}

func NewStateMachineMock() *StateMachineMock {
	gob.Register(LogEntry{})
	return &StateMachineMock{commands: make([]*LogEntry, 0)}
}

func (s *StateMachineMock) Apply(entry *LogEntry) interface{} {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.commands = append(s.commands, entry)
	return len(s.commands)
}

func (s *StateMachineMock) Snapshot() (Snapshot, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	var data bytes.Buffer
	enc := gob.NewEncoder(&data)
	if err := enc.Encode(s.commands); err != nil {
		return Snapshot{}, errors.WrapError(err, "error saving snapshot: %s", err.Error())
	}

	var lastIncludedIndex uint64
	var lastIncludedTerm uint64
	if len(s.commands) == 0 {
		lastIncludedIndex = 0
		lastIncludedTerm = 0
	} else {
		lastIncludedIndex = s.commands[len(s.commands)-1].Index
		lastIncludedTerm = s.commands[len(s.commands)-1].Term
	}

	return Snapshot{LastIncludedIndex: lastIncludedIndex, LastIncludedTerm: lastIncludedTerm, Data: data.Bytes()}, nil
}

func (s *StateMachineMock) Restore(snapshot *Snapshot) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	var commands []*LogEntry
	data := bytes.NewBuffer(snapshot.Data)
	dec := gob.NewDecoder(data)
	if err := dec.Decode(&commands); err != nil {
		return errors.WrapError(err, "error restoring from snapshot: %s", err.Error())
	}

	s.commands = commands

	return nil
}

type TestCluster struct {
	// The testing instance associated with the cluster.
	t *testing.T

	// The peers associated with each server, where
	// peers[i] corresponds to the peers for servers[i].
	peers [][]*GrpcPeer

	// The servers making up the cluster.
	servers []*GrpcServer

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
	servers := make([]*GrpcServer, numServers)
	snapshotStores := make([]SnapshotStorage, numServers)
	stateMachines := make([]StateMachine, numServers)
	logs := make([]Log, numServers)
	stores := make([]Storage, numServers)
	replicateCh := make([]chan CommandResponse, numServers)
	responses := make([]map[uint64]CommandResponse, numServers)
	peers := makeGrpcPeers(numServers)
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

		server, err := NewGrpcServer(id, peers[i], logs[i], stores[i], snapshotStores[i], stateMachines[i], address, replicateCh[i],
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

	// The snapshot storage is not concurrent safe. We need to ensure that
	// the raft implementation is not writing to it when read.
	tc.servers[index].raft.mu.Lock()
	snapshots := tc.snapshotStores[index].ListSnapshots()
	tc.servers[index].raft.mu.Unlock()

	for _, snapshot := range snapshots {
		if snapshot.LastIncludedIndex+1 == response.Index {
			// Decode the snapshot data into entries.
			var appliedEntries []*LogEntry
			data := bytes.NewBuffer(snapshot.Data)
			dec := gob.NewDecoder(data)
			if err := dec.Decode(&appliedEntries); err != nil {
				tc.serverErrors[index] = fmt.Sprintf("failed to decode applied entries: %s", err)
				return
			}

			// Update this server's responses with the commands included in the snapshot.
			for _, entry := range appliedEntries {
				tc.commandResponses[index][entry.Index] = CommandResponse{Index: entry.Index, Term: entry.Term, Command: entry.Data}
			}

			tc.commandResponses[index][response.Index] = response
			tc.lastApplied[index] = response.Index
			return
		}
	}

	tc.serverErrors[index] = fmt.Sprintf(errOutOfOrder, index, tc.lastApplied[index]+1, response.Index)
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
		// If the index was greater than the expected index then either
		// a snapshot must have been installed or there was an error.
		if response.Index > tc.lastApplied[index]+1 {
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

	newServer, err := NewGrpcServer(serverID, tc.peers[index], tc.logs[index], tc.stores[index], tc.snapshotStores[index],
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
