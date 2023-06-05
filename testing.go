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

	"github.com/jmsadair/raft/internal/util"
	"github.com/stretchr/testify/assert"
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
	errEmptySnapshot             = "cluster took snapshot that was empty: server = %d"
	errIncorrectNumberSnapshots  = "cluster has incorrrect number of snapshots: server = %d, expectedNumberSnapshots = %d, actualNumberSnapshots = %d"
	errIncorrectSnapshotIndex    = "cluster took snapshot with incorrect last included index: server = %d, lastIncludedIndex = %d, actualLastIncludedIdex = %d"
	errIncorrectSnapshotTerm     = "cluster took snapshot with incorrect last included term: server = %d, lastIncludedTerm = %d, actualLastIncludedTerm = %d"
	errDifferentCommandSameIndex = "cluster applied different commands at the same index: index = %d, command1 = %s, command2 = %s"
	errReconnectingPeer          = "error reconnecting peer: peer = %d, connectingTo = %d, err = %s"
	errDisconnectingPeer         = "error disconnecting peer: peer = %d, disconnectingFrom = %d, err = %s"
	errStateMachineEncode        = "error encoding state machine state: %s"
	errStateMachineDecode        = "error decoding state machine state: %s"
)

func validateLogEntry(t *testing.T, entry *LogEntry, expectedIndex uint64, expectedTerm uint64, expectedData []byte) {
	assert.Equal(t, expectedIndex, entry.Index, "entry has incorrect index")
	assert.Equal(t, expectedTerm, entry.Term, "entry has incorrect term")
	assert.Equal(t, expectedData, entry.Data, "entry has incorrect data")
}

func validateSnapshot(t *testing.T, expected *Snapshot, actual *Snapshot) {
	assert.Equal(t, expected.LastIncludedIndex, actual.LastIncludedIndex, "last included index does not match")
	assert.Equal(t, expected.LastIncludedTerm, actual.LastIncludedTerm, "last included term does not match")
	assert.Equal(t, expected.Data, actual.Data, "data does not match")
}

func makeCommands(numCommands int) []Command {
	commands := make([]Command, numCommands)
	for i := 1; i <= numCommands; i++ {
		commands[i-1] = Command{Bytes: []byte(fmt.Sprintf("command %d", i))}
	}

	return commands
}

func makePeerMaps(numServers int) []map[string]net.Addr {
	port := 8080
	clusterPeers := make([]map[string]net.Addr, numServers)
	for i := 0; i < numServers; i++ {
		clusterPeers[i] = make(map[string]net.Addr, numServers)
		for j := 0; j < numServers; j++ {
			ip := fmt.Sprintf("127.0.0.%d", j)
			peerID := fmt.Sprint(j)
			clusterPeers[i][peerID] = &net.TCPAddr{IP: net.ParseIP(ip), Port: port}
		}
	}

	return clusterPeers
}

type stateMachineEncoderMock struct{}

func (s *stateMachineEncoderMock) encode(entries []*LogEntry) ([]byte, error) {
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	if err := enc.Encode(entries); err != nil {
		return buf.Bytes(), err
	}
	return buf.Bytes(), nil
}

type stateMachineDecoderMock struct{}

func (s *stateMachineDecoderMock) decode(data []byte) ([]*LogEntry, error) {
	var commands []*LogEntry
	buf := bytes.NewBuffer(data)
	dec := gob.NewDecoder(buf)
	if err := dec.Decode(&commands); err != nil {
		return commands, err
	}
	return commands, nil
}

type stateMachineMock struct {
	commands []*LogEntry
	mu       sync.Mutex
}

func newStateMachineMock() *stateMachineMock {
	gob.Register(LogEntry{})
	return &stateMachineMock{commands: make([]*LogEntry, 0)}
}

func (s *stateMachineMock) Apply(entry *LogEntry) interface{} {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.commands = append(s.commands, entry)
	return len(s.commands)
}

func (s *stateMachineMock) Snapshot() (Snapshot, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	encoder := stateMachineEncoderMock{}
	bytes, err := encoder.encode(s.commands)
	if err != nil {
		return Snapshot{}, fmt.Errorf(errStateMachineEncode, err.Error())
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

	return Snapshot{LastIncludedIndex: lastIncludedIndex, LastIncludedTerm: lastIncludedTerm, Data: bytes}, nil
}

func (s *stateMachineMock) Restore(snapshot *Snapshot) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	decoder := stateMachineDecoderMock{}
	entries, err := decoder.decode(snapshot.Data)
	if err != nil {
		return fmt.Errorf(errStateMachineDecode, err.Error())
	}

	s.commands = entries

	return nil
}

type storagePaths struct {
	logPath             string
	storagePath         string
	snapshotStoragePath string
}

type testCluster struct {
	// The testing instance associated with the cluster.
	t *testing.T

	// The servers making up the cluster.
	servers []*Server

	// The peers fore each server, where peers[i] is the peers
	// for servers[i].
	peers []map[string]net.Addr

	// The associated storage paths for each server, where
	// paths[i] is the paths for servers[i].
	paths []storagePaths

	// The servers which are disconnected, where disconnected[i] being
	// true indicates servers[i] is disconnected.
	disconnected []bool

	// The state machine associated with each server, where fsm[i]
	// corresponds to the state machine for servers[i].
	fsm []*stateMachineMock

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

	// Indicates whether auto snapshotting will be used.
	autoSnapshotting bool

	// The maximum number of log entries per snapshot if auto
	// snapshotting is enabled.
	snapshotSize int

	mu sync.Mutex

	wg sync.WaitGroup
}

func newCluster(t *testing.T, numServers int, autoSnapshotting bool, snapshotSize int) *testCluster {
	servers := make([]*Server, numServers)
	fsm := make([]*stateMachineMock, numServers)
	replicateCh := make([]chan CommandResponse, numServers)
	responses := make([]map[uint64]CommandResponse, numServers)
	serverErrors := make([]string, numServers)
	lastApplied := make([]uint64, numServers)
	disconnected := make([]bool, numServers)
	paths := make([]storagePaths, numServers)

	// The paths for all of the persistent storage associated with the cluster.
	tmpDir := t.TempDir()
	snapshotFileFmt := tmpDir + "/raft-snapshots-%d"
	logFileFmt := tmpDir + "/raft-log-%d"
	storageFileFmt := tmpDir + "/raft-storage-%d"

	// Make peer map for each server.
	peers := makePeerMaps(numServers)

	for i := 0; i < numServers; i++ {
		replicateCh[i] = make(chan CommandResponse)
		fsm[i] = newStateMachineMock()
		responses[i] = make(map[uint64]CommandResponse)
		paths[i] = storagePaths{logPath: fmt.Sprintf(logFileFmt, i), storagePath: fmt.Sprintf(storageFileFmt, i),
			snapshotStoragePath: fmt.Sprintf(snapshotFileFmt, i)}
		id := fmt.Sprint(i)

		server, err := NewServer(id, peers[i], fsm[i], paths[i].logPath, paths[i].storagePath, paths[i].snapshotStoragePath, replicateCh[i],
			WithAutoSnapshotting(autoSnapshotting), WithMaxLogEntriesPerSnapshot(snapshotSize))
		if err != nil {
			t.Fatalf(errCreateClusterServer, i, err.Error())
		}

		servers[i] = server
	}

	return &testCluster{
		t:                t,
		servers:          servers,
		disconnected:     disconnected,
		peers:            peers,
		paths:            paths,
		fsm:              fsm,
		responseCh:       replicateCh,
		commandResponses: responses,
		serverErrors:     serverErrors,
		lastApplied:      lastApplied,
		shutdownCh:       make(chan interface{}),
		autoSnapshotting: autoSnapshotting,
		snapshotSize:     snapshotSize,
	}
}

func (tc *testCluster) startCluster() {
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

func (tc *testCluster) stopCluster() {
	for _, server := range tc.servers {
		server.Stop()
	}
	close(tc.shutdownCh)
	tc.wg.Wait()
}

func (tc *testCluster) submit(command Command, retry bool, expectFail bool, expectedApplied int) {
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

func (tc *testCluster) checkLeaders(expectNoLeader bool) int {
	// Any leaders detected.
	leaders := make([]int, 0)

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
			if status.State == Leader && !tc.disconnected[i] {
				index, _ := strconv.Atoi(status.ID)
				leaders = append(leaders, index)
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
		return -1
	}

	return leaders[0]
}

func (tc *testCluster) checkLogs(index int, response CommandResponse) {
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

func (tc *testCluster) checkSnapshot(index int, response CommandResponse) {
	tc.mu.Lock()
	defer tc.mu.Unlock()

	snapshots := tc.servers[index].ListSnapshots()

	for _, snapshot := range snapshots {
		if snapshot.LastIncludedIndex+1 == response.Index {
			// Decode the snapshot data into entries.
			decoder := stateMachineDecoderMock{}
			appliedEntries, err := decoder.decode(snapshot.Data)
			if err != nil {
				tc.serverErrors[index] = err.Error()
				return
			}

			if len(appliedEntries) == 0 {
				tc.serverErrors[index] = fmt.Sprintf(errEmptySnapshot, index)
				return
			}

			actualLastIncludedIndex := appliedEntries[len(appliedEntries)-1].Index
			actualLastIncludedTerm := appliedEntries[len(appliedEntries)-1].Term

			// Sanity check last included index matches with the last included index in the snapshot bytes.
			if actualLastIncludedIndex != snapshot.LastIncludedIndex {
				tc.serverErrors[index] = fmt.Sprintf(errIncorrectSnapshotIndex, index, snapshot.LastIncludedIndex, actualLastIncludedIndex)
			}

			// Sanity chec last included term matches with the last included term in the snapshot bytes.
			if actualLastIncludedTerm != snapshot.LastIncludedTerm {
				tc.serverErrors[index] = fmt.Sprintf(errIncorrectSnapshotTerm, index, snapshot.LastIncludedTerm, actualLastIncludedTerm)
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

func (tc *testCluster) checkApplied(index uint64, expectedApplied int) bool {
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

func (tc *testCluster) applyLoop(index int) {
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

func (tc *testCluster) crashServer(server int) {
	// Do not acquire lock here - will cause deadlock.
	tc.disconnectServer(server)
	tc.servers[server].Stop()
}

func (tc *testCluster) restartServer(server int) {
	tc.mu.Lock()
	defer tc.mu.Unlock()

	serverID := fmt.Sprint(server)

	tc.responseCh[server] = make(chan CommandResponse)
	tc.fsm[server] = newStateMachineMock()

	newServer, err := NewServer(serverID, tc.peers[server], tc.fsm[server], tc.paths[server].logPath,
		tc.paths[server].storagePath, tc.paths[server].snapshotStoragePath, tc.responseCh[server],
		WithAutoSnapshotting(tc.autoSnapshotting), WithMaxLogEntriesPerSnapshot(tc.snapshotSize))
	if err != nil {
		tc.t.Fatalf(errStartingClusterServer, server, err.Error())
	}

	snapshot, _ := tc.servers[server].raft.snapshotStorage.LastSnapshot()
	tc.lastApplied[server] = snapshot.LastIncludedIndex
	tc.servers[server] = newServer
	tc.commandResponses[server] = make(map[uint64]CommandResponse)

	tc.wg.Add(1)
	go tc.applyLoop(server)

	readyCh := make(chan interface{})
	defer close(readyCh)
	if err := newServer.Start(readyCh); err != nil {
		tc.t.Fatalf(errStartingClusterServer, server, err.Error())
	}

	for i := 0; i < len(tc.servers); i++ {
		if err := tc.servers[i].raft.connectPeer(serverID); err != nil {
			tc.t.Fatalf(errReconnectingPeer, i, server, err.Error())
		}
	}

	tc.disconnected[server] = false
}

func (tc *testCluster) createPartition() {
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
				if err := tc.servers[i].raft.disconnectPeer(fmt.Sprint(j)); err != nil {
					tc.t.Fatalf(errDisconnectingPeer, i, j, err.Error())
				}
				if err := tc.servers[j].raft.disconnectPeer(fmt.Sprint(i)); err != nil {
					tc.t.Fatalf(errDisconnectingPeer, j, i, err.Error())
				}
			}

			if err := tc.servers[i].raft.disconnectPeer(fmt.Sprint(i)); err != nil {
				tc.t.Fatalf(errDisconnectingPeer, i, i, err.Error())
			}
		}
	}

	for index := range partitionSet {
		tc.disconnected[index] = true
	}
}

func (tc *testCluster) reconnectServer(server int) {
	tc.mu.Lock()
	defer tc.mu.Unlock()

	serverID := fmt.Sprint(server)

	// Reconnect this server to itself. Note that this has no effect
	// on the operation of the cluster. The only purpose of this is to
	// indicate that this server is connected and should operate as expected.
	if err := tc.servers[server].raft.disconnectPeer(serverID); err != nil {
		tc.t.Fatalf(errReconnectingPeer, server, server, err.Error())
	}

	for i := 0; i < len(tc.servers); i++ {
		if server == i {
			continue
		}
		if err := tc.servers[i].raft.connectPeer(serverID); err != nil {
			tc.t.Fatalf(errReconnectingPeer, i, server, err.Error())
		}
		if err := tc.servers[server].raft.connectPeer(fmt.Sprint(i)); err != nil {
			tc.t.Fatalf(errReconnectingPeer, server, i, err.Error())
		}
	}

	tc.disconnected[server] = false
}

func (tc *testCluster) reconnectAllServers() {
	tc.mu.Lock()
	defer tc.mu.Unlock()

	for i := 0; i < len(tc.servers); i++ {
		for j := 0; j < len(tc.servers); j++ {
			if err := tc.servers[i].raft.connectPeer(fmt.Sprint(j)); err != nil {
				tc.t.Fatalf(errReconnectingPeer, i, j, err.Error())
			}
		}
	}

	for i := 0; i < len(tc.servers); i++ {
		tc.disconnected[i] = false
	}
}

func (tc *testCluster) disconnectServer(server int) {
	tc.mu.Lock()
	defer tc.mu.Unlock()

	serverID := fmt.Sprint(server)

	// Disconnect this index from itself. Note that this has no effect
	// on the operation of the cluster. The only purpose of this is to
	// indicate that this index is disconnected and will not operate
	// as expected.
	if err := tc.servers[server].raft.disconnectPeer(serverID); err != nil {
		tc.t.Fatalf(errDisconnectingPeer, server, server, err.Error())
	}

	for i := 0; i < len(tc.servers); i++ {
		if i == server {
			continue
		}
		if err := tc.servers[i].raft.disconnectPeer(serverID); err != nil {
			tc.t.Fatalf(errDisconnectingPeer, i, server, err.Error())
		}
		if err := tc.servers[server].raft.disconnectPeer(fmt.Sprint(i)); err != nil {
			tc.t.Fatalf(errDisconnectingPeer, server, i, err.Error())
		}
	}

	tc.disconnected[server] = true
}

func (tc *testCluster) takeAndValidateSnapshot(server int) {
	tc.mu.Lock()
	defer tc.mu.Unlock()

	oldSnaphsots := tc.servers[server].ListSnapshots()
	lastIncludedIndex, lastIncludedTerm := tc.servers[server].TakeSnapshot()
	newSnapshots := tc.servers[server].ListSnapshots()

	// A snapshot was just taken so there should be one additional snapshot in the snapshot store.
	if len(newSnapshots) != len(oldSnaphsots)+1 {
		tc.t.Fatalf(errIncorrectNumberSnapshots, server, len(oldSnaphsots)+1, len(newSnapshots))
	}

	// The most recent snapshot.
	snapshot := newSnapshots[len(newSnapshots)-1]

	// Decode the snapshot data into entries.
	decoder := stateMachineDecoderMock{}
	appliedEntries, err := decoder.decode(snapshot.Data)
	if err != nil {
		tc.t.Fatal(err)
		return
	}

	// Snapshots should never be empty.
	if len(appliedEntries) == 0 {
		tc.t.Fatalf(errEmptySnapshot, server)
		return
	}

	actualLastIncludedIndex := appliedEntries[len(appliedEntries)-1].Index
	actualLastIncludedTerm := appliedEntries[len(appliedEntries)-1].Term

	// Make sure last included index returned by server matches with the last included index in the snapshot bytes.
	if actualLastIncludedIndex != lastIncludedIndex {
		tc.t.Fatalf(errIncorrectSnapshotIndex, server, lastIncludedIndex, actualLastIncludedIndex)
	}

	// Make sure last included term returned by server matches with the last included term in the snapshot bytes.
	if actualLastIncludedTerm != lastIncludedTerm {
		tc.t.Fatalf(errIncorrectSnapshotTerm, server, lastIncludedTerm, actualLastIncludedTerm)
	}

	// Make sure last included index in snapshot matches with the last included index in the snapshot bytes.
	if actualLastIncludedIndex != snapshot.LastIncludedIndex {
		tc.t.Fatalf(errIncorrectSnapshotIndex, server, snapshot.LastIncludedIndex, actualLastIncludedIndex)
	}

	// Make sure last included term in snapshot matches with the last included term in the snapshot bytes.
	if actualLastIncludedTerm != snapshot.LastIncludedTerm {
		tc.t.Fatalf(errIncorrectSnapshotTerm, server, snapshot.LastIncludedTerm, actualLastIncludedTerm)
	}
}
