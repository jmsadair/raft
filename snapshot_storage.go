package raft

import (
	"bufio"
	"io"
	"os"

	"github.com/jmsadair/raft/internal/errors"
)

const (
	errSnapshotStoreNotOpen = "snapshot storage is not open: path = %s"
	errFailedSnapshotSave   = "failed to save snapshot to snapshot storage: %s"
	errFailedSnapshotSync   = "failed to sync snapshot storage file: %s"
	errFailedSnapshotFlush  = "failed flushing data from snapshot storage file writer: %s"
	errFailedSnapshotOpen   = "failed to open snapshot storage file: path = %s, err = %s"
	errFailedSnapshotEncode = "failed to encode snapshot: %s"
	errFailedSnapshotDecode = "failed to decode snapshot: %s"
)

// Snapshot represents a snapshot of the replicated state machine.
type Snapshot struct {
	// Last index included in snapshot
	LastIncludedIndex uint64

	// Last term included in snapshot.
	LastIncludedTerm uint64

	// State of replicated state machine.
	Data []byte
}

// NewSnapshot creates a new Snapshot instance with the provided state data
// from the replicated state machine.
func NewSnapshot(lastIncludedIndex uint64, lastIncludedTerm uint64, data []byte) *Snapshot {
	return &Snapshot{LastIncludedIndex: lastIncludedIndex, LastIncludedTerm: lastIncludedTerm, Data: data}
}

// SnapshotStorage is an interface representing the internal component of Raft that is responsible
// for managing snapshots.
type SnapshotStorage interface {
	// Open opens the snapshot storage for reading and writing snapshots.
	Open() error

	// Replay reads the persisted state of the snapshot store
	// into memory.
	Replay() error

	// Close closes the snapshot storage.
	Close() error

	// LastSnapshot gets the most recently saved snapshot, if it exists.
	LastSnapshot() (Snapshot, bool)

	// SaveSnapshot saves the provided snapshot to durable storage.
	SaveSnapshot(snapshot *Snapshot) error

	// ListSnapshots returns an array of the snapshots that have been saved.
	ListSnapshots() []Snapshot
}

// persistentSnapshotStorage is an implementation of the SnapshotStorage interface that manages snapshots
// and persists them to durable storage. This implementation is not concurrent safe and should only be used
// within the Raft implementation.
type persistentSnapshotStorage struct {
	// All snapshots that have been saved to this storage. Empty if no snapshots
	// have been saved or if the snapshot store is not open.
	snapshots []Snapshot

	// The path to the file where the snapshot storage is persisted.
	path string

	// The file associated with the snapshot storage, nil if snapshot storage is closed.
	file *os.File
}

// newPersistentSnapshotStorage creates a new instance of PersistentSnapshotStorage at the
// provided path.
func newPersistentSnapshotStorage(path string) *persistentSnapshotStorage {
	return &persistentSnapshotStorage{path: path}
}

func (p *persistentSnapshotStorage) Open() error {
	if p.file != nil {
		return nil
	}

	file, err := os.OpenFile(p.path, os.O_RDWR|os.O_CREATE, 0777)
	if err != nil {
		return errors.WrapError(err, errFailedSnapshotOpen, p.path, err.Error())
	}

	p.file = file
	p.snapshots = make([]Snapshot, 0)

	return nil
}

func (p *persistentSnapshotStorage) Replay() error {
	if p.file == nil {
		return errors.WrapError(nil, errSnapshotStoreNotOpen, p.path)
	}

	reader := bufio.NewReader(p.file)
	snapshotDecoder := snapshotDecoder{}

	for {
		snapshot, err := snapshotDecoder.decode(reader)
		if err == io.EOF {
			break
		}
		if err != nil {
			return errors.WrapError(err, errFailedSnapshotDecode, err.Error())
		}
		p.snapshots = append(p.snapshots, snapshot)
	}

	return nil
}

func (p *persistentSnapshotStorage) Close() error {
	if p.file == nil {
		return nil
	}

	p.file.Close()
	p.snapshots = nil
	p.file = nil

	return nil
}

func (p *persistentSnapshotStorage) LastSnapshot() (Snapshot, bool) {
	if len(p.snapshots) == 0 {
		return Snapshot{}, false
	}
	return p.snapshots[len(p.snapshots)-1], true
}

func (p *persistentSnapshotStorage) ListSnapshots() []Snapshot {
	if len(p.snapshots) == 0 {
		return []Snapshot{}
	}
	return p.snapshots
}

func (p *persistentSnapshotStorage) SaveSnapshot(snapshot *Snapshot) error {
	if p.file == nil {
		return errors.WrapError(nil, errSnapshotStoreNotOpen, p.path)
	}

	writer := bufio.NewWriter(p.file)
	snapshotEncoder := snapshotEncoder{}
	if err := snapshotEncoder.encode(writer, snapshot); err != nil {
		return errors.WrapError(err, errFailedSnapshotEncode, err.Error())
	}
	if err := writer.Flush(); err != nil {
		return errors.WrapError(err, errFailedSnapshotFlush, err.Error())
	}
	if err := p.file.Sync(); err != nil {
		return errors.WrapError(err, errFailedSnapshotSync, err.Error())
	}

	p.snapshots = append(p.snapshots, *snapshot)

	return nil
}
