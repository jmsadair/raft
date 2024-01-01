package raft

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/jmsadair/raft/internal/util"
)

type SnapshotMetadata struct {
	ID                uint64
	LastIncludedIndex uint64
	LastIncludedTerm  uint64
}

// SnapshotFile represents a snapshot of the replicated state machine.
type SnapshotFile struct {
	// The path to this snapshot.
	Path string

	// The metadata for the snapshot.
	Metadata SnapshotMetadata
}

// SnapshotStorage represents the component of Raft this manages persistently
// storing snapshots of the state machine.
type SnapshotStorage interface {
	NewSnapshotFile(lastIncludedIndex, lastIncludedTerm uint64) (io.WriteCloser, error)
	SnapshotReader(id uint64) (io.ReadCloser, error)
}

type snapshotWriteCloser struct {
	tmpFile *os.File
	path    string
}

func (s *snapshotWriteCloser) Write(p []byte) (n int, err error) {
	return s.tmpFile.Write(p)
}

func (s *snapshotWriteCloser) Close() error {
	defer os.Remove(s.tmpFile.Name())
	if err := s.tmpFile.Sync(); err != nil {
		return err
	}
	if err := s.tmpFile.Close(); err != nil {
		return err
	}
	return os.Rename(s.tmpFile.Name(), s.path)
}

// persistentSnapshotStorage is an implementation of the SnapshotStorage interface. This
// implementation is not concurrent safe.
type persistentSnapshotStorage struct {
	// The directory where snapshots are persisted.
	snapshotDir string

	// All snapshot files in the snapshot directory.
	snapshots map[string]SnapshotFile

	// The unique ID that will be assigned to the next snapshot.
	id uint64
}

// NewSnapshotStorage creates a new snapshot storage.
// Snapshots will be stored at path/snapshots. If any directories
// on the path do not exist, they will be created.
func NewSnapshotStorage(path string) (SnapshotStorage, error) {
	snapshotPath := filepath.Join(path, "snapshots")
	if err := os.MkdirAll(snapshotPath, os.ModePerm); err != nil {
		return nil, err
	}

	snapshots := make(map[string]SnapshotFile)
	snapshotStorage := &persistentSnapshotStorage{
		snapshotDir: snapshotPath,
		snapshots:   snapshots,
	}

	if err := snapshotStorage.syncSnapsotFiles(); err != nil {
		return nil, err
	}

	for _, snapshotFile := range snapshotStorage.snapshots {
		snapshotStorage.id = util.Max[uint64](snapshotStorage.id, snapshotFile.Metadata.ID)
	}
	snapshotStorage.id++

	return snapshotStorage, nil
}

func (p *persistentSnapshotStorage) NewSnapshotFile(
	lastIncludedIndex, lastIncludedTerm uint64,
) (io.WriteCloser, error) {
	tmpFile, err := os.CreateTemp(p.snapshotDir, "tmp-")
	if err != nil {
		return nil, err
	}

	name := fmt.Sprintf("snapshot-%d.bin", p.id)
	path := filepath.Join(p.snapshotDir, name)
	writer := &snapshotWriteCloser{path: path, tmpFile: tmpFile}

	metadata := &SnapshotMetadata{
		ID:                p.id,
		LastIncludedIndex: lastIncludedIndex,
		LastIncludedTerm:  lastIncludedTerm,
	}
	if err := encodeSnapshotMetadata(writer, metadata); err != nil {
		return nil, err
	}

	p.id++

	return writer, nil
}

func (p *persistentSnapshotStorage) SnapshotReader(id uint64) (io.ReadCloser, error) {
	if err := p.syncSnapsotFiles(); err != nil {
		return nil, err
	}

	for path, snapshotFile := range p.snapshots {
		if id == snapshotFile.Metadata.ID || (id == 0 && p.id == snapshotFile.Metadata.ID+1) {
			file, err := os.Open(path)
			if err != nil {
				return nil, err
			}
			_, err = decodeSnapshotMetadata(file)
			if err != nil {
				return nil, err
			}
			return file, nil
		}
	}

	return nil, nil
}

func (p *persistentSnapshotStorage) syncSnapsotFiles() error {
	pattern := filepath.Join(p.snapshotDir, "snapshot-%d.bin")
	entries, err := filepath.Glob(pattern)
	if err != nil {
		return err
	}

	for _, entry := range entries {
		if _, ok := p.snapshots[entry]; ok {
			continue
		}

		snapshot, err := os.Open(entry)
		if err != nil {
			return err
		}

		reader := bufio.NewReader(snapshot)
		metadata, err := decodeSnapshotMetadata(reader)
		if err != nil {
			return err
		}

		p.snapshots[entry] = SnapshotFile{Path: entry, Metadata: metadata}
	}

	return nil
}
