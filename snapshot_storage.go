package raft

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
)

// SnapshotMetadata contains all metadata associated with a snapshot.
type SnapshotMetadata struct {
	// The unique identifier of a snapshot.
	ID uint64 `json:"id"`

	// The last log index included in the snapshot.
	LastIncludedIndex uint64 `json:"last_included_index"`

	// The last log term included in the snapshot.
	LastIncludedTerm uint64 `json:"last_included_term"`
}

// SnapshotStorage represents the component of Raft this manages persistently
// storing snapshots of the state machine.
type SnapshotStorage interface {
	// NewSnapshotFile creates a new snapshot file that is ready to be written to.
	NewSnapshotFile(lastIncludedIndex, lastIncludedTerm uint64) (io.WriteCloser, error)

	// Snapshot returns the snapshot with the provided ID, if it exists. If the ID is 0,
	// the most recently taken snapshot is returned.
	SnapshotReader(id uint64) (io.ReadSeekCloser, error)

	// Snapshots returns an array containing the metadata of all snapshots that have been
	// taken sorted from least recent to most recent.
	Snapshots() ([]SnapshotMetadata, error)
}

// snapshotWriter implements the io.WriteCloser interface.
type snapshotWriter struct {
	// The actual directory that will contain the snapshot and its metadata
	// once the snapshot has safely been written to disk.
	dir string

	// The snapshot file.
	file *os.File
}

func (s *snapshotWriter) Write(p []byte) (n int, err error) {
	return s.file.Write(p)
}

func (s *snapshotWriter) Close() error {
	if s.file == nil {
		return nil
	}

	// Ensure any written data is on disk.
	if err := s.file.Sync(); err != nil {
		return err
	}
	if err := s.file.Close(); err != nil {
		return err
	}

	// Perform an atomic rename of the temporary directory
	// containing the snapshot and its metadata.
	tmpDir := filepath.Dir(s.file.Name())
	return os.Rename(tmpDir, s.dir)
}

// persistentSnapshotStorage is an implementation of the SnapshotStorage interface. This
// implementation is not concurrent safe.
type persistentSnapshotStorage struct {
	// The directory where snapshots are persisted.
	snapshotDir string

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
	return &persistentSnapshotStorage{snapshotDir: snapshotPath}, nil
}

func (p *persistentSnapshotStorage) NewSnapshotFile(
	lastIncludedIndex, lastIncludedTerm uint64,
) (io.WriteCloser, error) {
	// The temporary directory that will contain the snapshot and its metadata.
	// This directory will be renamed once the snapshot has been safely written to disk.
	tmpDir, err := os.MkdirTemp(p.snapshotDir, "tmp-")
	if err != nil {
		return nil, err
	}

	// Create the file the snapshot will be written to.
	snapshotFilename := filepath.Join(tmpDir, fmt.Sprintf("snapshot-%d.bin", p.id))
	snapshotFile, err := os.Create(snapshotFilename)
	if err != nil {
		return nil, err
	}

	// Create metadata file and write metadata to it.
	metadataFilename := filepath.Join(tmpDir, fmt.Sprintf("metadata-%d.json", p.id))
	metadataFile, err := os.Create(metadataFilename)
	if err != nil {
		return nil, err
	}
	metadata := SnapshotMetadata{
		ID:                p.id,
		LastIncludedIndex: lastIncludedIndex,
		LastIncludedTerm:  lastIncludedTerm,
	}
	bytes, err := json.Marshal(metadata)
	if err != nil {
		return nil, err
	}
	if _, err := metadataFile.Write(bytes); err != nil {
		return nil, err
	}
	if err := metadataFile.Sync(); err != nil {
		return nil, err
	}
	if err := metadataFile.Close(); err != nil {
		return nil, err
	}

	actualDir := filepath.Join(p.snapshotDir, fmt.Sprintf("snapshot-%d", p.id))
	writer := &snapshotWriter{dir: actualDir, file: snapshotFile}

	p.id++

	return writer, nil
}

func (p *persistentSnapshotStorage) SnapshotReader(id uint64) (io.ReadSeekCloser, error) {
	if id > 0 {
		snapshotFilename := filepath.Join(
			p.snapshotDir,
			fmt.Sprintf("snapshot-%d/snapshot-%d.bin", id, id),
		)
		return os.Open(snapshotFilename)
	}

	// Find the most recently taken snapshot.
	pattern := filepath.Join(p.snapshotDir, "snapshot-%d/snapshot-%d.bin")
	snapshotFilenames, err := filepath.Glob(pattern)
	if err != nil {
		return nil, err
	}
	sort.Strings(snapshotFilenames)
	if len(snapshotFilenames) == 0 {
		return nil, nil
	}
	mostRecentSnapshot := snapshotFilenames[len(snapshotFilenames)-1]
	return os.Open(mostRecentSnapshot)
}

func (p *persistentSnapshotStorage) Snapshots() ([]SnapshotMetadata, error) {
	pattern := filepath.Join(p.snapshotDir, "snapshot-%d/metadata-%d.bin")
	metadataFilenames, err := filepath.Glob(pattern)
	if err != nil {
		return nil, err
	}
	sort.Strings(metadataFilenames)
	snapshots := make([]SnapshotMetadata, len(metadataFilenames))
	for _, metadataFilename := range metadataFilenames {
		bytes, err := os.ReadFile(metadataFilename)
		if err != nil {
			return nil, err
		}
		metadata := SnapshotMetadata{}
		if err := json.Unmarshal(bytes, &metadata); err != nil {
			return nil, err
		}
		snapshots = append(snapshots, metadata)
	}
	return snapshots, nil
}
