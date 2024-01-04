package raft

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
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

// SnapshotStorage represents the component of Raft that manages snapshots created
// by the state machine.
type SnapshotStorage interface {
	// NewSnapshotFile creates a new snapshot file and returns a writer for it.
	// It is the caller's responsibility to close the file.
	NewSnapshotFile(lastIncludedIndex, lastIncludedTerm uint64) (io.WriteCloser, error)

	// Snapshot returns the snapshot with the provided ID, if it exists. If the ID is 0,
	// the most recently taken snapshot is returned. It is the caller's responsibility
	// to close the file.
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

	// The temporary directory that contains the snapshot and its metadata
	// while it is being written.
	tmpDir string

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
	return os.Rename(s.tmpDir, s.dir)
}

// persistentSnapshotStorage is an implementation of the SnapshotStorage interface. This
// implementation is not concurrent safe.
type persistentSnapshotStorage struct {
	// The directory where snapshots are persisted.
	snapshotDir string

	// The unique ID that will be assigned to the next snapshot.
	id uint64
}

// NewSnapshotStorage creates a new snapshot storage at the provided path.
func NewSnapshotStorage(path string) (SnapshotStorage, error) {
	snapshotPath := filepath.Join(path, "snapshots")
	if err := os.MkdirAll(snapshotPath, os.ModePerm); err != nil {
		return nil, err
	}

	store := &persistentSnapshotStorage{snapshotDir: snapshotPath, id: 1}

	snapshots, err := store.Snapshots()
	if err != nil {
		return nil, err
	}
	if len(snapshots) > 0 {
		store.id = snapshots[len(snapshots)-1].ID + 1
	}

	return store, nil
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
	writer := &snapshotWriter{dir: actualDir, tmpDir: tmpDir, file: snapshotFile}

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

	snapshots, err := p.Snapshots()
	if err != nil {
		return nil, err
	}
	if len(snapshots) == 0 {
		return nil, nil
	}
	mostRecentID := snapshots[len(snapshots)-1].ID
	snapshotFilename := filepath.Join(
		p.snapshotDir,
		fmt.Sprintf("snapshot-%d/snapshot-%d.bin", mostRecentID, mostRecentID),
	)
	return os.Open(snapshotFilename)
}

func (p *persistentSnapshotStorage) Snapshots() ([]SnapshotMetadata, error) {
	pattern := regexp.MustCompile(`snapshot-(\d+)/metadata-\d+\.json$`)
	filenames, err := p.findFiles(pattern.MatchString)
	if err != nil {
		return nil, err
	}
	snapshots := make([]SnapshotMetadata, 0, len(filenames))
	for _, filename := range filenames {
		bytes, err := os.ReadFile(filename)
		if err != nil {
			return nil, err
		}
		metadata := SnapshotMetadata{}
		if err := json.Unmarshal(bytes, &metadata); err != nil {
			return nil, err
		}
		snapshots = append(snapshots, metadata)
	}

	sort.Slice(snapshots, func(i, j int) bool {
		metadata1 := snapshots[i]
		metadata2 := snapshots[j]
		return metadata1.ID < metadata2.ID
	})

	return snapshots, nil
}

func (p *persistentSnapshotStorage) findFiles(isMatch func(string) bool) ([]string, error) {
	filenames := []string{}
	err := filepath.WalkDir(p.snapshotDir, func(path string, d os.DirEntry, err error) error {
		if err == nil && !d.IsDir() && isMatch(path) {
			filenames = append(filenames, path)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return filenames, nil
}
