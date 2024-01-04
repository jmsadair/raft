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

// SnapshotWriter represents a component for persisting snapshot data
// to disk.
type SnapshotWriter interface {
	io.WriteCloser

	// Discard will close the file and delete all data assosciated with
	// the snapshot.
	Discard() error

	// Offset will return the current offset of the writer in the snapshot file.
	Offset() int64

	// Metadata returns the metadata associated with the snapshot file.
	Metadata() SnapshotMetadata
}

// SnapshotReader represents a component for reading a snapshot that has already
// been safely persisted to disk.
type SnapshotReader interface {
	io.ReadSeekCloser

	// Metadata returns the metadata associated with the snapshot file.
	Metadata() SnapshotMetadata
}

// SnapshotStorage represents the component of Raft that manages snapshots created
// by the state machine.
type SnapshotStorage interface {
	// NewSnapshotFile creates a new snapshot file and returns a writer for it.
	// It is the caller's responsibility to close the file or discard it when they
	// are done with it.
	NewSnapshotFile(lastIncludedIndex, lastIncludedTerm uint64) (SnapshotWriter, error)

	// Snapshot returns the snapshot with the provided ID, if it exists. If the ID is 0,
	// the most recently taken snapshot is returned. It is the caller's responsibility
	// to close the file.
	SnapshotReader(id uint64) (SnapshotReader, error)

	// Snapshots returns an array containing the metadata of all snapshots that have been
	// taken sorted from least recent to most recent.
	Snapshots() ([]SnapshotMetadata, error)
}

// snapshotWriter implements the SnapshotWriter interface.
type snapshotWriter struct {
	// The actual directory that will contain the snapshot and its metadata
	// once the snapshot has safely been written to disk.
	dir string

	// The temporary directory that contains the snapshot and its metadata
	// while it is being written.
	tmpDir string

	// The snapshot file.
	file *os.File

	// The current offset in the snapshot file.
	offset int64

	// The metadata associated with the snapshot.
	metadata SnapshotMetadata
}

func (s *snapshotWriter) Write(p []byte) (n int, err error) {
	n, err = s.file.Write(p)
	s.offset += int64(n)
	return n, err
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

	s.file = nil
	s.offset = 0

	// Perform an atomic rename of the temporary directory
	// containing the snapshot and its metadata.
	return os.Rename(s.tmpDir, s.dir)
}

func (s *snapshotWriter) Discard() error {
	if s.file == nil {
		return nil
	}
	if err := s.file.Close(); err != nil {
		return err
	}

	s.file = nil
	s.offset = 0

	return os.RemoveAll(s.tmpDir)
}

func (s *snapshotWriter) Offset() int64 {
	return s.offset
}

func (s *snapshotWriter) Metadata() SnapshotMetadata {
	return s.metadata
}

type snapshotReader struct {
	// The file associated with the snapshot.
	file *os.File

	// The metadata associated with the snapshot.
	metadata SnapshotMetadata
}

func (s *snapshotReader) Read(p []byte) (n int, err error) {
	return s.file.Read(p)
}

func (s *snapshotReader) Seek(offset int64, whence int) (int64, error) {
	return s.file.Seek(offset, whence)
}

func (s *snapshotReader) Close() error {
	return s.file.Close()
}

func (s *snapshotReader) Metadata() SnapshotMetadata {
	return s.metadata
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
) (SnapshotWriter, error) {
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
	metadata := SnapshotMetadata{
		LastIncludedIndex: lastIncludedIndex,
		LastIncludedTerm:  lastIncludedTerm,
		ID:                p.id,
	}
	if err := writeMetadata(metadataFilename, &metadata); err != nil {
		return nil, err
	}

	actualDir := filepath.Join(p.snapshotDir, fmt.Sprintf("snapshot-%d", p.id))
	writer := &snapshotWriter{
		dir:      actualDir,
		tmpDir:   tmpDir,
		file:     snapshotFile,
		metadata: metadata,
	}

	p.id++

	return writer, nil
}

func (p *persistentSnapshotStorage) SnapshotReader(id uint64) (SnapshotReader, error) {
	if p.id <= 1 {
		return nil, nil
	}
	if id == 0 {
		id = p.id - 1
	}

	snapshotFilename := filepath.Join(
		p.snapshotDir,
		fmt.Sprintf("snapshot-%d/snapshot-%d.bin", id, id),
	)
	metadataFilename := filepath.Join(
		p.snapshotDir,
		fmt.Sprintf("snapshot-%d/metadata-%d.json", id, id),
	)
	snapshotFile, err := os.Open(snapshotFilename)
	if err != nil {
		return nil, err
	}
	metadata, err := readMetadata(metadataFilename)
	if err != nil {
		return nil, err
	}

	return &snapshotReader{file: snapshotFile, metadata: metadata}, nil
}

func (p *persistentSnapshotStorage) Snapshots() ([]SnapshotMetadata, error) {
	pattern := regexp.MustCompile(`snapshot-(\d+)/metadata-\d+\.json$`)
	filenames, err := p.findFiles(pattern.MatchString)
	if err != nil {
		return nil, err
	}
	snapshots := make([]SnapshotMetadata, 0, len(filenames))
	for _, filename := range filenames {
		metadata, err := readMetadata(filename)
		if err != nil {
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

func readMetadata(filename string) (SnapshotMetadata, error) {
	data, err := os.ReadFile(filename)
	if err != nil {
		return SnapshotMetadata{}, err
	}
	metadata := SnapshotMetadata{}
	if err := json.Unmarshal(data, &metadata); err != nil {
		return metadata, err
	}
	return metadata, nil
}

func writeMetadata(filename string, metadata *SnapshotMetadata) error {
	metadataFile, err := os.Create(filename)
	if err != nil {
		return err
	}

	bytes, err := json.Marshal(metadata)
	if err != nil {
		return err
	}
	if _, err := metadataFile.Write(bytes); err != nil {
		return err
	}
	if err := metadataFile.Sync(); err != nil {
		return err
	}

	return metadataFile.Close()
}
