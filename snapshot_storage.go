package raft

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"time"

	"github.com/jmsadair/raft/internal/errors"
)

const (
	metadataBase    = "metadata.json"
	snapshotBase    = "snapshot.bin"
	snapshotDirBase = "snapshots"
)

// SnapshotMetadata contains all metadata associated with a snapshot.
type SnapshotMetadata struct {
	// The last log index included in the snapshot.
	LastIncludedIndex uint64 `json:"last_included_index"`

	// The last log term included in the snapshot.
	LastIncludedTerm uint64 `json:"last_included_term"`
}

func encodeMetadata(w io.Writer, metadata *SnapshotMetadata) error {
	bytes, err := json.Marshal(metadata)
	if err != nil {
		return err
	}
	if _, err := w.Write(bytes); err != nil {
		return err
	}
	return nil
}

func decodeMetadata(r io.Reader) (SnapshotMetadata, error) {
	data, err := io.ReadAll(r)
	if err != nil {
		return SnapshotMetadata{}, err
	}
	metadata := SnapshotMetadata{}
	if err := json.Unmarshal(data, &metadata); err != nil {
		return metadata, err
	}
	return metadata, nil
}

// SnapshotFile represents a component for reading and writing
// snapshots.
type SnapshotFile interface {
	io.ReadWriteSeeker
	io.Closer

	// Metadata returns the metadata associated with the snapshot file.
	Metadata() SnapshotMetadata

	// Discard deletes the snapshot and its metadata if it has not yet been
	// saved.
	Discard() error
}

// SnapshotStorage represents the component of Raft that manages snapshots created
// by the state machine.
type SnapshotStorage interface {
	// NewSnapshotFile creates a new snapshot file. It is the caller's responsibility to
	// close the file or discard it when they are done with it.
	NewSnapshotFile(lastIncludedIndex, lastIncludedTerm uint64) (SnapshotFile, error)

	// SnapshotFile returns the most recent snapshot file. It is the
	// caller's responsibility to close the file when they are done with it.
	SnapshotFile() (SnapshotFile, error)
}

// snapshotFile implements the SnapshotFile interface.
type snapshotFile struct {
	// The actual directory that will contain the snapshot and its metadata
	// once the snapshot has safely been written to disk.
	dir string

	// The temporary directory that contains the snapshot and its metadata
	// while it is being written.
	tmpDir string

	// The snapshot file.
	file *os.File

	// The metadata associated with the snapshot.
	metadata SnapshotMetadata
}

func (s *snapshotFile) Write(p []byte) (n int, err error) {
	return s.file.Write(p)
}

func (s *snapshotFile) Read(p []byte) (n int, err error) {
	return s.file.Read(p)
}

func (s *snapshotFile) Seek(offset int64, whence int) (int64, error) {
	return s.file.Seek(offset, whence)
}

func (s *snapshotFile) Close() error {
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

	defer func() {
		s.file = nil
	}()

	// Perform an atomic rename of the temporary directory
	// containing the snapshot and its metadata to its
	// permanent name since it is safely on disk now.
	if filepath.Dir(s.file.Name()) == s.tmpDir {
		return os.Rename(s.tmpDir, s.dir)
	}

	return nil
}

func (s *snapshotFile) Discard() error {
	if s.file == nil || filepath.Dir(s.file.Name()) != s.tmpDir {
		return nil
	}
	if err := s.file.Close(); err != nil {
		return err
	}
	s.file = nil
	return os.RemoveAll(s.tmpDir)
}

func (s *snapshotFile) Metadata() SnapshotMetadata {
	return s.metadata
}

// persistentSnapshotStorage is an implementation of the SnapshotStorage interface. This
// implementation is not concurrent safe.
type persistentSnapshotStorage struct {
	// The directory where snapshots are persisted.
	snapshotDir string
}

// NewSnapshotStorage creates a new snapshot storage at the provided path.
func NewSnapshotStorage(path string) (SnapshotStorage, error) {
	snapshotPath := filepath.Join(path, snapshotDirBase)
	if err := os.MkdirAll(snapshotPath, os.ModePerm); err != nil {
		return nil, errors.WrapError(err, "failed to create snapshot directory")
	}
	return &persistentSnapshotStorage{snapshotDir: snapshotPath}, nil
}

func (p *persistentSnapshotStorage) NewSnapshotFile(
	lastIncludedIndex, lastIncludedTerm uint64,
) (SnapshotFile, error) {
	// The temporary directory that will contain the snapshot and its metadata.
	// This directory will be renamed once the snapshot has been safely written to disk.
	tmpDir, err := os.MkdirTemp(p.snapshotDir, "tmp-")
	if err != nil {
		return nil, errors.WrapError(err, "failed to create directory for snapshot")
	}

	// Create the file the snapshot data will be written to.
	dataFile, err := os.Create(filepath.Join(tmpDir, snapshotBase))
	if err != nil {
		return nil, errors.WrapError(err, "failed to create file for snapshot data")
	}

	// Create metadata file and write the metadata to it.
	metadataFile, err := os.Create(filepath.Join(tmpDir, metadataBase))
	if err != nil {
		return nil, errors.WrapError(err, "failed to create file for snapshot metadata")
	}
	metadata := SnapshotMetadata{
		LastIncludedIndex: lastIncludedIndex,
		LastIncludedTerm:  lastIncludedTerm,
	}
	if err := encodeMetadata(metadataFile, &metadata); err != nil {
		return nil, errors.WrapError(err, "failed to encode snapshot metadata")
	}
	if err := metadataFile.Sync(); err != nil {
		return nil, errors.WrapError(err, "failed to sync snapshot metadata file")
	}
	if err := metadataFile.Close(); err != nil {
		return nil, errors.WrapError(err, "failed to close snapshot metadata file")
	}

	return &snapshotFile{
		dir:      filepath.Join(p.snapshotDir, buildDirectoryBase()),
		tmpDir:   tmpDir,
		file:     dataFile,
		metadata: metadata,
	}, nil
}

func (p *persistentSnapshotStorage) SnapshotFile() (SnapshotFile, error) {
	// Retrieve the most recent snapshot if there is one.
	dirNames, err := p.directories()
	if err != nil {
		return nil, err
	}
	if len(dirNames) == 0 {
		return nil, nil
	}
	dirName := dirNames[len(dirNames)-1]

	// Make the file containing the snapshot data prepared for reading.
	dataFile, err := os.Open(filepath.Join(dirName, snapshotBase))
	if err != nil {
		return nil, errors.WrapError(err, "failed to open snapshot data file")
	}

	// Read the metadata from the metadata file.
	metadataFile, err := os.Open(filepath.Join(dirName, metadataBase))
	if err != nil {
		return nil, errors.WrapError(err, "failed to open snapshot metadata file")
	}
	metadata, err := decodeMetadata(metadataFile)
	if err != nil {
		return nil, errors.WrapError(err, "failed to decode snapshot metadata")
	}

	return &snapshotFile{
		file:     dataFile,
		metadata: metadata,
	}, nil
}

func (p *persistentSnapshotStorage) directories() ([]string, error) {
	entries, err := os.ReadDir(p.snapshotDir)
	if err != nil {
		return nil, errors.WrapError(err, "failed to read snapshot directory entries")
	}

	// Filter out any entries that are not snapshot entries.
	dirNames := make([]string, 0, len(entries))
	pattern, err := regexp.Compile(`snapshot-(\d+)$`)
	if err != nil {
		return nil, errors.WrapError(err, "failed to compile snapshot directory regexp")
	}
	for _, entry := range entries {
		if entry.IsDir() && pattern.MatchString(entry.Name()) {
			dirNames = append(dirNames, filepath.Join(p.snapshotDir, entry.Name()))
		}
	}

	// Sort the entries by their timestamp.
	sort.Slice(dirNames, func(i, j int) bool {
		var timestamp1 int64
		var timestamp2 int64
		pattern := "snapshot-%d"
		fmt.Sscanf(dirNames[i], pattern, &timestamp1)
		fmt.Sscanf(dirNames[j], pattern, &timestamp2)
		return timestamp1 < timestamp2
	})

	return dirNames, nil
}

func buildDirectoryBase() string {
	now := time.Now().UnixNano()
	return fmt.Sprintf("snapshot-%v", now)
}
