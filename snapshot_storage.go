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

	"github.com/jmsadair/raft/internal/fileutil"
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

	// The most up-to-date configuration in the snapshot.
	Configuration []byte `json:"configuration"`
}

func encodeMetadata(w io.Writer, metadata *SnapshotMetadata) error {
	data, err := json.Marshal(metadata)
	if err != nil {
		return fmt.Errorf("could not marshal metadata: %w", err)
	}
	if _, err := w.Write(data); err != nil {
		return fmt.Errorf("could not write metadata: %w", err)
	}
	return nil
}

func decodeMetadata(r io.Reader) (SnapshotMetadata, error) {
	data, err := io.ReadAll(r)
	if err != nil {
		return SnapshotMetadata{}, fmt.Errorf("could not read metadata: %w", err)
	}
	metadata := SnapshotMetadata{}
	if err := json.Unmarshal(data, &metadata); err != nil {
		return metadata, fmt.Errorf("could not unmarshal metadata: %w", err)
	}
	return metadata, nil
}

// SnapshotFile represents a component for reading and writing snapshots.
type SnapshotFile interface {
	io.ReadWriteSeeker
	io.Closer

	// Metadata returns the metadata associated with the snapshot file.
	Metadata() SnapshotMetadata

	// Discard deletes the snapshot and its metadata if it is incomplete.
	Discard() error
}

// SnapshotStorage represents the component of Raft that manages snapshots created
// by the state machine.
type SnapshotStorage interface {
	// NewSnapshotFile creates a new snapshot file. It is the caller's responsibility to
	// close the file or discard it when they are done with it.
	NewSnapshotFile(
		lastIncludedIndex uint64,
		lastIncludedTerm uint64,
		configuration []byte,
	) (SnapshotFile, error)

	// SnapshotFile returns the most recent snapshot file. It is the
	// caller's responsibility to close the file when they are done with it.
	SnapshotFile() (SnapshotFile, error)
}

// snapshotFile implements the SnapshotFile interface.
type snapshotFile struct {
	io.ReadWriteSeeker

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

func (s *snapshotFile) Close() error {
	if s.file == nil {
		return nil
	}

	defer func() {
		s.file = nil
	}()

	// If this file is in a temporary directory and a failure
	// occurs, delete the directory.
	isTmpDir := filepath.Dir(s.file.Name()) == s.tmpDir
	success := false
	defer func() {
		if isTmpDir && !success {
			_ = os.RemoveAll(s.tmpDir)
		}
	}()

	// Ensure any written data is on disk.
	if err := s.file.Sync(); err != nil {
		return fmt.Errorf("could not sync file: %w", err)
	}
	if err := s.file.Close(); err != nil {
		return fmt.Errorf("could not close file: %w", err)
	}

	// Perform an atomic rename of the temporary directory
	// containing the snapshot and its metadata to its
	// permanent name since it is safely on disk now.
	if isTmpDir {
		if err := os.Rename(s.tmpDir, s.dir); err != nil {
			return fmt.Errorf("could not perform rename: %w", err)
		}
	}

	// If there was a rename, it was successful.
	success = true

	return nil
}

func (s *snapshotFile) Discard() error {
	if s.file == nil || filepath.Dir(s.file.Name()) != s.tmpDir {
		return nil
	}
	if err := s.file.Close(); err != nil {
		return fmt.Errorf("could not close file: %w", err)
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

// NewSnapshotStorage creates a new SnapshotStorage instance.
//
// Snapshots will be stored at path/snapshots. Any directories
// on the path that do not exist will be created. Each snapshot
// that is created will have its own directory that is named using
// a timestamp taken at the time of its creation. Each of these
// directories will contain two separate files - one for the content of
// the snapshot and one for its metadata.
func NewSnapshotStorage(path string) (SnapshotStorage, error) {
	snapshotPath := filepath.Join(path, snapshotDirBase)
	if err := os.MkdirAll(snapshotPath, os.ModePerm); err != nil {
		return nil, fmt.Errorf("could not create snapshot directory for snapshot storage: %w", err)
	}

	// Delete any temporary files or directories that may have been partially written before a crash.
	if err := fileutil.RemoveTmpFiles(snapshotPath); err != nil {
		return nil, fmt.Errorf("could not remove temporary files: %w", err)
	}

	return &persistentSnapshotStorage{snapshotDir: snapshotPath}, nil
}

func (p *persistentSnapshotStorage) NewSnapshotFile(
	lastIncludedIndex uint64, lastIncludedTerm uint64, configuration []byte,
) (SnapshotFile, error) {
	// The temporary directory that will contain the snapshot and its metadata.
	// This directory will be renamed once the snapshot has been safely written to disk.
	tmpDir, err := os.MkdirTemp(p.snapshotDir, "tmp-snapshot")
	if err != nil {
		return nil, fmt.Errorf("could not  directory for snapshot: %w", err)
	}

	// Create the file the snapshot data will be written to.
	dataFile, err := os.Create(filepath.Join(tmpDir, snapshotBase))
	if err != nil {
		return nil, fmt.Errorf("could not create file for snapshot data: %w", err)
	}

	// Create metadata file and write the metadata to it.
	metadataFile, err := os.Create(filepath.Join(tmpDir, metadataBase))
	if err != nil {
		return nil, fmt.Errorf("could not create file for snapshot metadata: %w", err)
	}
	metadata := SnapshotMetadata{
		LastIncludedIndex: lastIncludedIndex,
		LastIncludedTerm:  lastIncludedTerm,
		Configuration:     configuration,
	}
	if err := encodeMetadata(metadataFile, &metadata); err != nil {
		return nil, fmt.Errorf("could not encode snapshot metadata: %w", err)
	}
	if err := metadataFile.Sync(); err != nil {
		return nil, fmt.Errorf("could not sync snapshot metadata file: %w", err)
	}
	if err := metadataFile.Close(); err != nil {
		return nil, fmt.Errorf("could not close snapshot metadata file: %w", err)
	}

	return &snapshotFile{
		ReadWriteSeeker: dataFile,
		dir:             filepath.Join(p.snapshotDir, buildDirectoryBase()),
		tmpDir:          tmpDir,
		file:            dataFile,
		metadata:        metadata,
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
		return nil, fmt.Errorf("could not open snapshot data file: %w", err)
	}

	// Read the metadata from the metadata file.
	metadataFile, err := os.Open(filepath.Join(dirName, metadataBase))
	if err != nil {
		return nil, fmt.Errorf("could not open snapshot metadata file: %w", err)
	}
	metadata, err := decodeMetadata(metadataFile)
	if err != nil {
		return nil, fmt.Errorf("could not decode snapshot metadata: %w", err)
	}

	return &snapshotFile{
		ReadWriteSeeker: dataFile,
		file:            dataFile,
		metadata:        metadata,
	}, nil
}

func (p *persistentSnapshotStorage) directories() ([]string, error) {
	entries, err := os.ReadDir(p.snapshotDir)
	if err != nil {
		return nil, fmt.Errorf("could not read snapshot directory entries: %w", err)
	}

	// Filter out any entries that are not snapshot entries.
	dirNames := make([]string, 0, len(entries))
	pattern, err := regexp.Compile(`snapshot-(\d+)$`)
	if err != nil {
		return nil, fmt.Errorf("could not compile snapshot directory regexp: %w", err)
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
