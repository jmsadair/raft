package raft

import "github.com/jmsadair/raft/internal/errors"

type SnapshotStorageMock struct {
	snapshots []Snapshot
}

func NewSnapshotStorageMock() *SnapshotStorageMock {
	return &SnapshotStorageMock{snapshots: make([]Snapshot, 0)}
}

func (s *SnapshotStorageMock) LastSnapshot() (Snapshot, error) {
	if len(s.snapshots) == 0 {
		return Snapshot{}, errors.WrapError(nil, "failed to get last snapshot: no snapshots exist")
	}
	return s.snapshots[len(s.snapshots)-1], nil
}

func (s *SnapshotStorageMock) SaveSnapshot(snapshot *Snapshot) error {
	s.snapshots = append(s.snapshots, *snapshot)
	return nil
}

func (s *SnapshotStorageMock) ListSnapshots() ([]Snapshot, error) {
	return s.snapshots, nil
}
