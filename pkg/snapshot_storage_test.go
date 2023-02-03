package raft

import (
	"sync"

	"github.com/jmsadair/raft/internal/errors"
)

type SnapshotStorageMock struct {
	snapshots []Snapshot
	mu        sync.Mutex
}

func NewSnapshotStorageMock() *SnapshotStorageMock {
	return &SnapshotStorageMock{snapshots: make([]Snapshot, 0)}
}

func (s *SnapshotStorageMock) LastSnapshot() (Snapshot, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.snapshots) == 0 {
		return Snapshot{}, errors.WrapError(nil, "failed to get last snapshot: no snapshots exist")
	}
	return s.snapshots[len(s.snapshots)-1], nil
}

func (s *SnapshotStorageMock) SaveSnapshot(snapshot *Snapshot) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.snapshots = append(s.snapshots, *snapshot)
	return nil
}

func (s *SnapshotStorageMock) ListSnapshots() ([]Snapshot, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.snapshots, nil
}
