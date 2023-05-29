# Run Raft tests with snapshots enabled and a max snapshot size of 25.
test-raft-snapshotting:
	SNAPSHOTS=true SNAPSHOT_SIZE=25 go test ./pkg -count=1 -failfast -v -race -timeout=10m

# Run Raft tests with snapshots enabled, a max snapshot size of 25, and coverage.
test-raft-snapshotting-cov:
	SNAPSHOTS=true SNAPSHOT_SIZE=25 go test ./pkg -count=1 -failfast -v -race -timeout=10m -coverprofile=coverage.out

# Run Raft tests without snapshots enabled.
test-raft: 
	SNAPSHOTS=false go test ./pkg -count=1 -failfast -v -race -timeout=10m

# Run Raft tests without snapshots enabled and coverage.
test-raft-cov:
	SNAPSHOTS=false go test ./pkg -count=1 -failfast -v -race -timeout=10m -coverprofile=coverage.out