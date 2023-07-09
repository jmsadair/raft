test-snapshotting:
	SNAPSHOTS=true SNAPSHOT_SIZE=25 go test . -count=1 -failfast -v -race -timeout=10m

test-snapshotting-cov:
	SNAPSHOTS=true SNAPSHOT_SIZE=25 go test . -count=1 -failfast -v -race -timeout=10m -coverprofile=coverage.out

test: 
	SNAPSHOTS=false go test . -count=1 -failfast -v -race -timeout=10m

test-cov:
	SNAPSHOTS=false go test . -count=1 -failfast -v -race -timeout=10m -coverprofile=coverage.out

proto:
	protoc --go_out=. --go_opt=paths=source_relative     --go-grpc_out=. --go-grpc_opt=paths=source_relative     internal/protobuf/*.proto