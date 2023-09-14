module github.com/jmsadair/raft

go 1.19

require (
	github.com/fortytw2/leaktest v1.3.0
	github.com/pkg/errors v0.9.1
	github.com/stretchr/testify v1.8.4
	golang.org/x/exp v0.0.0-20230108222341-4b8118a2686a
	google.golang.org/grpc v1.58.1
	google.golang.org/protobuf v1.31.0
)

require (
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/golang/protobuf v1.5.3 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	golang.org/x/net v0.12.0 // indirect
	golang.org/x/sys v0.10.0 // indirect
	golang.org/x/text v0.11.0 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20230711160842-782d3b101e98 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)

retract v0.1.0 // Failing test suite.
