package raft

// Storage supports the durable storage of key-value pairs.
type Storage interface {
	// Set sets the value associated with the key.
	Set(key []byte, value []byte) error

	// SetUint64 sets the value associated with the key.
	SetUint64(key []byte, value uint64) error

	// Get retrieves the value associated with the key if it
	// exists. If the key does not exist, returns an empty byte
	// array.
	Get(key []byte) ([]byte, error)

	// GetUint64 gets the value associated with the key as a
	// 64-bit unsigned integer. If the key does not exist,
	// returns 0.
	GetUint64(key []byte) (uint64, error)
}
