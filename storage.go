package raft

// Storage supports the durable storage of key-value pairs.
// Must be concurrent safe.
type Storage interface {
	// Set sets the value associated with the key.
	Set(key []byte, value []byte) error

	// Get retrieves the value associated with the key,
	// if the key exists.
	Get(key []byte) ([]byte, error)
}
