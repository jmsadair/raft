package raft

import "time"

// lease represents a lease held by a leader in raft.
// The lease is considered valid until its expiration time, which is
// calculated as the sum of the current time and the lease's duration
// at the moment of lease renewal.
//
// The leader is allowed to serve read-only requests as long as its
// lease is valid. When a lease expires, the leader must stop serving
// read-only requests until the lease is renewed.
//
// Renewing the lease extends its validity by the duration period
// from the current time. To check if a lease is valid at a given
// moment, use the isValid method.
type lease struct {
	// Time at which the lease expires.
	expiration time.Time

	// The duration of a lease (typically an election timeout).
	duration time.Duration
}

// newLease creates a new instance of a lease that will be valid
// for the provided time duration. The newly created lease will not
// initially be valid, it must be renewed first.
func newLease(duration time.Duration) *lease {
	return &lease{duration: duration, expiration: time.Now()}
}

// renew resets the expiration time of the lease to the current
// time plus the duration of the lease.
func (l *lease) renew() {
	l.expiration = time.Now().Add(l.duration)
}

// isValid returns true if the current time is less than
// the expiration time of the lease and false otherwise.
func (l *lease) isValid() bool {
	return time.Now().Before(l.expiration)
}
