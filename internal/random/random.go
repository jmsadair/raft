package random

import (
	"math/rand"
	"time"
)

// RandomTimeout generates a random duration between the provided min and max durations.
func RandomTimeout(min time.Duration, max time.Duration) time.Duration {
	n := rand.Int63n(max.Milliseconds()-min.Milliseconds()) + min.Milliseconds()
	return time.Duration(n)
}

// RandomInt generates a random integer between the provided min and max integers (inclusive of min, exclusive of max).
func RandomInt(min int, max int) int {
	return min + rand.Intn(max-min)
}
