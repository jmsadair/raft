package util

import (
	"math/rand"
	"time"

	"golang.org/x/exp/constraints"
)

func Min[T constraints.Ordered](a, b T) T {
	if a < b {
		return a
	}
	return b
}

func Max[T constraints.Ordered](a, b T) T {
	if a > b {
		return a
	}
	return b
}

func RandomTimeout(min time.Duration, max time.Duration) <-chan time.Time {
	rand.Seed(time.Now().UnixNano())
	n := rand.Int63n(max.Milliseconds()-min.Milliseconds()) + min.Milliseconds()
	return time.After(time.Duration(n) * time.Millisecond)
}
