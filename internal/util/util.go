package util

import (
	"math/rand"
	"os"
	"path/filepath"
	"strings"
	"time"

	"golang.org/x/exp/constraints"
)

// Min returns the smaller of the two provided values.
// It uses generic type T, which must satisfy the ordered constraints (e.g., integers, floats).
func Min[T constraints.Ordered](a, b T) T {
	if a < b {
		return a
	}
	return b
}

// Max returns the larger of the two provided values.
// It uses generic type T, which must satisfy the ordered constraints (e.g., integers, floats).
func Max[T constraints.Ordered](a, b T) T {
	if a > b {
		return a
	}
	return b
}

// RandomTimeout generates a random duration between the provided min and max durations.
func RandomTimeout(min time.Duration, max time.Duration) time.Duration {
	n := rand.Int63n(max.Milliseconds()-min.Milliseconds()) + min.Milliseconds()
	return time.Duration(n)
}

// RandomInt generates a random integer between the provided min and max integers (inclusive of min, exclusive of max).
func RandomInt(min int, max int) int {
	return min + rand.Intn(max-min)
}

// RemoveTmpFiles will remove all files in the root directory
// and its sub-directories with a name that has a 'tmp' prefix.
func RemoveTmpFiles(rootDir string) error {
	return filepath.Walk(rootDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !strings.HasPrefix(info.Name(), "tmp") {
			return nil
		}
		return os.RemoveAll(path)
	})
}
