package numeric

import "golang.org/x/exp/constraints"

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
