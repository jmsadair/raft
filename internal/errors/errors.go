package errors

import (
	"fmt"

	"github.com/pkg/errors"
)

// RaftError represents a custom error type for Raft with optional inner error and a custom message.
type RaftError struct {
	Inner   error
	Message string
}

// New creates a new instance of RaftError with only a formatted message.
func New(format string, args ...interface{}) *RaftError {
	return &RaftError{Message: fmt.Sprintf(format, args...)}
}

// WrapError creates a new instance of RaftError with an inner error and a formatted message.
func WrapError(inner error, messagef string, messageArgs ...interface{}) *RaftError {
	return &RaftError{
		Inner:   errors.WithStack(inner),
		Message: fmt.Sprintf(messagef, messageArgs...),
	}
}

// UnwrapError returns the inner error of the RaftError.
// If there's no inner error, it will return nil.
func (e *RaftError) UnwrapError() error {
	return e.Inner
}

// Error implements the error interface for RaftError.
// It returns the error message along with the stack trace of the RaftError.
func (e *RaftError) Error() string {
	if e.Inner != nil {
		return fmt.Sprintf("%s\n%s", e.Message, e.Inner.Error())
	}
	return e.Message
}
