package errors

import (
	"fmt"

	"github.com/pkg/errors"
)

type RaftError struct {
	Inner   error
	Message string
}

func New(format string, args ...interface{}) *RaftError {
	return &RaftError{Message: fmt.Sprintf(format, args...)}
}

func WrapError(inner error, messagef string, messageArgs ...interface{}) *RaftError {
	return &RaftError{
		Inner:   errors.WithStack(inner),
		Message: fmt.Sprintf(messagef, messageArgs...),
	}
}

func (e *RaftError) UnwrapError() error {
	return e.Inner
}

func (e *RaftError) Error() string {
	return e.Message
}
