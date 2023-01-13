package errors

import (
	"fmt"
)

type RaftError struct {
	Inner   error
	Message string
}

func WrapError(inner error, messagef string, messageArgs ...interface{}) error {
	return &RaftError{Inner: inner, Message: fmt.Sprintf(messagef, messageArgs...)}
}

func (e RaftError) Error() string {
	return e.Message
}
