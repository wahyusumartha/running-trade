package redis

import (
	"errors"
	"fmt"
)

var (
	// ErrKeyNotFound indicates that the requested key does not exists
	ErrKeyNotFound = errors.New("key not found")

	// ErrConnectionFailed indicates a connection issue with redis server
	ErrConnectionFailed = errors.New("redis connection failed")

	// ErrInvalidOperation indicates an invalid operation for the current state
	ErrInvalidOperation = errors.New("invalid redis operation")
)

type StorageError struct {
	Op  string
	Key string
	Err error
}

func (e *StorageError) Error() string {
	if e.Key != "" {
		return fmt.Sprintf("%s %s: %v", e.Op, e.Key, e.Err)
	}

	return fmt.Sprintf("%s: %v", e.Op, e.Err)
}

func (e *StorageError) Unwrap() error {
	return e.Err
}

func NewStorageError(
	op, key string,
	err error,
) *StorageError {
	return &StorageError{
		Op:  op,
		Key: key,
		Err: err,
	}
}
