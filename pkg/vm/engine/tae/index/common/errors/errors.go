package errors

import "errors"

var (
	ErrTypeNotSupported = errors.New("index: invalid column type")
	ErrKeyNotFound      = errors.New("index: key not found")
	ErrKeyDuplicate     = errors.New("index: duplicate key occurred")
	ErrTypeMismatch     = errors.New("index: type mismatch")
)
