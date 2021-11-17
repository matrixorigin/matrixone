package db

import (
	"errors"

	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/metadata/v1"
)

var (
	ErrClosed            = errors.New("aoe: closed")
	ErrUnsupported       = errors.New("aoe: unsupported")
	ErrNotFound          = errors.New("aoe: notfound")
	ErrUnexpectedWalRole = errors.New("aoe: unexpected wal role setted")
	ErrTimeout           = errors.New("aoe: timeout")
	ErrStaleErr          = errors.New("aoe: stale")
	ErrIdempotence       = metadata.IdempotenceErr
	ErrResourceDeleted   = errors.New("aoe: resource is deleted")
)
