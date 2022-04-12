package common

import (
	"errors"
	"sync/atomic"
)

var (
	ClosedErr = errors.New("closed")
)

type Closable interface {
	IsClosed() bool
	TryClose() bool
}

type ClosedState struct {
	closed int32
}

func (c *ClosedState) IsClosed() bool {
	return atomic.LoadInt32(&c.closed) == int32(1)
}

func (c *ClosedState) TryClose() bool {
	if !atomic.CompareAndSwapInt32(&c.closed, int32(0), int32(1)) {
		return false
	}
	return true
}
