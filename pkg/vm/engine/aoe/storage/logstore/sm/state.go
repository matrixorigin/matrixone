package sm

import "sync/atomic"

type ClosedState struct {
	closed int32
}

func (s *ClosedState) TryClose() bool {
	return atomic.CompareAndSwapInt32(&s.closed, int32(0), int32(1))
}

func (s *ClosedState) IsClosed() bool {
	return atomic.LoadInt32(&s.closed) == int32(1)
}
