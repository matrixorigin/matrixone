package metadata

import (
	"sync/atomic"
	"unsafe"
)

type IdempotentChecker struct {
	IdempotentIndex *LogIndex
}

func (checker *IdempotentChecker) GetIdempotentIndex() *LogIndex {
	ptr := atomic.LoadPointer((*unsafe.Pointer)(unsafe.Pointer(&checker.IdempotentIndex)))
	if ptr == nil {
		return nil
	}
	return (*LogIndex)(ptr)
}

func (checker *IdempotentChecker) ConsumeIdempotentIndex(index *LogIndex) (*LogIndex, bool) {
	curr := checker.GetIdempotentIndex()
	if curr == nil {
		return nil, true
	}
	ok := false
	comp := curr.CompareID(index)
	if comp < 0 {
		checker.ResetIdempotentIndex()
		curr = nil
		ok = true
	} else if comp == 0 {
		checker.ResetIdempotentIndex()
		ok = true
	}
	return curr, ok
}

func (checker *IdempotentChecker) ResetIdempotentIndex() {
	ptr := atomic.LoadPointer((*unsafe.Pointer)(unsafe.Pointer(&checker.IdempotentIndex)))
	if ptr == nil {
		panic("logic error")
	}
	var netIndex *LogIndex
	nptr := (*unsafe.Pointer)(unsafe.Pointer(&netIndex))
	if !atomic.CompareAndSwapPointer((*unsafe.Pointer)(unsafe.Pointer(&checker.IdempotentIndex)), ptr, *nptr) {
		panic("logic error")
	}
}

func (checker *IdempotentChecker) InitIdempotentIndex(index *LogIndex) {
	checker.IdempotentIndex = index
}
