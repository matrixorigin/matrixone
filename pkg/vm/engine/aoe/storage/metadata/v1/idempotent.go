// Copyright 2021 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package metadata

import (
	"sync/atomic"
	"unsafe"
)

type IdempotentChecker struct {
	IdempotentIndex *LogIndex
	MaxIndex        *LogIndex
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

func (checker *IdempotentChecker) InitMaxIndex(index *LogIndex) {
	checker.MaxIndex = index
}

func (checker *IdempotentChecker) InReplaying(index *LogIndex) bool {
	if checker.MaxIndex == nil {
		return false
	}
	return checker.MaxIndex.Compare(index) >= 0
}
