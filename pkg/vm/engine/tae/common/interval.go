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

package common

import (
	"fmt"
	"sync/atomic"
	"unsafe"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
)

type ClosedInterval struct {
	Start, End uint64
}

const (
	CloseIntervalSize int64 = int64(unsafe.Sizeof(ClosedInterval{}))
)

func EncodeCloseInterval(i *ClosedInterval) []byte {
	return unsafe.Slice((*byte)(unsafe.Pointer(i)), CloseIntervalSize)
}

func (i *ClosedInterval) String() string {
	return fmt.Sprintf("[%d,%d]", i.Start, i.End)
}

func (i *ClosedInterval) Append(id uint64) error {
	if i.Start == i.End && i.Start == 0 {
		i.Start = id
		i.End = id
		return nil
	}
	if id != i.End+1 {
		// logutil.Debugf("invalid interval %v %v", i, id)
		return moerr.NewInternalErrorNoCtx("invalid interval")
	}
	i.End = id
	return nil
}

func (i *ClosedInterval) Contains(o ClosedInterval) bool {
	if i == nil {
		return false
	}
	return i.Start <= o.Start && i.End >= o.End
}

func (i *ClosedInterval) TryMerge(o ClosedInterval) bool {
	if o.Start > i.End+1 || i.Start > o.End+1 {
		return false
	}
	if i.Start > o.Start {
		i.Start = o.Start
	}
	if i.End < o.End {
		i.End = o.End
	}

	return true
}

func (i *ClosedInterval) IsCoveredByInt(idx uint64) bool {
	return idx >= i.End
}

func (i *ClosedInterval) AtomicUpdateEnd(v uint64) {
	atomic.StoreUint64(&i.End, v)
}

func (i *ClosedInterval) LT(o *ClosedInterval) bool {
	return i.End < o.Start
}

func (i *ClosedInterval) GT(o *ClosedInterval) bool {
	return i.Start < o.End
}
