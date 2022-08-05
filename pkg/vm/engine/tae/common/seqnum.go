// Copyright 2022 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package common

import (
	"github.com/matrixorigin/matrixone/pkg/encoding"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/txn/clock"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/types"
	"math"
	"sync/atomic"
)

var (
	GlobalSeqNum uint64 = 0
	//just for test
	GlobalTsAlloctor *TsAlloctor
)

func init() {
	GlobalTsAlloctor = NewTsAlloctor(MockClock(1))
}
func NextGlobalSeqNum() uint64 {
	return atomic.AddUint64(&GlobalSeqNum, uint64(1))
}

func GetGlobalSeqNum() uint64 {
	return atomic.LoadUint64(&GlobalSeqNum)
}

type IdAlloctor struct {
	id uint64
}

func NewIdAlloctor(from uint64) *IdAlloctor {
	if from == 0 {
		panic("should not be 0")
	}

	return &IdAlloctor{
		id: from - 1,
	}
}

func (alloc *IdAlloctor) Alloc() uint64 {
	return atomic.AddUint64(&alloc.id, uint64(1))
}

func (alloc *IdAlloctor) Get() uint64 {
	return atomic.LoadUint64(&alloc.id)
}

func (alloc *IdAlloctor) SetStart(start uint64) {
	alloc.id = start
}

type TsAlloctor struct {
	clock clock.Clock
}

func NewTsAlloctor(clock clock.Clock) *TsAlloctor {
	return &TsAlloctor{clock: clock}
}

func (alloc *TsAlloctor) Alloc() types.TS {
	now, _ := alloc.clock.Now()
	var ts types.TS

	s1 := ts[4:12]
	copy(s1, encoding.EncodeInt64(now.PhysicalTime))

	s2 := ts[:4]
	copy(s2, encoding.EncodeUint32(now.LogicalTime))

	return ts
}

func (alloc *TsAlloctor) Get() types.TS {
	//now, _ := alloc.clock.Now()

	var ts types.TS
	s1 := ts[4:12]
	//copy(s1, encoding.EncodeInt64(now.Prev().PhysicalTime))
	copy(s1, encoding.EncodeInt64(alloc.clock.Get().PhysicalTime))

	s2 := ts[:4]
	//copy(s2, encoding.EncodeUint32(now.Prev().LogicalTime))
	copy(s2, encoding.EncodeUint32(alloc.clock.Get().LogicalTime))

	return ts

}

func (alloc *TsAlloctor) SetStart(start types.TS) {
	//var ts timestamp.Timestamp
	if start.Greater(alloc.Get()) {
		alloc.clock.Update(timestamp.Timestamp{PhysicalTime: encoding.DecodeInt64(start[4:12]),
			LogicalTime: encoding.DecodeUint32(start[:4])})
	}
}

//start >= 1
func MockClock(start int64) clock.Clock {
	ts := start
	return clock.NewHLCClock(func() int64 {
		return atomic.AddInt64(&ts, 1)
	}, math.MaxInt64)
}
func NextGlobalTsForTest() types.TS {
	return GlobalTsAlloctor.Alloc()
}
