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

package types

import (
	"github.com/matrixorigin/matrixone/pkg/encoding"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/txn/clock"
	"sync/atomic"
	"time"
)

var (
	//just for test
	GlobalTsAlloctor *TsAlloctor
)

func init() {
	GlobalTsAlloctor = NewTsAlloctor(NewMockHLCClock(1))
}

type TsAlloctor struct {
	clock clock.Clock
}

func NewTsAlloctor(clock clock.Clock) *TsAlloctor {
	return &TsAlloctor{clock: clock}
}

func (alloc *TsAlloctor) Alloc() TS {
	now, _ := alloc.clock.Now()
	var ts TS
	copy(ts[4:12], encoding.EncodeInt64(now.PhysicalTime))
	copy(ts[:4], encoding.EncodeUint32(now.LogicalTime))
	return ts
}

// TODO::will be removed
func (alloc *TsAlloctor) Get() TS {
	if mockClock, ok := alloc.clock.(*MockHLCClock); ok {
		var ts TS
		copy(ts[4:12], encoding.EncodeInt64(mockClock.Get().PhysicalTime))
		//copy(ts[:4], encoding.EncodeUint32(mockClock.Get().LogicalTime))
		return ts
	}
	panic("HLCClock does not support Get()")
}

func (alloc *TsAlloctor) SetStart(start TS) {
	//if start.Greater(alloc.Get()) {
	alloc.clock.Update(timestamp.Timestamp{PhysicalTime: encoding.DecodeInt64(start[4:12]),
		LogicalTime: encoding.DecodeUint32(start[:4])})
	//}
}

func NextGlobalTsForTest() TS {
	return GlobalTsAlloctor.Alloc()
}

type MockHLCClock struct {
	pTime int64
	//always be 0
	//lTime     uint32
	maxOffset time.Duration
}

// Just for test , start >= 1
func NewMockHLCClock(start int64) *MockHLCClock {
	return &MockHLCClock{pTime: start}
}

func (c *MockHLCClock) Now() (timestamp.Timestamp, timestamp.Timestamp) {
	now := timestamp.Timestamp{
		PhysicalTime: atomic.AddInt64(&c.pTime, 1),
		//LogicalTime:  c.lTime,
	}
	return now, timestamp.Timestamp{PhysicalTime: now.PhysicalTime + int64(c.maxOffset)}
}

// TODO::will be removed
func (c *MockHLCClock) Get() timestamp.Timestamp {
	return timestamp.Timestamp{
		PhysicalTime: atomic.LoadInt64(&c.pTime),
		//LogicalTime:  c.lTime,
	}
}

func (c *MockHLCClock) Update(m timestamp.Timestamp) {
	atomic.StoreInt64(&c.pTime, m.PhysicalTime)
	//atomic.StoreUint32(&c.lTime, m.LogicalTime)
}

func (c *MockHLCClock) HasNetworkLatency() bool {
	return false
}

func (c *MockHLCClock) MaxOffset() time.Duration {
	return c.maxOffset
}
