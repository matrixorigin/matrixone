package types

import (
	"github.com/matrixorigin/matrixone/pkg/encoding"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/txn/clock"
	"math"
	"sync/atomic"
)

var (
	//just for test
	GlobalTsAlloctor *TsAlloctor
)

func init() {
	GlobalTsAlloctor = NewTsAlloctor(MockClock(1))
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

	s1 := ts[4:12]
	copy(s1, encoding.EncodeInt64(now.PhysicalTime))

	s2 := ts[:4]
	copy(s2, encoding.EncodeUint32(now.LogicalTime))

	return ts
}

func (alloc *TsAlloctor) Get() TS {
	//now, _ := alloc.clock.Now()

	var ts TS
	s1 := ts[4:12]
	//copy(s1, encoding.EncodeInt64(now.Prev().PhysicalTime))
	copy(s1, encoding.EncodeInt64(alloc.clock.Get().PhysicalTime))

	s2 := ts[:4]
	//copy(s2, encoding.EncodeUint32(now.Prev().LogicalTime))
	copy(s2, encoding.EncodeUint32(alloc.clock.Get().LogicalTime))

	return ts

}

func (alloc *TsAlloctor) SetStart(start TS) {
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
func NextGlobalTsForTest() TS {
	return GlobalTsAlloctor.Alloc()
}
