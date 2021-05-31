package md

import (
	"errors"
	"fmt"
	"sync/atomic"
	"time"
)

func NowMicro() int64 {
	return (time.Now().UnixNano() / 1000)
}

func NewTimeStamp() *TimeStamp {
	ts := &TimeStamp{
		CreatedOn: NowMicro(),
	}
	return ts
}

func (ts *TimeStamp) IsDeleted() bool {
	val := atomic.LoadInt64(&(ts.DeltetedOn))
	if val == 0 {
		return false
	}
	return true
}

func (ts *TimeStamp) Deltete(t int64) error {
	val := atomic.LoadInt64(&(ts.DeltetedOn))
	if val != 0 {
		return errors.New("already deleted")
	}
	ok := atomic.CompareAndSwapInt64(&(ts.DeltetedOn), val, t)
	if !ok {
		return errors.New("already deleted")
	}
	return nil
}

func (ts *TimeStamp) Select(t int64) bool {
	delon := atomic.LoadInt64(&(ts.DeltetedOn))
	if delon != 0 {
		if delon <= t {
			return false
		}
	}
	return ts.CreatedOn < t
}

func (ts *TimeStamp) String() string {
	s := fmt.Sprintf("ts(%d,%d,%d)", ts.CreatedOn, ts.UpdatedOn, ts.DeltetedOn)
	return s
}

func (state *BoundSate) GetBoundState() BoundSate {
	return *state
}

func (state *BoundSate) Detach() error {
	if *state == Detatched || *state == STANDLONE {
		return errors.New("detatched or stalone already")
	}
	*state = Detatched
	return nil
}

func (state *BoundSate) Attach() error {
	if *state == Attached {
		return errors.New("alreay attached")
	}
	*state = Attached
	return nil
}

func (seq *Sequence) GetSegmentID() uint64 {
	return atomic.AddUint64(&(seq.NextSegmentID), uint64(1))
}

func (seq *Sequence) GetBlockID() uint64 {
	return atomic.AddUint64(&(seq.NextBlockID), uint64(1))
}

func (seq *Sequence) GetTableID() uint64 {
	return atomic.AddUint64(&(seq.NextTableID), uint64(1))
}
