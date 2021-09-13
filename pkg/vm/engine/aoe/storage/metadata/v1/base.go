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

// func (ts *TimeStamp) IsDeleted() bool {
// 	val := atomic.LoadInt64(&(ts.DeltetedOn))
// 	if val == 0 {
// 		return false
// 	}
// 	return true
// }

func (ts *TimeStamp) Delete(t int64) error {
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

func (ts *TimeStamp) IsDeleted(t int64) bool {
	delon := atomic.LoadInt64(&(ts.DeltetedOn))
	if delon != 0 {
		if delon <= t {
			return true
		}
	}
	return false
}

func (ts *TimeStamp) IsCreated(t int64) bool {
	return ts.CreatedOn < t
}

func (ts *TimeStamp) Select(t int64) bool {
	if ts.IsDeleted(t) {
		return false
	}
	return ts.IsCreated(t)
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
		panic(fmt.Sprintf("detatched or stalone already: %d", *state))
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

func (seq *Sequence) GetPartitionID() uint64 {
	return atomic.AddUint64(&(seq.NextPartitionID), uint64(1))
}

func (seq *Sequence) GetIndexID() uint64 {
	return atomic.AddUint64(&(seq.NextIndexID), uint64(1))
}
