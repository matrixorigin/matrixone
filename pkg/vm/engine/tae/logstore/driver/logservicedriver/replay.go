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

package logservicedriver

import (
	"math"

	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logstore/driver"
)

type replayer struct {
	readMaxSize int

	truncatedLogserviceLsn uint64

	minDriverLsn uint64
	maxDriverLsn uint64

	driverLsnLogserviceLsnMap map[uint64]uint64 //start-lsn

	replayHandle  driver.ApplyHandle
	replayedLsn   uint64
	inited        bool
	safeLsn       uint64
	nextToReadLsn uint64
	d             *LogServiceDriver
}

func newReplayer(h driver.ApplyHandle, readmaxsize int, d *LogServiceDriver) *replayer {
	truncated := d.getLogserviceTruncate()
	logutil.Infof("truncated %d", truncated)
	return &replayer{
		minDriverLsn:              math.MaxUint64,
		driverLsnLogserviceLsnMap: make(map[uint64]uint64),
		replayHandle:              h,
		readMaxSize:               readmaxsize,
		nextToReadLsn:             truncated + 1,
		replayedLsn:               math.MaxUint64,
		d:                         d,
	}
}

func (r *replayer) replay() {
	var err error
	r.truncatedLogserviceLsn, err = r.d.GetTruncated()
	if err != nil {
		panic(err) //retry
	}
	for !r.readRecords() {
		for r.replayedLsn < r.safeLsn {
			err := r.replayLogserviceEntry(r.replayedLsn + 1)
			if err != nil {
				panic(err)
			}
		}
	}
	err = r.replayLogserviceEntry(r.replayedLsn + 1)
	for err != ErrAllRecordsRead {
		err = r.replayLogserviceEntry(r.replayedLsn + 1)

	}
	r.d.lsns = make([]uint64, 0)
}

func (r *replayer) readRecords() (readEnd bool) {
	nextLsn, safeLsn := r.d.readFromLogServiceInReplay(r.nextToReadLsn, r.readMaxSize, func(lsn uint64, record *recordEntry) {
		drlsn := record.GetMinLsn()
		r.driverLsnLogserviceLsnMap[drlsn] = lsn
		if drlsn < r.replayedLsn {
			if r.inited {
				panic("logic err")
			}
			r.replayedLsn = drlsn - 1
		}
	})
	if nextLsn == r.nextToReadLsn {
		return true
	}
	r.nextToReadLsn = nextLsn
	if safeLsn > r.safeLsn {
		r.safeLsn = safeLsn
	}
	return false
}

func (r *replayer) replayLogserviceEntry(lsn uint64) error {
	logserviceLsn, ok := r.driverLsnLogserviceLsnMap[lsn]
	if !ok {
		if len(r.driverLsnLogserviceLsnMap) == 0 {
			return ErrAllRecordsRead
		}
		panic("logic error")
	}
	record, err := r.d.readFromCache(logserviceLsn)
	if err == ErrAllRecordsRead {
		return err
	}
	if err != nil {
		panic(err)
	}
	intervals := record.replay(r.replayHandle)
	r.d.onReplayRecordEntry(logserviceLsn, intervals)
	r.onReplayDriverLsn(intervals.GetMax())
	r.onReplayDriverLsn(intervals.GetMin())
	r.d.dropRecordByLsn(logserviceLsn)
	r.replayedLsn = record.GetMaxLsn()
	r.inited = true
	delete(r.driverLsnLogserviceLsnMap, lsn)
	return nil
}
func (r *replayer) onReplayDriverLsn(lsn uint64) {
	if lsn == 0 {
		return
	}
	if lsn < r.minDriverLsn {
		r.minDriverLsn = lsn
	}
	if lsn > r.maxDriverLsn {
		r.maxDriverLsn = lsn
	}
}
