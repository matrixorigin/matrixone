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
	"fmt"
	"math"
	"sync"
	"sync/atomic"
	"time"

	"github.com/RoaringBitmap/roaring/roaring64"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
)

var ErrDriverLsnNotFound = moerr.NewInternalErrorNoCtx("driver info: driver lsn not found")
var ErrRetryTimeOut = moerr.NewInternalErrorNoCtx("driver info: retry time out")

type driverInfo struct {
	addr                   map[uint64]*common.ClosedIntervals //logservicelsn-driverlsn TODO drop on truncate
	validLsn               *roaring64.Bitmap
	addrMu                 sync.RWMutex
	driverLsn              uint64 //
	syncing                uint64
	synced                 uint64
	syncedMu               sync.RWMutex
	driverLsnMu            sync.RWMutex
	truncating             atomic.Uint64 //
	truncatedLogserviceLsn uint64        //

	appending  uint64
	appended   *common.ClosedIntervals
	appendedMu sync.RWMutex
	commitCond sync.Cond
	inReplay   bool
}

func newDriverInfo() *driverInfo {
	return &driverInfo{
		addr:        make(map[uint64]*common.ClosedIntervals),
		validLsn:    roaring64.NewBitmap(),
		addrMu:      sync.RWMutex{},
		driverLsnMu: sync.RWMutex{},
		appended:    common.NewClosedIntervals(),
		appendedMu:  sync.RWMutex{},
		commitCond:  *sync.NewCond(new(sync.Mutex)),
	}
}

func (d *LogServiceDriver) GetCurrSeqNum() uint64 {
	d.driverLsnMu.Lock()
	lsn := d.driverLsn
	d.driverLsnMu.Unlock()
	return lsn
}
func (info *driverInfo) PreReplay() {
	info.inReplay = true
}
func (info *driverInfo) PostReplay() {
	info.inReplay = false
}
func (info *driverInfo) IsReplaying() bool {
	return info.inReplay
}
func (info *driverInfo) onReplay(r *replayer) {
	info.driverLsn = r.maxDriverLsn
	info.synced = r.maxDriverLsn
	info.syncing = r.maxDriverLsn
	if r.minDriverLsn != math.MaxUint64 {
		info.truncating.Store(r.minDriverLsn - 1)
	}
	info.truncatedLogserviceLsn = r.truncatedLogserviceLsn
	info.appended.TryMerge(*common.NewClosedIntervalsBySlice(r.appended))
}

func (info *driverInfo) onReplayRecordEntry(lsn uint64, driverLsns *common.ClosedIntervals) {
	info.addr[lsn] = driverLsns
	info.validLsn.Add(lsn)
}

func (info *driverInfo) getNextValidLogserviceLsn(lsn uint64) uint64 {
	info.addrMu.Lock()
	defer info.addrMu.Unlock()
	if info.validLsn.IsEmpty() {
		return 0
	}
	max := info.validLsn.Maximum()
	if lsn >= max {
		return max
	}
	lsn++
	for !info.validLsn.Contains(lsn) {
		lsn++
	}
	return lsn
}

func (info *driverInfo) isToTruncate(logserviceLsn, driverLsn uint64) bool {
	maxlsn := info.getMaxDriverLsn(logserviceLsn)
	if maxlsn == 0 {
		return false
	}
	return maxlsn <= driverLsn
}

func (info *driverInfo) getMaxDriverLsn(logserviceLsn uint64) uint64 {
	info.addrMu.RLock()
	intervals, ok := info.addr[logserviceLsn]
	if !ok {
		info.addrMu.RUnlock()
		return 0
	}
	lsn := intervals.GetMax()
	info.addrMu.RUnlock()
	return lsn
}

func (info *driverInfo) allocateDriverLsn() uint64 {
	info.driverLsn++
	lsn := info.driverLsn
	return lsn
}

func (info *driverInfo) getDriverLsn() uint64 {
	info.driverLsnMu.RLock()
	lsn := info.driverLsn
	info.driverLsnMu.RUnlock()
	return lsn
}

func (info *driverInfo) getAppended() uint64 {
	info.appendedMu.RLock()
	defer info.appendedMu.RUnlock()
	if info.appended == nil || len(info.appended.Intervals) == 0 || info.appended.Intervals[0].Start != 1 {
		return 0
	}
	return info.appended.Intervals[0].End
}

func (info *driverInfo) retryAllocateAppendLsnWithTimeout(maxPendding uint64, timeout time.Duration) (lsn uint64, err error) {
	lsn, err = info.tryAllocate(maxPendding)
	if err == ErrTooMuchPenddings {
		err = RetryWithTimeout(timeout, func() (shouldReturn bool) {
			info.commitCond.L.Lock()
			lsn, err = info.tryAllocate(maxPendding)
			if err != ErrTooMuchPenddings {
				info.commitCond.L.Unlock()
				return true
			}
			info.commitCond.Wait()
			info.commitCond.L.Unlock()
			lsn, err = info.tryAllocate(maxPendding)
			return err != ErrTooMuchPenddings
		})
	}
	return
}

func (info *driverInfo) tryAllocate(maxPendding uint64) (lsn uint64, err error) {
	appended := info.getAppended()
	if info.appending-appended >= maxPendding {
		return 0, ErrTooMuchPenddings
	}
	info.appending++
	return info.appending, nil
}

func (info *driverInfo) logAppend(appender *driverAppender) {
	info.addrMu.Lock()
	array := make([]uint64, 0)
	for key := range appender.entry.Meta.addr {
		array = append(array, key)
	}
	info.validLsn.Add(appender.logserviceLsn)
	interval := common.NewClosedIntervalsBySlice(array)
	info.addr[appender.logserviceLsn] = interval
	info.addrMu.Unlock()
	if interval.GetMin() != info.syncing+1 {
		panic(fmt.Sprintf("logic err, expect %d, min is %d", info.syncing+1, interval.GetMin()))
	}
	if len(interval.Intervals) != 1 {
		logutil.Debugf("interval is %v", interval)
		panic("logic err")
	}
	info.syncing = interval.GetMax()
}

func (info *driverInfo) gcAddr(logserviceLsn uint64) {
	info.addrMu.Lock()
	defer info.addrMu.Unlock()
	lsnToDelete := make([]uint64, 0)
	for serviceLsn := range info.addr {
		if serviceLsn < logserviceLsn {
			lsnToDelete = append(lsnToDelete, serviceLsn)
		}
	}
	info.validLsn.RemoveRange(0, logserviceLsn)
	for _, lsn := range lsnToDelete {
		delete(info.addr, lsn)
	}
}

func (info *driverInfo) getSynced() uint64 {
	info.syncedMu.RLock()
	lsn := info.synced
	info.syncedMu.RUnlock()
	return lsn
}
func (info *driverInfo) onAppend(appended []uint64) {
	info.syncedMu.Lock()
	info.synced = info.syncing
	info.syncedMu.Unlock()

	appendedArray := common.NewClosedIntervalsBySlice(appended)
	info.appendedMu.Lock()
	info.appended.TryMerge(*appendedArray)
	info.appendedMu.Unlock()

	info.commitCond.L.Lock()
	info.commitCond.Broadcast()
	info.commitCond.L.Unlock()
}

func (info *driverInfo) tryGetLogServiceLsnByDriverLsn(driverLsn uint64) (uint64, error) {
	lsn, err := info.getLogServiceLsnByDriverLsn(driverLsn)
	if err == ErrDriverLsnNotFound {
		if lsn <= info.getDriverLsn() {
			for i := 0; i < 10; i++ {
				logutil.Infof("retry get logserviceLsn, driverlsn=%d", driverLsn)
				info.commitCond.L.Lock()
				lsn, err = info.getLogServiceLsnByDriverLsn(driverLsn)
				if err == nil {
					info.commitCond.L.Unlock()
					break
				}
				info.commitCond.Wait()
				info.commitCond.L.Unlock()
			}
			if err != nil {
				return 0, ErrRetryTimeOut
			}
		}
	}
	return lsn, err
}

func (info *driverInfo) getLogServiceLsnByDriverLsn(driverLsn uint64) (uint64, error) {
	info.addrMu.RLock()
	defer info.addrMu.RUnlock()
	for lsn, intervals := range info.addr {
		if intervals.Contains(*common.NewClosedIntervalsByInt(driverLsn)) {
			return lsn, nil
		}
	}
	return 0, ErrDriverLsnNotFound
}
