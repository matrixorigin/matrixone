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

	// writeController is used to control the write token
	// it controles the max write token issued and all finished write tokens
	// to avoid too much pendding writes
	// Example:
	// maxIssuedToken = 100
	// maxFinishedToken = 50
	// maxPendding = 60
	// then we can only issue another 10 write token to avoid too much pendding writes
	// In the real world, the maxFinishedToken is always being updated and it is very
	// rare to reach the maxPendding
	writeController struct {
		sync.RWMutex
		// max write token issued
		maxIssuedToken uint64
		// all finished write tokens
		finishedTokens *common.ClosedIntervals
	}

	commitCond sync.Cond
	inReplay   bool
}

func newDriverInfo() *driverInfo {
	d := &driverInfo{
		addr:       make(map[uint64]*common.ClosedIntervals),
		validLsn:   roaring64.NewBitmap(),
		commitCond: *sync.NewCond(new(sync.Mutex)),
	}
	d.writeController.finishedTokens = common.NewClosedIntervals()
	return d
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
	info.writeController.finishedTokens.TryMerge(common.NewClosedIntervalsBySlice(r.writeTokens))
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

func (info *driverInfo) getMaxFinishedToken() uint64 {
	info.writeController.RLock()
	defer info.writeController.RUnlock()
	finishedTokens := info.writeController.finishedTokens
	if finishedTokens == nil ||
		len(finishedTokens.Intervals) == 0 ||
		finishedTokens.Intervals[0].Start != 1 {
		return 0
	}
	return finishedTokens.Intervals[0].End
}

func (info *driverInfo) applyWriteToken(
	maxPendding uint64, timeout time.Duration,
) (token uint64, err error) {
	token, err = info.tryApplyWriteToken(maxPendding)
	if err == ErrTooMuchPenddings {
		err = RetryWithTimeout(
			timeout,
			func() (shouldReturn bool) {
				info.commitCond.L.Lock()
				token, err = info.tryApplyWriteToken(maxPendding)
				if err != ErrTooMuchPenddings {
					info.commitCond.L.Unlock()
					return true
				}
				info.commitCond.Wait()
				info.commitCond.L.Unlock()
				token, err = info.tryApplyWriteToken(maxPendding)
				return err != ErrTooMuchPenddings
			},
		)
	}
	return
}

// NOTE: must be called in serial
func (info *driverInfo) tryApplyWriteToken(
	maxPendding uint64,
) (token uint64, err error) {
	maxFinishedToken := info.getMaxFinishedToken()
	if info.writeController.maxIssuedToken-maxFinishedToken >= maxPendding {
		return 0, ErrTooMuchPenddings
	}
	info.writeController.maxIssuedToken++
	return info.writeController.maxIssuedToken, nil
}

func (info *driverInfo) logAppend(appender *driverAppender) {
	info.addrMu.Lock()
	array := make([]uint64, 0, len(appender.entry.Meta.addr))
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
func (info *driverInfo) putbackWriteTokens(tokens []uint64) {
	info.syncedMu.Lock()
	info.synced = info.syncing
	info.syncedMu.Unlock()

	finishedToken := common.NewClosedIntervalsBySlice(tokens)
	info.writeController.Lock()
	info.writeController.finishedTokens.TryMerge(finishedToken)
	info.writeController.Unlock()

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
