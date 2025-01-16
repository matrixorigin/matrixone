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

var ErrDSNNotFound = moerr.NewInternalErrorNoCtx("dsn not found")
var ErrRetryTimeOut = moerr.NewInternalErrorNoCtx("retry time out")

type DSNStats struct {
	Truncated uint64
	Min       uint64
	Max       uint64
	Written   []uint64
}

type driverInfo struct {
	// PSN: physical sequence number. here is the lsn from logservice
	psn struct {
		mu sync.RWMutex
		// key: PSN, value: DSNs
		dsnMap  map[uint64]*common.ClosedIntervals
		records roaring64.Bitmap
	}

	// dsn: driver sequence number
	// it is monotonically continuously increasing
	// PSN:[DSN:LSN, DSN:LSN, DSN:LSN, ...]
	// One : Many
	dsn   uint64
	dsnmu sync.RWMutex

	syncing  uint64
	synced   uint64
	syncedMu sync.RWMutex

	truncateDSNIntent atomic.Uint64 //
	truncatedPSN      uint64        //

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
}

func newDriverInfo() *driverInfo {
	d := &driverInfo{
		commitCond: *sync.NewCond(new(sync.Mutex)),
	}
	d.psn.dsnMap = make(map[uint64]*common.ClosedIntervals)
	d.writeController.finishedTokens = common.NewClosedIntervals()
	return d
}

func (info *driverInfo) GetDSN() uint64 {
	info.dsnmu.RLock()
	lsn := info.dsn
	info.dsnmu.RUnlock()
	return lsn
}

func (info *driverInfo) resetDSNStats(stats *DSNStats) {
	info.dsn = stats.Max
	info.synced = stats.Max
	info.syncing = stats.Max
	if stats.Min != math.MaxUint64 {
		info.truncateDSNIntent.Store(stats.Min - 1)
	}
	info.truncatedPSN = stats.Truncated
	info.writeController.finishedTokens = common.NewClosedIntervalsBySlice(stats.Written)
}

// psn: physical sequence number
// dsns: dsns in the log entry of the psn
func (info *driverInfo) recordPSNInfo(
	psn uint64, dsns *common.ClosedIntervals,
) {
	info.psn.dsnMap[psn] = dsns
	info.psn.records.Add(psn)
}

func (info *driverInfo) getNextValidPSN(psn uint64) uint64 {
	info.psn.mu.RLock()
	defer info.psn.mu.RUnlock()
	if info.psn.records.IsEmpty() {
		return 0
	}
	maxPSN := info.psn.records.Maximum()
	// [psn >= maxPSN]
	if psn >= maxPSN {
		return maxPSN
	}
	// [psn < maxPSN]
	// PXU TODO: psn++???
	psn++
	for !info.psn.records.Contains(psn) {
		psn++
	}
	return psn
}

func (info *driverInfo) isToTruncate(psn, dsn uint64) bool {
	maxDSN := info.getMaxDSN(psn)

	// psn cannot be found in the psn.dsnMap
	if maxDSN == 0 {
		return false
	}

	// the maxDSN of the psn is less equal to the dsn
	return maxDSN <= dsn
}

func (info *driverInfo) getMaxDSN(psn uint64) uint64 {
	info.psn.mu.RLock()
	intervals, ok := info.psn.dsnMap[psn]
	if !ok {
		info.psn.mu.RUnlock()
		return 0
	}
	lsn := intervals.GetMax()
	info.psn.mu.RUnlock()
	return lsn
}

func (info *driverInfo) allocateDSNLocked() uint64 {
	info.dsn++
	return info.dsn
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
	info.psn.mu.Lock()
	array := make([]uint64, 0, len(appender.entry.Meta.addr))
	for key := range appender.entry.Meta.addr {
		array = append(array, key)
	}
	info.psn.records.Add(appender.logserviceLsn)
	interval := common.NewClosedIntervalsBySlice(array)
	info.psn.dsnMap[appender.logserviceLsn] = interval
	info.psn.mu.Unlock()
	if interval.GetMin() != info.syncing+1 {
		panic(fmt.Sprintf("logic err, expect %d, min is %d", info.syncing+1, interval.GetMin()))
	}
	if len(interval.Intervals) != 1 {
		logutil.Debugf("interval is %v", interval)
		panic("logic err")
	}
	info.syncing = interval.GetMax()
}

func (info *driverInfo) gcPSN(psn uint64) {
	info.psn.mu.Lock()
	defer info.psn.mu.Unlock()
	candidates := make([]uint64, 0)
	// collect all the PSN that is less than the given PSN
	for sn := range info.psn.dsnMap {
		if sn < psn {
			candidates = append(candidates, sn)
		}
	}
	// remove 0 to the given PSN from the validPSN
	info.psn.records.RemoveRange(0, psn)

	// remove the PSN from the map
	for _, lsn := range candidates {
		delete(info.psn.dsnMap, lsn)
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
