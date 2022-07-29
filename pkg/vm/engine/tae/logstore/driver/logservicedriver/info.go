package logservicedriver

import (
	"errors"
	"sync"
	"time"

	"github.com/RoaringBitmap/roaring/roaring64"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
)

var ErrDriverLsnNotFound = errors.New("driver info: driver lsn not found")
var ErrRetryTimeOut = errors.New("driver info: retry time out")

type driverInfo struct {
	addr        map[uint64]*common.ClosedIntervals //logservicelsn-driverlsn TODO drop on truncate
	validLsn    *roaring64.Bitmap
	addrMu      sync.RWMutex
	driverLsn   uint64
	driverLsnMu sync.RWMutex

	truncating             uint64 //
	truncatedLogserviceLsn uint64 //

	appending            uint64
	appended             *common.ClosedIntervals
	appendedMu           sync.RWMutex
	logserviceAppended   *common.ClosedIntervals //
	logserviceAppendedMu sync.RWMutex
	commitCond           sync.Cond
}

func newDriverInfo() *driverInfo {
	return &driverInfo{
		addr:                 make(map[uint64]*common.ClosedIntervals),
		validLsn: roaring64.NewBitmap(),
		addrMu:               sync.RWMutex{},
		driverLsnMu:          sync.RWMutex{},
		appended:             common.NewClosedIntervals(),
		logserviceAppended:   common.NewClosedIntervals(),
		appendedMu:           sync.RWMutex{},
		logserviceAppendedMu: sync.RWMutex{},
		commitCond:           *sync.NewCond(new(sync.Mutex)),
	}
}
func (info *driverInfo) onReplayRecordEntry(lsn uint64, driverLsns *common.ClosedIntervals){
	info.addr[lsn]=driverLsns
	info.validLsn.Add(lsn)
}
func (info *driverInfo) getNextValidLogserviceLsn(lsn uint64)uint64{
	lsn++
	for !info.validLsn.Contains(lsn){
		lsn++
	}
	return lsn
}
func (info *driverInfo) isToTruncate(logserviceLsn, driverLsn uint64) bool {
	maxlsn := info.getMaxDriverLsn(logserviceLsn)
	logutil.Infof("service %d, max %d, target %d", logserviceLsn, maxlsn, driverLsn)
	if maxlsn == 0 {
		return false
	}
	return maxlsn <= driverLsn
}

func (info *driverInfo) getMaxDriverLsn(logserviceLsn uint64) uint64 {
	info.addrMu.RLock()
	intervals, ok := info.addr[logserviceLsn]
	logutil.Infof("interval %v", intervals)
	if !ok {
		info.addrMu.RUnlock()
		return 0
	}
	lsn := intervals.GetMax()
	info.addrMu.RUnlock()
	return lsn
}

func (info *driverInfo) allocateDriverLsn() uint64 {
	info.driverLsnMu.Lock()
	info.driverLsn++
	lsn := info.driverLsn
	info.driverLsnMu.Unlock()
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
		RetryWithTimeout(timeout, func() (shouldReturn bool) {
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
	for key := range appender.entry.meta.addr {
		array = append(array, key)
	}
	info.validLsn.Add(appender.logserviceLsn)
	info.addr[appender.logserviceLsn] = common.NewClosedIntervalsBySlice(array)
	info.addrMu.Unlock()
}

func (info *driverInfo) onAppend(appended, logserviceAppended []uint64) {
	appendedArray := common.NewClosedIntervalsBySlice(appended)
	info.appendedMu.Lock()
	info.appended.TryMerge(*appendedArray)
	info.appendedMu.Unlock()

	logserviceAppendedArray := common.NewClosedIntervalsBySlice(logserviceAppended)
	info.logserviceAppendedMu.Lock()
	info.logserviceAppended.TryMerge(*logserviceAppendedArray)
	info.logserviceAppendedMu.Unlock()
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
				if err == nil {
					break
				}
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
