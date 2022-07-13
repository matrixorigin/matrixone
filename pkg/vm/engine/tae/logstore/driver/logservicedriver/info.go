package logservicedriver

import (
	"errors"
	"sync"

	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
)

var ErrDriverLsnNotFound = errors.New("driver info: driver lsn not found")

type driverInfo struct {
	addr       map[uint64]*common.ClosedIntervals //logservicelsn-driverlsn TODO drop on truncate
	addrMu     sync.RWMutex
	appending  uint64
	appended   *common.ClosedIntervals
	appendedMu sync.RWMutex
	logserviceAppended *common.ClosedIntervals
	logserviceAppendedMu sync.RWMutex
}

func newDriverInfo()*driverInfo{
	return &driverInfo{
		addr: make(map[uint64]*common.ClosedIntervals),
		addrMu: sync.RWMutex{},
		appended: common.NewClosedIntervals(),
		appendedMu: sync.RWMutex{},
		logserviceAppendedMu: sync.RWMutex{},
	}
}

func (info *driverInfo) getAppended() uint64 {
	info.appendedMu.RLock()
	defer info.appendedMu.RUnlock()
	if info.appended == nil || len(info.appended.Intervals) == 0 || info.appended.Intervals[0].Start != 1 {
		return 0
	}
	return info.appended.Intervals[0].End
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
	info.addr[appender.logserviceLsn] = common.NewClosedIntervalsBySlice(array)
	info.addrMu.Unlock()
}

func (info *driverInfo) onAppend(appended,logserviceAppended []uint64) {
	appendedArray := common.NewClosedIntervalsBySlice(appended)
	info.appendedMu.Lock()
	info.appended.TryMerge(*appendedArray)
	info.appendedMu.Unlock()
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
