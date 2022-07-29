package logservicedriver

import "github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logstore/driver"

type replayer struct {
	readMaxSize int

	minLogserviceLsn uint64

	minDriverLsn uint64
	maxDriverLsn uint64

	replayHandle  driver.ApplyHandle
	replayedLsn   uint64
	safeLsn       uint64
	nextToReadLsn uint64
	d             *LogServiceDriver
}

func (r *replayer) Replay(h driver.ApplyHandle) {
	//get truncated
	var err error
	r.minLogserviceLsn, err = r.d.GetTruncated()
	if err != nil {
		panic(err) //retry
	}
	//readfrom logservice
	for !r.readRecords() {
		for r.replayedLsn < r.safeLsn {
			//replay entry
			r.replayLogserviceEntry(r.replayedLsn + 1)
			//update replayedLsn and drop replayed entries
			r.replayedLsn++
			r.d.dropRecordByLsn(r.replayedLsn)
		}
	}
}

func (r *replayer) readRecords() (readEnd bool) {
	nextLsn, safeLsn := r.d.readFromLogServiceInReplay(r.nextToReadLsn, r.readMaxSize)
	if nextLsn == r.nextToReadLsn {
		return true
	}
	r.nextToReadLsn = nextLsn
	if safeLsn > r.safeLsn {
		r.safeLsn = safeLsn
	}
	return false
}

func (r *replayer) replayLogserviceEntry(lsn uint64) {
	record, err := r.d.readFromCache(lsn)
	if err != nil {
		panic(err)
	}
	intervals := record.replay(r.replayHandle)
	r.d.onReplayRecordEntry(lsn, intervals)
	r.onReplayDriverLsn(intervals.GetMax())
	r.onReplayDriverLsn(intervals.GetMin())
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
