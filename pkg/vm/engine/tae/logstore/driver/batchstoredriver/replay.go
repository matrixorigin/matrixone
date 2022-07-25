package batchstoredriver

import (
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logstore/driver"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logstore/driver/entry"
)

// wal ckping

type replayer struct {
	version int
	pos     int

	applyEntry driver.ApplyHandle

	//syncbase
	addrs  map[int]*common.ClosedIntervals
	maxlsn uint64
}

func newReplayer(h driver.ApplyHandle) *replayer {
	return &replayer{
		addrs:      make(map[int]*common.ClosedIntervals),
		applyEntry: h,
	}
}

func (r *replayer) updateaddrs(version int, lsn uint64) {
	interval, ok := r.addrs[version]
	if !ok {
		interval = common.NewClosedIntervals()
		r.addrs[version] = interval
	}
	interval.TryMerge(*common.NewClosedIntervalsByInt(lsn))
}

func (r *replayer) updateGroupLSN(lsn uint64) {
	if lsn > r.maxlsn {
		r.maxlsn = lsn
	}
}

func (r *replayer) onReplayEntry(e *entry.Entry) error {
	e.SetInfo()
	info := e.Info
	if info == nil {
		return nil
	}
	r.updateaddrs(r.version, e.Lsn)
	r.updateGroupLSN(e.Lsn)
	return nil
}

func (r *replayer) replayHandler(vfile *vFile) error {
	if vfile.version != r.version {
		r.pos = 0
		r.version = vfile.version
	}
	e := entry.NewEmptyEntry()
	_, err := e.ReadAt(vfile.File, r.pos)
	if err != nil {
		return err
	}
	if err := r.onReplayEntry(e); err != nil {
		return err
	}
	vfile.onReplay(r.pos, e.Lsn)
	r.applyEntry(e)
	r.pos += e.GetSize()
	return nil
}
