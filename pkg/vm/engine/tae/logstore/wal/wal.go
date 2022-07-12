package wal

import (
	"sync"

	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logstore/driver"
	driverEntry "github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logstore/driver/entry"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logstore/entry"
)

type WalImpl struct {
	*WalInfo
	*common.ClosedState

	driver driver.Driver

	appendWg          sync.WaitGroup
	appendMu          sync.RWMutex
	driverAppendQueue chan any
	doneWithErrQueue  chan any
	logInfoQueue      chan any

	checkpointQueue chan any

	truncatingQueue chan any
	truncateQueue   chan any
}

func (w *WalImpl) Append(gid uint32, e entry.Entry) (lsn uint64, err error) {
	if w.IsClosed() {
		return 0, common.ClosedErr
	}
	w.appendMu.Lock()
	defer w.appendMu.Unlock()
	w.appendWg.Add(1)
	if w.IsClosed() {
		w.appendWg.Done()
		return 0, common.ClosedErr
	}

	lsn = w.allocateLsn(gid)
	v1 := e.GetInfo()
	var info *entry.Info
	if v1 == nil {
		info = &entry.Info{}
		e.SetInfo(info)
	} else {
		info = v1.(*entry.Info)
	}
	info.Group = gid
	info.GroupLSN = lsn
	w.driverAppendQueue <- e
	return
}

func (w *WalImpl) onDriverAppendQueue1(items []any) any {
	results := make([]*driverEntry.Entry, 0)
	for _, item := range items {
		e := item.(entry.Entry)
		driverEntry := driverEntry.NewEntry(e)
		w.driver.Append(driverEntry)
		results = append(results, driverEntry)
	}
	return results
}

func (w *WalImpl) onDoneWithErrQueue(items []any) any {
	result := make([]*driverEntry.Entry, 0)
	for _, v := range items {
		batch := v.([]*driverEntry.Entry)
		for _, e := range batch {
			e.WaitDone()
			info := e.Entry.GetInfo()
			e.Entry.DoneWithErr(nil)
			if info != nil {
				e.Info = info.(*entry.Info)
				result = append(result, e)
			}
		}
	}
	return result
}

func (w *WalImpl) onLogInfoQueue(items []any) any {
	for _, v := range items {
		batch := v.([]*driverEntry.Entry)
		for _, e := range batch {
			w.logDriverLsn(e)
		}
	}
	w.onAppend()
	return nil
}
