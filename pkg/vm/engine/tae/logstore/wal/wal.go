package wal

import (
	"sync"

	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logstore/driver"
	driverEntry "github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logstore/driver/entry"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logstore/entry"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logstore/sm"
)

var DefaultMaxBatchSize = 100

type WalImpl struct {
	*WalInfo
	common.ClosedState

	driver driver.Driver

	appendWg          sync.WaitGroup
	appendMu          sync.RWMutex
	driverAppendQueue sm.Queue
	doneWithErrQueue  sm.Queue
	logInfoQueue      sm.Queue

	checkpointQueue sm.Queue

	truncatingQueue sm.Queue
	truncateQueue   sm.Queue
}

func NewLogStore(driver driver.Driver) *WalImpl {
	w := &WalImpl{
		WalInfo:  newWalInfo(),
		driver:   driver,
		appendWg: sync.WaitGroup{},
		appendMu: sync.RWMutex{},
	}
	w.driverAppendQueue = sm.NewSafeQueue(DefaultMaxBatchSize*100, DefaultMaxBatchSize, w.onDriverAppendQueue)
	w.doneWithErrQueue = sm.NewSafeQueue(DefaultMaxBatchSize*100, DefaultMaxBatchSize, w.onDoneWithErrQueue)
	w.logInfoQueue = sm.NewSafeQueue(DefaultMaxBatchSize*100, DefaultMaxBatchSize, w.onLogInfoQueue)
	w.checkpointQueue = sm.NewSafeQueue(DefaultMaxBatchSize*100, DefaultMaxBatchSize, w.onLogCKPInfoQueue)
	w.truncatingQueue = sm.NewSafeQueue(DefaultMaxBatchSize*100, DefaultMaxBatchSize, w.onTruncatingQueue)
	w.truncateQueue = sm.NewSafeQueue(DefaultMaxBatchSize*100, DefaultMaxBatchSize, w.onTruncateQueue)
	w.Start()
	return w
}
func (w *WalImpl) Start() {
	w.driverAppendQueue.Start()
	w.doneWithErrQueue.Start()
	w.logInfoQueue.Start()
	w.checkpointQueue.Start()
	w.truncatingQueue.Start()
	w.truncateQueue.Start()
}
func (w *WalImpl) Close() error {
	if !w.TryClose() {
		return nil
	}
	err := w.driver.Close()
	if err != nil {
		return err
	}
	w.appendMu.RLock()
	w.appendWg.Wait()
	w.appendMu.RUnlock()
	w.driverAppendQueue.Stop()
	w.doneWithErrQueue.Stop()
	w.logInfoQueue.Stop()
	w.checkpointQueue.Stop()
	w.truncatingQueue.Stop()
	w.truncateQueue.Stop()
	return nil
}
func (w *WalImpl) Append(gid uint32, e entry.Entry) (lsn uint64, err error) {
	_, lsn, err = w.doAppend(gid, e)
	return
}

func (w *WalImpl) doAppend(gid uint32, e entry.Entry) (drEntry *driverEntry.Entry, lsn uint64, err error) {
	if w.IsClosed() {
		return nil, 0, common.ErrClose
	}
	w.appendMu.Lock()
	defer w.appendMu.Unlock()
	w.appendWg.Add(1)
	if w.IsClosed() {
		w.appendWg.Done()
		return nil, 0, common.ErrClose
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
	drEntry = driverEntry.NewEntry(e)
	w.driverAppendQueue.Enqueue(drEntry)
	return
}

func (w *WalImpl) onDriverAppendQueue(items ...any) {
	for _, item := range items {
		driverEntry := item.(*driverEntry.Entry)
		driverEntry.Entry.PrepareWrite()
		w.driver.Append(driverEntry)
		w.doneWithErrQueue.Enqueue(driverEntry)
	}
}

func (w *WalImpl) onDoneWithErrQueue(items ...any) {
	for _, item := range items {
		e := item.(*driverEntry.Entry)
		e.WaitDone()
		e.Entry.DoneWithErr(nil)
		w.logInfoQueue.Enqueue(e)
	}
	w.appendWg.Add(-len(items))
}

func (w *WalImpl) onLogInfoQueue(items ...any) {
	for _, item := range items {
		e := item.(*driverEntry.Entry)
		w.logDriverLsn(e)
	}
	w.onAppend()
}
