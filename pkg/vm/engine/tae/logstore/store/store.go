package store

import (
	"sync"

	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logstore/driver"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logstore/driver/batchstoredriver"
	driverEntry "github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logstore/driver/entry"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logstore/entry"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logstore/sm"
)

var DefaultMaxBatchSize = 10000

type StoreImpl struct {
	*StoreInfo
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

func NewStoreWithBatchStoreDriver(dir, name string, cfg *batchstoredriver.StoreCfg) Store {
	driver, err := batchstoredriver.NewBaseStore(dir, name, cfg)
	if err != nil {
		panic(err)
	}
	return NewStore(driver)
}
func NewStore(driver driver.Driver) *StoreImpl {
	w := &StoreImpl{
		StoreInfo: newWalInfo(),
		driver:    driver,
		appendWg:  sync.WaitGroup{},
		appendMu:  sync.RWMutex{},
	}
	w.driverAppendQueue = sm.NewSafeQueue(DefaultMaxBatchSize*10, DefaultMaxBatchSize, w.onDriverAppendQueue)
	w.doneWithErrQueue = sm.NewSafeQueue(DefaultMaxBatchSize*10, DefaultMaxBatchSize, w.onDoneWithErrQueue)
	w.logInfoQueue = sm.NewSafeQueue(DefaultMaxBatchSize*10, DefaultMaxBatchSize, w.onLogInfoQueue)
	w.checkpointQueue = sm.NewSafeQueue(DefaultMaxBatchSize*10, DefaultMaxBatchSize, w.onLogCKPInfoQueue)
	w.truncatingQueue = sm.NewSafeQueue(DefaultMaxBatchSize*10, DefaultMaxBatchSize, w.onTruncatingQueue)
	w.truncateQueue = sm.NewSafeQueue(DefaultMaxBatchSize*10, DefaultMaxBatchSize, w.onTruncateQueue)
	w.Start()
	return w
}
func (w *StoreImpl) Start() {
	w.driverAppendQueue.Start()
	w.doneWithErrQueue.Start()
	w.logInfoQueue.Start()
	w.checkpointQueue.Start()
	w.truncatingQueue.Start()
	w.truncateQueue.Start()
}
func (w *StoreImpl) Close() error {
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
	w.driver.Close()
	return nil
}
func (w *StoreImpl) Append(gid uint32, e entry.Entry) (lsn uint64, err error) {
	_, lsn, err = w.doAppend(gid, e)
	return
}

func (w *StoreImpl) doAppend(gid uint32, e entry.Entry) (drEntry *driverEntry.Entry, lsn uint64, err error) {
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
	// e.DoneWithErr(nil)
	// return
	w.driverAppendQueue.Enqueue(drEntry)
	return
}

func (w *StoreImpl) onDriverAppendQueue(items ...any) {
	for _, item := range items {
		driverEntry := item.(*driverEntry.Entry)
		driverEntry.Entry.PrepareWrite()
		w.driver.Append(driverEntry)
		// driverEntry.Entry.DoneWithErr(nil)
		w.doneWithErrQueue.Enqueue(driverEntry)
	}
}

func (w *StoreImpl) onDoneWithErrQueue(items ...any) {
	for _, item := range items {
		e := item.(*driverEntry.Entry)
		e.WaitDone()
		e.Entry.DoneWithErr(nil)
		w.logInfoQueue.Enqueue(e)
	}
	w.appendWg.Add(-len(items))
}

func (w *StoreImpl) onLogInfoQueue(items ...any) {
	for _, item := range items {
		e := item.(*driverEntry.Entry)
		w.logDriverLsn(e)
	}
	w.onAppend()
}
