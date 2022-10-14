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

package store

import (
	"sync"

	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logstore/driver"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logstore/driver/batchstoredriver"
	driverEntry "github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logstore/driver/entry"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logstore/driver/logservicedriver"
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

func NewStoreWithLogserviceDriver(factory logservicedriver.LogServiceClientFactory) Store {
	cfg := logservicedriver.NewDefaultConfig(factory)
	driver := logservicedriver.NewLogServiceDriver(cfg)
	return NewStore(driver)
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
	w.appendMu.RLock()
	w.appendWg.Wait()
	w.appendMu.RUnlock()
	w.driverAppendQueue.Stop()
	w.doneWithErrQueue.Stop()
	w.logInfoQueue.Stop()
	w.checkpointQueue.Stop()
	w.truncatingQueue.Stop()
	w.truncateQueue.Stop()
	err := w.driver.Close()
	if err != nil {
		return err
	}
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
	_, err = w.driverAppendQueue.Enqueue(drEntry)
	if err != nil {
		panic(err)
	}
	return
}

func (w *StoreImpl) onDriverAppendQueue(items ...any) {
	for _, item := range items {
		driverEntry := item.(*driverEntry.Entry)
		driverEntry.Entry.PrepareWrite()
		err := w.driver.Append(driverEntry)
		if err != nil {
			panic(err)
		}
		// driverEntry.Entry.DoneWithErr(nil)
		_, err = w.doneWithErrQueue.Enqueue(driverEntry)
		if err != nil {
			panic(err)
		}
	}
}

func (w *StoreImpl) onDoneWithErrQueue(items ...any) {
	for _, item := range items {
		e := item.(*driverEntry.Entry)
		err := e.WaitDone()
		if err != nil {
			panic(err)
		}
		e.Entry.DoneWithErr(nil)
		_, err = w.logInfoQueue.Enqueue(e)
		if err != nil {
			panic(err)
		}
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
