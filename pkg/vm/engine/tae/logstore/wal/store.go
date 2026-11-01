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

package wal

import (
	"sync"

	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logstore/driver"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logstore/driver/batchstoredriver"
	driverEntry "github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logstore/driver/entry"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logstore/driver/logservicedriver"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logstore/entry"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logstore/sm"
)

var DefaultMaxBatchSize = 10000

type StoreImpl struct {
	sm.ClosedState
	*StoreInfo

	driver        driver.Driver
	maxRecordSize int

	appendMu sync.Mutex
	appendWg sync.WaitGroup

	driverAppendQueue sm.Queue
	doneWithErrQueue  sm.Queue
	logInfoQueue      sm.Queue
	checkpointQueue   sm.Queue
}

func NewLogserviceHandle(
	factory logservicedriver.LogServiceClientFactory,
	maxRecordSize int,
) *StoreImpl {
	cfg := logservicedriver.NewConfig(
		"",
		logservicedriver.WithConfigOptClientFactory(factory),
		logservicedriver.WithConfigOptMaxWALRecordSize(maxRecordSize),
	)
	driver := logservicedriver.NewLogServiceDriver(&cfg)
	return NewStore(driver)
}

func NewLocalHandle(
	dir, name string, cfg *batchstoredriver.StoreCfg,
) Store {
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
	}
	if sized, ok := driver.(interface{ MaxWALRecordSize() int }); ok {
		w.maxRecordSize = sized.MaxWALRecordSize()
	}
	w.driverAppendQueue = sm.NewSafeQueue(DefaultMaxBatchSize*10, DefaultMaxBatchSize, w.onDriverAppendQueue)
	w.doneWithErrQueue = sm.NewSafeQueue(DefaultMaxBatchSize*10, DefaultMaxBatchSize, w.onDoneWithErrQueue)
	w.logInfoQueue = sm.NewSafeQueue(DefaultMaxBatchSize*10, DefaultMaxBatchSize, w.onLogInfoQueue)
	w.checkpointQueue = sm.NewSafeQueue(DefaultMaxBatchSize*10, DefaultMaxBatchSize, w.onCheckpointIntent)
	w.start()
	return w
}

func (w *StoreImpl) Close() error {
	if !w.TryClose() {
		return nil
	}
	w.appendWg.Wait()
	w.driverAppendQueue.Stop()
	w.doneWithErrQueue.Stop()
	w.logInfoQueue.Stop()
	w.checkpointQueue.Stop()
	err := w.driver.Close()
	if err != nil {
		return err
	}
	return nil
}

func (w *StoreImpl) AppendEntry(gid uint32, e entry.Entry) (lsn uint64, err error) {
	_, lsn, err = w.doAppend(gid, e)
	return
}

func (w *StoreImpl) start() {
	w.driverAppendQueue.Start()
	w.doneWithErrQueue.Start()
	w.logInfoQueue.Start()
	w.checkpointQueue.Start()
}

func (w *StoreImpl) doAppend(gid uint32, e entry.Entry) (drEntry *driverEntry.Entry, lsn uint64, err error) {
	w.appendWg.Add(1)
	accepted := false
	defer func() {
		if !accepted {
			w.appendWg.Done()
		}
	}()
	if w.IsClosed() {
		return nil, 0, sm.ErrClose
	}
	w.appendMu.Lock()
	defer w.appendMu.Unlock()
	lsn = w.peekNextLSN(gid)
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
	if w.maxRecordSize > 0 && gid == GroupUserTxn {
		// A rejected record must not consume an LSN. Materialize callbacks
		// and measure the complete one-entry Log Service record first.
		e.PrepareWrite()
		if err = drEntry.Prepare(); err != nil {
			e.DoneWithErr(err)
			return nil, 0, err
		}
		actual := logservicedriver.EmptyLogEntrySize + drEntry.GetSize() + logservicedriver.OneEntryFooterSize
		if actual > w.maxRecordSize {
			err = NewRecordTooLargeError(actual, w.maxRecordSize)
			e.DoneWithErr(err)
			return nil, 0, err
		}
	}
	lsn = w.nextLSN(gid)
	// e.DoneWithErr(nil)
	// return
	_, err = w.driverAppendQueue.Enqueue(drEntry)
	if err != nil {
		panic(err)
	}
	accepted = true
	return
}

// TODO: error handling
func (w *StoreImpl) onDriverAppendQueue(items ...any) {
	for _, item := range items {
		driverEntry := item.(*driverEntry.Entry)
		if w.maxRecordSize == 0 || driverEntry.Entry.GetInfo().(*entry.Info).Group != GroupUserTxn {
			driverEntry.Entry.PrepareWrite()
		}
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

// TODO: error handling
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
		w.logDSN(e)
	}
}
