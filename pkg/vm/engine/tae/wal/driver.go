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

	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logstore/driver/batchstoredriver"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logstore/sm"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logstore/store"
)

type entryWithInfo struct{
	e LogEntry
	info any
}

type walDriver struct {
	*walInfo
	sync.RWMutex
	impl store.Store
	own  bool

	logInfoQueue sm.Queue
	checkpointQueue sm.Queue
}

func NewDriver(dir, name string, cfg *batchstoredriver.StoreCfg) Driver {
	impl:= store.NewStoreWithBatchStoreDriver(dir, name, cfg)
	driver := NewDriverWithStore(impl, true)
	return driver
}

func NewDriverWithStore(impl store.Store, own bool) Driver {
	driver := &walDriver{
		walInfo: newWalInfo(),
		impl: impl,
		own: own,
	}
	driver.logInfoQueue = sm.NewSafeQueue(1000, 1000, driver.onLogInfo)
	driver.checkpointQueue=sm.NewSafeQueue(1000,1000,driver.onCheckpoint)
	return driver
}

func (driver *walDriver) GetCheckpointed() uint64 {
	return driver.impl.GetCheckpointed(GroupC)
}

func (driver *walDriver) Replay(handle store.ApplyHandle)error{
	return driver.impl.Replay(handle)
}

func (driver *walDriver) GetPenddingCnt() uint64 {
	return driver.impl.GetPendding(GroupC)
}

func (driver *walDriver) GetCurrSeqNum() uint64 {
	return driver.impl.GetCurrSeqNum(GroupC)
}

func (driver *walDriver) LoadEntry(groupID uint32, lsn uint64) (LogEntry, error) {
	return driver.impl.Load(groupID, lsn)
}

func (driver *walDriver) AppendEntry(group uint32, e LogEntry) (uint64, error) {
	id, err := driver.impl.Append(group, e)
	ent:=&entryWithInfo{
		e: e,
		info:e.GetInfo(),
	}
	driver.logInfoQueue.Enqueue(ent)
	return id, err
}

func (driver *walDriver) Close() error {
	if driver.own {
		return driver.impl.Close()
	}
	return nil
}
