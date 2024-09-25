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
	"context"
	"sync"
	"time"

	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logstore/driver/batchstoredriver"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logstore/driver/logservicedriver"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logstore/store"
)

type DriverConfig struct {
	BatchStoreConfig   *batchstoredriver.StoreCfg
	CheckpointDuration time.Duration
}

type walDriver struct {
	sync.RWMutex
	impl store.Store
	own  bool

	ckpDuration   time.Duration
	cancelfn      context.CancelFunc
	cancelContext context.Context
	wg            sync.WaitGroup
}

func NewDriverWithLogservice(ctx context.Context, factory logservicedriver.LogServiceClientFactory) Driver {
	ckpDuration := time.Second * 5
	impl := store.NewStoreWithLogserviceDriver(factory)
	driver := NewDriverWithStore(ctx, impl, true, ckpDuration)
	return driver
}

func NewDriverWithBatchStore(ctx context.Context, dir, name string, cfg *DriverConfig) Driver {
	var batchStoreCfg *batchstoredriver.StoreCfg
	ckpDuration := time.Second * 5
	if cfg != nil {
		batchStoreCfg = cfg.BatchStoreConfig
		ckpDuration = cfg.CheckpointDuration
	}
	impl := store.NewStoreWithBatchStoreDriver(dir, name, batchStoreCfg)
	driver := NewDriverWithStore(ctx, impl, true, ckpDuration)
	return driver
}

func NewDriverWithStore(ctx context.Context, impl store.Store, own bool, ckpDuration time.Duration) Driver {
	if ckpDuration == 0 {
		ckpDuration = time.Second
	}
	driver := &walDriver{
		impl:        impl,
		own:         own,
		wg:          sync.WaitGroup{},
		ckpDuration: ckpDuration,
	}
	driver.cancelContext, driver.cancelfn = context.WithCancel(ctx)
	driver.wg.Add(1)
	return driver
}
func (driver *walDriver) Start() {}
func (driver *walDriver) GetCheckpointed() uint64 {
	return driver.impl.GetCheckpointed(GroupPrepare)
}
func (driver *walDriver) replayhandle(handle store.ApplyHandle) store.ApplyHandle {
	return func(group uint32, commitId uint64, payload []byte, typ uint16, info any) {
		handle(group, commitId, payload, typ, nil)
	}
}
func (driver *walDriver) Replay(handle store.ApplyHandle) error {
	return driver.impl.Replay(driver.replayhandle(handle))
}

func (driver *walDriver) GetPenddingCnt() uint64 {
	return driver.impl.GetPendding(GroupPrepare)
}

func (driver *walDriver) GetCurrSeqNum() uint64 {
	return driver.impl.GetCurrSeqNum(GroupPrepare)
}

func (driver *walDriver) LoadEntry(groupID uint32, lsn uint64) (LogEntry, error) {
	return driver.impl.Load(groupID, lsn)
}

func (driver *walDriver) AppendEntry(group uint32, e LogEntry) (uint64, error) {
	id, err := driver.impl.Append(group, e)
	return id, err
}

func (driver *walDriver) Close() error {
	driver.cancelfn()
	if driver.own {
		return driver.impl.Close()
	}
	driver.wg.Wait()
	return nil
}

// for UT
func (driver *walDriver) GetTruncated() uint64 {
	return driver.impl.GetTruncated()
}
