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
	"math/rand"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logstore/driver/batchstoredriver"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logstore/entry"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logstore/store"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/testutils"
	"github.com/panjf2000/ants/v2"
	"github.com/stretchr/testify/assert"
)

const (
	ModuleName = "TAEWAL"
)

func initWal(t *testing.T) (Driver, string) {
	dir := testutils.InitTestEnv(ModuleName, t)
	cfg := &batchstoredriver.StoreCfg{
		RotateChecker: batchstoredriver.NewMaxSizeRotateChecker(int(common.K) * 2),
	}
	dcfg := &DriverConfig{
		BatchStoreConfig:   cfg,
		CheckpointDuration: time.Millisecond * 10,
	}
	driver := NewDriver(dir, "store", dcfg)
	return driver, dir
}

func restart(t *testing.T, driver Driver, dir string) Driver {
	assert.NoError(t, driver.Close())
	cfg := &batchstoredriver.StoreCfg{
		RotateChecker: batchstoredriver.NewMaxSizeRotateChecker(int(common.K) * 2),
	}
	dcfg := &DriverConfig{
		BatchStoreConfig:   cfg,
		CheckpointDuration: time.Millisecond * 10,
	}
	driver = NewDriver(dir, "store", dcfg)
	return driver
}

func appendGroupC(t *testing.T, driver Driver, tid string) entry.Entry {
	e := entry.GetBase()
	info := &entry.Info{
		TxnId: tid,
	}
	err := e.SetPayload([]byte(strconv.Itoa(rand.Intn(10))))
	assert.NoError(t, err)
	e.SetInfo(info)
	_, err = driver.AppendEntry(GroupPrepare, e)
	assert.NoError(t, err)
	return e
}

func appendGroupUC(t *testing.T, driver Driver, tid string) entry.Entry {
	e := entry.GetBase()
	info := &entry.Info{
		Uncommits: tid,
	}
	err := e.SetPayload([]byte(strconv.Itoa(rand.Intn(10))))
	assert.NoError(t, err)
	e.SetInfo(info)
	_, err = driver.AppendEntry(GroupUC, e)
	assert.NoError(t, err)
	return e
}

func fuzzyCheckpointGroupC(t *testing.T, driver Driver, lsn uint64, offset, length, size uint32) entry.Entry {
	if length == 0 {
		panic("invalid length")
	}
	if offset+length > size {
		panic("invalid size")
	}
	index := make([]*store.Index, 0)
	for i := uint32(0); i < length; i++ {
		idx := &store.Index{LSN: lsn, CSN: offset + i, Size: size}
		index = append(index, idx)
	}
	e, err := driver.Checkpoint(index)
	assert.NoError(t, err)
	return e
}

func getCheckpointed(driver Driver, group uint32) (lsn uint64) {
	dr := driver.(*walDriver)
	lsn = dr.impl.GetCheckpointed(group)
	return
}

func getCurrSeqNum(driver Driver, group uint32) (lsn uint64) {
	dr := driver.(*walDriver)
	lsn = dr.impl.GetCurrSeqNum(group)
	return
}

func getLsn(e entry.Entry) (group uint32, lsn uint64) {
	v := e.GetInfo()
	if v == nil {
		return
	}
	info := v.(*entry.Info)
	return info.Group, info.GroupLSN
}

// append C, append UC
// ckp C
// check whether UC is checkpointed
func TestCheckpointUC(t *testing.T) {
	driver, dir := initWal(t)

	wg := &sync.WaitGroup{}
	appendworker, _ := ants.NewPool(100)
	ckpworker, _ := ants.NewPool(100)

	ckpfn := func(lsn uint64) func() {
		return func() {
			ckpEntry := fuzzyCheckpointGroupC(t, driver, lsn, 0, 2, 2)
			assert.NoError(t, ckpEntry.WaitDone())
			ckpEntry.Free()
			wg.Done()
		}
	}
	appendfn := func(tid string) func() {
		return func() {
			uncommitEntry := appendGroupUC(t, driver, tid)
			commitEntry := appendGroupC(t, driver, tid)
			_, commitLsn := getLsn(commitEntry)
			assert.NoError(t, uncommitEntry.WaitDone())
			assert.NoError(t, commitEntry.WaitDone())
			wg.Add(1)
			_ = ckpworker.Submit(ckpfn(commitLsn))
			commitEntry.Free()
			uncommitEntry.Free()
			wg.Done()
		}
	}

	for i := 0; i < 100; i++ {
		wg.Add(1)
		tid := common.NewTxnIDAllocator().Alloc()
		_ = appendworker.Submit(appendfn(string(tid)))
	}
	wg.Wait()

	driver = restart(t, driver, dir)

	testutils.WaitExpect(4000, func() bool {
		return getCurrSeqNum(driver, GroupUC) == getCheckpointed(driver, GroupUC)
	})
	assert.Equal(t, getCurrSeqNum(driver, GroupUC), getCheckpointed(driver, GroupUC))

	assert.NoError(t, driver.Close())
}
