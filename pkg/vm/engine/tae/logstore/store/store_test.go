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
	"math/rand"
	"os"
	"strconv"
	"sync"
	"testing"

	// "net/http"
	// _ "net/http/pprof"

	// "github.com/lni/vfs"
	// "github.com/matrixorigin/matrixone/pkg/logservice"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logstore/driver"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logstore/driver/batchstoredriver"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/testutils"

	// "github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logstore/driver/logservicedriver"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logstore/entry"

	"github.com/panjf2000/ants/v2"
	"github.com/stretchr/testify/assert"
)

// var buf []byte
// func init() {
// 	go http.ListenAndServe("0.0.0.0:6060", nil)
// 	var bs bytes.Buffer
// 	for i := 0; i < 3000; i++ {
// 		bs.WriteString("helloyou")
// 	}
// 	buf = bs.Bytes()
// }

func newTestDriver(t *testing.T, size int) driver.Driver {
	dir := "/tmp/logstore/teststore/store"
	name := "mock"
	os.RemoveAll(dir)
	cfg := &batchstoredriver.StoreCfg{
		RotateChecker: batchstoredriver.NewMaxSizeRotateChecker(size),
	}
	s, err := batchstoredriver.NewBaseStore(dir, name, cfg)
	assert.NoError(t, err)
	return s
}

func restartTestDriver(t *testing.T, size int) driver.Driver {
	dir := "/tmp/logstore/teststore/store"
	name := "mock"
	cfg := &batchstoredriver.StoreCfg{
		RotateChecker: batchstoredriver.NewMaxSizeRotateChecker(size),
	}
	s, err := batchstoredriver.NewBaseStore(dir, name, cfg)
	assert.NoError(t, err)
	return s
}

//	func newTestLogserviceDriver(t *testing.T) (driver.Driver, *logservice.Service) {
//		fs := vfs.NewStrictMem()
//		service, ccfg, err := logservice.NewTestService(fs)
//		assert.NoError(t, err)
//		cfg := logservicedriver.NewTestConfig(&ccfg)
//		driver := logservicedriver.NewLogServiceDriver(cfg)
//		return driver, service
//	}
func TestAppendRead(t *testing.T) {
	driver := newTestDriver(t, int(common.M)*64)
	wal := NewStore(driver)
	defer wal.Close()

	e := entry.GetBase()
	err := e.SetPayload([]byte("payload"))
	if err != nil {
		panic(err)
	}
	lsn, err := wal.Append(10, e)
	assert.NoError(t, err)

	err = e.WaitDone()
	assert.NoError(t, err)

	e2, err := wal.Load(10, lsn)
	assert.NoError(t, err)
	assert.Equal(t, e.GetPayload(), e2.GetPayload())
	e.Free()
	e2.Free()
}

func mockEntry() entry.Entry {
	e := entry.GetBase()
	err := e.SetPayload([]byte(strconv.Itoa(rand.Intn(10))))
	if err != nil {
		panic(err)
	}
	// payload:=make([]byte,common.K)
	// copy(payload,buf)
	// e.SetPayload(payload)
	return e
}

// func testPerformance(t *testing.T) {
// 	// driver := newTestDriver(t)
// 	driver, server := newTestLogserviceDriver(t)
// 	defer server.Close()
// 	wal := NewStore(driver)
// 	defer wal.Close()

// 	entryCount := 50000
// 	// entries := make([]entry.Entry, 0)
// 	wg := sync.WaitGroup{}
// 	worker, _ := ants.NewPool(100)
// 	appendfn := func(i int, group uint32) func() {
// 		return func() {
// 			// e := entries[i]
// 			e := mockEntry()
// 			wal.Append(group, e)
// 			// assert.NoError(t, err)
// 			e.WaitDone()
// 			// assert.NoError(t, e.WaitDone())
// 			e.Free()
// 			wg.Done()
// 		}
// 	}

// 	// t0:=time.Now()
// 	// for i := 0; i < entryCount; i++ {
// 	// 	e := mockEntry()
// 	// 	entries = append(entries, e)
// 	// }
// 	// logutil.Infof("make %d entries takes %v", entryCount, time.Since(t0))
// 	t0 := time.Now()
// 	for i := 0; i < entryCount; i++ {
// 		group := uint32(10 + rand.Intn(3))
// 		wg.Add(1)
// 		worker.Submit(appendfn(i, group))
// 	}
// 	// wg.Wait()
// 	logutil.Infof("%d entries takes %v", entryCount, time.Since(t0))
// 	// for i := 0; i < entryCount; i++ {
// 	// 	e := entries[i]
// 	// 	e.Free()
// 	// }

// }
func TestWal(t *testing.T) {
	driver := newTestDriver(t, int(common.M)*64)
	wal := NewStore(driver)
	defer wal.Close()

	entryCount := 5
	entries := make([]entry.Entry, 0)
	wg := sync.WaitGroup{}
	worker, _ := ants.NewPool(10000)
	defer worker.Release()
	appendfn := func(i int, group uint32) func() {
		return func() {
			e := entries[i]
			lsn, err := wal.Append(group, e)
			assert.NoError(t, err)
			assert.NoError(t, e.WaitDone())
			entryGroupID, entryLSN := e.GetLsn()
			assert.Equal(t, group, entryGroupID)
			assert.Equal(t, lsn, entryLSN)

			currLsn := wal.GetCurrSeqNum(group)
			assert.LessOrEqual(t, lsn, currLsn)
			wg.Done()
		}
	}

	truncatefn := func(i int) func() {
		return func() {
			e := entries[i]
			assert.NoError(t, e.WaitDone())
			entryGroupID, entryLSN := e.GetLsn()
			idxes := []*Index{{LSN: entryLSN, CSN: 0, Size: 1}}
			ckpEntry, err := wal.FuzzyCheckpoint(entryGroupID, idxes)
			assert.NoError(t, err)
			err = ckpEntry.WaitDone()
			assert.NoError(t, err)
			ckpGroup, ckpLsn := ckpEntry.GetLsn()
			_, err = wal.Load(ckpGroup, ckpLsn)
			assert.Equal(t, GroupCKP, ckpGroup)
			assert.NoError(t, err)
			ckpEntry.Free()
			wg.Done()
		}
	}

	readfn := func(i int) func() {
		return func() {
			e := entries[i]
			assert.NoError(t, e.WaitDone())
			entryGroupID, entryLSN := e.GetLsn()
			e2, err := wal.Load(entryGroupID, entryLSN)
			assert.NoError(t, err)
			// entryGroupID2, entryLSN2 := e2.GetLsn()
			// assert.Equal(t, entryGroupID, entryGroupID2)
			// assert.Equal(t, entryLSN, entryLSN2)
			// assert.Equal(t, e.GetPayload(), e2.GetPayload())
			// testutils.WaitExpect(4000, func() bool {
			// 	return wal.GetSynced(entryGroupID) >= entryLSN
			// })
			// synced := wal.GetSynced(entryGroupID)
			// assert.LessOrEqual(t, entryLSN, synced)
			e2.Free()
			wg.Done()
		}
	}

	for i := 0; i < entryCount; i++ {
		e := mockEntry()
		entries = append(entries, e)
	}
	// t0:= time.Now()
	for i := 0; i < entryCount; i++ {
		group := uint32(10 + rand.Intn(3))
		// group := uint32(5)
		wg.Add(1)
		_ = worker.Submit(appendfn(i, group))
		wg.Add(1)
		_ = worker.Submit(readfn(i))
	}
	wg.Wait()
	// logutil.Infof("%d entries takes %v",entryCount,time.Since(t0))
	for i := 0; i < entryCount; i++ {
		wg.Add(1)
		_ = worker.Submit(truncatefn(i))
	}
	wg.Wait()
	for i := 0; i < entryCount; i++ {
		e := entries[i]
		e.Free()
	}
}

func TestReplay(t *testing.T) {
	driver := newTestDriver(t, int(common.K)*3)
	wal := NewStore(driver)

	e := entry.GetBase()
	err := e.SetPayload([]byte("payload"))
	if err != nil {
		panic(err)
	}
	_, err = wal.Append(10, e)
	assert.NoError(t, err)

	err = e.WaitDone()
	assert.NoError(t, err)
	e.Free()

	e2 := entry.GetBase()
	err = e2.SetPayload(make([]byte, int(common.K*3)))
	if err != nil {
		panic(err)
	}
	_, err = wal.Append(10, e2)
	assert.NoError(t, err)

	err = e2.WaitDone()
	assert.NoError(t, err)
	e2.Free()

	e2 = entry.GetBase()
	err = e2.SetPayload(make([]byte, int(common.K*3)))
	if err != nil {
		panic(err)
	}
	_, err = wal.Append(10, e2)
	assert.NoError(t, err)

	err = e2.WaitDone()
	assert.NoError(t, err)
	e2.Free()

	wal.Close()

	driver = restartTestDriver(t, int(common.K)*3)
	wal = NewStore(driver)
	wal.Close()
}

func TestTruncate(t *testing.T) {
	driver := newTestDriver(t, int(common.M)*64)
	wal := NewStore(driver)
	defer wal.Close()
	entryCount := 5
	group := entry.GTCustomizedStart
	for i := 0; i < entryCount; i++ {
		e := mockEntry()
		lsn, err := wal.Append(group, e)
		assert.NoError(t, err)
		assert.NoError(t, e.WaitDone())
		entryGroupID, entryLSN := e.GetLsn()
		assert.Equal(t, group, entryGroupID)
		assert.Equal(t, lsn, entryLSN)
		currLsn := wal.GetCurrSeqNum(group)
		assert.LessOrEqual(t, lsn, currLsn)
		assert.NoError(t, e.WaitDone())
	}

	currLsn := wal.GetCurrSeqNum(group)
	drcurrLsn := driver.GetCurrSeqNum()
	ckpEntry, err := wal.RangeCheckpoint(group, 0, currLsn)
	assert.NoError(t, err)
	assert.NoError(t, ckpEntry.WaitDone())
	testutils.WaitExpect(4000, func() bool {
		truncated, err := driver.GetTruncated()
		assert.NoError(t, err)
		return truncated >= drcurrLsn
	})
	truncated, err := driver.GetTruncated()
	assert.NoError(t, err)
	t.Logf("truncated %d, current %d", truncated, drcurrLsn)
	assert.GreaterOrEqual(t, truncated, drcurrLsn)

	for i := 0; i < entryCount; i++ {
		e := mockEntry()
		lsn, err := wal.Append(group, e)
		assert.NoError(t, err)
		assert.NoError(t, e.WaitDone())
		entryGroupID, entryLSN := e.GetLsn()
		assert.Equal(t, group, entryGroupID)
		assert.Equal(t, lsn, entryLSN)
		currLsn := wal.GetCurrSeqNum(group)
		assert.LessOrEqual(t, lsn, currLsn)
		assert.NoError(t, e.WaitDone())
	}

	currLsn = wal.GetCurrSeqNum(group)
	drcurrLsn = driver.GetCurrSeqNum()
	ckpEntry, err = wal.RangeCheckpoint(group, 0, currLsn)
	assert.NoError(t, err)
	assert.NoError(t, ckpEntry.WaitDone())
	testutils.WaitExpect(4000, func() bool {
		truncated, err := driver.GetTruncated()
		assert.NoError(t, err)
		return truncated >= drcurrLsn
	})
	truncated, err = driver.GetTruncated()
	assert.NoError(t, err)
	t.Logf("truncated %d, current %d", truncated, drcurrLsn)
	assert.GreaterOrEqual(t, truncated, drcurrLsn)
}
