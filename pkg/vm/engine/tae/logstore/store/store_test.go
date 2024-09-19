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
	"strconv"
	"testing"

	// "net/http"
	// _ "net/http/pprof"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logstore/driver"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logstore/driver/batchstoredriver"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/testutils"

	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logstore/entry"

	"github.com/stretchr/testify/assert"
)

const (
	ModuleName = "logstore"
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
	dir := testutils.InitTestEnv(ModuleName, t)
	name := "test"
	cfg := &batchstoredriver.StoreCfg{
		RotateChecker: batchstoredriver.NewMaxSizeRotateChecker(size),
	}
	s, err := batchstoredriver.NewBaseStore(dir, name, cfg)
	assert.NoError(t, err)
	return s
}

func restartTestDriver(t *testing.T, size int) driver.Driver {
	dir := testutils.InitTestEnv(ModuleName, t)
	name := "test"
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
	driver := newTestDriver(t, int(mpool.MB)*64)
	wal := NewStore(driver)
	defer wal.Close()

	e := entry.GetBase()
	e.SetType(entry.IOET_WALEntry_Test)
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
	e.SetType(entry.IOET_WALEntry_Test)
	err := e.SetPayload([]byte(strconv.Itoa(rand.Intn(10))))
	if err != nil {
		panic(err)
	}
	// payload:=make([]byte,mpool.KB)
	// copy(payload,buf)
	// e.SetPayload(payload)
	return e
}

func TestReplay(t *testing.T) {
	driver := newTestDriver(t, int(mpool.KB)*3)
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
	err = e2.SetPayload(make([]byte, int(mpool.KB*3)))
	if err != nil {
		panic(err)
	}
	_, err = wal.Append(10, e2)
	assert.NoError(t, err)

	err = e2.WaitDone()
	assert.NoError(t, err)
	e2.Free()

	e2 = entry.GetBase()
	err = e2.SetPayload(make([]byte, int(mpool.KB*3)))
	if err != nil {
		panic(err)
	}
	_, err = wal.Append(10, e2)
	assert.NoError(t, err)

	err = e2.WaitDone()
	assert.NoError(t, err)
	e2.Free()

	wal.Close()

	driver = restartTestDriver(t, int(mpool.KB)*3)
	wal = NewStore(driver)
	wal.Close()
}

func TestTruncate(t *testing.T) {
	driver := newTestDriver(t, int(mpool.MB)*64)
	wal := NewStore(driver)
	defer wal.Close()
	entryCount := 5
	group := entry.GTCustomized
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
	assert.GreaterOrEqual(t, truncated, currLsn)
}
