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
	"math/rand"
	"strconv"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logstore/driver"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logstore/driver/batchstoredriver"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logstore/driver/logservicedriver"
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

// func TestAppendRead(t *testing.T) {
// 	driver := newTestDriver(t, int(mpool.MB)*64)
// 	wal := NewStore(driver)
// 	defer wal.Close()

// 	e := entry.GetBase()
// 	e.SetType(entry.IOET_WALEntry_Test)
// 	err := e.SetPayload([]byte("payload"))
// 	if err != nil {
// 		panic(err)
// 	}
// 	lsn, err := wal.Append(10, e)
// 	assert.NoError(t, err)

// 	err = e.WaitDone()
// 	assert.NoError(t, err)

// 	e2, err := wal.Load(10, lsn)
// 	assert.NoError(t, err)
// 	assert.Equal(t, e.GetPayload(), e2.GetPayload())
// 	e.Free()
// 	e2.Free()
// }

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
	_, err = wal.AppendEntry(10, e)
	assert.NoError(t, err)

	err = e.WaitDone()
	assert.NoError(t, err)
	e.Free()

	e2 := entry.GetBase()
	err = e2.SetPayload(make([]byte, int(mpool.KB*3)))
	if err != nil {
		panic(err)
	}
	_, err = wal.AppendEntry(10, e2)
	assert.NoError(t, err)

	err = e2.WaitDone()
	assert.NoError(t, err)
	e2.Free()

	e2 = entry.GetBase()
	err = e2.SetPayload(make([]byte, int(mpool.KB*3)))
	if err != nil {
		panic(err)
	}
	_, err = wal.AppendEntry(10, e2)
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
	group := GroupUserTxn
	for i := 0; i < entryCount; i++ {
		e := mockEntry()
		lsn, err := wal.AppendEntry(group, e)
		assert.NoError(t, err)
		assert.NoError(t, e.WaitDone())
		entryGroupID, entryLSN := e.GetLsn()
		assert.Equal(t, group, entryGroupID)
		assert.Equal(t, lsn, entryLSN)
		currLsn := wal.GetLSNWatermark()
		assert.LessOrEqual(t, lsn, currLsn)
		assert.NoError(t, e.WaitDone())
	}

	currLsn := wal.GetLSNWatermark()
	drcurrLsn := driver.GetDSN()
	ckpEntry, err := wal.RangeCheckpoint(0, currLsn)
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
		lsn, err := wal.AppendEntry(group, e)
		assert.NoError(t, err)
		assert.NoError(t, e.WaitDone())
		entryGroupID, entryLSN := e.GetLsn()
		assert.Equal(t, group, entryGroupID)
		assert.Equal(t, lsn, entryLSN)
		currLsn := wal.GetLSNWatermark()
		assert.LessOrEqual(t, lsn, currLsn)
		assert.NoError(t, e.WaitDone())
	}

	currLsn = wal.GetLSNWatermark()
	drcurrLsn = driver.GetDSN()
	ckpEntry, err = wal.RangeCheckpoint(0, currLsn)
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

// 1. append 2 entries for group GroupUserTxn
// 2. append 2 entries for group 200000
// 3. append 2 entries for group GroupUserTxn
// 4. RangeCheckpoint(0, 3) => Check DSN and LSN and Checkpointed
// 5. append 2 entries for group 200000
// 6. append 2 entries for group GroupUserTxn
// 7. RangeCheckpoint(0, 5) => Check DSN and LSN and Checkpointed
// 8. append 2 entries for group GroupUserTxn
// 9. Restart => Check DSN and LSN and Checkpointed
func TestReplayWithCheckpoint(t *testing.T) {
	_, clientFactory := logservicedriver.NewMockServiceAndClientFactory()
	wal := NewLogserviceHandle(clientFactory)
	replayHandle := func(
		group uint32,
		commitId uint64,
		payload []byte,
		typ uint16,
		info any,
	) driver.ReplayEntryState {
		return driver.RE_Nomal
	}
	modeGetter := func() driver.ReplayMode {
		return driver.ReplayMode_ReplayForWrite
	}
	err := wal.Replay(context.Background(), replayHandle, modeGetter, nil)
	assert.NoError(t, err)

	groups := []uint32{GroupUserTxn, 200000, GroupUserTxn}

	for _, group := range groups {
		for i := 0; i < 2; i++ {
			e := mockEntry()
			lsn, err := wal.AppendEntry(group, e)
			assert.NoError(t, err)
			assert.NoError(t, e.WaitDone())
			entryGroupID, entryLSN := e.GetLsn()
			assert.Equal(t, group, entryGroupID)
			assert.Equal(t, lsn, entryLSN)
			currLsn := wal.GetLSNWatermark()
			assert.LessOrEqual(t, lsn, currLsn)
		}
	}

	assert.Equal(t, uint64(0), wal.GetCheckpointed())
	assert.Equal(t, uint64(0), wal.GetTruncated())
	assert.Equal(t, uint64(4), wal.GetLSNWatermark())

	// map[11:map[1:1 2:2 3:5 4:6] 200000:map[1:3 2:4]]
	mappingExpected := map[uint32]map[uint64]uint64{
		11:     {1: 1, 2: 2, 3: 5, 4: 6},
		200000: {1: 3, 2: 4},
	}
	testutils.WaitExpect(4000, func() bool {
		wal.lsn2dsn.mu.RLock()
		defer wal.lsn2dsn.mu.RUnlock()
		mapping := wal.lsn2dsn.mapping[11]
		return len(mapping) == 4
	})

	assert.Equalf(t, mappingExpected, wal.lsn2dsn.mapping, "mapping: %v", wal.lsn2dsn.mapping)

	ckpEntry, err := wal.RangeCheckpoint(0, 3)
	assert.NoError(t, err)
	assert.NoError(t, ckpEntry.WaitDone())
	testutils.WaitExpect(4000, func() bool {
		return wal.GetCheckpointed() == uint64(3)
	})
	assert.Equal(t, uint64(3), wal.GetCheckpointed())
	assert.Equal(t, uint64(1), wal.GetPenddingCnt())
	testutils.WaitExpect(4000, func() bool {
		return wal.GetTruncated() == uint64(5)
	})
	assert.Equal(t, uint64(5), wal.GetTruncated())

	for _, group := range groups[:2] {
		for i := 0; i < 2; i++ {
			e := mockEntry()
			lsn, err := wal.AppendEntry(group, e)
			assert.NoError(t, err)
			assert.NoError(t, e.WaitDone())
			entryGroupID, entryLSN := e.GetLsn()
			assert.Equal(t, group, entryGroupID)
			assert.Equal(t, lsn, entryLSN)
			currLsn := wal.GetLSNWatermark()
			assert.LessOrEqual(t, lsn, currLsn)
		}
	}

	assert.Equal(t, uint64(3), wal.GetCheckpointed())
	assert.Equal(t, uint64(3), wal.GetPenddingCnt())
	assert.Equal(t, uint64(5), wal.GetTruncated())

	ckpEntry, err = wal.RangeCheckpoint(0, 5)
	assert.NoError(t, err)
	assert.NoError(t, ckpEntry.WaitDone())
	testutils.WaitExpect(4000, func() bool {
		return wal.GetCheckpointed() == uint64(5)
	})
	assert.Equal(t, uint64(6), wal.GetLSNWatermark())
	assert.Equal(t, uint64(5), wal.GetCheckpointed())
	assert.Equal(t, uint64(1), wal.GetPenddingCnt())
	testutils.WaitExpect(4000, func() bool {
		return wal.GetTruncated() == uint64(8)
	})
	assert.Equal(t, uint64(8), wal.GetTruncated())

	for _, group := range groups[:1] {
		for i := 0; i < 2; i++ {
			e := mockEntry()
			lsn, err := wal.AppendEntry(group, e)
			assert.NoError(t, err)
			assert.NoError(t, e.WaitDone())
			entryGroupID, entryLSN := e.GetLsn()
			assert.Equal(t, group, entryGroupID)
			assert.Equal(t, lsn, entryLSN)
			currLsn := wal.GetLSNWatermark()
			assert.LessOrEqual(t, lsn, currLsn)
		}
	}

	assert.Equal(t, uint64(5), wal.GetCheckpointed())
	assert.Equal(t, uint64(3), wal.GetPenddingCnt())
	assert.Equal(t, uint64(8), wal.GetTruncated())
	assert.Equal(t, uint64(8), wal.GetLSNWatermark())

	_, err = wal.RangeCheckpoint(0, 3)
	assert.ErrorIs(t, err, ErrStaleCheckpointIntent)

	wal.Close()

	wal = NewLogserviceHandle(clientFactory)
	defer wal.Close()

	err = wal.Replay(context.Background(), replayHandle, modeGetter, nil)
	assert.NoError(t, err)
	assert.Equal(t, uint64(8), wal.GetLSNWatermark())
	assert.Equal(t, uint64(5), wal.GetCheckpointed())
	assert.Equal(t, uint64(8), wal.GetTruncated())
	assert.Equal(t, uint64(3), wal.GetPenddingCnt())
	assert.Equal(t, map[uint64]uint64{6: 9, 7: 13, 8: 14}, wal.lsn2dsn.mapping[11])
	assert.Equal(t, map[uint64]uint64{3: 10, 4: 11}, wal.lsn2dsn.mapping[200000])

}
