package wal

import (
	"math/rand"
	"os"
	"strconv"
	"sync"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logstore/driver"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logstore/driver/batchstoredriver"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logstore/entry"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/testutils"
	taeWal "github.com/matrixorigin/matrixone/pkg/vm/engine/tae/wal"
	"github.com/panjf2000/ants/v2"
	"github.com/stretchr/testify/assert"
)

func newTestDriver(t *testing.T) driver.Driver {
	dir := "/tmp/logstore/teststore"
	name := "mock"
	os.RemoveAll(dir)
	cfg := &batchstoredriver.StoreCfg{
		RotateChecker: batchstoredriver.NewMaxSizeRotateChecker(int(common.K) * 3),
	}
	s, err := batchstoredriver.NewBaseStore(dir, name, cfg)
	assert.NoError(t, err)
	return s
}
func TestAppendRead(t *testing.T) {
	driver := newTestDriver(t)
	wal := NewLogStore(driver)
	defer wal.Close()

	e := entry.GetBase()
	e.SetPayload([]byte("payload"))
	lsn, err := wal.Append(10, e)
	assert.NoError(t, err)

	e.WaitDone()

	e2, err := wal.Load(10, lsn)
	assert.NoError(t, err)
	assert.Equal(t, e.GetPayload(), e2.GetPayload())
	e.Free()
	e2.Free()
}

func mockEntry() entry.Entry {
	e := entry.GetBase()
	e.SetPayload([]byte(strconv.Itoa(rand.Intn(10))))
	return e
}

func TestWal(t *testing.T) {
	driver := newTestDriver(t)
	wal := NewLogStore(driver)
	defer wal.Close()

	entryCount := 100
	entries := make([]entry.Entry, 0)
	wg := sync.WaitGroup{}
	worker, _ := ants.NewPool(1000)
	appendfn := func(i int, group uint32) func() {
		return func() {
			e := entries[i]
			lsn, err := wal.Append(group, e)
			assert.NoError(t, err)
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
			idxes := []*taeWal.Index{{LSN: entryLSN, CSN: 0, Size: 1}}
			ckpEntry := wal.FuzzyCheckpoint(entryGroupID, idxes)
			ckpEntry.WaitDone()
			ckpGroup, ckpLsn := ckpEntry.GetLsn()
			_, err := wal.Load(ckpGroup, ckpLsn)
			assert.Equal(t,GroupCKP,ckpGroup)
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
			entryGroupID2, entryLSN2 := e2.GetLsn()
			assert.Equal(t, entryGroupID, entryGroupID2)
			assert.Equal(t, entryLSN, entryLSN2)
			assert.Equal(t, e.GetPayload(), e2.GetPayload())
			testutils.WaitExpect(4000, func() bool {
				return wal.GetSynced(entryGroupID) >= entryLSN
			})
			synced := wal.GetSynced(entryGroupID)
			assert.LessOrEqual(t, entryLSN, synced)
			e2.Free()
			wg.Done()
		}
	}


	for i := 0; i < entryCount; i++ {
		e := mockEntry()
		entries = append(entries, e)
	}
	for i := 0; i < entryCount; i++ {
		group:=uint32(10+rand.Intn(3))
		// group := uint32(5)
		wg.Add(1)
		worker.Submit(appendfn(i, group))
		wg.Add(1)
		worker.Submit(readfn(i))
	}
	wg.Wait()
	for i := 0; i < entryCount; i++ {
		wg.Add(1)
		worker.Submit(truncatefn(i))
	}
	wg.Wait()
	for i := 0; i < entryCount; i++ {
		e := entries[i]
		e.Free()
	}
}
