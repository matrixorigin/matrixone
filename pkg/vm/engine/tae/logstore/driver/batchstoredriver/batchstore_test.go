package batchstoredriver

import (
	"os"
	"sync"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logstore/driver/entry"
	storeEntry "github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logstore/entry"
	"github.com/panjf2000/ants/v2"
	"github.com/stretchr/testify/assert"
)

func TestDriver(t *testing.T) {
	dir := "/tmp/logstore/teststore"
	name := "mock"
	os.RemoveAll(dir)
	cfg := &StoreCfg{
		RotateChecker: NewMaxSizeRotateChecker(int(common.K) * 3),
	}
	s, err := NewBaseStore(dir, name, cfg)
	assert.NoError(t, err)
	defer s.Close()

	entryCnt := 100
	entries := make([]*entry.Entry, 0)
	for i := 0; i < entryCnt; i++ {
		e := entry.MockEntry()
		entries = append(entries, e)
	}

	wg := sync.WaitGroup{}
	worker, _ := ants.NewPool(10)
	appendfn := func(i int) func() {
		return func() {
			e := entries[i]
			err := s.Append(e)
			assert.NoError(t, err)
			lsn := s.GetCurrSeqNum()
			assert.GreaterOrEqual(t,lsn,e.Lsn)
			wg.Done()
		}
	}

	readfn := func(i int) func() {
		return func() {
			e := entries[i]
			assert.NoError(t, e.WaitDone())
			e2, err := s.Read(e.Lsn)
			assert.NoError(t, err)
			assert.Equal(t, e2.Entry.GetInfo().(*storeEntry.Info).GroupLSN, e.Info.GroupLSN)
			e2.Entry.Free()
			wg.Done()
		}
	}

	truncatefn := func(i int) func() {
		return func() {
			e := entries[i]
			err := s.Truncate(e.Lsn)
			assert.NoError(t, err)
			e.Entry.Free()
			lsn,err:=s.GetTruncated()
			assert.NoError(t, err)
			assert.GreaterOrEqual(t,lsn,e.Lsn)
			wg.Done()
		}
	}

	for i := range entries {
		wg.Add(1)
		worker.Submit(appendfn(i))
		wg.Add(1)
		worker.Submit(readfn(i))
	}
	wg.Wait()
	for i := range entries {
		wg.Add(1)
		worker.Submit(truncatefn(i))
	}
	wg.Wait()

	cnt := s.GetPenddingCnt()
	assert.Equal(t,uint64(0),cnt)
}
