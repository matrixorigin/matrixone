package logservicedriver

import (
	"sync"
	"testing"
	"time"

	"github.com/lni/vfs"
	"github.com/matrixorigin/matrixone/pkg/logservice"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logstore/driver/entry"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/testutils"
	"github.com/panjf2000/ants/v2"

	// "github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logstore/entry"
	"github.com/stretchr/testify/assert"
)

func initTest(t *testing.T) (*logservice.Service, *logservice.ClientConfig) {
	fs := vfs.NewStrictMem()
	service, ccfg, err := logservice.NewTestService(fs)
	assert.NoError(t, err)
	return service, &ccfg
}

func restartDriver(t *testing.T, d *LogServiceDriver) *LogServiceDriver {
	assert.NoError(t, d.Close())
	return NewLogServiceDriver(d.config)
}

func TestAppendRead(t *testing.T) {
	service, ccfg := initTest(t)
	defer service.Close()

	cfg := NewTestConfig(ccfg)
	driver := NewLogServiceDriver(cfg)

	entryCount := 100
	wg:=&sync.WaitGroup{}
	worker, _ := ants.NewPool(100)
	entries := make([]*entry.Entry, entryCount)
	appendfn := func(i int) func() {
		return func() {
			e := entry.MockEntry()
			driver.Append(e)
			entries[i] = e
			wg.Done()
		}
	}

	reanfn := func(i int) func() {
		return func() {
			e := entries[i]
			e.WaitDone()
			e2, err := driver.Read(e.Lsn)
			assert.NoError(t, err)
			assert.Equal(t, e2.Lsn, e.Lsn)
			_,lsn1:=e.Entry.GetLsn()
			_,lsn2:=e2.Entry.GetLsn()
			assert.Equal(t, lsn1, lsn2)
			wg.Done()
			e2.Entry.Free()
		}
	}

	for i := 0; i < entryCount; i++ {
		wg.Add(1)
		worker.Submit(appendfn(i))
	}
	wg.Wait()


	for i := 0; i < entryCount; i++ {
		wg.Add(1)
		worker.Submit(reanfn(i))
	}
	wg.Wait()

	// driver = restartDriver(t, driver)

	// for i := 0; i < entryCount; i++ {
	// 	wg.Add(1)
	// 	worker.Submit(reanfn(i))
	// }
	// wg.Wait()

	for _,e:=range entries{
		e.Entry.Free()
	}

	driver.Close()
}

func TestTruncate(t *testing.T) {
	service, ccfg := initTest(t)
	defer service.Close()

	cfg := NewTestConfig(ccfg)
	driver := NewLogServiceDriver(cfg)
	defer func() {
		assert.NoError(t, driver.Close())
	}()

	entries := make([]*entry.Entry, 0)
	for i := 0; i < 10; i++ {
		e := entry.MockEntry()
		driver.Append(e)
		entries = append(entries, e)
	}

	for _, e := range entries {
		e.WaitDone()
	}

	time.Sleep(time.Second)
	for _, e := range entries {
		assert.NoError(t, driver.Truncate(e.Lsn))
		testutils.WaitExpect(400, func() bool {
			trucated, err := driver.GetTruncated()
			assert.NoError(t, err)
			return trucated == e.Lsn
		})
		truncated, err := driver.GetTruncated()
		assert.NoError(t, err)
		assert.Equal(t, truncated, e.Lsn)
	}

	// driver = restartDriver(t, driver)
}
