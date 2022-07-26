package logservicedriver

import (
	"testing"

	"github.com/lni/vfs"
	"github.com/matrixorigin/matrixone/pkg/logservice"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logstore/driver/entry"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/testutils"

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

	for _, e := range entries {
		e2, err := driver.Read(e.Lsn)
		assert.NoError(t, err)
		assert.Equal(t, e2.Lsn, e.Lsn)
	}

	driver = restartDriver(t, driver)
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

	for _, e := range entries {
		assert.NoError(t, driver.Truncate(e.Lsn))
		testutils.WaitExpect(400, func() bool {
			trucated, err := driver.GetTruncated()
			assert.NoError(t, err)
			return trucated == e.Lsn
		})
		trucated, err := driver.GetTruncated()
		assert.NoError(t, err)
		assert.Equal(t, trucated, e.Lsn)
	}

	driver = restartDriver(t, driver)
}
