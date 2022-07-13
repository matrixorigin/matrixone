package logservicedriver

import (
	"context"
	"testing"
	"time"

	"github.com/lni/vfs"
	"github.com/matrixorigin/matrixone/pkg/logservice"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logstore/driver/entry"

	// "github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logstore/entry"
	"github.com/stretchr/testify/assert"
)

//TODO update
//copy from logservice
func TestDemo(t *testing.T) {
	fs := vfs.NewStrictMem()
	service, ccfg, err := logservice.NewTestService(fs)
	assert.NoError(t, err)
	defer service.Close()

	// you need to decision what timeout value to use
	// you also need to retry requests on timeout
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	// get two clients, you can get many clients to be used concurrently
	// but each client itself is not goroutine safe
	client1, err := logservice.NewClient(ctx, ccfg)
	assert.NoError(t, err)
	defer client1.Close()
	client2, err := logservice.NewClient(ctx, ccfg)
	assert.NoError(t, err)
	defer client2.Close()

	// Don't use the Data field unless you know what you are doing
	rec1 := client1.GetLogRecord(5)
	rec2 := client2.GetLogRecord(5)
	copy(rec1.Payload(), []byte("hello"))
	copy(rec2.Payload(), []byte("world"))

	// append the records
	lsn1, err := client1.Append(ctx, rec1)
	assert.NoError(t, err)
	lsn2, err := client2.Append(ctx, rec2)
	assert.NoError(t, err)
	assert.Equal(t, lsn1+1, lsn2)

	// read them back and check whether the returned data is expected
	recs, lsn, err := client1.Read(ctx, lsn1, 1024)
	assert.NoError(t, err)
	assert.Equal(t, lsn1, lsn)
	assert.Equal(t, 2, len(recs))
	assert.Equal(t, rec1.Payload(), recs[0].Payload())
	assert.Equal(t, rec2.Payload(), recs[1].Payload())
}

func initTest(t *testing.T) (*logservice.Service, *logservice.ClientConfig) {
	fs := vfs.NewStrictMem()
	service, ccfg, err := logservice.NewTestService(fs)
	assert.NoError(t, err)
	return service, &ccfg
}

func TestAppend(t *testing.T) {
	service, ccfg := initTest(t)
	defer service.Close()

	cfg := newTestConfig(ccfg)
	driver := NewLogServiceDriver(cfg)
	defer func() {
		assert.NoError(t, driver.Close())
	}()

	entries := make([]*entry.Entry, 0)
	for i := 0; i < 5000; i++ {
		e := entry.MockEntry()
		driver.Append(e)
		entries = append(entries, e)
	}

	for _, e := range entries {
		e.WaitDone()
	}

}
