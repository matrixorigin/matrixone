package logservicedriver

import (
	"context"
	"sync"
	"time"

	"github.com/matrixorigin/matrixone/pkg/logservice"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logstore/driver/entry"
)

type driverAppender struct {
	flushed         bool
	client          logservice.Client
	appendlsn       uint64
	logserviceLsn   uint64
	entry           *recordEntry
	contextDuration time.Duration
	wg              sync.WaitGroup //wait client
}

func newDriverAppender() *driverAppender {
	return &driverAppender{
		entry: newRecordEntry(),
		wg:    sync.WaitGroup{},
	}
}

func (a *driverAppender) appendEntry(e *entry.Entry) {
	a.entry.append(e)
}

func (a *driverAppender) append() {
	ctx, cancel := context.WithTimeout(context.Background(), a.contextDuration)
	defer cancel()
	a.entry.record = a.client.GetLogRecord(a.entry.prepareRecord())
	a.entry.makeRecord()
	lsn, err := a.client.Append(ctx, a.entry.record)
	if err != nil {
		panic(err)
	}
	a.logserviceLsn = lsn
	a.wg.Done()
}

func (a *driverAppender) waitDone() {
	a.wg.Wait()
}

func (a *driverAppender) freeEntries(){
	for _,e:=range a.entry.entries{
		e.DoneWithErr(nil)
	}
}