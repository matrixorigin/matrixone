package logservicedriver

import (
	"context"
	"sync"
	"time"

	"github.com/matrixorigin/matrixone/pkg/logservice"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logstore/driver/entry"
)

type driverAppender struct {
	client          logservice.Client
	appendlsn       uint64
	logserviceLsn   uint64
	entry           *recordEntry
	contextDuration time.Duration
	wg              sync.WaitGroup
}

func (a *driverAppender) appendEntry(e *entry.Entry) {
	a.entry.append(e)
}

func (a *driverAppender) append() {
	ctx, cancel := context.WithTimeout(context.Background(), a.contextDuration)
	defer cancel()
	lsn, err := a.client.Append(ctx, a.entry.makeRecord())
	if err != nil {
		panic(err)
	}
	a.logserviceLsn = lsn
	a.wg.Done()
}

func (a *driverAppender) waitDone() {
	a.wg.Wait()
}
