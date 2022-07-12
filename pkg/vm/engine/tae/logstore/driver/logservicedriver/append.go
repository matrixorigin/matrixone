package logservicedriver

import (
	"errors"

	"github.com/matrixorigin/matrixone/pkg/logservice"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logstore/driver/entry"
)

var ErrTooMuchPenddings = errors.New("too much penddings")

func (d *LogServiceDriver) Append(e *entry.Entry) {
	appender := d.getAppender(e.GetSize())
	appender.appendEntry(e)
}

func (d *LogServiceDriver) getAppender(size int) *driverAppender {
	return nil
}

func (d *LogServiceDriver) onAppendQueue(items []any) any {
	for _, item := range items {
		appender := item.(*driverAppender)
		appender.client, appender.appendlsn = d.getClient()
		appender.contextDuration = d.config.NewClientDuration
		appender.wg.Add(1)
		go appender.append()
	}
	return items
}

func (d *LogServiceDriver) getClient() (client logservice.Client, lsn uint64) {
	lsn, err := d.tryAllocate(uint64(d.config.ClientAppendDuration))
	if err != nil {
		panic(err) //TODO retry
	}
	client = d.clientPool.Get().(logservice.Client)
	return
}

func (d *LogServiceDriver) onAppendedQueue(items []any) any {
	appended := make([]uint64, 0)
	for _, v := range items {
		batch := v.([]any)
		for _, item := range batch {
			appender := item.(*driverAppender)
			appender.waitDone()
			d.logAppend(appender)
			appended = append(appended, appender.appendlsn)
		}
	}
	d.onAppend(appended)
	return nil
}
