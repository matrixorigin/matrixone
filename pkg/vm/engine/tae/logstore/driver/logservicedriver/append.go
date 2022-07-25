package logservicedriver

import (
	"errors"

	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logstore/driver/entry"
)

var ErrTooMuchPenddings = errors.New("too much penddings")

type flushEntry struct{}

func (d *LogServiceDriver) Append(e *entry.Entry) {
	e.Lsn = d.allocateDriverLsn()
	d.preAppendQueue <- e
}

func (d *LogServiceDriver) getAppender(size int) *driverAppender {
	if int(d.appendable.entry.payloadSize)+size > d.config.RecordSize {
		d.appendAppender()
	}
	return d.appendable
}

func (d *LogServiceDriver) appendAppender() {
	d.appendQueue <- d.appendable
	d.appendable = newDriverAppender()
}

func (d *LogServiceDriver) flushAppend() {
	d.preAppendQueue <- &flushEntry{}
}

func (d *LogServiceDriver) doFlush() {
	if d.appendable.flushed && d.appendable.entry.payloadSize != 0 {
		d.appendAppender()
	} else {
		d.appendable.flushed = true
	}
}

func (d *LogServiceDriver) onPreAppend(items []any, _ chan any) {
	for _, item := range items {
		switch v := item.(type) {
		case *entry.Entry:
			appender := d.getAppender(v.GetSize())
			appender.appendEntry(v)
		case *flushEntry:
			d.doFlush()
		}
	}
}

func (d *LogServiceDriver) onAppendQueue(items []any, q chan any) {
	for _, item := range items {
		appender := item.(*driverAppender)
		appender.client, appender.appendlsn = d.getClient()
		appender.entry.SetAppended(d.getAppended())
		appender.contextDuration = d.config.NewClientDuration
		appender.wg.Add(1)
		go appender.append()
	}
	q <- items
}

func (d *LogServiceDriver) getClient() (client *clientWithRecord, lsn uint64) {
	lsn, err := d.tryAllocate(uint64(d.config.ClientAppendDuration))
	if err != nil {
		panic(err) //TODO retry
	}
	client = d.clientPool.Get().(*clientWithRecord)
	return
}

func (d *LogServiceDriver) onAppendedQueue(items []any, q chan any) {
	appenders := make([]*driverAppender, 0)
	for _, v := range items {
		batch := v.([]any)
		for _, item := range batch {
			appender := item.(*driverAppender)
			appender.waitDone()
			d.clientPool.Put(appender.client)
			appender.freeEntries()
			appenders = append(appenders, appender)
		}
	}
	q <- appenders
}

func (d *LogServiceDriver) onPostAppendQueue(items []any, _ chan any) {
	appended := make([]uint64, 0)
	logserviceAppended := make([]uint64, 0)
	for _, v := range items {
		batch := v.([]*driverAppender)
		for _, appender := range batch {
			d.logAppend(appender)
			appended = append(appended, appender.appendlsn)
			logserviceAppended = append(logserviceAppended, appender.logserviceLsn)
		}
	}
	d.onAppend(appended, logserviceAppended)
}
