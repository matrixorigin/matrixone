package logservicedriver

import (
	"errors"
	"time"

	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logstore/driver/entry"
)

var ErrTooMuchPenddings = errors.New("too much penddings")

func (d *LogServiceDriver) Append(e *entry.Entry) error {
	e.Lsn = d.allocateDriverLsn()
	d.preAppendLoop.Enqueue(e)
	return nil
}

func (d *LogServiceDriver) getAppender(size int) *driverAppender {
	if int(d.appendable.entry.payloadSize)+size > d.config.RecordSize {
		d.appendAppender()
	}
	return d.appendable
}

func (d *LogServiceDriver) appendAppender() {
	d.appendtimes++
	d.onAppendQueue(d.appendable)
	d.appendedQueue<-d.appendable
	d.appendable = newDriverAppender()
}

func (d *LogServiceDriver) onPreAppend(items ...any) {
	for _, item := range items {
		e := item.(*entry.Entry)
		appender := d.getAppender(e.GetSize())
		appender.appendEntry(e)
	}
	d.appendAppender()
}

func (d *LogServiceDriver) onAppendQueue(appender *driverAppender) {
		appender.client, appender.appendlsn = d.getClient()
		appender.entry.SetAppended(d.getAppended())
		appender.contextDuration = d.config.NewClientDuration
		appender.wg.Add(1)
		go appender.append()
}

func (d *LogServiceDriver) getClient() (client *clientWithRecord, lsn uint64) {
	lsn, err := d.retryAllocateAppendLsnWithTimeout(uint64(d.config.AppenderMaxCount),time.Second)
	if err != nil {
		panic(err) //TODO retry
	}
	client,err = d.clientPool.Get()
	if err != nil {
		panic(err) //TODO retry
	}
	return
}

func (d *LogServiceDriver) onAppendedQueue(items []any, q chan any) {
	appenders := make([]*driverAppender, 0)

	for _, item := range items {
			appender := item.(*driverAppender)
			appender.waitDone()
			d.clientPool.Put(appender.client)
			appender.freeEntries()
			appenders = append(appenders, appender)
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
