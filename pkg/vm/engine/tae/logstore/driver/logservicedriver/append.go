// Copyright 2021 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package logservicedriver

import (
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logstore/driver/entry"
	"go.uber.org/zap"
)

var ErrTooMuchPenddings = moerr.NewInternalErrorNoCtx("too much penddings")

func (d *LogServiceDriver) Append(e *entry.Entry) error {
	d.driverLsnMu.Lock()
	e.Lsn = d.allocateDriverLsn()
	_, err := d.preAppendLoop.Enqueue(e)
	if err != nil {
		panic(err)
	}
	d.driverLsnMu.Unlock()
	return nil
}

func (d *LogServiceDriver) getAppender() *driverAppender {
	if int(d.currentAppender.entry.payloadSize) > d.config.RecordSize {
		d.flushCurrentAppender()
	}
	return d.currentAppender
}

// this function flushes the current appender to the append queue and
// creates a new appender as the current appender
func (d *LogServiceDriver) flushCurrentAppender() {
	d.appendtimes++
	d.enqueueAppender(d.currentAppender)
	d.appendedQueue <- d.currentAppender
	d.currentAppender = newDriverAppender()
}

func (d *LogServiceDriver) onPreAppend(items ...any) {
	for _, item := range items {
		e := item.(*entry.Entry)
		appender := d.getAppender()
		appender.appendEntry(e)
	}
	d.flushCurrentAppender()
}

func (d *LogServiceDriver) enqueueAppender(appender *driverAppender) {
	appender.client, appender.writeToken = d.getClient()
	appender.entry.SetAppended(d.getSynced())
	appender.contextDuration = d.config.NewClientDuration
	appender.wg.Add(1)
	d.appendPool.Submit(func() {
		appender.append(d.config.RetryTimeout, d.config.ClientAppendDuration)
	})
}

// Node: this function is called in serial because it also allocates the global sequence number
// the global sequence number(GSN) should be monotonically continuously increasing
func (d *LogServiceDriver) getClient() (client *clientWithRecord, token uint64) {
	var err error
	if token, err = d.applyWriteToken(
		uint64(d.config.ClientMaxCount), time.Second,
	); err != nil {
		// should never happen
		panic(err)
	}
	if client, err = d.clientPool.Get(); err == nil {
		return
	}

	var (
		retryCount = 0
		now        = time.Now()
		logger     = logutil.Info
	)
	if err = RetryWithTimeout(d.config.RetryTimeout, func() (shouldReturn bool) {
		retryCount++
		client, err = d.clientPool.Get()
		return err == nil
	}); err != nil {
		logger = logutil.Error
	}
	logger(
		"Wal-Get-Client",
		zap.Int("retry-count", retryCount),
		zap.Error(err),
		zap.Duration("duration", time.Since(now)),
	)
	if err != nil {
		panic(err)
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
	tokens := make([]uint64, 0, len(items))
	for _, v := range items {
		batch := v.([]*driverAppender)
		for _, appender := range batch {
			d.logAppend(appender)
			tokens = append(tokens, appender.writeToken)
		}
	}
	d.putbackWriteTokens(tokens)
}
