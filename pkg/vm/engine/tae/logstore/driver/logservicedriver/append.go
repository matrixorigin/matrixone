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
	d.dsnmu.Lock()
	e.DSN = d.allocateDSNLocked()
	_, err := d.doAppendLoop.Enqueue(e)
	if err != nil {
		panic(err)
	}
	d.dsnmu.Unlock()
	return nil
}

func (d *LogServiceDriver) getAppender() *driverAppender {
	if int(d.currentAppender.writer.Size()) > d.config.RecordSize {
		d.flushCurrentAppender()
	}
	return d.currentAppender
}

// this function flushes the current appender to the append queue and
// creates a new appender as the current appender
func (d *LogServiceDriver) flushCurrentAppender() {
	d.scheduleAppend(d.currentAppender)
	d.appendWaitQueue <- d.currentAppender
	d.currentAppender = newDriverAppender()
}

func (d *LogServiceDriver) onAppendRequests(items ...any) {
	for _, item := range items {
		e := item.(*entry.Entry)
		appender := d.getAppender()
		appender.addEntry(e)
	}
	d.flushCurrentAppender()
}

func (d *LogServiceDriver) scheduleAppend(appender *driverAppender) {
	appender.client, appender.writeToken = d.getClientForWrite()
	appender.writer.SetSafeDSN(d.getSynced())
	appender.contextDuration = d.config.NewClientDuration
	appender.wg.Add(1)
	d.appendPool.Submit(func() {
		defer appender.wg.Done()
		if err := appender.commit(
			10,
			d.config.ClientAppendDuration,
		); err != nil {
			logutil.Fatal(
				"Wal-Cannot-Append",
				zap.Error(err),
			)
		}
	})
}

// Node:
// this function must be called in serial due to the write token
func (d *LogServiceDriver) getClientForWrite() (client *wrappedClient, token uint64) {
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

func (d *LogServiceDriver) onWaitAppendRequests(items []any, nextQueue chan any) {
	appenders := make([]*driverAppender, 0, len(items))
	for _, item := range items {
		appender := item.(*driverAppender)
		appender.waitDone()
		d.clientPool.Put(appender.client)
		appender.notifyDone()
		appenders = append(appenders, appender)
	}
	nextQueue <- appenders
}

func (d *LogServiceDriver) onAppendDone(items []any, _ chan any) {
	tokens := make([]uint64, 0, len(items))
	for _, v := range items {
		appenders := v.([]*driverAppender)
		for _, appender := range appenders {
			d.logAppend(appender)
			tokens = append(tokens, appender.writeToken)
		}
	}
	d.putbackWriteTokens(tokens)
}
