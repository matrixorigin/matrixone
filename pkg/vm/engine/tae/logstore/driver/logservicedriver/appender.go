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
	"context"
	"sync"
	"time"

	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/util/trace"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logstore/driver/entry"

	"go.uber.org/zap"
)

type driverAppender struct {
	client          *clientWithRecord
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

func (a *driverAppender) append(retryTimout, appendTimeout time.Duration) {
	retryTimout = 10 * time.Millisecond
	appendTimeout = time.Millisecond
	size := a.entry.prepareRecord()
	// if size > int(common.K)*20 { //todo
	// 	panic(moerr.NewInternalError("record size %d, larger than max size 20K", size))
	// }
	a.client.TryResize(size)
	logutil.Debugf("Log Service Driver: append start prepare %p", a.client.record.Data)
	record := a.client.record
	copy(record.Payload(), a.entry.payload)
	record.ResizePayload(size)
	defer logSlowAppend()()
	ctx, cancel := context.WithTimeout(context.Background(), appendTimeout)

	var timeoutSpan trace.Span
	ctx, timeoutSpan = trace.Start(ctx, "appender",
		trace.WithProfileGoroutine(),
		trace.WithProfileHeap(),
		trace.WithProfileCpuSecs(time.Second*10))
	defer timeoutSpan.End()

	logutil.Debug("append", zap.Duration("retry", retryTimout), zap.Duration("append", appendTimeout))
	logutil.Debugf("Log Service Driver: append start %p", a.client.record.Data)
	lsn, err := a.client.c.Append(ctx, record)
	if err != nil {
		logutil.Error("append failed", zap.Error(err), trace.ContextField(ctx))
	}
	cancel()
	if err != nil {

		err = RetryWithTimeout(retryTimout, func() (shouldReturn bool) {
			ctx, cancel := context.WithTimeout(ctx, appendTimeout)
			ctx, timeoutSpan = trace.Start(ctx, "appender retry",
				trace.WithProfileGoroutine(),
				trace.WithProfileHeap(),
				trace.WithProfileCpuSecs(time.Second*10))
			defer timeoutSpan.End()
			lsn, err = a.client.c.Append(ctx, record)
			cancel()
			if err != nil {
				logutil.Error("append failed", zap.Error(err), trace.ContextField(ctx))
			}
			return err == nil
		})
	}
	logutil.Debugf("Log Service Driver: append end %p", a.client.record.Data)
	if err != nil {
		logutil.Infof("size is %d", size)
		logutil.Panic("error", zap.Error(err), trace.ContextField(ctx),
			zap.Duration("retry", retryTimout), zap.Duration("append", appendTimeout))
	}
	a.logserviceLsn = lsn
	a.wg.Done()
}

func (a *driverAppender) waitDone() {
	a.wg.Wait()
}

func (a *driverAppender) freeEntries() {
	for _, e := range a.entry.entries {
		e.DoneWithErr(nil)
	}
}

func logSlowAppend() func() {
	const slowAppend = 1 * time.Second
	start := time.Now()
	return func() {
		elapsed := time.Since(start)
		if elapsed >= slowAppend {
			logutil.Warnf("append to logservice took %s", elapsed)
		}
	}
}
