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
	gotrace "runtime/trace"
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	v2 "github.com/matrixorigin/matrixone/pkg/util/metric/v2"
	"github.com/matrixorigin/matrixone/pkg/util/trace"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logstore/driver/entry"
)

const SlowAppendThreshold = 1 * time.Second

type driverAppender struct {
	client          *wrappedClient
	writeToken      uint64
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
	_, task := gotrace.NewTask(context.Background(), "logservice.append")
	start := time.Now()
	defer func() {
		v2.LogTailAppendDurationHistogram.Observe(time.Since(start).Seconds())
		task.End()
	}()

	size := a.entry.prepareRecord()
	// if size > int(common.K)*20 { //todo
	// 	panic(moerr.NewInternalError("record size %d, larger than max size 20K", size))
	// }
	a.client.TryResize(size)
	logutil.Debugf("Log Service Driver: append start prepare %p", a.client.record.Data)
	record := a.client.record
	copy(record.Payload(), a.entry.payload)
	record.ResizePayload(size)
	defer logSlowAppend(size, a.writeToken)()
	ctx, cancel := context.WithTimeoutCause(context.Background(), appendTimeout, moerr.CauseDriverAppender1)

	var timeoutSpan trace.Span
	// Before issue#10467 is resolved, we skip this span,
	// avoiding creating too many goroutines, which affects the performance.
	ctx, timeoutSpan = trace.Debug(ctx, "appender",
		trace.WithProfileGoroutine(),
		trace.WithProfileHeap(),
		trace.WithProfileCpuSecs(time.Second*10))
	defer timeoutSpan.End()

	v2.LogTailBytesHistogram.Observe(float64(size))
	logutil.Debugf("Log Service Driver: append start %p", a.client.record.Data)
	lsn, err := a.client.c.Append(ctx, record)
	if err != nil {
		err = moerr.AttachCause(ctx, err)
		logutil.Error(
			"WAL-Append-Error",
			zap.Error(err),
			zap.Uint64("write-token", a.writeToken),
			zap.Int("client-id", a.client.id),
			zap.Int("size", size),
		)
	}
	cancel()
	if err != nil {
		retryTimes := 0
		err = RetryWithTimeout(retryTimout, func() (shouldReturn bool) {
			retryTimes++
			ctx, cancel := context.WithTimeoutCause(context.Background(), appendTimeout, moerr.CauseDriverAppender2)
			ctx, timeoutSpan = trace.Debug(ctx, "appender retry",
				trace.WithProfileGoroutine(),
				trace.WithProfileHeap(),
				trace.WithProfileCpuSecs(time.Second*10))
			defer timeoutSpan.End()
			lsn, err = a.client.c.Append(ctx, record)
			err = moerr.AttachCause(ctx, err)
			cancel()
			if err != nil {
				logutil.Error(
					"WAL-Append-Error",
					zap.Error(err),
					zap.Uint64("write-token", a.writeToken),
					zap.Int("client-id", a.client.id),
					zap.Int("size", size),
					zap.Int("retry", retryTimes),
				)
			}
			return err == nil
		})
	}
	if err != nil {
		logutil.Error(
			"WAL-Append-Error",
			zap.Error(err),
			zap.Uint64("write-token", a.writeToken),
			zap.Int("client-id", a.client.id),
			zap.Int("size", size),
		)
		logutil.Panic(err.Error())
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

func logSlowAppend(
	size int,
	writeToken uint64,
) func() {
	start := time.Now()
	return func() {
		elapsed := time.Since(start)
		if elapsed >= SlowAppendThreshold {
			logutil.Warn(
				"SLOW-LOG-AppendWAL",
				zap.Duration("latency", elapsed),
				zap.Int("size", size),
				zap.Uint64("write-token", writeToken),
			)
		}
	}
}
