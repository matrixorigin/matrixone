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

var (
	_committerPool = sync.Pool{
		New: func() any {
			return newGroupCommitter()
		},
	}
)

func getCommitter() *groupCommitter {
	return _committerPool.Get().(*groupCommitter)
}

func putCommitter(c *groupCommitter) {
	c.Reset()
	_committerPool.Put(c)
}

type groupCommitter struct {
	sync.WaitGroup

	client     *wrappedClient
	writeToken uint64
	psn        uint64
	writer     *LogEntryWriter
}

func newGroupCommitter() *groupCommitter {
	return &groupCommitter{
		writer: NewLogEntryWriter(),
	}
}

func (a *groupCommitter) Reset() {
	a.client = nil
	a.writeToken = 0
	a.psn = 0
	a.writer.Reset()
}

func (a *groupCommitter) AddIntent(e *entry.Entry) {
	if err := a.writer.AppendEntry(e); err != nil {
		panic(err)
	}
}

func (a *groupCommitter) Commit(
	retryTimes int,
	timeout time.Duration,
) (err error) {
	_, task := gotrace.NewTask(context.Background(), "logservice.append")
	start := time.Now()
	defer func() {
		v2.LogTailAppendDurationHistogram.Observe(time.Since(start).Seconds())
		task.End()
	}()

	entry := a.writer.Finish()

	v2.LogTailBytesHistogram.Observe(float64(entry.Size()))
	defer logSlowAppend(entry.Size(), a.writeToken)()

	var (
		ctx         context.Context
		timeoutSpan trace.Span
	)
	// Before issue#10467 is resolved, we skip this span,
	// avoiding creating too many goroutines, which affects the performance.
	ctx, timeoutSpan = trace.Debug(
		context.Background(),
		"appender",
		trace.WithProfileGoroutine(),
		trace.WithProfileHeap(),
		trace.WithProfileCpuSecs(time.Second*10),
	)
	defer timeoutSpan.End()

	a.psn, err = a.client.Append(
		ctx, entry, time.Second*10, 10, moerr.CauseDriverAppender1,
	)
	return
}

func (a *groupCommitter) PutbackClient(pool *clientpool) {
	pool.Put(a.client)
	a.client = nil
}

func (a *groupCommitter) NotifyCommitted() {
	a.writer.NotifyDone(nil)
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
