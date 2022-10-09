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
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logstore/driver/entry"
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

func (a *driverAppender) append(appendTimeout time.Duration) {
	size := a.entry.prepareRecord()
	// if size > int(common.K)*20 { //todo
	// 	panic(moerr.NewInternalError("record size %d, larger than max size 20K", size))
	// }
	a.client.TryResize(size)
	record := a.client.record
	copy(record.Payload(), a.entry.payload)
	record.ResizePayload(size)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	lsn, err := a.client.c.Append(ctx, a.client.record)
	cancel()
	if err != nil {
		err = RetryWithTimeout(appendTimeout, func() (shouldReturn bool) {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
			lsn, err = a.client.c.Append(ctx, a.client.record)
			cancel()
			return err == nil
		})
	}
	if err != nil {
		logutil.Infof("size is %d", size)
		panic(err)
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
