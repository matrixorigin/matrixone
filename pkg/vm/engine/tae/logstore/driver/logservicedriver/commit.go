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
var ErrNeedReplayForWrite = moerr.NewInternalErrorNoCtx("need replay for write")

func (d *LogServiceDriver) Append(e *entry.Entry) (err error) {
	if !d.canWrite() {
		return ErrNeedReplayForWrite
	}

	_, err = d.commitLoop.Enqueue(e)
	return
}

func (d *LogServiceDriver) getCommitter() *groupCommitter {
	//if int(d.committer.writer.Size()) > d.config.ClientBufSize {
	//	d.flushCurrentCommitter()
	//}

	if len(d.committer.writer.entries) > 30 {
		d.flushCurrentCommitter()
	}

	return d.committer
}

// this function flushes the current committer to the append queue and
// creates a new committer as the current committer
func (d *LogServiceDriver) flushCurrentCommitter() {
	d.asyncCommit(d.committer)
	d.commitWaitQueue <- d.committer
	d.committer = getCommitter()
}

//func (d *LogServiceDriver) onCommitIntents(items ...any) {
//	for _, item := range items {
//		e := item.(*entry.Entry)
//		e.DSN = d.allocateDSN()
//		d.pending = append(d.pending, e)
//	}
//
//	d.flushPending()
//}
//
//func (d *LogServiceDriver) flushPending() {
//	entries := d.pending[:]
//	d.pending = nil
//
//	d.splitWorker.Submit(func() {
//		var committer *groupCommitter
//		for _, e := range entries {
//			if committer == nil {
//				committer = getCommitter()
//			}
//
//			if err := e.Entry.ExecuteGroupWalPreCallbacks(); err != nil {
//				logutil.Fatal(
//					"Wal-Cannot-Split",
//					zap.Error(err),
//				)
//			}
//
//			committer.AddIntent(e)
//
//			if int(committer.writer.Size()) > d.config.ClientBufSize {
//				d.flushCurrentCommitter(committer)
//				committer = nil
//			}
//		}
//
//		if committer != nil {
//			d.flushCurrentCommitter(committer)
//		}
//	})
//}

func (d *LogServiceDriver) onCommitIntents(items ...any) {
	for _, item := range items {
		e := item.(*entry.Entry)
		e.DSN = d.allocateDSN()
		committer := d.getCommitter()
		committer.AddIntent(e)
	}
	d.flushCurrentCommitter()
}

func (d *LogServiceDriver) asyncCommit(committer *groupCommitter) {
	// apply write token and bind the client for committing
	var err error
	if committer.client, err = d.getClientForWrite(); err != nil {
		panic(err)
	}

	// set the safe DSN for the committer
	// the safe DSN is the DSN of the last committed entry
	// it is used to apply the DSN in consecutive sequence
	committer.writer.SetSafeDSN(d.getCommittedDSNWatermark())

	committer.Add(1)
	d.appendWorker.Submit(func() {
		defer committer.Done()
		if err := committer.Commit(); err != nil {
			logutil.Fatal(
				"Wal-Cannot-Append",
				zap.Error(err),
			)
		}
	})
}

// get a client from the client pool for writing user data
// user data: record with DSN
// Truncate Logrecord is not user data
func (d *LogServiceDriver) getClientForWrite() (client *wrappedClient, err error) {
	now := time.Now()
	defer func() {
		if err != nil || time.Since(now) > time.Second*2 {
			logger := logutil.Info
			if err != nil {
				logger = logutil.Error
			}
			logger(
				"Wal-Get-Client",
				zap.Duration("duration", time.Since(now)),
				zap.Error(err),
			)
		}
	}()

	client, err = d.clientPool.GetWithWriteToken()
	return
}

func (d *LogServiceDriver) onWaitCommitted(items []any, nextQueue chan any) {
	for _, item := range items {
		committer := item.(*groupCommitter)
		committer.Wait()
		committer.PutbackClient()
		committer.NotifyCommitted()
		d.recordCommitInfo(committer)
		putCommitter(committer)
	}
}
