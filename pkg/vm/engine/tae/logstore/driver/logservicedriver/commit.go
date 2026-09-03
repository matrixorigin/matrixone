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
	"errors"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logstore/driver/entry"
	"github.com/panjf2000/ants/v2"
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
	if int(d.committer.writer.ApproxSize()) > d.config.ClientBufSize {
		d.flushCurrentCommitter()
	}

	if len(d.committer.writer.entries) >= d.config.ClientMaxEntryCount {
		d.flushCurrentCommitter()
	}

	return d.committer
}

// this function flushes the current committer to the append queue and
// creates a new committer as the current committer
func (d *LogServiceDriver) flushCurrentCommitter() {
	d.addPendingWait()
	d.asyncCommit(d.committer)
	d.commitWaitQueue <- d.committer
	d.committer = getCommitter()
}

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
		// The committer owns every entry already appended to its writer. Complete
		// those waiters before fail-stop so none of them can wait forever.
		committer.setError(err)
		committer.NotifyError(err)
		panic(err)
	}

	// set the safe DSN for the committer
	// the safe DSN is the DSN of the last committed entry
	// it is used to apply the DSN in consecutive sequence
	committer.writer.SetSafeDSN(d.getCommittedDSNWatermark())

	committer.startCommit()
	if err := d.submitCommit(func() {
		defer committer.finishCommit()
		if err2 := committer.Commit(); err2 != nil {
			committer.setError(err2)
			committer.NotifyError(err2)
			d.onAppendFailure(err2)
		}
	}); err != nil {
		// A close race can reject the worker submission after the committer has
		// acquired a client and accepted entries. Complete those entries here;
		// the wait loop will observe the error and must not advance the committed
		// DSN watermark for a committer that never reached LogService.
		committer.setError(err)
		committer.NotifyError(err)
		// No wait-loop callback will own this committer on a rejected
		// submission, so return the client here.  The normal append path keeps
		// ownership until onWaitCommitted and is handled below.
		committer.PutbackClient()
		committer.finishCommit()
	}
}

// submitCommit keeps normal traffic from turning the short interval between
// committer completion and ants worker reuse into a WAL failure. During close,
// closeC makes the retry bounded so intake can still stop at the driver's
// deadline.
func (d *LogServiceDriver) submitCommit(task func()) error {
	for {
		err := d.workers.Submit(task)
		if !errors.Is(err, ants.ErrPoolOverload) {
			return err
		}

		timer := time.NewTimer(time.Millisecond)
		select {
		case <-d.closeC:
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			return err
		case <-timer.C:
		}
	}
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
		committer.waitCommit()
		if committer.client != nil {
			committer.PutbackClient()
		}
		if err := committer.getError(); err != nil {
			committer.NotifyError(err)
		} else {
			committer.NotifyCommitted()
			d.recordCommitInfo(committer)
		}
		putCommitter(committer)
		d.donePendingWait()
	}
}
