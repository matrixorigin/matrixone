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

func (d *LogServiceDriver) getCommitter() *groupCommitter {
	if int(d.committer.writer.Size()) > d.config.RecordSize {
		d.flushCurrentCommitter()
	}
	return d.committer
}

// this function flushes the current committer to the append queue and
// creates a new committer as the current committer
func (d *LogServiceDriver) flushCurrentCommitter() {
	d.prepareCommit(d.committer)
	d.appendWaitQueue <- d.committer
	d.committer = newGroupCommitter()
}

func (d *LogServiceDriver) onCommitIntents(items ...any) {
	for _, item := range items {
		e := item.(*entry.Entry)
		committer := d.getCommitter()
		committer.addEntry(e)
	}
	d.flushCurrentCommitter()
}

func (d *LogServiceDriver) prepareCommit(committer *groupCommitter) {
	// apply write token and bind the client for committing
	committer.client, committer.writeToken = d.getClientForWrite()

	// set the safe DSN for the committer
	// the safe DSN is the DSN of the last committed entry
	// it is used to apply the DSN in consecutive sequence
	committer.writer.SetSafeDSN(d.getCommittedDSNWatermark())

	committer.Add(1)
	d.appendPool.Submit(func() {
		defer committer.Done()
		if err := committer.commit(
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

func (d *LogServiceDriver) onWaitCommitted(items []any, nextQueue chan any) {
	committers := make([]*groupCommitter, 0, len(items))
	for _, item := range items {
		committer := item.(*groupCommitter)
		committer.Wait()
		d.clientPool.Put(committer.client)
		committer.notifyDone()
		committers = append(committers, committer)
	}
	nextQueue <- committers
}

func (d *LogServiceDriver) onCommitDone(items []any, _ chan any) {
	tokens := make([]uint64, 0, len(items))
	for _, v := range items {
		committers := v.([]*groupCommitter)
		for _, committer := range committers {
			d.recordCommitInfo(committer)
			tokens = append(tokens, committer.writeToken)
		}
	}

	d.commitWatermark()
	d.putbackWriteTokens(tokens)
}
