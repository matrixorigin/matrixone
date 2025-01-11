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
	"time"

	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logstore/driver"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logstore/driver/entry"
	"go.uber.org/zap"
)

type replayer2 struct {
	readBatchSize int

	driver *LogServiceDriver
	handle driver.ApplyHandle

	readCache readCache

	stats struct {
		readDuration time.Duration
		readCount    int
		applyCount   int
	}
}

func newReplayer2(
	handle driver.ApplyHandle,
	driver *LogServiceDriver,
	readBatchSize int,
) *replayer2 {
	return &replayer2{
		handle:        handle,
		driver:        driver,
		readBatchSize: readBatchSize,
		readCache:     newReadCache(),
	}
}

func (r *replayer2) Replay(ctx context.Context) (err error) {
	var (
		resultC      = make(chan error, 1)
		entryStreamC = make(chan *entry.Entry, 20)
	)

	// a dedicated goroutine to replay entries from the entryStreamC
	go r.streamReplaying(ctx, entryStreamC, resultC)

	// wait for the replay to finish
	// replayErr := <-resultC
	return
}

func (r *replayer2) streamReplaying(
	ctx context.Context,
	sourceC <-chan *entry.Entry,
	resultC chan error,
) (err error) {
	var (
		replayCount   int
		applyCount    int
		entry         *entry.Entry
		ok            bool
		zapFields     []zap.Field
		applyDuration time.Duration
	)
	defer func() {
		logger := logutil.Info
		if err != nil {
			logger = logutil.Error
		}
		zapFields = append(zapFields, zap.Duration("apply-duration", applyDuration))
		zapFields = append(zapFields, zap.Int("lsn-replay-count", replayCount))
		zapFields = append(zapFields, zap.Int("lsn-apply-count", applyCount))
		zapFields = append(zapFields, zap.Error(err))
		logger(
			"Wal-Replay-Entries",
			zapFields...,
		)
		resultC <- err
	}()
	for {
		select {
		case <-ctx.Done():
			err = context.Cause(ctx)
			return
		case entry, ok = <-sourceC:
			if !ok {
				return
			}
			replayCount++

			t0 := time.Now()

			// state == driver.RE_Nomal means the entry is applied successfully
			// here log the first applied entry
			if state := r.handle(entry); state == driver.RE_Nomal && applyCount == 0 {
				_, lsn := entry.Entry.GetLsn()
				zapFields = append(zapFields, zap.Uint64("first-apply-dsn", entry.Lsn))
				zapFields = append(zapFields, zap.Uint64("first-apply-lsn", lsn))
				applyCount++
			}
			entry.DoneWithErr(nil)
			entry.Entry.Free()

			applyDuration += time.Since(t0)
		}
	}
}

func (r *replayer2) readNextBatch(
	ctx context.Context,
	fromPSN uint64,
	maxSize int,
) (done bool, err error) {
	return
}
