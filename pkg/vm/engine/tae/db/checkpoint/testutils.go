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

package checkpoint

import (
	"context"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db/dbutils"
	"go.uber.org/zap"
)

type TestRunner interface {
	EnableCheckpoint()
	DisableCheckpoint()

	CleanPenddingCheckpoint()
	ForceGlobalCheckpoint(end types.TS, versionInterval time.Duration) error
	ForceGlobalCheckpointSynchronously(ctx context.Context, end types.TS, versionInterval time.Duration) error
	ForceCheckpointForBackup(end types.TS) (string, error)
	ForceIncrementalCheckpoint2(end types.TS) error
	MaxLSNInRange(end types.TS) uint64

	GCNeeded() bool
}

// DisableCheckpoint stops generating checkpoint
func (r *runner) DisableCheckpoint() {
	r.disabled.Store(true)
}

func (r *runner) EnableCheckpoint() {
	r.disabled.Store(false)
}

func (r *runner) CleanPenddingCheckpoint() {
	r.store.CleanPenddingCheckpoint()
}

func (r *runner) ForceGlobalCheckpoint(end types.TS, interval time.Duration) error {
	if interval == 0 {
		interval = r.options.globalVersionInterval
	}
	if r.GetPenddingIncrementalCount() != 0 {
		end2 := r.MaxIncrementalCheckpoint().GetEnd()
		if end2.GE(&end) {
			r.globalCheckpointQueue.Enqueue(&globalCheckpointContext{
				force:    true,
				end:      end2,
				interval: interval,
			})
			return nil
		}
	}
	var (
		retryTime int
		now       = time.Now()
		err       error
	)
	defer func() {
		logger := logutil.Info
		if err != nil {
			logger = logutil.Error
		}
		logger(
			"ForceGlobalCheckpoint-End",
			zap.Int("retry-time", retryTime),
			zap.Duration("cost", time.Since(now)),
			zap.String("ts", end.ToString()),
			zap.Error(err),
		)
	}()

	timeout := time.After(interval)
	for {
		select {
		case <-timeout:
			return moerr.NewInternalError(r.ctx, "timeout")
		default:
			err = r.ForceIncrementalCheckpoint2(end)
			if err != nil {
				if dbutils.IsRetrieableCheckpoint(err) {
					retryTime++
					interval := interval.Milliseconds() / 400
					time.Sleep(time.Duration(interval))
					break
				}
				return err
			}
			r.globalCheckpointQueue.Enqueue(&globalCheckpointContext{
				force:    true,
				end:      end,
				interval: interval,
			})
			return nil
		}
	}
}

func (r *runner) ForceGlobalCheckpointSynchronously(ctx context.Context, end types.TS, versionInterval time.Duration) error {
	r.ForceGlobalCheckpoint(end, versionInterval)

	op := func() (ok bool, err error) {
		global := r.store.MaxGlobalCheckpoint()
		if global == nil {
			return false, nil
		}
		ok = global.end.GE(&end)
		return
	}
	err := common.RetryWithIntervalAndTimeout(
		op,
		time.Minute,
		time.Millisecond*400,
		false,
	)
	if err != nil {
		return moerr.NewInternalErrorf(ctx, "force global checkpoint failed: %v", err)
	}
	return nil
}

func (r *runner) ForceIncrementalCheckpoint2(ts types.TS) (err error) {
	var intent Intent
	if intent, err = r.TryScheduleCheckpoint(ts, true); err != nil {
		return
	}
	if intent == nil {
		return
	}
	// TODO: use context
	timeout := time.After(time.Minute * 2)
	now := time.Now()
	defer func() {
		logger := logutil.Info
		if err != nil {
			logger = logutil.Error
		}
		logger(
			"ForceIncrementalCheckpoint-End",
			zap.String("entry", intent.String()),
			zap.Duration("cost", time.Since(now)),
			zap.Error(err),
		)
	}()

	select {
	case <-r.ctx.Done():
		err = context.Cause(r.ctx)
		return
	case <-timeout:
		err = moerr.NewInternalErrorNoCtx("timeout")
		return
	case <-intent.Wait():
	}
	return
}

func (r *runner) ForceCheckpointForBackup(end types.TS) (location string, err error) {
	prev := r.MaxIncrementalCheckpoint()
	if prev != nil && !prev.IsFinished() {
		return "", moerr.NewInternalError(r.ctx, "prev checkpoint not finished")
	}
	// ut causes all Ickp to be gc too fast, leaving a Gckp
	if prev == nil {
		prev = r.MaxGlobalCheckpoint()
	}
	start := types.TS{}
	if prev != nil {
		start = prev.end.Next()
	}
	entry := NewCheckpointEntry(r.rt.SID(), start, end, ET_Incremental)
	// TODO: change me
	r.store.AddNewIncrementalEntry(entry)
	now := time.Now()
	var files []string
	if _, files, err = r.doIncrementalCheckpoint(entry); err != nil {
		return
	}
	var lsn, lsnToTruncate uint64
	lsn = r.source.GetMaxLSN(entry.start, entry.end)
	if lsn > r.options.reservedWALEntryCount {
		lsnToTruncate = lsn - r.options.reservedWALEntryCount
	}
	entry.ckpLSN = lsn
	entry.truncateLSN = lsnToTruncate
	var file string
	if file, err = r.saveCheckpoint(entry.start, entry.end, lsn, lsnToTruncate); err != nil {
		return
	}
	files = append(files, file)
	backupTime := time.Now().UTC()
	currTs := types.BuildTS(backupTime.UnixNano(), 0)
	backup := NewCheckpointEntry(r.rt.SID(), end.Next(), currTs, ET_Incremental)
	location, err = r.doCheckpointForBackup(backup)
	if err != nil {
		return
	}
	entry.SetState(ST_Finished)
	e, err := r.wal.RangeCheckpoint(1, lsnToTruncate, files...)
	if err != nil {
		panic(err)
	}
	if err = e.WaitDone(); err != nil {
		panic(err)
	}
	logutil.Infof("checkpoint for backup %s, takes %s", entry.String(), time.Since(now))
	return location, nil
}
