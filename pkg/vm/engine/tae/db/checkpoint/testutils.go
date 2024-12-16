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
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logtail"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/wal"
	"go.uber.org/zap"
)

type TestRunner interface {
	EnableCheckpoint()
	DisableCheckpoint()

	CleanPenddingCheckpoint()
	ForceGlobalCheckpoint(end types.TS, versionInterval time.Duration) error
	ForceGlobalCheckpointSynchronously(ctx context.Context, end types.TS, versionInterval time.Duration) error
	ForceCheckpointForBackup(end types.TS) (string, error)
	ForceIncrementalCheckpoint(end types.TS, truncate bool) error
	MaxLSNInRange(end types.TS) uint64

	ExistPendingEntryToGC() bool
	MaxGlobalCheckpoint() *CheckpointEntry
	MaxIncrementalCheckpoint() *CheckpointEntry
	GetDirtyCollector() logtail.Collector
}

// DisableCheckpoint stops generating checkpoint
func (r *runner) DisableCheckpoint() {
	r.disabled.Store(true)
}

func (r *runner) EnableCheckpoint() {
	r.disabled.Store(false)
}

func (r *runner) CleanPenddingCheckpoint() {
	prev := r.MaxIncrementalCheckpoint()
	if prev == nil {
		return
	}
	if !prev.IsFinished() {
		r.storage.Lock()
		r.storage.incrementals.Delete(prev)
		r.storage.Unlock()
	}
	if prev.IsRunning() {
		logutil.Warnf("Delete a running checkpoint entry")
	}
	prev = r.MaxGlobalCheckpoint()
	if prev == nil {
		return
	}
	if !prev.IsFinished() {
		r.storage.Lock()
		r.storage.incrementals.Delete(prev)
		r.storage.Unlock()
	}
	if prev.IsRunning() {
		logutil.Warnf("Delete a running checkpoint entry")
	}
}

func (r *runner) ForceGlobalCheckpoint(end types.TS, interval time.Duration) error {
	if interval == 0 {
		interval = r.options.globalVersionInterval
	}
	if r.GetPenddingIncrementalCount() != 0 {
		end = r.MaxIncrementalCheckpoint().GetEnd()
		r.globalCheckpointQueue.Enqueue(&globalCheckpointContext{
			force:    true,
			end:      end,
			interval: interval,
		})
		return nil
	}
	retryTime := 0
	timeout := time.After(interval)
	var err error
	defer func() {
		if err != nil || retryTime > 0 {
			logutil.Error("ForceGlobalCheckpoint-End",
				zap.Error(err),
				zap.Uint64("retryTime", uint64(retryTime)))
			return
		}
	}()
	for {
		select {
		case <-timeout:
			return moerr.NewInternalError(r.ctx, "timeout")
		default:
			err = r.ForceIncrementalCheckpoint(end, false)
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
	prevGlobalEnd := types.TS{}
	r.storage.RLock()
	global, _ := r.storage.globals.Max()
	r.storage.RUnlock()
	if global != nil {
		prevGlobalEnd = global.end
	}

	r.ForceGlobalCheckpoint(end, versionInterval)

	op := func() (ok bool, err error) {
		r.storage.RLock()
		global, _ := r.storage.globals.Max()
		r.storage.RUnlock()
		if global == nil {
			return false, nil
		}
		return global.end.GT(&prevGlobalEnd), nil
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

func (r *runner) ForceIncrementalCheckpoint(end types.TS, truncate bool) error {
	now := time.Now()
	prev := r.MaxIncrementalCheckpoint()
	if prev != nil && !prev.IsFinished() {
		return moerr.NewPrevCheckpointNotFinished()
	}

	if prev != nil && end.LE(&prev.end) {
		return nil
	}
	var (
		err      error
		errPhase string
		start    types.TS
		fatal    bool
		fields   []zap.Field
	)

	if prev == nil {
		global := r.MaxGlobalCheckpoint()
		if global != nil {
			start = global.end
		}
	} else {
		start = prev.end.Next()
	}

	entry := NewCheckpointEntry(r.rt.SID(), start, end, ET_Incremental)
	logutil.Info(
		"Checkpoint-Start-Force",
		zap.String("entry", entry.String()),
	)

	defer func() {
		if err != nil {
			logger := logutil.Error
			if fatal {
				logger = logutil.Fatal
			}
			logger(
				"Checkpoint-Error-Force",
				zap.String("entry", entry.String()),
				zap.String("phase", errPhase),
				zap.Error(err),
				zap.Duration("cost", time.Since(now)),
			)
		} else {
			fields = append(fields, zap.Duration("cost", time.Since(now)))
			fields = append(fields, zap.String("entry", entry.String()))
			logutil.Info(
				"Checkpoint-End-Force",
				fields...,
			)
		}
	}()

	r.storage.Lock()
	r.storage.incrementals.Set(entry)
	r.storage.Unlock()

	var files []string
	if fields, files, err = r.doIncrementalCheckpoint(entry); err != nil {
		errPhase = "do-ckp"
		return err
	}

	var lsn, lsnToTruncate uint64
	if truncate {
		lsn = r.source.GetMaxLSN(entry.start, entry.end)
		if lsn > r.options.reservedWALEntryCount {
			lsnToTruncate = lsn - r.options.reservedWALEntryCount
		}
		entry.ckpLSN = lsn
		entry.truncateLSN = lsnToTruncate
	}

	var file string
	if file, err = r.saveCheckpoint(
		entry.start, entry.end, lsn, lsnToTruncate,
	); err != nil {
		errPhase = "save-ckp"
		return err
	}
	files = append(files, file)
	entry.SetState(ST_Finished)
	if truncate {
		var e wal.LogEntry
		if e, err = r.wal.RangeCheckpoint(1, lsnToTruncate, files...); err != nil {
			errPhase = "wal-ckp"
			fatal = true
			return err
		}
		if err = e.WaitDone(); err != nil {
			errPhase = "wait-wal-ckp"
			fatal = true
			return err
		}
	}
	return nil
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
	r.storage.Lock()
	r.storage.incrementals.Set(entry)
	now := time.Now()
	r.storage.Unlock()
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
