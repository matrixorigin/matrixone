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
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db/dbutils"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/util/fault"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
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
	IsAllChangesFlushed(start, end types.TS, printTree bool) bool
	MaxLSNInRange(end types.TS) uint64

	ExistPendingEntryToGC() bool
	MaxGlobalCheckpoint() *CheckpointEntry
	MaxCheckpoint() *CheckpointEntry
	ForceFlush(ts types.TS, ctx context.Context, duration time.Duration) (err error)
	ForceFlushWithInterval(ts types.TS, ctx context.Context, forceDuration, flushInterval time.Duration) (err error)
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
	prev := r.MaxCheckpoint()
	if prev == nil {
		return
	}
	if !prev.IsFinished() {
		r.storage.Lock()
		r.storage.entries.Delete(prev)
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
		r.storage.entries.Delete(prev)
		r.storage.Unlock()
	}
	if prev.IsRunning() {
		logutil.Warnf("Delete a running checkpoint entry")
	}
}

func (r *runner) ForceGlobalCheckpoint(end types.TS, versionInterval time.Duration) error {
	if versionInterval == 0 {
		versionInterval = r.options.globalVersionInterval
	}
	if r.GetPenddingIncrementalCount() != 0 {
		end = r.MaxCheckpoint().GetEnd()
		r.globalCheckpointQueue.Enqueue(&globalCheckpointContext{
			force:    true,
			end:      end,
			interval: versionInterval,
		})
		return nil
	}
	timeout := time.After(versionInterval)
	for {
		select {
		case <-timeout:
			return moerr.NewInternalError(r.ctx, "timeout")
		default:
			err := r.ForceIncrementalCheckpoint(end, false)
			if err != nil {
				if dbutils.IsRetrieableCheckpoint(err) {
					interval := versionInterval.Milliseconds() / 400
					time.Sleep(time.Duration(interval))
					break
				}
				return err
			}
			r.globalCheckpointQueue.Enqueue(&globalCheckpointContext{
				force:    true,
				end:      end,
				interval: versionInterval,
			})
			return nil
		}
	}
}

func (r *runner) ForceGlobalCheckpointSynchronously(ctx context.Context, end types.TS, versionInterval time.Duration) error {
	prevGlobalEnd := types.TS{}
	global, _ := r.storage.globals.Max()
	if global != nil {
		prevGlobalEnd = global.end
	}

	r.ForceGlobalCheckpoint(end, versionInterval)

	op := func() (ok bool, err error) {
		global, _ := r.storage.globals.Max()
		if global == nil {
			return false, nil
		}
		return global.end.Greater(&prevGlobalEnd), nil
	}
	err := common.RetryWithIntervalAndTimeout(
		op,
		time.Minute,
		r.options.forceFlushCheckInterval, false)
	if err != nil {
		return moerr.NewInternalError(ctx, "force global checkpoint failed: %v", err)
	}
	return nil
}

func (r *runner) ForceFlushWithInterval(ts types.TS, ctx context.Context, forceDuration, flushInterval time.Duration) (err error) {
	makeCtx := func() *DirtyCtx {
		tree := r.source.ScanInRangePruned(types.TS{}, ts)
		tree.GetTree().Compact()
		if tree.IsEmpty() {
			return nil
		}
		entry := logtail.NewDirtyTreeEntry(types.TS{}, ts, tree.GetTree())
		dirtyCtx := new(DirtyCtx)
		dirtyCtx.tree = entry
		dirtyCtx.force = true
		// logutil.Infof("try flush %v",tree.String())
		return dirtyCtx
	}
	op := func() (ok bool, err error) {
		dirtyCtx := makeCtx()
		if dirtyCtx == nil {
			return true, nil
		}
		if _, err = r.dirtyEntryQueue.Enqueue(dirtyCtx); err != nil {
			return true, nil
		}
		return false, nil
	}

	if forceDuration == 0 {
		forceDuration = r.options.forceFlushTimeout
	}
	err = common.RetryWithIntervalAndTimeout(
		op,
		forceDuration,
		flushInterval, false)
	if err != nil {
		return moerr.NewInternalError(ctx, "force flush failed: %v", err)
	}
	_, sarg, _ := fault.TriggerFault("tae: flush timeout")
	if sarg != "" {
		err = moerr.NewInternalError(ctx, sarg)
	}
	return

}
func (r *runner) ForceFlush(ts types.TS, ctx context.Context, forceDuration time.Duration) (err error) {
	return r.ForceFlushWithInterval(ts, ctx, forceDuration, r.options.forceFlushCheckInterval)
}

func (r *runner) ForceIncrementalCheckpoint(end types.TS, truncate bool) error {
	now := time.Now()
	prev := r.MaxCheckpoint()
	if prev != nil && !prev.IsFinished() {
		return moerr.NewPrevCheckpointNotFinished()
	}

	if prev != nil && end.LessEq(&prev.end) {
		return nil
	}
	var (
		err      error
		errPhase string
		start    types.TS
		fatal    bool
		fields   []zap.Field
	)

	if prev != nil {
		start = prev.end.Next()
	}

	entry := NewCheckpointEntry(start, end, ET_Incremental)
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
	r.storage.entries.Set(entry)
	r.storage.Unlock()

	if fields, err = r.doIncrementalCheckpoint(entry); err != nil {
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

	if err = r.saveCheckpoint(
		entry.start, entry.end, lsn, lsnToTruncate,
	); err != nil {
		errPhase = "save-ckp"
		return err
	}
	entry.SetState(ST_Finished)

	if truncate {
		var e wal.LogEntry
		if e, err = r.wal.RangeCheckpoint(1, lsnToTruncate); err != nil {
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
	prev := r.MaxCheckpoint()
	if prev != nil && !prev.IsFinished() {
		return "", moerr.NewInternalError(r.ctx, "prev checkpoint not finished")
	}
	start := types.TS{}
	if prev != nil {
		start = prev.end.Next()
	}
	entry := NewCheckpointEntry(start, end, ET_Incremental)
	r.storage.Lock()
	r.storage.entries.Set(entry)
	now := time.Now()
	r.storage.Unlock()
	if _, err = r.doIncrementalCheckpoint(entry); err != nil {
		return
	}
	var lsn, lsnToTruncate uint64
	lsn = r.source.GetMaxLSN(entry.start, entry.end)
	if lsn > r.options.reservedWALEntryCount {
		lsnToTruncate = lsn - r.options.reservedWALEntryCount
	}
	entry.ckpLSN = lsn
	entry.truncateLSN = lsnToTruncate
	if err = r.saveCheckpoint(entry.start, entry.end, lsn, lsnToTruncate); err != nil {
		return
	}
	backupTime := time.Now().UTC()
	currTs := types.BuildTS(backupTime.UnixNano(), 0)
	backup := NewCheckpointEntry(end.Next(), currTs, ET_Incremental)
	location, err = r.doCheckpointForBackup(backup)
	if err != nil {
		return
	}
	entry.SetState(ST_Finished)
	e, err := r.wal.RangeCheckpoint(1, lsnToTruncate)
	if err != nil {
		panic(err)
	}
	if err = e.WaitDone(); err != nil {
		panic(err)
	}
	logutil.Infof("checkpoint for backup %s, takes %s", entry.String(), time.Since(now))
	return location, nil
}

func (r *runner) IsAllChangesFlushed(start, end types.TS, printTree bool) bool {
	tree := r.source.ScanInRangePruned(start, end)
	tree.GetTree().Compact()
	if printTree && !tree.IsEmpty() {
		logutil.Infof("%v", tree.String())
	}
	return tree.IsEmpty()
}
