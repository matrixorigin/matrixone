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
	"go.uber.org/zap"
)

type TestRunner interface {
	EnableCheckpoint(*CheckpointCfg)
	DisableCheckpoint(ctx context.Context) (*CheckpointCfg, error)

	// TODO: remove the below apis
	CleanPenddingCheckpoint()
	ForceCheckpointForBackup(*CheckpointCfg, types.TS) (string, error)
	ForceGCKP(context.Context, types.TS, time.Duration) error
	ForceICKP(context.Context, *types.TS) error
	MaxLSNInRange(end types.TS) uint64
	GetICKPIntentOnlyForTest() *CheckpointEntry

	WaitRunningCKPDoneForTest(ctx context.Context, gckp bool) error

	GCNeeded() bool
}

// only for UT
func (r *runner) WaitRunningCKPDoneForTest(
	ctx context.Context,
	gckp bool,
) (err error) {

	for {
		job, err := r.getRunningCKPJob(gckp)
		if err != nil || job == nil {
			return err
		}
		select {
		case <-ctx.Done():
			return context.Cause(ctx)
		case <-job.WaitC():
		}
		time.Sleep(time.Millisecond * 10)
	}
}

func (r *runner) GetICKPIntentOnlyForTest() *CheckpointEntry {
	return r.store.GetICKPIntent()
}

// DisableCheckpoint stops generating checkpoint
func (r *runner) DisableCheckpoint(ctx context.Context) (cfg *CheckpointCfg, err error) {
	cfg = r.StopExecutor(ErrCheckpointDisabled)
	return
}

func (r *runner) EnableCheckpoint(cfg *CheckpointCfg) {
	r.StartExecutor(cfg)
}

func (r *runner) CleanPenddingCheckpoint() {
	r.store.CleanPenddingCheckpoint()
}

func (r *runner) ForceGCKP(
	ctx context.Context, end types.TS, interval time.Duration,
) (err error) {
	var (
		maxEntry *CheckpointEntry
		now      = time.Now()
	)
	defer func() {
		logger := logutil.Info
		if err != nil {
			logger = logutil.Error
		}
		var entryStr string
		if maxEntry != nil {
			entryStr = maxEntry.String()
		}
		logger(
			"Force-GCKP-End",
			zap.Duration("cost", time.Since(now)),
			zap.String("ts", end.ToString()),
			zap.String("entry", entryStr),
			zap.Error(err),
		)
	}()

	if err = r.ForceICKP(ctx, &end); err != nil {
		return
	}

	maxEntry = r.store.MaxIncrementalCheckpoint()

	// should not happend
	if maxEntry == nil || maxEntry.end.LT(&end) {
		err = ErrPendingCheckpoint
		return
	}

	request := &gckpContext{
		force:    true,
		end:      maxEntry.end,
		interval: interval,
	}

	if err = r.TryTriggerExecuteGCKP(request); err != nil {
		return
	}

	var job *checkpointJob

	var retryTimes int

	wait := func() {
		interval := time.Millisecond * 10 * time.Duration(retryTimes+1)
		time.Sleep(interval)
		if retryTimes < 10 {
			retryTimes++
		}
	}

	ctx, cancel := context.WithTimeout(ctx, time.Minute*2)
	defer cancel()

	for {
		select {
		case <-ctx.Done():
			err = context.Cause(ctx)
			return
		case <-r.ctx.Done():
			err = context.Cause(r.ctx)
			return
		default:
		}

		global := r.store.MaxGlobalCheckpoint()
		// if the max global contains the end, quick return
		if global != nil && global.IsFinished() && global.end.GE(&end) {
			return
		}

		if job, err = r.getRunningCKPJob(true); err != nil {
			return
		}

		// if there is no running job or the running job is not the right one
		// try to trigger the global checkpoint and wait for the next round
		if job == nil || job.gckpCtx.end.LT(&end) {
			wait()
			continue
		}

		// [job != nil && job.gckpCtx.end >= end]
		// wait for the job to finish
		select {
		case <-ctx.Done():
			err = context.Cause(ctx)
			return
		case <-r.ctx.Done():
			err = context.Cause(r.ctx)
			return
		case <-job.WaitC():
			err = job.Err()
			return
		}
	}
}

func (r *runner) ForceICKP(ctx context.Context, ts *types.TS) (err error) {
	var (
		intent Intent
		now    = time.Now()
	)
	defer func() {
		logger := logutil.Info
		if err != nil {
			logger = logutil.Error
		}
		var intentStr string
		if intent != nil {
			intentStr = intent.String()
		}
		logger(
			"ICKP-Schedule-Force-End",
			zap.String("ts", ts.ToString()),
			zap.Duration("cost", time.Since(now)),
			zap.String("intent", intentStr),
			zap.Error(err),
		)
	}()

	ctx, cancel := context.WithTimeout(ctx, time.Minute*2)
	defer cancel()

	for {
		if intent, err = r.TryScheduleCheckpoint(*ts, true); err != nil {
			// for retryable error, we should retry
			if err == ErrPendingCheckpoint {
				err = nil
				time.Sleep(time.Millisecond * 100)
				continue
			}
			return
		}
		if intent == nil {
			return
		}
		select {
		case <-ctx.Done():
			err = context.Cause(ctx)
			return
		case <-r.ctx.Done():
			err = context.Cause(r.ctx)
			return
		case <-intent.Wait():
			checkpointed := r.store.MaxIncrementalCheckpoint()
			if checkpointed == nil || checkpointed.end.LT(ts) {
				continue
			}
			intent = checkpointed
			return
		}
	}
}

func (r *runner) ForceCheckpointForBackup(cfg *CheckpointCfg, end types.TS) (location string, err error) {
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
	if cfg == nil {
		cfg = new(CheckpointCfg)
		cfg.FillDefaults()
	}
	if _, files, err = r.doIncrementalCheckpoint(cfg, entry); err != nil {
		return
	}
	var lsn, lsnToTruncate uint64
	lsn = r.source.GetMaxLSN(entry.start, entry.end)
	if lsn > cfg.IncrementalReservedWALCount {
		lsnToTruncate = lsn - cfg.IncrementalReservedWALCount
	}
	entry.ckpLSN = lsn
	entry.truncateLSN = lsnToTruncate
	var file string
	if file, err = r.saveCheckpoint(entry.start, entry.end); err != nil {
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
