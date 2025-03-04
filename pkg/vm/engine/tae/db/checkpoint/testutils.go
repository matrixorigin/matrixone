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
	"fmt"
	"time"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logtail"
	"go.uber.org/zap"
)

type TestRunner interface {
	EnableCheckpoint(*CheckpointCfg)
	DisableCheckpoint(ctx context.Context) (*CheckpointCfg, error)

	// special file for backup
	CreateSpecialCheckpointFile(ctx context.Context, start, end types.TS) (string, error)

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

func (r *runner) ForceGCKP(
	ctx context.Context, end types.TS, histroyRetention time.Duration,
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
		force:            true,
		end:              maxEntry.end,
		histroyRetention: histroyRetention,
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

	ctx, cancel := context.WithTimeout(ctx, time.Minute*5)
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

func (r *runner) CreateSpecialCheckpointFile(
	ctx context.Context,
	start types.TS,
	end types.TS,
) (location string, err error) {
	now := time.Now()
	defer func() {
		logger := logutil.Info
		if err != nil {
			logger = logutil.Error
		}
		if err != nil || time.Since(now) > 5*time.Second {
			logger(
				"CKP-Create-Special-File",
				zap.String("location", location),
				zap.Error(err),
				zap.Duration("duration", time.Since(now)),
				zap.String("start", start.ToString()),
				zap.String("end", end.ToString()),
			)
		}
	}()

	select {
	case <-ctx.Done():
		err = context.Cause(ctx)
		return
	default:
	}

	factory := logtail.BackupCheckpointDataFactory(start, end, r.rt.Fs)
	var data *logtail.CheckpointData_V2
	if data, err = factory(r.catalog); err != nil {
		return
	}
	defer data.Close()

	cfg := r.GetCfg()
	if cfg == nil {
		cfg = new(CheckpointCfg)
		cfg.FillDefaults()
	}
	var (
		cnLocation, tnLocation objectio.Location
	)
	if cnLocation, tnLocation, _, err = data.WriteTo(
		ctx, r.rt.Fs,
	); err != nil {
		return
	}

	location = fmt.Sprintf(
		"%s:%d:%s:%s:%s",
		cnLocation.String(),
		logtail.CheckpointCurrentVersion,
		end.ToString(),
		tnLocation.String(),
		start.ToString(),
	)
	return
}
