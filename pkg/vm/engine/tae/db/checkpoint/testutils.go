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
	"errors"
	"fmt"
	"time"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	v2 "github.com/matrixorigin/matrixone/pkg/util/metric/v2"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logtail"
	"go.uber.org/zap"
)

const (
	defaultForceICKPTimeout    = 2 * time.Minute
	forceICKPRetryInitialDelay = 100 * time.Millisecond
	forceICKPRetryMaximumDelay = time.Second
)

func nextForceICKPRetryDelay(current time.Duration) time.Duration {
	if current <= 0 {
		return forceICKPRetryInitialDelay
	}
	if current >= forceICKPRetryMaximumDelay/2 {
		return forceICKPRetryMaximumDelay
	}
	return current * 2
}

func waitForCheckpointRetry(
	ctx, runnerCtx context.Context,
	delay time.Duration,
) error {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return context.Cause(ctx)
	case <-runnerCtx.Done():
		return context.Cause(runnerCtx)
	case <-timer.C:
		return nil
	}
}

func contextForForceICKP(
	ctx context.Context,
) (context.Context, context.CancelFunc) {
	// A caller-provided deadline is part of the operation's contract. In
	// particular, backup uses a longer request deadline because checkpoint
	// duration scales with dirty data. Keep the historical timeout only as a
	// safety net for callers that provide no deadline at all.
	if _, ok := ctx.Deadline(); ok {
		return context.WithCancel(ctx)
	}
	return context.WithTimeout(ctx, defaultForceICKPTimeout)
}

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
	r.forceGCKPRequests.Add(1)
	defer r.releaseForceGCKPReservation()

	var (
		maxEntry *CheckpointEntry
		now      = time.Now()
	)
	defer func() {
		logger := logutil.Info
		if err != nil {
			if errors.Is(err, ErrGCKPNeedsFreshICKP) {
				logger = logutil.Debug
			} else {
				logger = logutil.Error
			}
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

	maxEntry = r.store.MaxCheckpoint()

	// ForceICKP may return after an automatic GCKP has already covered end and
	// checkpoint GC has removed its incremental predecessor from the store. The
	// finished GCKP can still be used as the predecessor of a new GCKP, but it
	// cannot satisfy this request by itself: checkpoint metadata does not prove
	// which history retention was used to create it.
	if maxEntry == nil || maxEntry.end.LT(&end) {
		err = ErrPendingCheckpoint
		return
	}
	if !maxEntry.IsIncremental() {
		err = ErrGCKPNeedsFreshICKP
		return
	}

	request := &gckpContext{
		force:            true,
		end:              maxEntry.end,
		histroyRetention: histroyRetention,
		truncateLSN:      maxEntry.truncateLSN,
		ckpLSN:           maxEntry.ckpLSN,
		predecessor:      maxEntry,
		done:             make(chan error, 1),
	}

	if err = r.TryTriggerExecuteGCKP(request); err != nil {
		return
	}

	ctx, cancel := context.WithTimeout(ctx, time.Minute*5)
	defer cancel()

	select {
	case <-ctx.Done():
		err = context.Cause(ctx)
	case <-r.ctx.Done():
		err = context.Cause(r.ctx)
	case err = <-request.done:
	}
	return
}

func (r *runner) releaseForceGCKPReservation() {
	if r.forceGCKPRequests.Add(-1) != 0 || r.forceGCKPRequests.Load() != 0 {
		return
	}

	// An ICKP can commit after the force GCKP has pinned its predecessor. Its
	// automatic trigger was deliberately suppressed by the reservation, so the
	// last force waiter must hand that trigger back instead of losing it until a
	// future ICKP happens to commit.
	incremental := r.store.MaxIncrementalCheckpoint()
	if incremental == nil {
		return
	}
	incrementalEnd := incremental.GetEnd()
	if global := r.store.MaxGlobalCheckpoint(); global != nil {
		globalEnd := global.GetEnd()
		if globalEnd.GT(&incrementalEnd) {
			return
		}
	}
	executor := r.executor.Load()
	if executor == nil || r.forceGCKPRequests.Load() != 0 {
		return
	}
	_ = r.TryTriggerExecuteGCKP(&gckpContext{
		end:              incrementalEnd,
		histroyRetention: executor.cfg.GlobalHistoryDuration,
		ckpLSN:           incremental.LSN(),
		truncateLSN:      incremental.GetTruncateLsn(),
		predecessor:      incremental,
	})
}

func (r *runner) ForceICKP(ctx context.Context, ts *types.TS) (err error) {
	var (
		intent            Intent
		now               = time.Now()
		retryCount        int
		pendingRetries    int
		noProgressRetries int
		retryDelay        time.Duration
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
			zap.Int("retry-count", retryCount),
			zap.Int("pending-retries", pendingRetries),
			zap.Int("no-progress-retries", noProgressRetries),
			zap.Error(err),
		)
	}()

	ctx, cancel := contextForForceICKP(ctx)
	defer cancel()

	for {
		if intent, err = r.TryScheduleCheckpoint(*ts, true); err != nil {
			// for retryable error, we should retry
			if err == ErrPendingCheckpoint {
				err = nil
				retryDelay = nextForceICKPRetryDelay(retryDelay)
				retryCount++
				pendingRetries++
				v2.TaskForceICKPPendingRetryCounter.Inc()
				if err = waitForCheckpointRetry(ctx, r.ctx, retryDelay); err != nil {
					return
				}
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
				retryDelay = nextForceICKPRetryDelay(retryDelay)
				retryCount++
				noProgressRetries++
				v2.TaskForceICKPNoProgressRetryCounter.Inc()
				if err = waitForCheckpointRetry(ctx, r.ctx, retryDelay); err != nil {
					return
				}
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
		loc objectio.Location
	)
	if loc, _, err = data.Sync(
		ctx, r.rt.Fs,
	); err != nil {
		return
	}

	location = fmt.Sprintf(
		"%s:%d:%s:%s:%s",
		loc.String(),
		logtail.CheckpointCurrentVersion,
		end.ToString(),
		loc.String(),
		start.ToString(),
	)
	return
}
