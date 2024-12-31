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
	"sync/atomic"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/perfcounter"
	v2 "github.com/matrixorigin/matrixone/pkg/util/metric/v2"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logstore/sm"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logstore/store"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logtail"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/wal"
	"go.uber.org/zap"
)

type checkpointJob struct {
	doneCh   chan struct{}
	executor *checkpointExecutor

	runICKPFunc func(context.Context, *runner) error

	gckpCtx     *gckpContext
	runGCKPFunc func(context.Context, *gckpContext, *runner) error

	err error
}

func (job *checkpointJob) RunGCKP(ctx context.Context) (err error) {
	if job.runGCKPFunc != nil {
		return job.runGCKPFunc(ctx, job.gckpCtx, job.executor.runner)
	}

	// two for chaos test
	// one to ensure it has entered `RunGCKP`
	objectio.WaitInjected(objectio.FJ_GCKPWait1)
	// two to block the execution till being notified
	objectio.WaitInjected(objectio.FJ_GCKPWait1)

	_, err = job.doGlobalCheckpoint(
		job.gckpCtx.end,
		job.gckpCtx.ckpLSN,
		job.gckpCtx.truncateLSN,
		job.gckpCtx.histroyRetention,
	)

	return
}

func (job *checkpointJob) doGlobalCheckpoint(
	end types.TS, ckpLSN, truncateLSN uint64, interval time.Duration,
) (entry *CheckpointEntry, err error) {
	var (
		errPhase string
		fields   []zap.Field
		now      = time.Now()
		runner   = job.executor.runner
	)

	entry = NewCheckpointEntry(
		runner.rt.SID(),
		types.TS{},
		end.Next(),
		ET_Global,
	)
	entry.ckpLSN = ckpLSN
	entry.truncateLSN = truncateLSN

	logutil.Info(
		"GCKP-Execute-Start",
		zap.String("entry", entry.String()),
	)

	defer func() {
		if err != nil {
			logutil.Error(
				"GCKP-Execute-Error",
				zap.String("entry", entry.String()),
				zap.String("phase", errPhase),
				zap.Error(err),
				zap.Duration("cost", time.Since(now)),
			)
		} else {
			fields = append(fields, zap.Duration("cost", time.Since(now)))
			fields = append(fields, zap.String("entry", entry.String()))
			logutil.Info(
				"GCKP-Execute-End",
				fields...,
			)
		}
	}()

	if ok := runner.store.AddGCKPIntent(entry); !ok {
		err = ErrBadIntent
		return
	}

	var data *logtail.CheckpointData
	factory := logtail.GlobalCheckpointDataFactory(runner.rt.SID(), entry.end, interval)

	if data, err = factory(runner.catalog); err != nil {
		runner.store.RemoveGCKPIntent()
		errPhase = "collect"
		return
	}
	defer data.Close()

	fields = data.ExportStats("")

	cnLocation, tnLocation, files, err := data.WriteTo(
		job.executor.ctx, job.executor.cfg.BlockMaxRowsHint, job.executor.cfg.SizeHint, runner.rt.Fs.Service,
	)
	if err != nil {
		runner.store.RemoveGCKPIntent()
		errPhase = "flush"
		return
	}

	entry.SetLocation(cnLocation, tnLocation)

	files = append(files, cnLocation.Name().String())
	var name string
	if name, err = runner.saveCheckpoint(entry.start, entry.end); err != nil {
		runner.store.RemoveGCKPIntent()
		errPhase = "save"
		return
	}
	defer func() {
		entry.SetState(ST_Finished)
	}()

	files = append(files, name)

	fileEntry, err := store.BuildFilesEntry(files)
	if err != nil {
		return
	}

	runner.wal.AllocateLSN(store.GroupFiles, fileEntry)
	err = runner.wal.AppendEntry(fileEntry)
	if err != nil {
		return
	}
	perfcounter.Update(job.executor.ctx, func(counter *perfcounter.CounterSet) {
		counter.TAE.CheckPoint.DoGlobalCheckPoint.Add(1)
	})
	return
}

func (job *checkpointJob) RunICKP(ctx context.Context) (err error) {
	if job.runICKPFunc != nil {
		return job.runICKPFunc(ctx, job.executor.runner)
	}
	select {
	case <-ctx.Done():
		return context.Cause(ctx)
	default:
	}

	runner := job.executor.runner

	entry, rollback := runner.store.TakeICKPIntent()
	if entry == nil {
		return
	}

	var (
		errPhase      string
		lsnToTruncate uint64
		lsn           uint64
		fatal         bool
		fields        []zap.Field
		now           = time.Now()
	)

	logutil.Info(
		"ICKP-Execute-Start",
		zap.String("entry", entry.String()),
	)

	defer func() {
		if err != nil {
			var logger func(msg string, fields ...zap.Field)
			if fatal {
				logger = logutil.Fatal
			} else {
				logger = logutil.Error
			}
			logger(
				"ICKP-Execute-Error",
				zap.String("entry", entry.String()),
				zap.Error(err),
				zap.String("phase", errPhase),
				zap.Duration("cost", time.Since(now)),
			)
		} else {
			fields = append(fields, zap.Duration("cost", time.Since(now)))
			fields = append(fields, zap.Uint64("truncate", lsnToTruncate))
			fields = append(fields, zap.Uint64("lsn", lsn))
			fields = append(fields, zap.Uint64("reserve", job.executor.cfg.IncrementalReservedWALCount))
			fields = append(fields, zap.String("entry", entry.String()))
			fields = append(fields, zap.Duration("age", entry.Age()))
			logutil.Info(
				"ICKP-Execute-End",
				fields...,
			)
		}
	}()

	var files []string
	var file string
	if fields, files, err = job.executor.doIncrementalCheckpoint(entry); err != nil {
		errPhase = "do-ckp"
		rollback()
		return
	}

	lsn = runner.source.GetMaxLSN(entry.start, entry.end)
	if lsn > job.executor.cfg.IncrementalReservedWALCount {
		lsnToTruncate = lsn - job.executor.cfg.IncrementalReservedWALCount
	}
	entry.SetLSN(lsn, lsnToTruncate)

	if prepared := runner.store.PrepareCommitICKPIntent(entry); !prepared {
		errPhase = "prepare"
		rollback()
		err = moerr.NewInternalErrorNoCtxf("cannot prepare ickp")
		return
	}

	if file, err = runner.saveCheckpoint(
		entry.start, entry.end,
	); err != nil {
		errPhase = "save-ckp"
		runner.store.RollbackICKPIntent(entry)
		rollback()
		return
	}

	defer runner.store.CommitICKPIntent(entry)
	v2.TaskCkpEntryPendingDurationHistogram.Observe(entry.Age().Seconds())

	files = append(files, file)

	// PXU TODO: if crash here, the checkpoint log entry will be lost
	var logEntry wal.LogEntry
	if logEntry, err = runner.wal.RangeCheckpoint(1, lsnToTruncate, files...); err != nil {
		errPhase = "wal-ckp"
		fatal = true
		return
	}
	if err = logEntry.WaitDone(); err != nil {
		errPhase = "wait-wal-ckp-done"
		fatal = true
		return
	}

	runner.postCheckpointQueue.Enqueue(entry)
	runner.TryTriggerExecuteGCKP(&gckpContext{
		end:              entry.end,
		histroyRetention: job.executor.cfg.GlobalHistoryDuration,
		ckpLSN:           lsn,
		truncateLSN:      lsnToTruncate,
	})

	return nil
}

func (job *checkpointJob) WaitC() <-chan struct{} {
	return job.doneCh
}

// should be called after WaitC
func (job *checkpointJob) Err() error {
	return job.err
}

func (job *checkpointJob) Done(err error) {
	job.err = err
	close(job.doneCh)
}

type checkpointExecutor struct {
	cfg CheckpointCfg

	// checkpoint policy
	incrementalPolicy *timeBasedPolicy
	globalPolicy      *countBasedPolicy

	ctx         context.Context
	cancel      context.CancelCauseFunc
	active      atomic.Bool
	runningICKP atomic.Pointer[checkpointJob]
	runningGCKP atomic.Pointer[checkpointJob]

	runner      *runner
	runICKPFunc func(context.Context, *runner) error
	runGCKPFunc func(context.Context, *gckpContext, *runner) error

	ickpQueue sm.Queue
	gckpQueue sm.Queue
}

func newCheckpointExecutor(
	runner *runner,
	cfg *CheckpointCfg,
) *checkpointExecutor {
	ctx := context.Background()
	if runner != nil {
		ctx = runner.ctx
	}
	if cfg == nil {
		cfg = new(CheckpointCfg)
	}
	ctx, cancel := context.WithCancelCause(ctx)
	e := &checkpointExecutor{
		runner: runner,
		ctx:    ctx,
		cancel: cancel,
		cfg:    *cfg,
	}
	e.fillDefaults()

	e.incrementalPolicy = &timeBasedPolicy{interval: e.cfg.IncrementalInterval}
	e.globalPolicy = &countBasedPolicy{minCount: int(e.cfg.GlobalMinCount)}

	e.ickpQueue = sm.NewSafeQueue(1000, 100, e.onICKPEntries)
	e.gckpQueue = sm.NewSafeQueue(1000, 100, e.onGCKPEntries)
	e.ickpQueue.Start()
	e.gckpQueue.Start()

	e.active.Store(true)
	logutil.Info(
		"CKP-Executor-Started",
		zap.String("cfg", e.cfg.String()),
	)
	return e
}

func (executor *checkpointExecutor) GetCfg() *CheckpointCfg {
	return &executor.cfg
}

func (executor *checkpointExecutor) fillDefaults() {
	executor.cfg.FillDefaults()
}

func (executor *checkpointExecutor) RunningCKPJob(gckp bool) *checkpointJob {
	if gckp {
		return executor.runningGCKP.Load()
	}
	return executor.runningICKP.Load()
}

func (executor *checkpointExecutor) StopWithCause(cause error) {
	if updated := executor.active.CompareAndSwap(true, false); !updated {
		return
	}
	if cause == nil {
		cause = ErrCheckpointDisabled
	}
	executor.cancel(cause)
	job := executor.runningGCKP.Load()
	if job != nil {
		<-job.WaitC()
	}
	executor.runningGCKP.Store(nil)
	job = executor.runningICKP.Load()
	if job != nil {
		<-job.WaitC()
	}
	executor.runningICKP.Store(nil)
	executor.ickpQueue.Stop()
	executor.gckpQueue.Stop()
	logutil.Info(
		"CKP-Executor-Stopped",
		zap.Error(cause),
		zap.String("cfg", executor.cfg.String()),
	)
}
