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
	"github.com/matrixorigin/matrixone/pkg/logutil"
	v2 "github.com/matrixorigin/matrixone/pkg/util/metric/v2"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/wal"
	"go.uber.org/zap"
)

type checkpointJob struct {
	doneCh chan struct{}
	runner *runner

	runICKPFunc func(context.Context, *runner) error
}

func (job *checkpointJob) RunICKP(ctx context.Context) (err error) {
	if job.runICKPFunc != nil {
		return job.runICKPFunc(ctx, job.runner)
	}
	select {
	case <-ctx.Done():
		return context.Cause(ctx)
	default:
	}

	entry, rollback := job.runner.store.TakeICKPIntent()
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
			fields = append(fields, zap.Uint64("reserve", job.runner.options.reservedWALEntryCount))
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
	if fields, files, err = job.runner.doIncrementalCheckpoint(entry); err != nil {
		errPhase = "do-ckp"
		rollback()
		return
	}

	lsn = job.runner.source.GetMaxLSN(entry.start, entry.end)
	if lsn > job.runner.options.reservedWALEntryCount {
		lsnToTruncate = lsn - job.runner.options.reservedWALEntryCount
	}

	entry.SetLSN(lsn, lsnToTruncate)
	if !job.runner.store.CommitICKPIntent(entry, false) {
		errPhase = "commit"
		rollback()
		err = moerr.NewInternalErrorNoCtxf("cannot commit ickp")
		return
	}
	v2.TaskCkpEntryPendingDurationHistogram.Observe(entry.Age().Seconds())
	defer entry.Done()

	if file, err = job.runner.saveCheckpoint(
		entry.start, entry.end, lsn, lsnToTruncate,
	); err != nil {
		errPhase = "save-ckp"
		rollback()
		return
	}

	files = append(files, file)

	// PXU TODO: if crash here, the checkpoint log entry will be lost
	var logEntry wal.LogEntry
	if logEntry, err = job.runner.wal.RangeCheckpoint(1, lsnToTruncate, files...); err != nil {
		errPhase = "wal-ckp"
		fatal = true
		return
	}
	if err = logEntry.WaitDone(); err != nil {
		errPhase = "wait-wal-ckp-done"
		fatal = true
		return
	}

	job.runner.postCheckpointQueue.Enqueue(entry)
	job.runner.globalCheckpointQueue.Enqueue(&globalCheckpointContext{
		end:         entry.end,
		interval:    job.runner.options.globalVersionInterval,
		ckpLSN:      lsn,
		truncateLSN: lsnToTruncate,
	})

	return nil
}

func (job *checkpointJob) WaitC() <-chan struct{} {
	return job.doneCh
}

func (job *checkpointJob) Done() {
	close(job.doneCh)
}

type checkpointExecutor struct {
	ctx     context.Context
	cancel  context.CancelCauseFunc
	active  atomic.Bool
	running atomic.Pointer[checkpointJob]

	runner      *runner
	runICKPFunc func(context.Context, *runner) error
}

func newCheckpointExecutor(
	runner *runner,
) *checkpointExecutor {
	ctx, cancel := context.WithCancelCause(context.Background())
	e := &checkpointExecutor{
		runner: runner,
		ctx:    ctx,
		cancel: cancel,
	}
	e.active.Store(true)
	return e
}

func (e *checkpointExecutor) Stop() {
	e.active.Store(false)
	e.cancel(ErrCheckpointDisabled)
	job := e.running.Load()
	if job != nil {
		<-job.WaitC()
	}
	e.running.Store(nil)
	e.runner = nil
}

func (e *checkpointExecutor) RunICKPJob() (err error) {
	if !e.active.Load() {
		err = ErrCheckpointDisabled
		return
	}
	if e.running.Load() != nil {
		err = ErrPendingCheckpoint
	}
	job := &checkpointJob{
		doneCh:      make(chan struct{}),
		runner:      e.runner,
		runICKPFunc: e.runICKPFunc,
	}
	if !e.running.CompareAndSwap(nil, job) {
		err = ErrPendingCheckpoint
		return
	}
	defer func() {
		job.Done()
		e.running.Store(nil)
	}()
	err = job.RunICKP(e.ctx)
	return
}
